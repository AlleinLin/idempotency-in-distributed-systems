"""Full 12-Raft-process chain with an actual RabbitMQ/Kafka cluster."""

import argparse
import asyncio
import json
import time
import traceback
from pathlib import Path

from broker_adapters import create_adapter
from common import canon
from run_lab import Lab, wait_for


def broker_failure(lab):
    before = lab.native.metadata("before-leader-kill")
    event = lab.operation(lab.baseline[0])["event"]
    if lab.broker_kind == "rabbit":
        row = json.loads(before["queues"])[0]
        leader = row["leader"]
        index = int(leader.split("@")[0].removeprefix("part04q"))
    else:
        published = [
            json.loads(line)
            for line in (lab.out / "relay.jsonl").read_text(encoding="utf-8").splitlines()
        ]
        position = next(
            h for h in published if h["event"] == "publish_confirmed" and h["envelope"] == event
        )
        partition = str(position["partition"])
        leader = before["topics"]["purchase.paid"][partition]["leader"]
        index = leader - 1
    name = f"{lab.broker_kind}-{index}"
    lab.native.stop(name)
    lab.trace("native_broker_leader_killed", name=name, leader=leader, event_id=event["event_id"])

    async def republish():
        adapter = create_adapter(lab.cfg, "relay")
        await adapter.setup()
        return await adapter.publish(event)

    ack = wait_for(
        lambda: asyncio.run(republish()), seconds=45, label="republish on surviving broker majority"
    )
    lab.trace("native_broker_majority_confirmed", event=event, metadata=ack)
    time.sleep(2)
    after = lab.native.metadata("after-leader-kill")
    if lab.broker_kind == "rabbit":
        assert json.loads(after["queues"])[0]["leader"] != leader
    else:
        assert after["topics"]["purchase.paid"][partition]["leader"] != leader
    lab.native.restart(name)
    time.sleep(3)
    lab.native.metadata("after-old-broker-restart")
    lab.save("10-broker-failover")


def rabbit_mandatory(lab):
    import pika

    connection = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1", 37400))
    channel = connection.channel()
    channel.confirm_delivery()
    outcome = None
    try:
        channel.basic_publish(
            exchange="purchases",
            routing_key="missing-route",
            body=b"unroutable-control",
            mandatory=True,
            properties=pika.BasicProperties(delivery_mode=2),
        )
    except pika.exceptions.UnroutableError as exc:
        outcome = {
            "type": "basic.return",
            "reply_code": exc.messages[0].method.reply_code,
            "routing_key": exc.messages[0].method.routing_key,
            "body": exc.messages[0].body.decode(),
            "messages": len(exc.messages),
        }
    connection.close()
    assert outcome and outcome["reply_code"] == 312
    (lab.out / "mandatory-return.json").write_text(canon(outcome), encoding="utf-8")
    lab.trace("rabbit_unroutable_return", result=outcome)


def kafka_transactions(lab):
    from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition

    endpoints = lab.cfg["broker"]["bootstrap"]
    seed = Producer({"bootstrap.servers": endpoints, "enable.idempotence": True, "acks": "all"})
    seed.produce("transactions-input", key=b"input-1", value=b"original-input")
    assert seed.flush(15) == 0
    consumer = Consumer(
        {
            "bootstrap.servers": endpoints,
            "group.id": "tx-transformer",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
            "isolation.level": "read_committed",
        }
    )
    consumer.subscribe(["transactions-input"])
    message = wait_for(lambda: consumer.poll(0.3), seconds=30, label="transaction input")
    assert not message.error()
    producer = Producer(
        {
            "bootstrap.servers": endpoints,
            "transactional.id": "part04-transformer-1",
            "enable.idempotence": True,
            "acks": "all",
        }
    )
    producer.init_transactions(30)
    offsets = [TopicPartition(message.topic(), message.partition(), message.offset() + 1)]
    producer.begin_transaction()
    producer.produce("transactions-test", key=b"input-1", value=b"aborted-output")
    assert producer.flush(15) == 0
    producer.send_offsets_to_transaction(offsets, consumer.consumer_group_metadata(), 15)
    producer.abort_transaction(15)
    after_abort = consumer.committed([TopicPartition(message.topic(), message.partition())], 10)[
        0
    ].offset
    # Consumer position advanced during the aborted attempt; recovery seeks explicitly.
    consumer.seek(TopicPartition(message.topic(), message.partition(), message.offset()))
    again = wait_for(lambda: consumer.poll(0.3), seconds=15, label="input re-read after abort")
    assert again.offset() == message.offset()
    producer.begin_transaction()
    producer.produce("transactions-test", key=b"input-1", value=b"committed-output")
    assert producer.flush(15) == 0
    producer.send_offsets_to_transaction(offsets, consumer.consumer_group_metadata(), 15)
    producer.commit_transaction(20)
    after_commit = consumer.committed([TopicPartition(message.topic(), message.partition())], 10)[
        0
    ].offset
    observations = {}
    for isolation in ("read_committed", "read_uncommitted"):
        reader = Consumer(
            {
                "bootstrap.servers": endpoints,
                "group.id": "inspect-" + isolation,
                "enable.auto.commit": False,
                "isolation.level": isolation,
                "enable.partition.eof": True,
            }
        )
        reader.assign([TopicPartition("transactions-test", 0, 0)])
        values = []
        positions = []
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            m = reader.poll(0.4)
            if not m:
                continue
            if m.error():
                if m.error().code() == -191:
                    break
                raise KafkaException(m.error())
            values.append(m.value().decode())
            positions.append(m.offset())
        observations[isolation] = {"values": values, "offsets": positions}
        reader.close()
    # Reusing a transactional.id fences the previous producer epoch.
    replacement = Producer(
        {"bootstrap.servers": endpoints, "transactional.id": "part04-transformer-1"}
    )
    replacement.init_transactions(30)
    fenced = None
    try:
        producer.begin_transaction()
        producer.produce("transactions-test", value=b"fenced-must-not-commit")
        producer.flush(10)
        producer.commit_transaction(15)
    except KafkaException as exc:
        fenced = {
            "code": exc.args[0].code(),
            "name": exc.args[0].name(),
            "fatal": exc.args[0].fatal(),
        }
    result = {
        "input": {
            "topic": message.topic(),
            "partition": message.partition(),
            "offset": message.offset(),
            "value": message.value().decode(),
        },
        "group_offset_after_abort": after_abort,
        "group_offset_after_commit": after_commit,
        "input_reread_offset": again.offset(),
        "observations": observations,
        "old_producer_fenced": fenced,
    }
    (lab.out / "kafka-transactions.json").write_text(canon(result), encoding="utf-8")
    lab.trace("kafka_transaction_result", result=result)
    consumer.close()
    assert observations["read_committed"]["values"] == ["committed-output"]
    assert observations["read_uncommitted"]["values"] == ["aborted-output", "committed-output"]
    assert after_abort < 0 and after_commit == message.offset() + 1 and fenced


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", choices=["kafka", "rabbit"], required=True)
    parser.add_argument("--run-dir", required=True)
    args = parser.parse_args()
    lab = Lab(Path(args.run_dir), broker=args.broker, external_dispatch=True)
    ok = False
    try:
        lab.initialize()
        lab.cases()
        if args.broker == "rabbit":
            rabbit_mandatory(lab)
        else:
            kafka_transactions(lab)
        broker_failure(lab)
        lab.save("final-state")
        ok = True
    except Exception:
        (lab.out / "failure.txt").write_text(traceback.format_exc(), encoding="utf-8")
        raise
    finally:
        lab.cleanup()
        (lab.out / "run-status.json").write_text(canon({"completed": ok}), encoding="utf-8")
