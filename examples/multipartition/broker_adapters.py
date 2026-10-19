"""Broker envelopes carry the complete business EV unchanged."""

import asyncio
import uuid
from dataclasses import dataclass

from boundary import strict_loads
from common import canon


@dataclass
class Delivery:
    event: dict
    metadata: dict
    handle: object
    raw: bytes = b""
    error: str = ""


def decode_delivery(raw, metadata, handle):
    try:
        event = strict_loads(raw)
        if type(event) is not dict:
            raise ValueError("event envelope must be an object")
        return Delivery(event, metadata, handle, raw)
    except ValueError as exc:
        return Delivery(None, metadata, handle, raw, str(exc))


class NatsAdapter:
    def __init__(self, config, role):
        self.config, self.role = config, role

    async def setup(self):
        import nats
        from nats.js.api import AckPolicy, ConsumerConfig, StorageType, StreamConfig

        self.nc = await nats.connect(self.config["nats"])
        self.js = self.nc.jetstream()
        try:
            await self.js.stream_info("PURCHASES")
        except Exception:
            await self.js.add_stream(
                StreamConfig(name="PURCHASES", subjects=["purchase.paid"], storage=StorageType.FILE)
            )
        if self.role == "consume":
            self.sub = await self.js.pull_subscribe(
                "purchase.paid",
                durable="fulfill",
                config=ConsumerConfig(
                    durable_name="fulfill",
                    ack_policy=AckPolicy.EXPLICIT,
                    ack_wait=0.8,
                    max_ack_pending=64,
                ),
            )

    async def publish(self, event):
        ack = await self.js.publish("purchase.paid", canon(event).encode())
        return {"broker": "nats", "stream": ack.stream, "sequence": ack.seq}

    async def fetch(self):
        try:
            messages = await self.sub.fetch(1, timeout=0.6)
        except asyncio.TimeoutError:
            return []
        return [
            decode_delivery(m.data, {"broker": "nats", "raw": str(m.metadata)}, m) for m in messages
        ]

    async def ack(self, delivery):
        await delivery.handle.ack_sync(timeout=2)

    async def retry(self, delivery):
        await delivery.handle.nak()


class RabbitAdapter:
    def __init__(self, config, role):
        self.config, self.role = config["broker"], role
        self.connection = None

    async def setup(self):
        self.connect()

    def connect(self):
        import pika

        if self.connection and self.connection.is_open:
            self.connection.close()
        endpoints = [
            pika.ConnectionParameters(
                h["host"],
                h["port"],
                heartbeat=15,
                socket_timeout=2,
                blocked_connection_timeout=5,
                connection_attempts=1,
            )
            for h in self.config["hosts"]
        ]
        self.connection = pika.BlockingConnection(endpoints)
        self.channel = self.connection.channel()
        self.channel_id = str(uuid.uuid4())
        if self.role == "relay":
            self.channel.confirm_delivery()
        else:
            self.channel.basic_qos(prefetch_count=1)

    def ready(self):
        if not self.connection or self.connection.is_closed or self.channel.is_closed:
            self.connect()

    async def publish(self, event):
        import pika

        self.ready()
        self.channel.basic_publish(
            exchange=self.config["exchange"],
            routing_key="purchase.paid",
            body=canon(event).encode(),
            mandatory=True,
            properties=pika.BasicProperties(
                delivery_mode=2, content_type="application/json", message_id=event["event_id"]
            ),
        )
        return {
            "broker": "rabbit",
            "publisher_channel": self.channel_id,
            "confirmed": True,
            "mandatory": True,
        }

    async def fetch(self):
        self.ready()
        method, properties, body = self.channel.basic_get(self.config["queue"], auto_ack=False)
        if method is None:
            self.connection.process_data_events(time_limit=0.05)
            return []
        metadata = {
            "broker": "rabbit",
            "channel_id": self.channel_id,
            "delivery_tag": method.delivery_tag,
            "redelivered": method.redelivered,
            "message_id": properties.message_id,
            "queue": self.config["queue"],
        }
        return [decode_delivery(body, metadata, (self.channel_id, method.delivery_tag))]

    async def ack(self, delivery):
        if delivery.handle[0] != self.channel_id:
            raise ValueError("delivery belongs to closed RabbitMQ channel")
        self.channel.basic_ack(delivery_tag=delivery.handle[1])
        self.connection.process_data_events(time_limit=0.02)

    async def retry(self, delivery):
        if delivery.handle[0] != self.channel_id:
            raise ValueError("delivery belongs to closed RabbitMQ channel")
        self.channel.basic_nack(delivery_tag=delivery.handle[1], requeue=True)
        self.connection.process_data_events(time_limit=0.02)


class KafkaAdapter:
    def __init__(self, config, role):
        self.config, self.role = config["broker"], role

    async def setup(self):
        from confluent_kafka import Consumer, Producer

        common = {"bootstrap.servers": self.config["bootstrap"], "socket.timeout.ms": 3000}
        if self.role == "relay":
            self.producer = Producer(
                {
                    **common,
                    "enable.idempotence": True,
                    "acks": "all",
                    "delivery.timeout.ms": 12000,
                    "request.timeout.ms": 3000,
                }
            )
        else:
            self.consumer = Consumer(
                {
                    **common,
                    "group.id": self.config["group"],
                    "enable.auto.commit": False,
                    "enable.auto.offset.store": False,
                    "auto.offset.reset": "earliest",
                    "isolation.level": "read_committed",
                    "session.timeout.ms": 6000,
                    "heartbeat.interval.ms": 1000,
                    "max.poll.interval.ms": 60000,
                    "partition.assignment.strategy": "range",
                }
            )
            self.consumer.subscribe([self.config["topic"]])

    async def publish(self, event):
        result = []

        def delivered(error, message):
            result.append((error, message))

        self.producer.produce(
            self.config["topic"],
            key=event["event_id"].encode(),
            value=canon(event).encode(),
            on_delivery=delivered,
        )
        self.producer.flush(15)
        if not result:
            raise TimeoutError("Kafka delivery outcome unknown")
        error, message = result[0]
        if error:
            raise RuntimeError(str(error))
        return {
            "broker": "kafka",
            "topic": message.topic(),
            "partition": message.partition(),
            "offset": message.offset(),
            "confirmed": True,
        }

    async def fetch(self):
        message = self.consumer.poll(0.5)
        if message is None:
            return []
        if message.error():
            raise RuntimeError(str(message.error()))
        return [
            decode_delivery(
                message.value(),
                {
                    "broker": "kafka",
                    "topic": message.topic(),
                    "partition": message.partition(),
                    "offset": message.offset(),
                    "key": message.key().decode(errors="replace") if message.key() else None,
                },
                message,
            )
        ]

    async def ack(self, delivery):
        partitions = self.consumer.commit(message=delivery.handle, asynchronous=False)
        if any(p.error for p in partitions):
            raise RuntimeError(str(partitions))

    async def retry(self, delivery):
        from confluent_kafka import TopicPartition

        # poll advanced the local position. Rewind before processing later offsets.
        self.consumer.seek(
            TopicPartition(
                delivery.metadata["topic"],
                delivery.metadata["partition"],
                delivery.metadata["offset"],
            )
        )


def create_adapter(config, role):
    kind = config.get("broker", {}).get("kind", "nats")
    return {"nats": NatsAdapter, "rabbit": RabbitAdapter, "kafka": KafkaAdapter}[kind](config, role)
