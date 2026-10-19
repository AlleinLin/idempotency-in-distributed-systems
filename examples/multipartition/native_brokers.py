"""Private, native broker processes; no services, global ports or firewall changes."""

import json
import os
import socket
import subprocess
import time
from pathlib import Path

from runtime import installation, java_binary

ROOT = Path(__file__).parent


def await_value(fn, seconds=100):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        try:
            value = fn()
            if value:
                return value
        except Exception as exc:
            last = repr(exc)
        time.sleep(0.3)
    raise TimeoutError(last)


def port_open(port):
    with socket.socket() as sock:
        return sock.connect_ex(("127.0.0.1", port)) == 0


class NativeBrokers:
    def __init__(self, kind, out, trace):
        self.kind, self.out, self.trace = kind, Path(out), trace
        self.out.mkdir(exist_ok=True)
        self.processes = {}
        self.logs = []
        self.commands = {}
        self.environments = {}
        self.private_ports = []

    def start(self, name, command, env=None):
        log = (self.out / (name + ".log")).open("ab")
        self.logs.append(log)
        process = subprocess.Popen(
            [str(x) for x in command], cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT
        )
        self.processes[name] = process
        self.commands[name] = command
        self.environments[name] = env
        self.trace(
            "broker_process_start",
            broker=self.kind,
            name=name,
            pid=process.pid,
            command=[str(x) for x in command],
        )
        return process

    def stop(self, name):
        process = self.processes[name]
        if process.poll() is None:
            # Rabbit's foreground batch wrapper owns erl.exe descendants.
            if self.kind == "rabbit" and name.startswith("rabbit-"):
                subprocess.run(
                    ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                    capture_output=True,
                    timeout=20,
                )
            else:
                process.kill()
            process.wait(timeout=20)
        self.trace("broker_process_exit", name=name, pid=process.pid, returncode=process.returncode)

    def restart(self, name):
        return self.start(name, self.commands[name], self.environments[name])

    def close(self):
        for name in reversed(list(self.processes)):
            self.stop(name)
        for log in self.logs:
            log.close()
        for _ in range(20):
            remaining = [port for port in self.private_ports if port_open(port)]
            if not remaining:
                break
            time.sleep(0.2)
        (self.out / "cleanup.json").write_text(
            json.dumps(
                {
                    "processes": {
                        name: {"pid": p.pid, "returncode": p.poll()}
                        for name, p in self.processes.items()
                    },
                    "private_ports_still_open": remaining,
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        if remaining:
            raise RuntimeError(f"private broker listeners remain: {remaining}")

    def run(self, name, command, env=None, timeout=60):
        result = subprocess.run(
            [str(x) for x in command],
            cwd=ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=timeout,
        )
        (self.out / (name + ".txt")).write_bytes(result.stdout)
        self.trace(
            "broker_admin",
            name=name,
            returncode=result.returncode,
            output=result.stdout.decode("utf-8", errors="replace"),
        )
        if result.returncode:
            raise RuntimeError(f"{name}: {result.stdout[-3000:]!r}")
        return result.stdout.decode("utf-8", errors="replace")

    def initialize(self):
        if os.name != "nt":
            raise RuntimeError("The optional Kafka/RabbitMQ launcher currently requires Windows.")
        return self.kafka() if self.kind == "kafka" else self.rabbit()

    def kafka(self):
        from confluent_kafka.admin import AdminClient, NewTopic

        self.private_ports = list(range(38100, 38103)) + list(range(38200, 38203))
        for port in self.private_ports:
            with socket.socket() as sock:
                sock.bind(("127.0.0.1", port))
        self.kafka_home = installation("KAFKA_HOME")
        classpath = str(self.kafka_home / "libs/*")
        self.run("java-runtime", [java_binary(), "-XshowSettings:properties", "-version"])
        java = [java_binary(), "-Xms96m", "-Xmx256m", "-cp", classpath]
        cluster_id = (
            self.run("cluster-id", java + ["kafka.tools.StorageTool", "random-uuid"])
            .strip()
            .splitlines()[-1]
        )
        voters = ",".join(f"{i + 1}@127.0.0.1:{38200 + i}" for i in range(3))
        for i in range(3):
            data = self.out / f"kafka-{i}-data"
            # java.util.Properties.load(InputStream) reads ISO-8859-1.
            text = (
                "\n".join(
                    [
                        "process.roles=broker,controller",
                        f"node.id={i + 1}",
                        f"controller.quorum.voters={voters}",
                        f"listeners=PLAINTEXT://127.0.0.1:{38100 + i},CONTROLLER://127.0.0.1:{38200 + i}",
                        f"advertised.listeners=PLAINTEXT://127.0.0.1:{getattr(self, 'kafka_advertised_port_base', 38100) + i}",
                        "controller.listener.names=CONTROLLER",
                        "listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
                        "inter.broker.listener.name=PLAINTEXT",
                        f"log.dirs={data.as_posix()}",
                        "num.partitions=3",
                        "default.replication.factor=3",
                        "min.insync.replicas=2",
                        "offsets.topic.replication.factor=3",
                        "transaction.state.log.replication.factor=3",
                        "transaction.state.log.min.isr=2",
                        "group.initial.rebalance.delay.ms=0",
                        "num.network.threads=2",
                        "num.io.threads=2",
                        "log.retention.hours=24",
                        "log.segment.bytes=10485760",
                    ]
                )
                + "\n"
            )
            cfg = self.out / f"kafka-{i}.properties"
            cfg.write_bytes(text.encode("ascii", "backslashreplace"))
            self.run(
                f"format-{i}",
                java + ["kafka.tools.StorageTool", "format", "-t", cluster_id, "-c", cfg],
            )
        for i in range(3):
            self.start(f"kafka-{i}", java + ["kafka.Kafka", self.out / f"kafka-{i}.properties"])
        endpoints = ",".join(f"127.0.0.1:{38100 + i}" for i in range(3))
        self.admin = AdminClient({"bootstrap.servers": endpoints, "socket.timeout.ms": 3000})
        await_value(lambda: len(self.admin.list_topics(timeout=3).brokers) == 3)
        topics = [
            NewTopic("purchase.paid", 3, 3, config={"min.insync.replicas": "2"}),
            NewTopic("transactions-test", 1, 3, config={"min.insync.replicas": "2"}),
            NewTopic("transactions-input", 1, 3, config={"min.insync.replicas": "2"}),
        ]
        for _, future in self.admin.create_topics(topics).items():
            future.result(timeout=30)
        await_value(
            lambda: all(
                len(p.isrs) == 3
                for topic in self.admin.list_topics(timeout=3).topics.values()
                if topic.topic in ("purchase.paid", "transactions-test")
                for p in topic.partitions.values()
            )
        )
        result = {
            "kind": "kafka",
            "bootstrap": endpoints,
            "topic": "purchase.paid",
            "group": "fulfill",
            "cluster_id": cluster_id,
            "version": "4.4.0",
        }
        self.metadata("initial")
        return result

    def metadata(self, label):
        if self.kind == "kafka":
            md = self.admin.list_topics(timeout=8)
            result = {
                "brokers": {
                    str(k): {"host": b.host, "port": b.port} for k, b in md.brokers.items()
                },
                "topics": {
                    name: {
                        str(k): {"leader": p.leader, "replicas": p.replicas, "isrs": p.isrs}
                        for k, p in topic.partitions.items()
                    }
                    for name, topic in md.topics.items()
                    if not name.startswith("__")
                },
            }
        else:
            index = next(i for i in range(3) if self.processes[f"rabbit-{i}"].poll() is None)
            result = {
                "cluster": self.ctl(
                    index, label + "-cluster", "cluster_status", "--formatter=json"
                ),
                "queues": self.ctl(
                    index,
                    label + "-queues",
                    "list_queues",
                    "name",
                    "type",
                    "leader",
                    "members",
                    "online",
                    "--formatter=json",
                ),
            }
        (self.out / (label + "-metadata.json")).write_text(
            json.dumps(result, indent=2), encoding="utf-8"
        )
        self.trace("broker_metadata", label=label, result=result)
        return result

    def rabbit_env(self, index):
        env = os.environ.copy()
        root = installation("ERLANG_HOME")
        base = self.out / f"rabbit-{index}-data"
        base.mkdir(exist_ok=True)
        env.update(
            ERLANG_HOME=str(root),
            ERL_EPMD_PORT="37390",
            ERL_LIBS=str(installation("RABBITMQ_HOME") / "plugins"),
            RABBITMQ_BASE=str(base),
            RABBITMQ_MNESIA_BASE=str(base / "mnesia"),
            RABBITMQ_LOG_BASE=str(base / "log"),
            RABBITMQ_CONFIG_FILE=str(self.out / f"rabbit-{index}"),
            RABBITMQ_NODENAME=f"part04q{index}@localhost",
            RABBITMQ_NODE_PORT=str(37400 + index),
            RABBITMQ_DIST_PORT=str(37500 + index),
            RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS="+S 2:2 -setcookie part04-private-experiment",
            RABBITMQ_CTL_ERL_ARGS="+S 1:1 -setcookie part04-private-experiment",
            RABBITMQ_CTL_DIST_PORT_MIN="37600",
            RABBITMQ_CTL_DIST_PORT_MAX="37610",
        )
        return env

    def ctl(self, index, label, *args):
        script = installation("RABBITMQ_HOME") / "sbin/rabbitmqctl.bat"
        return self.run(
            label,
            ["cmd.exe", "/d", "/c", str(script), "-n", f"part04q{index}@localhost", *args],
            self.rabbit_env(index),
            timeout=80,
        )

    def rabbit(self):
        import pika

        self.private_ports = [37390, *range(37400, 37403), *range(37500, 37503)]
        for port in self.private_ports:
            with socket.socket() as sock:
                sock.bind(("127.0.0.1", port))
        epmd = installation("ERLANG_HOME") / "bin/epmd.exe"
        if not epmd.exists():
            epmd = next((installation("ERLANG_HOME")).glob("erts-*/bin/epmd.exe"))
        env = os.environ.copy()
        env["ERL_EPMD_PORT"] = "37390"
        self.start("epmd", [epmd, "-port", "37390"], env)
        await_value(lambda: port_open(37390), 15)
        for i in range(3):
            config = [
                "listeners.tcp.default = " + str(37400 + i),
                "loopback_users.guest = true",
                "cluster_partition_handling = pause_minority",
                "disk_free_limit.absolute = 100MB",
                "vm_memory_high_watermark.absolute = 512MB",
            ]
            (self.out / f"rabbit-{i}.conf").write_text("\n".join(config) + "\n", encoding="utf-8")
            self.start(
                f"rabbit-{i}",
                ["cmd.exe", "/d", "/c", installation("RABBITMQ_HOME") / "sbin/rabbitmq-server.bat"],
                self.rabbit_env(i),
            )
        await_value(lambda: all(port_open(37400 + i) for i in range(3)), 120)
        for i in (1, 2):
            self.ctl(i, f"stop-app-{i}", "stop_app")
            self.ctl(i, f"reset-{i}", "reset")
            self.ctl(i, f"join-{i}", "join_cluster", "part04q0@localhost")
            self.ctl(i, f"start-app-{i}", "start_app")
        connection = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1", 37400))
        ch = connection.channel()
        ch.exchange_declare(exchange="purchases", exchange_type="direct", durable=True)
        ch.queue_declare(
            queue="purchase.paid",
            durable=True,
            arguments={"x-queue-type": "quorum", "x-quorum-initial-group-size": 3},
        )
        ch.queue_bind(queue="purchase.paid", exchange="purchases", routing_key="purchase.paid")
        connection.close()
        self.metadata("initial")
        return {
            "kind": "rabbit",
            "hosts": [{"host": "127.0.0.1", "port": 37400 + i} for i in range(3)],
            "exchange": "purchases",
            "queue": "purchase.paid",
            "version": "4.3.6",
            "erlang": "28.5.0.8",
        }
