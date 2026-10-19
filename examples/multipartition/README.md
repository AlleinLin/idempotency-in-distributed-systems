# 多分区系统与机制实验

系统由四个独立 Raft 组组成：一个控制组与三个数据组，每组启动三个真实进程。Python 服务负责路由、支付平台、代理、消息发布与消费；权威状态机使用 Go 和 HashiCorp Raft。每次运行保存调用历史、源文件、依赖版本、二进制摘要、状态快照和进程退出记录。

## 核心实验

需要 Python 3.11+、Go 1.24+ 和 [NATS Server](https://github.com/nats-io/nats-server/releases)（原实验使用 2.15.1，启用 JetStream）。

从本目录创建并激活 Python 虚拟环境，然后安装：

```bash
python -m pip install -r requirements.txt
cd authority
go test ./...
```

Windows 编译并运行：

```powershell
go build -o ../authority.exe .
cd ..
$env:NATS_SERVER = 'C:\tools\nats-server\nats-server.exe'
python -X utf8 run_lab.py --run-dir runs/first
python -X utf8 check_run.py --run-dir runs/first
```

Linux / macOS 编译并运行：

```bash
go build -o ../authority-server .
cd ..
export NATS_SERVER=/usr/local/bin/nats-server
python -X utf8 run_lab.py --run-dir runs/first
python -X utf8 check_run.py --run-dir runs/first
```

`NATS_SERVER` 可省略，此时从 PATH 查找 `nats-server`。`AUTHORITY_BINARY` 可指定自编译权威二进制，默认使用本目录的 `authority.exe`（Windows）或 `authority-server`（其他系统）。

`runs/first` 必须不存在。默认使用 36100–36111、36200–36211、36300–36301、36310–36313 的本机端口，启动前逐一检查。主实验覆盖稳定身份、同键异参、各数据组故障切换、恢复、会话关闭与回收、分区迁移。独立检查器从输出账本与调用历史重建检查；返回码为 0 表示通过。

## 代码导航

| 模块 | 职责 |
| --- | --- |
| `authority/` | Raft 状态机、输入边界、快照保护、超时及交接测试 |
| `runtime.py` | 外部工具发现与运行依赖配置 |
| `run_lab.py`、`services.py`、`common.py` | 部署编排、服务角色与权威客户端 |
| `boundary.py`、`ownership_invariants.py`、`expected_spec.py` | 输入规范、所有权与独立规格 |
| `evidence.py`、`check_run.py` | 执行证据与独立核对 |
| `migration.py`、`coordination.py` | 迁移和协调机制 |
| `run_recovery.py`、`run_handoff_recovery.py`、`run_timeout_races.py` | 恢复、交接与超时竞态 |
| `run_bad_ownership.py`、`checker_sensitivity.py` | 错误所有权及检查器敏感性 |
| `run_native_chain.py`、`run_broker_smoke.py`、`native_brokers.py` | 原生 Kafka / RabbitMQ |
| `run_etcd.py`、`check_etcd.py` | etcd 租约、fencing、切换及隔离快照恢复 |
| `batch_lab.py`、`block_lab.py`、`cache_lab.py`、`occ_lab.py` | 批处理、块存储、缓存与 OCC |
| `chain_node.py`、`function_runtime_lab.py`、`durable_*_lab.py` | 链式复制、函数与持久化工作流 |
| `deploy_regions.py`、`initialize_multimachine.py` | Windows 多机启动文件生成与初始化 |

其他 `check_*.py` 文件核对对应实验的输出。运行编排脚本可用 `--help` 查看参数；服务模块和机制实验需要父实验生成的配置与状态，按入口参数准备环境。

## 扩展依赖

| 环境变量 | 用途 |
| --- | --- |
| `AUTHORITY_BINARY` | 权威状态机可执行文件 |
| `NATS_SERVER` | NATS Server 可执行文件 |
| `ETCD_BIN` | `etcd`、`etcdctl`、`etcdutl` 所在目录 |
| `ETCD_PROVENANCE` | etcd 发行包来源及官方摘要核验记录，JSON |
| `KAFKA_HOME` | 含 `libs/` 的 Kafka 安装目录 |
| `ERLANG_HOME` | Erlang 安装目录 |
| `RABBITMQ_HOME` | 含 `sbin/` 和 `plugins/` 的 RabbitMQ 目录 |
| `JAVA_HOME` | JDK 目录；省略时从 PATH 查找 java |

Kafka / RabbitMQ 脚本使用 Windows 批处理及进程树管理，原环境分别为 Kafka 4.4.0、RabbitMQ 4.3.6、Erlang 28.5.0.8。安装 `requirements-brokers.txt` 后再配置上表路径；这些运行时从上游安装，不放入仓库。

etcd 实验使用 3.7.2，需要同时安装三个命令。`ETCD_PROVENANCE` 指向用户核实过的发行包记录，格式见 `etcd-provenance.example.json`。填写下载包的 SHA-256 与从上游独立获得的官方摘要，保留实际下载包以备复核。实验记录各二进制摘要和版本，并拒绝不完整的来源记录。

```powershell
$env:ETCD_BIN = 'C:\tools\etcd'
$env:ETCD_PROVENANCE = 'C:\tools\etcd-provenance.json'
python -X utf8 run_etcd.py --run-dir runs/etcd-first
python -X utf8 check_etcd.py --run-dir runs/etcd-first
```

Ray 和 DBOS 分别使用 `requirements-ray.txt`、`requirements-workflows.txt`，建议安装在各自虚拟环境中。DBOS 入口读取已有运行目录内的 `config.json`，启动与恢复分别使用 `--mode start`、`--mode recover`。这些扩展实验各有自己的服务与配置前提，核心 NATS 安装覆盖上面的主实验。

多机生成器使用 `--hosts` 指定 A/B/C/platform/worker 到主机名的 JSON，`--output` 指定新输出目录。远端 Python、权威与 NATS 命令分别通过 `--python`、`--authority`、`--nats-server` 指定。生成器写出启动文件，部署人员负责网络、依赖分发与服务托管。
