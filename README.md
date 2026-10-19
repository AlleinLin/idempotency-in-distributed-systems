# 分布式系统中的幂等性

### 从一次重复扣款开始

**Allein** · [AlleinLin@outlook.com](mailto:AlleinLin@outlook.com)

[全书目录](docs/reading-guide.md) · [构建 PDF](docs/building.md) · [实验代码](examples/README.md) · [贡献指南](CONTRIBUTING.md) · [许可](#许可)

<img src="book/assets/cover-art/cover-background.png" alt="《分布式系统中的幂等性》封面背景" width="220" align="right">

一笔购买已经扣款，响应却在返回途中丢失。客户端再次发送请求，服务端该再次扣款，还是返回上一笔购买的结果？如果此时换了副本、支付平台仍在处理、消息被重新投递，甚至数据库恢复到了旧备份，答案又该从哪里取得？

本书沿着同一套购买系统，逐步回答这些问题。起点是一个能正常下单的单数据库服务，随后加入稳定操作身份、并发仲裁和原始结果保存，再把支付、消息、复制、记录回收、历史恢复与分区迁移接到同一条业务链路中。第四部分从整体架构出发，展开各子系统的职责、数据所有权、内部机制和运维条件。

仓库包含全书 **21 章**、概论、四部分结语与全书结语的 TeX 源文件，以及插图、可编辑 TikZ 图源和配套实验。默认字体环境下，整书为 **556 页**。

## 本书关注的问题

幂等保证涉及业务身份、持久化历史与实际效果之间的关系。一次调用失败，可能发生在请求到达之前、事务回滚之后，也可能发生在效果已经提交、调用方尚未知情的窗口。协议需要为这些不同事实安排相应的查询、重放和恢复路径。

全书反复追踪四件事：

| 问题 | 需要建立的关系 |
| --- | --- |
| 怎样认出同一次业务意图？ | 将账户、动作、会话和键放进明确的命名空间，并绑定原始输入 |
| 谁有权确定业务结果？ | 将权威事实、执行资格和派生状态放在清楚的责任域中 |
| 效果发生后，怎样继续完成链路？ | 保存原操作、外部结果、待办责任和交付进度，让恢复沿原身份继续 |
| 历史变化以后，承诺怎样保存？ | 检查复制、快照、回收、备份恢复和迁移是否仍保留必要证据 |

讨论通过数据表、事务、报文、状态机、时间线和故障推演展开。每个保证都对应具体的保存位置、仲裁步骤及成立前提。

## 内容与结构

### 第一部分：在一个事务域中把保证做实

从订单、账户、扣款流水和购买操作开始，依次引入响应丢失、稳定身份、唯一约束、原始结果重放与操作状态机。重点是把业务效果与用于识别它的证据放进同一个事务，并在并发和崩溃窗口中检验这份保证。

| 章 | 标题 |
| --- | --- |
| 1 | 先实现一个正常工作的下单服务 |
| 2 | 一次超时，为什么变成了两次扣款 |
| 3 | 系统怎样认出同一次购买 |
| 4 | 两个请求同时到达，第一版防重失效了 |
| 5 | 扣款已经成功，怎样把结果找回来 |
| 6 | 在每一步杀死服务，检验第一版协议 |

### 第二部分：走出本地事务，学会处理未知结果

支付平台拥有独立账本，消息系统拥有独立提交位置。跨越这些边界以后，恢复需要依赖下游的幂等与查询能力、持久化工作流、资源端 fencing、Outbox，以及消费者事务中的去重记录与业务效果。

| 章 | 标题 |
| --- | --- |
| 7 | 接入支付平台，本地事务的保证到哪里结束 |
| 8 | 支付一直停在处理中，谁来接着做 |
| 9 | 支付成功以后，可靠地通知下游 |
| 10 | 消费者处理完消息，却没来得及确认 |

### 第三部分：复制、遗忘和历史变化之后，承诺是否还在

复制切换能否保留操作事实，去重记录何时可以回收，业务代数能减少哪些历史成本，旧备份又会使哪些承诺失去依据？这一部分把问题推进到时间与历史变化，并在第 15 章汇合成包含退款、跨版本重放和恢复职责的完整协议。

| 章 | 标题 |
| --- | --- |
| 11 | 切换到另一个副本，系统还记得做过什么吗 |
| 12 | 去重记录越来越多，能不能删掉 |
| 13 | 换一种业务表达，能少记住一些历史吗 |
| 14 | 恢复旧备份，再把流量切到另一个地域 |
| 15 | 给最终系统写下它能兑现的保证 |

### 第四部分：分布式购买系统的整体设计与工程架构

从总体架构和端到端业务链路进入内部结构，展开消息子系统、分区复制、协调控制面、在线迁移、缓存与存储、跨分区事务、批处理和执行框架，最后讨论多地域部署、容量、观测与恢复。

RabbitMQ、Kafka、Redis、TiKV、Spanner、FaRM、Ray、Lambda、持久化工作流等系统，分别放在它们承担的职责和边界中讲解。

| 章 | 标题 |
| --- | --- |
| 16 | 总体架构、服务协作与业务链路 |
| 17 | 分区数据、复制与协调控制面 |
| 18 | Redis 节点、缓存与集群设计 |
| 19 | 分布式存储与跨分区事务 |
| 20 | 批处理、任务、Actor 与函数执行 |
| 21 | 多地域部署、容量运维与前沿架构 |

## 怎样阅读

具备 SQL 事务、HTTP 接口和基本并发知识即可从第一章开始。阅读前先看概论，了解各部分怎样围绕同一套系统展开。

| 阅读目标 | 建议路径 |
| --- | --- |
| 系统学习幂等协议 | 按 1–21 章顺序，结合各部分结语回看保证与前提 |
| 设计支付与消息链路 | 重点阅读 3–10 章，再看第 15、16 章的完整协议与系统链路 |
| 处理复制、回收和恢复 | 重点阅读 11–15 章，再结合第 17、19、21 章 |
| 学习整体系统设计 | 先读概论和第 15 章，再进入 16–21 章，按需要回查前文推导 |
| 从实验进入 | 从第一章服务开始，再运行事务恢复实验和多分区主实验 |

章节与节标题统一定义在 [`book/data/structure.tex`](book/data/structure.tex)，完整章节导航见[阅读指南](docs/reading-guide.md)。

## 构建整书 PDF

安装带中文支持的 TeX Live、MiKTeX 或 MacTeX，确保 `xelatex` 在 PATH 中。在仓库根执行：

```powershell
# Windows PowerShell
.\build.ps1
```

```bash
# Linux / macOS
bash book/build.sh
```

脚本至少编译两遍，并根据交叉引用状态继续编译，最多四遍。产物为：

```text
book/build/main.pdf
```

工程优先使用系统字体，并提供 Noto CJK、Fandol 和 Latin Modern 后备。字体差异会影响分页。Ubuntu 依赖安装、字体选择和日志排错见[构建指南](docs/building.md)。

GitHub Actions 的 `Build book` 工作流会构建并上传整书 PDF；可在仓库的 Actions 页面下载成功运行的产物。发行版 PDF 也适合通过 GitHub Releases 分发。

## 配套实验

三组示例分别保存入口、依赖和说明。运行时使用新的 `runs/<name>` 目录，便于保存故障历史、账本、快照和检查结果。

| 示例 | 环境 | 可以观察的机制 |
| --- | --- | --- |
| [第一章下单服务](examples/chapter-01/README.md) | Python 3.11+、PostgreSQL 18，可用 Docker Compose | 购买、扣款、订单和结果查询的本地事务 |
| [事务与恢复协议](examples/transactional/README.md) | Python 3.11+、PostgreSQL 18 工具；Python 仅用标准库 | 四个独立责任域、逻辑回退、投影冲突、退款与扫描恢复 |
| [多分区系统](examples/multipartition/README.md) | Python 3.11+、Go 1.24+、NATS Server | 四个三副本 Raft 组、服务协作、消息投递、切换、回收和迁移 |

### 第一章：启动购买服务

在 `examples/chapter-01/` 中执行：

```bash
docker compose up -d --wait
python -m venv .venv
```

激活虚拟环境后安装 `requirements.txt`，使用示例默认数据库连接串初始化空库并启动服务：

```bash
python -m pip install -r requirements.txt
python app.py --init-db
python app.py
```

服务监听 `127.0.0.1:8000`。请求示例、演示账户和数据库配置见[本章说明](examples/chapter-01/README.md)。

### 第 15 章：运行完整事务与恢复协议

将 PostgreSQL 的 `bin` 目录加入 PATH，或通过 `PG_BIN` 指定该目录。在 `examples/transactional/` 中执行：

```bash
python -X utf8 run_all.py --run-dir runs/first
```

入口创建四个空数据库集群，执行命名故障场景、核对跨表不变量，并停止本次创建的集群。结果摘要写入 `summary.json`，完整过程保存在同一运行目录。端口调整和退款模型入口见[协议实验说明](examples/transactional/README.md)。

### 第四部分：运行多分区主实验

安装 NATS Server 并加入 PATH。在 `examples/multipartition/` 中创建并激活虚拟环境后执行：

```bash
python -m pip install -r requirements.txt
cd authority
go test ./...
```

Windows 使用 `go build -o ../authority.exe .`，Linux / macOS 使用 `go build -o ../authority-server .`。回到 `examples/multipartition/` 后运行：

```bash
python -X utf8 run_lab.py --run-dir runs/first
python -X utf8 check_run.py --run-dir runs/first
```

主实验启动真实 Raft 与 NATS 进程；独立检查器从执行历史和账本核对结果。NATS 可用 `NATS_SERVER` 指定位置，权威二进制可用 `AUTHORITY_BINARY` 指定位置。

etcd、Kafka、RabbitMQ、Ray、DBOS 和多机部署相关入口见[多分区实验说明](examples/multipartition/README.md)。这些入口分别需要相应运行时和配置；Kafka / RabbitMQ 的原生编排脚本使用 Windows 进程管理，多机工具生成供部署使用的启动文件。

## 源码结构

```text
book/
├── main.tex                 整书入口
├── data/                    章、节、目录和章首引语
├── frontmatter/             封面、前言、概论
├── chapters/                21 章正文
├── backmatter/              各部分结语、全书结语、参考文献
├── figures/                 可编辑 TikZ 机制图
├── assets/                  排版使用的图片
├── styles/                  中文字体、图表和版式
└── build.ps1 / build.sh      XeLaTeX 构建入口
examples/
├── chapter-01/              第一章服务、SQL 与 Compose 配置
├── transactional/           四数据库协议与故障实验
└── multipartition/          Go 权威状态机、Python 服务与检查器
docs/                        阅读与构建指南
scripts/                     代码检查和产物清理
.github/                     构建工作流、Issue 与 PR 模板
```

书稿、原生图源和实验代码可以分别修改。调整协议或数据结构时，结合相关章节、图示和检查器核对相应假设。

## 开发与贡献

欢迎提交文字勘误、引用修正、协议反例、实验复现和代码改进。书稿问题注明章、节和原文；机制问题给出系统假设、事件顺序、持久化状态与预期结果。

在仓库根安装开发依赖并检查 Python：

```bash
python -m pip install -r requirements-dev.txt
python -m ruff format --check examples
python -m ruff check examples
```

Go 测试在 `examples/multipartition/authority/` 中执行 `go test ./...`。Windows 也可以运行 `scripts/check.ps1`，统一检查格式与现有 Go 测试。

构建目录、虚拟环境、数据库文件和编译二进制已列入 `.gitignore`。Windows 可用 `scripts/clean.ps1 -WhatIf` 预览产物清理。实验运行记录由使用者自行归档。

贡献流程及代码格式约定见 [CONTRIBUTING.md](CONTRIBUTING.md)。

## 许可

| 内容 | 许可 |
| --- | --- |
| 书稿与原创图示 | [CC BY-SA 4.0](LICENSE) |
| 示例代码、构建脚本与工程配置 | [MIT](LICENSE-CODE) |

引用的文献、章首引语和第三方软件遵循各自适用的权利与许可说明，详见 [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md)。
