# 事务、外部效果与恢复协议

第 15 章的可执行参考实现。四个独立 PostgreSQL 集群分别保存权威历史、本地投影、支付平台与消费者账本。实验覆盖进程退出、响应丢失、逻辑备份回退、投影冲突回滚、退款、跨版本重放和持久化扫描恢复，并用独立规格检查财务与关联不变量。

## 运行

需要 Python 3.11+ 和 PostgreSQL 18 的 `initdb`、`pg_ctl`、`psql`、`pg_dump`、`pg_restore`。Python 部分仅使用标准库。

把 PostgreSQL 的 `bin` 目录加入 PATH，或设置 `PG_BIN`：

```powershell
$env:PG_BIN = 'C:\Program Files\PostgreSQL\18\bin'
python -X utf8 run_all.py --run-dir runs/first
```

```bash
export PG_BIN=/usr/lib/postgresql/18/bin
python -X utf8 run_all.py --run-dir runs/first
```

运行用户应有权执行 `initdb`。默认端口为 authority 55487、local 55486、platform 55488、consumer 55489；可设置 `IDEMPOTENCY_PG_PORT_BASE`，从该端口依次分配这四个角色。运行拒绝覆盖已有目录或使用已占用端口。Windows 若因系统保留端口出现 `Permission denied`，可改用另一段端口，例如 `$env:IDEMPOTENCY_PG_PORT_BASE = '45480'`。

脚本创建四个空集群，执行实验，并在结束时停止本次创建的集群。检查结果见 `summary.json`，完整历史见 `history.json`，SQL、源码摘要和清理结果保存在同一运行目录。默认配置在回环地址使用 trust 认证，适合隔离的本地实验环境。

## 代码导航

| 文件 | 职责 |
| --- | --- |
| `db.py` | PostgreSQL 工具发现、独立连接、SQL 与状态快照 |
| `schema.sql` | 四个责任域的数据结构 |
| `protocol.py` | 购买、支付、投影、消费、退款与恢复协议 |
| `expected_spec.py` | 独立预期状态与跨表不变量 |
| `run_all.py` | 冷启动、24 段故障历史、检查与清理 |
| `worker_fault.py` | 在指定提交边界退出的子进程 |
| `refund_model.py` | 正确协议与错误协议的有界退款模型 |

独立运行退款模型：

```bash
python -c "from refund_model import explore; print(explore()); print(explore(True))"
```
