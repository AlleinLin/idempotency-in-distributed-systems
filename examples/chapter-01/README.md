# 第一章：正常工作的下单服务

这个起点展示购买操作、订单、账户与扣款流水怎样在一个 PostgreSQL 事务中提交。每次 POST 创建一次新购买，后续章节再引入稳定身份与重复请求处理。

## 运行

需要 Python 3.11+、PostgreSQL 18。可用 Docker Compose 创建本地教学数据库：

```bash
docker compose up -d --wait
python -m venv .venv
```

激活虚拟环境后：

```bash
python -m pip install -r requirements.txt
```

设置连接串，初始化空库，再启动服务：

```powershell
$env:DATABASE_URL = 'postgresql://tutorial:tutorial@127.0.0.1:55432/idempotency'
python app.py --init-db
python app.py
```

Linux / macOS 用 `export DATABASE_URL='postgresql://tutorial:tutorial@127.0.0.1:55432/idempotency'`。初始化命令用于空库；再次初始化应先准备一个新的教学数据库。

## 请求

将以下内容保存为 `purchase.json`：

```json
{"product_id":"paper-001"}
```

```bash
curl -i http://127.0.0.1:8000/purchases -H "Authorization: Bearer demo-alice" -H "Content-Type: application/json" --data-binary @purchase.json
curl -i "http://127.0.0.1:8000/purchases/<purchase_id>" -H "Authorization: Bearer demo-alice"
```

查询地址中的 ID 应替换为创建响应中的 `purchase_id`。Windows PowerShell 使用 `curl.exe` 调用原生 curl。

`schema.sql` 中的演示账户、`demo-alice` 等令牌及 Compose 数据库密码均为公开教学配置。`app.py` 包含基础输入验证和事务逻辑，依赖列在 `requirements.txt`。

停止数据库可用 `docker compose down`，持久化数据保存在 Compose 命名卷中。
