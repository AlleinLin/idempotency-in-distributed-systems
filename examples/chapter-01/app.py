"""第一章的本地教学服务。每次 POST 都创建一笔新购买，不提供重试防重。"""

import argparse
import json
import logging
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from uuid import UUID, uuid4

import psycopg
from psycopg.rows import dict_row

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://tutorial:tutorial@127.0.0.1:55432/idempotency",
)
PRODUCTS = {"paper-001": 3000, "paper-002": 8000}
# 这些凭据只模拟本地登录后的账户映射，不能作为部署用的认证实现。
DEMO_TOKENS = {"demo-alice": 1, "exercise-2": 2, "exercise-3": 3}
LOG = logging.getLogger("chapter01")


def connect():
    conn = psycopg.connect(DATABASE_URL, autocommit=True, row_factory=dict_row, connect_timeout=5)
    conn.isolation_level = psycopg.IsolationLevel.READ_COMMITTED
    return conn


def create_purchase(conn, account_id, product_id):
    amount_minor = PRODUCTS[product_id]
    purchase_id, order_id, debit_id = uuid4(), uuid4(), uuid4()
    with conn.transaction():
        conn.execute(
            """INSERT INTO purchases
               (purchase_id, account_id, product_id,
                amount_minor, currency, status)
               VALUES (%s, %s, %s, %s, 'CNY', 'pending')""",
            (purchase_id, account_id, product_id, amount_minor),
        )
        debit = conn.execute(
            """UPDATE accounts
               SET balance_minor = balance_minor - %s
               WHERE account_id = %s AND balance_minor >= %s
               RETURNING balance_minor""",
            (amount_minor, account_id, amount_minor),
        ).fetchone()
        if debit is None:
            status = "insufficient_funds"
            result_order, result_debit = None, None
        else:
            conn.execute(
                """INSERT INTO orders
                   (order_id, purchase_id, product_id,
                    amount_minor, currency, status)
                   VALUES (%s, %s, %s, %s, 'CNY', 'paid')""",
                (order_id, purchase_id, product_id, amount_minor),
            )
            conn.execute(
                """INSERT INTO debits
                   (debit_id, order_id, account_id,
                    amount_minor, balance_after_minor)
                   VALUES (%s, %s, %s, %s, %s)""",
                (debit_id, order_id, account_id, amount_minor, debit["balance_minor"]),
            )
            status = "succeeded"
            result_order, result_debit = str(order_id), str(debit_id)
        conn.execute(
            "UPDATE purchases SET status = %s WHERE purchase_id = %s",
            (status, purchase_id),
        )
        result = {
            "purchase_id": str(purchase_id),
            "product_id": product_id,
            "order_id": result_order,
            "debit_id": result_debit,
            "status": status,
            "amount_minor": amount_minor,
            "currency": "CNY",
        }
    # 退出上下文并确认提交后，才允许调用者使用这个结果发送响应。
    return result


def get_purchase(conn, account_id, purchase_id):
    row = conn.execute(
        """SELECT p.purchase_id, p.product_id, p.amount_minor,
                  p.currency, p.status, o.order_id, d.debit_id
           FROM purchases AS p
           LEFT JOIN orders AS o ON o.purchase_id = p.purchase_id
           LEFT JOIN debits AS d ON d.order_id = o.order_id
           WHERE p.purchase_id = %s AND p.account_id = %s""",
        (purchase_id, account_id),
    ).fetchone()
    if row is None:
        return None
    for field in ("purchase_id", "order_id", "debit_id"):
        if row[field] is not None:
            row[field] = str(row[field])
    return row


class Handler(BaseHTTPRequestHandler):
    # HTTP/1.0 closes each connection, keeping the teaching server's handling of
    # rejected or unread request bodies simple.
    protocol_version = "HTTP/1.0"

    def log_message(self, *_args):
        pass

    def respond(self, code, payload, *, location=None):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Request-ID", self.request_id)
        if location:
            self.send_header("Location", location)
        self.end_headers()
        LOG.info(
            "request_id=%s method=%s path=%s status=%s purchase_id=%s order_id=%s debit_id=%s",
            self.request_id,
            self.command,
            self.path,
            code,
            payload.get("purchase_id", "-"),
            payload.get("order_id", "-"),
            payload.get("debit_id", "-"),
        )
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            LOG.info("request_id=%s response_connection_closed", self.request_id)

    def account(self):
        authorization = self.headers.get("Authorization", "")
        if not authorization.startswith("Bearer "):
            return None
        return DEMO_TOKENS.get(authorization.removeprefix("Bearer "))

    def handle_request(self):
        self.request_id = str(uuid4())
        self.connection.settimeout(5)
        # 在关闭连接前读完本地示例允许的短请求正文，避免拒绝请求时
        # 未读取的正文造成连接复位。这里不接受分块传输或大请求。
        raw_body = b""
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = -1
        if self.command == "POST" and 0 < length <= 4096:
            raw_body = self.rfile.read(length)
        account_id = self.account()
        if account_id is None:
            self.respond(401, {"error": "unauthorized"})
            return
        try:
            if self.command == "POST" and self.path == "/purchases":
                if not 0 < length <= 4096 or self.headers.get("Transfer-Encoding"):
                    self.respond(400, {"error": "invalid_body"})
                    return
                if self.headers.get("Content-Type", "").split(";")[0].strip() != "application/json":
                    self.respond(400, {"error": "expected_json"})
                    return
                data = json.loads(raw_body)
                if (
                    not isinstance(data, dict)
                    or set(data) != {"product_id"}
                    or not isinstance(data["product_id"], str)
                    or data["product_id"] not in PRODUCTS
                ):
                    self.respond(400, {"error": "invalid_product"})
                    return
                with connect() as conn:
                    result = create_purchase(conn, account_id, data["product_id"])
                code = 201 if result["status"] == "succeeded" else 409
                self.respond(code, result, location="/purchases/" + result["purchase_id"])
            elif self.command == "GET" and self.path.startswith("/purchases/"):
                purchase_id = UUID(self.path.removeprefix("/purchases/"))
                with connect() as conn:
                    result = get_purchase(conn, account_id, purchase_id)
                if result is None:
                    self.respond(404, {"error": "not_found"})
                else:
                    self.respond(200, result)
            else:
                self.respond(404, {"error": "not_found"})
        except (ValueError, UnicodeDecodeError):
            self.respond(400, {"error": "invalid_input"})
        except psycopg.Error:
            LOG.exception("request_id=%s database_error", self.request_id)
            # 503 只报告本次交互未能正常完成，不证明数据库一定未提交。
            self.respond(503, {"error": "database_unavailable"})

    do_GET = handle_request
    do_POST = handle_request


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--init-db", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if args.init_db:
        # 初始化仅用于空库，已有表时会报错回滚，绝不自动删表或清空数据。
        sql = Path(__file__).with_name("schema.sql").read_text(encoding="utf-8")
        with connect() as conn:
            conn.execute(sql, prepare=False)
        print("已建立四张表和三个初始余额为10000的演示账户。")
        return
    port = int(os.environ.get("APP_PORT", "8000"))
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"教学服务监听 http://127.0.0.1:{port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
