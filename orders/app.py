"""Orders — не такаю уж и наивная реализация.

Оформление заказа: зарезервировать товар в Inventory, списать деньги в
Payment, записать заказ в базу. Пока Payment и Inventory работают без отказов, всё работает.
"""

import json
import logging
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import psycopg

PORT = int(os.environ.get("PORT", "8080"))
DATABASE_URL = os.environ["DATABASE_URL"]
PAYMENT_URL = os.environ["PAYMENT_URL"].rstrip("/")
INVENTORY_URL = os.environ["INVENTORY_URL"].rstrip("/")

PAYMENT_TIMEOUT = 6
LOOKUP_TIMEOUT = 0.8

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("orders")

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    id           TEXT PRIMARY KEY,
    user_id      TEXT NOT NULL,
    status       TEXT NOT NULL,
    amount_cents BIGINT NOT NULL,
    items        JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""


pay_sem = threading.Semaphore(5)
pay_lock = threading.Lock()
pay_down = False
pay_at = 0.0
probing = False


def db():
    return psycopg.connect(DATABASE_URL, autocommit=True)


def init_db():
    # База может подниматься дольше сервиса — ждём её.
    for attempt in range(60):
        try:
            with db() as conn:
                conn.execute(SCHEMA)
            return
        except psycopg.OperationalError as e:
            log.info("база недоступна (%s), жду", e)
            time.sleep(1)
    raise SystemExit("не дождался базы")


def call(method, url, body=None, key="", timeout=2):
    """HTTP-вызов Payment или Inventory. Возвращает (код, тело)."""
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if key:
        req.add_header("Idempotency-Key", key)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read() or b"null")
    except urllib.error.HTTPError as e:
        return e.code, None


class PaymentError(Exception):
    pass


def reserve(order_id, items):
    code, _ = call("POST", f"{INVENTORY_URL}/reservations", {
        "order_id": order_id,
        "items": [{"sku": i["sku"], "qty": i["qty"]} for i in items],
    })
    if code not in (200, 201):
        raise RuntimeError(f"inventory: {code}")


def release(order_id):
    try:
        call("DELETE", f"{INVENTORY_URL}/reservations/{order_id}")
    except OSError:
        pass


def set_status(order_id, status):
    with db() as conn:
        conn.execute("UPDATE orders SET status = %s WHERE id = %s", (status, order_id))


def wait_payment():
    global probing
    while True:
        with pay_lock:
            wait = pay_at - time.monotonic()
            if not pay_down or (wait <= 0 and not probing):
                if pay_down:
                    probing = True
                return
        time.sleep(max(0.1, wait))


def payment_result(timeout):
    global pay_down, pay_at, probing
    with pay_lock:
        probing = False
        pay_down = timeout
        if timeout:
            pay_at = time.monotonic() + 2


def lookup_paid(order_id):
    url = f"{PAYMENT_URL}/payments?order_id={urllib.parse.quote(order_id)}"
    try:
        code, body = call("GET", url, timeout=LOOKUP_TIMEOUT)
    except OSError:
        return False, False
    if code != 200:
        return False, True
    try:
        return len(json.loads(body or b"[]")) > 0, True
    except ValueError:
        return False, True


def charge_once(order_id, amount):
    with pay_sem:
        wait_payment()
        started = time.monotonic()
        try:
            code, _ = call("POST", f"{PAYMENT_URL}/payments", {
                "order_id": order_id,
                "amount_cents": amount,
                "currency": "RUB",
            }, key=order_id, timeout=PAYMENT_TIMEOUT)
        except OSError:
            if time.monotonic() - started > PAYMENT_TIMEOUT - 0.5:
                paid, ok = lookup_paid(order_id)
                if paid:
                    payment_result(False)
                    return "paid"
                if not ok:
                    payment_result(True)
                    return None
            payment_result(False)
            return None
        payment_result(False)
        if code in (200, 201):
            return "paid"
        if code == 402:
            return "rejected"
        return None


def process(order_id, items, amount):
    while True:
        try:
            reserve(order_id, items)
            break
        except Exception as e:
            log.warning("order %s: бронирование завершилось с ошибкой: %s", order_id, e)
            time.sleep(1)
    while True:
        status = charge_once(order_id, amount)
        if status == "paid":
            set_status(order_id, "paid")
            return
        if status == "rejected":
            release(order_id)
            set_status(order_id, "rejected")
            return
        time.sleep(0.15)


def valid(body):
    if not isinstance(body, dict):
        return False
    if not isinstance(body.get("user_id"), str) or not body["user_id"]:
        return False
    items = body.get("items")
    if not isinstance(items, list) or not items:
        return False
    for item in items:
        if not isinstance(item, dict):
            return False
        if not isinstance(item.get("sku"), str) or not item["sku"]:
            return False
        for key, minimum in (("qty", 1), ("price_cents", 0)):
            value = item.get(key)
            if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
                return False
    return True


def create_order(body):
    order_id = str(uuid.uuid4())
    amount = sum(i["qty"] * i["price_cents"] for i in body["items"])
    with db() as conn:
        conn.execute(
            "INSERT INTO orders (id, user_id, status, amount_cents, items) VALUES (%s, %s, %s, %s, %s)",
            (order_id, body["user_id"], "pending", amount, json.dumps(body["items"])),
        )
    threading.Thread(target=process, args=(order_id, body["items"], amount), daemon=True).start()
    return {"id": order_id, "status": "pending"}


def get_order(order_id):
    with db() as conn:
        row = conn.execute(
            "SELECT id, status, amount_cents FROM orders WHERE id = %s", (order_id,)
        ).fetchone()
    if row is None:
        return None
    return {"id": row[0], "status": row[1], "amount_cents": row[2]}


ORDER_PATH = re.compile(r"^/orders/([^/]+)$")


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def send_json(self, code, obj):
        data = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path == "/healthz":
            return self.send_json(200, {"status": "ok"})
        m = ORDER_PATH.match(self.path)
        if m:
            order = get_order(m.group(1))
            if order is None:
                return self.send_json(404, {"error": "not_found"})
            return self.send_json(200, order)
        self.send_json(404, {"error": "not_found"})

    def do_POST(self):
        if self.path != "/orders":
            return self.send_json(404, {"error": "not_found"})
        length = int(self.headers.get("Content-Length") or 0)
        try:
            body = json.loads(self.rfile.read(length) or b"null")
        except ValueError:
            body = None
        if not valid(body):
            return self.send_json(400, {"error": "bad_request"})
        try:
            self.send_json(201, create_order(body))
        except Exception as e:
            log.exception("POST /orders")
            self.send_json(500, {"error": "internal", "message": str(e)})

    def log_message(self, fmt, *args):
        pass


if __name__ == "__main__":
    init_db()
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    server.daemon_threads = True
    log.info("orders: слушаю :%d", PORT)
    server.serve_forever()
