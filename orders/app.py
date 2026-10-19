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
EVENTS_URL = os.environ["EVENTS_URL"].rstrip("/")

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
);
CREATE TABLE IF NOT EXISTS outbox (
    id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL UNIQUE REFERENCES orders(id),
    sent BOOLEAN NOT NULL DEFAULT false
);
CREATE TABLE IF NOT EXISTS deliveries (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    points BIGINT NOT NULL CHECK (points > 0)
);
CREATE INDEX IF NOT EXISTS deliveries_user ON deliveries(user_id)
"""


pay_sem = threading.Semaphore(5)
pay_lock = threading.Lock()
pay_down = False
pay_at = 0.0
probing = False


def db():
    return psycopg.connect(DATABASE_URL, autocommit=True)


def init_db():
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
    code, _ = call("DELETE", f"{INVENTORY_URL}/reservations/{order_id}")
    if code != 204:
        raise RuntimeError(f"inventory: {code}")


def set_status(order_id, status):
    with db() as conn:
        with conn.transaction():
            conn.execute("UPDATE orders SET status = %s WHERE id = %s", (status, order_id))
            if status == "paid":
                conn.execute(
                    "INSERT INTO outbox (id, order_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (f"order.paid:{order_id}", order_id),
                )


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
    return bool(body), True


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


def run_order(order_id, items, amount):
    while True:
        try:
            process(order_id, items, amount)
            return
        except Exception:
            log.exception("order %s", order_id)
            time.sleep(1)


def start_order(order_id, items, amount):
    threading.Thread(target=run_order, args=(order_id, items, amount), daemon=True).start()


def recover_orders():
    with db() as conn:
        rows = conn.execute(
            "SELECT id, items, amount_cents FROM orders WHERE status = 'pending'"
        ).fetchall()
    for row in rows:
        start_order(*row)


def publish_events():
    while True:
        try:
            with db() as conn:
                rows = conn.execute("SELECT id, order_id FROM outbox WHERE NOT sent LIMIT 100").fetchall()
            for event_id, order_id in rows:
                code, _ = call("POST", f"{EVENTS_URL}/events", {
                    "id": event_id, "type": "order.paid", "order_id": order_id,
                }, timeout=2)
                if code != 202:
                    break
                with db() as conn:
                    conn.execute("UPDATE outbox SET sent = true WHERE id = %s", (event_id,))
        except Exception as e:
            log.warning("события: %s", e)
        time.sleep(0.5)


def accept_delivery(body):
    if not isinstance(body, dict) or body.get("type") != "delivery.confirmed":
        return False
    if any(not isinstance(body.get(key), str) or not body[key] for key in ("id", "user_id")):
        return False
    points = body.get("bonus_points")
    if type(points) is not int or not 1 <= points <= 2**63 - 1:
        return False
    with db() as conn:
        conn.execute(
            "INSERT INTO deliveries (id, user_id, points) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
            (body["id"], body["user_id"], points),
        )
    return True


def get_bonus(user_id):
    with db() as conn:
        row = conn.execute(
            "SELECT COALESCE(SUM(points), 0) FROM deliveries WHERE user_id = %s", (user_id,)
        ).fetchone()
    return {"points": int(row[0])}


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
    start_order(order_id, body["items"], amount)
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
BONUS_PATH = re.compile(r"^/users/([^/]+)/bonus$")


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
        m = BONUS_PATH.match(self.path)
        if m:
            return self.send_json(200, get_bonus(urllib.parse.unquote(m.group(1))))
        m = ORDER_PATH.match(self.path)
        if m:
            order = get_order(m.group(1))
            if order is None:
                return self.send_json(404, {"error": "not_found"})
            return self.send_json(200, order)
        self.send_json(404, {"error": "not_found"})

    def do_POST(self):
        if self.path not in ("/orders", "/webhooks/delivery"):
            return self.send_json(404, {"error": "not_found"})
        length = int(self.headers.get("Content-Length") or 0)
        try:
            body = json.loads(self.rfile.read(length) or b"null")
        except ValueError:
            body = None
        if self.path == "/webhooks/delivery":
            try:
                if not accept_delivery(body):
                    return self.send_json(400, {"error": "bad_request"})
                return self.send_json(200, {"status": "ok"})
            except psycopg.Error:
                log.exception("POST /webhooks/delivery")
                return self.send_json(503, {"error": "unavailable"})
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
    recover_orders()
    threading.Thread(target=publish_events, daemon=True).start()
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    server.daemon_threads = True
    log.info("orders: слушаю :%d", PORT)
    server.serve_forever()
