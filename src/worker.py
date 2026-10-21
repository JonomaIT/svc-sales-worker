"""svc-sales-worker: processes the sales of the JonomaIT lab.

It consumes its sales-processing queue, which receives every sale published by svc-sales-api in
ANY region (fan-out across regions). It only consumes while its region is ACTIVE (ADR-012): a
PASSIVE region leaves its copy of the messages in the queue, so after a failover (the platform
flips the region state) it works through the backlog, and the idempotency table (global) skips
what the other region already did. Nothing is lost while a region is down.

For each sale, in this order, every step safe to repeat:
  1. already in the idempotency table?       -> duplicate, nothing to do
  2. receipt in the region's bucket            (replicated to the other region)
  3. sale marked PROCESSED in svc-sales-api's table (declared in uses)
  4. idempotency mark (expires after 7 days)
  5. message deleted
A crash in the middle only means a retry. A message that keeps failing (e.g. not JSON) moves to the
dead-letter queue after max_receive_count attempts.

  GET /health -> 200 "ok" (container health)
  GET /info   -> region, region state and counters

Everything arrives as environment variables: QUEUE_SALES_PROCESSING, TABLE_IDEMPOTENCY,
BUCKET_RECEIPTS, TABLE_SALES (data runner, ADR-011), REGION_STATE_PARAMETER (platform), AWS_REGION.
"""

import json
import os
import signal
import threading
import time
from collections import Counter
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(os.environ.get("PORT", "8080"))
REGION = os.environ.get("AWS_REGION", "unknown")
PASSIVE_WAIT = 15  # seconds between region state checks while PASSIVE
IDEMPOTENCY_TTL = 7 * 86400


def log(**fields):
    """One JSON line on stdout, read by CloudWatch Logs."""
    print(json.dumps({"time": now_iso(), "region": REGION, **fields}, default=str), flush=True)


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class RegionState:
    """ACTIVE or PASSIVE (ADR-012), read from the platform's parameter, cached for a few seconds.
    When it cannot be read the worker stays put: processing in two regions at once is the risk."""

    def __init__(self, ssm, name, ttl=15):
        self.ssm, self.name, self.ttl = ssm, name, ttl
        self.value, self.read_at = "UNKNOWN", None

    def get(self):
        if self.read_at is None or time.monotonic() - self.read_at > self.ttl:
            try:
                self.value = self.ssm.get_parameter(Name=self.name)["Parameter"]["Value"]
                self.read_at = time.monotonic()
            except Exception as err:
                log(msg="region state unavailable", error=str(err))
        return self.value


class Worker:
    def __init__(self, sqs, s3, sales, idempotency, state, queue_url, bucket, clock=time.time):
        self.sqs, self.s3, self.sales, self.idempotency, self.state = sqs, s3, sales, idempotency, state
        self.queue_url, self.bucket, self.clock = queue_url, bucket, clock
        self.stats = Counter()

    def handle(self, body):
        """Processes one sale; returns processed, duplicate or gone (deleted before processing)."""
        sale = json.loads(body)
        sale_id = sale["id"]
        if self.idempotency.get_item(Key={"id": sale_id}).get("Item"):
            return "duplicate"

        processed_at = now_iso()
        key = f"sales/{sale.get('created_at', processed_at)[:10].replace('-', '/')}/{sale_id}.json"
        receipt = {**sale, "status": "PROCESSED", "processed_at": processed_at, "processed_region": REGION}
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=json.dumps(receipt).encode(), ContentType="application/json")

        outcome = "processed"
        try:
            self.sales.update_item(
                Key={"id": sale_id},
                UpdateExpression="SET #status = :status, processed_at = :at, processed_region = :region, receipt_key = :key",
                ConditionExpression="attribute_exists(id)",
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues={":status": "PROCESSED", ":at": processed_at, ":region": REGION, ":key": key},
            )
        except Exception as err:
            if getattr(err, "response", {}).get("Error", {}).get("Code") != "ConditionalCheckFailedException":
                raise
            outcome = "gone"

        self.idempotency.put_item(Item={"id": sale_id, "processed_at": processed_at, "region": REGION, "expires_at": int(self.clock()) + IDEMPOTENCY_TTL})
        return outcome

    def poll_once(self):
        """One long poll (up to 20 s); only in the ACTIVE region. Returns the messages handled."""
        if self.state.get() != "ACTIVE":
            self.stats["passive_checks"] += 1
            return 0
        messages = self.sqs.receive_message(QueueUrl=self.queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=20).get("Messages", [])
        for msg in messages:
            try:
                outcome = self.handle(msg["Body"])
            except Exception as err:
                # Left in the queue: visible again after the visibility timeout, then the DLQ.
                self.stats["errors"] += 1
                log(msg="sale not processed", message_id=msg.get("MessageId"), error=str(err))
                continue
            self.sqs.delete_message(QueueUrl=self.queue_url, ReceiptHandle=msg["ReceiptHandle"])
            self.stats[outcome] += 1
            log(msg=f"sale {outcome}", message_id=msg.get("MessageId"))
        return len(messages)

    def run(self, stop):
        last_state = None
        while not stop.is_set():
            state = self.state.get()
            if state != last_state:
                log(msg="region state", state=state, consuming=state == "ACTIVE")
                last_state = state
            if state != "ACTIVE":
                stop.wait(PASSIVE_WAIT)
                continue
            try:
                self.poll_once()
            except Exception as err:
                log(msg="queue unavailable", error=str(err))
                stop.wait(5)


WORKER = None


class Handler(BaseHTTPRequestHandler):
    server_version = "svc-sales-worker"
    sys_version = ""

    def do_GET(self):  # noqa: N802 (http.server naming)
        if self.path == "/health":
            body, ctype = b"ok\n", "text/plain"
        elif self.path == "/info":
            body = json.dumps({"service": "svc-sales-worker", "region": REGION, "region_state": WORKER.state.get(), "stats": dict(WORKER.stats)}, indent=2).encode()
            ctype = "application/json"
        else:
            body, ctype = b'{"error": "not found"}', "application/json"
        self.send_response(200 if body != b'{"error": "not found"}' else 404)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        pass  # health checks only


def main():
    global WORKER
    import boto3
    from botocore.config import Config

    config = Config(connect_timeout=2, read_timeout=25, retries={"mode": "standard", "max_attempts": 3})
    dynamodb = boto3.resource("dynamodb", config=config)
    WORKER = Worker(
        sqs=boto3.client("sqs", config=config),
        s3=boto3.client("s3", config=config),
        sales=dynamodb.Table(os.environ["TABLE_SALES"]),
        idempotency=dynamodb.Table(os.environ["TABLE_IDEMPOTENCY"]),
        state=RegionState(boto3.client("ssm", config=config), os.environ["REGION_STATE_PARAMETER"]),
        queue_url=os.environ["QUEUE_SALES_PROCESSING"],
        bucket=os.environ["BUCKET_RECEIPTS"],
    )

    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()

    # ECS stops a task with SIGTERM: finish the current batch, then exit.
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    log(msg="started", queue=WORKER.queue_url)
    WORKER.run(stop)
    server.shutdown()
    log(msg="stopped", stats=dict(WORKER.stats))


if __name__ == "__main__":
    main()
