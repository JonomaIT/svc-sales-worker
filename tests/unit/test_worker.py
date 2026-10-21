"""Unit tests: the processing rules, with in-memory doubles instead of AWS. Run in the image build."""

import json
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))
import worker  # noqa: E402

SALE = {"id": "6f1c1f43-8f3e-4a52-9a32-1d2f7c0f0a11", "product": "coffee", "amount": "12.50", "created_at": "2026-10-20T15:04:05+00:00"}


class ConditionalCheckFailed(Exception):
    response = {"Error": {"Code": "ConditionalCheckFailedException"}}


class FakeTable:
    def __init__(self, items=None):
        self.items = dict(items or {})
        self.updates = []

    def get_item(self, Key):  # noqa: N803 (boto3 naming)
        item = self.items.get(Key["id"])
        return {"Item": item} if item else {}

    def put_item(self, Item):  # noqa: N803
        self.items[Item["id"]] = Item

    def update_item(self, Key, **kwargs):  # noqa: N803
        if Key["id"] not in self.items:
            raise ConditionalCheckFailed()
        self.updates.append(kwargs["ExpressionAttributeValues"])
        self.items[Key["id"]]["status"] = kwargs["ExpressionAttributeValues"][":status"]


class FakeS3:
    def __init__(self):
        self.objects = {}

    def put_object(self, Bucket, Key, Body, ContentType):  # noqa: N803
        self.objects[(Bucket, Key)] = json.loads(Body)


class FakeSQS:
    def __init__(self, bodies=()):
        self.messages = [{"MessageId": str(i), "ReceiptHandle": f"r{i}", "Body": b} for i, b in enumerate(bodies)]
        self.deleted, self.receives = [], 0

    def receive_message(self, **kwargs):
        self.receives += 1
        return {"Messages": [m for m in self.messages if m["ReceiptHandle"] not in self.deleted]}

    def delete_message(self, QueueUrl, ReceiptHandle):  # noqa: N803
        self.deleted.append(ReceiptHandle)


class FixedState:
    def __init__(self, value):
        self.value = value

    def get(self):
        return self.value


def make(state="ACTIVE", bodies=(), sales=None, idempotency=None):
    return worker.Worker(FakeSQS(bodies), FakeS3(), FakeTable(sales if sales is not None else {SALE["id"]: dict(SALE, status="PENDING")}),
                         FakeTable(idempotency), FixedState(state), "https://sqs/queue", "receipts-us-east-1", clock=lambda: 1_000)


class Handle(unittest.TestCase):
    def test_new_sale_is_processed(self):
        w = make()
        self.assertEqual(w.handle(json.dumps(SALE)), "processed")
        self.assertEqual(w.sales.items[SALE["id"]]["status"], "PROCESSED")
        self.assertIn(("receipts-us-east-1", f"sales/2026/10/20/{SALE['id']}.json"), w.s3.objects)
        self.assertEqual(w.idempotency.items[SALE["id"]]["expires_at"], 1_000 + 7 * 86400)

    def test_duplicate_is_skipped(self):
        w = make(idempotency={SALE["id"]: {"id": SALE["id"]}})
        self.assertEqual(w.handle(json.dumps(SALE)), "duplicate")
        self.assertEqual((w.s3.objects, w.sales.updates), ({}, []))

    def test_deleted_sale_is_gone_but_marked(self):
        w = make(sales={})
        self.assertEqual(w.handle(json.dumps(SALE)), "gone")
        self.assertIn(SALE["id"], w.idempotency.items, "a retry must not redo it")


class Poll(unittest.TestCase):
    def test_active_region_consumes_and_deletes(self):
        w = make(bodies=[json.dumps(SALE)])
        self.assertEqual(w.poll_once(), 1)
        self.assertEqual((w.sqs.deleted, w.stats["processed"]), (["r0"], 1))

    def test_passive_region_leaves_the_queue_alone(self):
        w = make(state="PASSIVE", bodies=[json.dumps(SALE)])
        self.assertEqual(w.poll_once(), 0)
        self.assertEqual((w.sqs.receives, w.sqs.deleted), (0, []), "the passive copy is the backlog after a failover")

    def test_unknown_state_does_not_consume(self):
        self.assertEqual(make(state="UNKNOWN", bodies=[json.dumps(SALE)]).poll_once(), 0)

    def test_bad_message_stays_for_the_dlq(self):
        w = make(bodies=["not json"])
        w.poll_once()
        self.assertEqual((w.sqs.deleted, w.stats["errors"]), ([], 1))


if __name__ == "__main__":
    unittest.main()
