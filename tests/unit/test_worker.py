"""Unit tests: the processing rules, with in-memory doubles instead of AWS. Run in the image build."""

import json
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))
import worker  # noqa: E402

SALE = {"id": "6f1c1f43-8f3e-4a52-9a32-1d2f7c0f0a11", "product": "coffee", "amount": "12.50", "created_at": "2026-10-20T15:04:05+00:00"}
OTHER = {**SALE, "id": "0b5d2c8e-3c51-4f0e-9d0a-5a1d2e3f4a5b", "created_region": "sa-east-1"}
OWN_QUEUE, STANDBY_QUEUE = "https://sqs/queue", "https://sqs/queue-standby"


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
    """Queues by URL: {url: [bodies]}."""

    def __init__(self, queues):
        self.messages = {
            url: [{"MessageId": f"{name}{i}", "ReceiptHandle": f"{name}{i}", "Body": b} for i, b in enumerate(bodies)]
            for name, (url, bodies) in zip(("r", "s"), queues.items())
        }
        self.deleted, self.received_from = [], []

    def receive_message(self, QueueUrl, **kwargs):  # noqa: N803
        self.received_from.append((QueueUrl, kwargs["WaitTimeSeconds"]))
        return {"Messages": [m for m in self.messages.get(QueueUrl, []) if m["ReceiptHandle"] not in self.deleted]}

    def delete_message(self, QueueUrl, ReceiptHandle):  # noqa: N803
        self.deleted.append(ReceiptHandle)


class Fixed:
    def __init__(self, value):
        self.value = value

    def get(self):
        return self.value


def make(state="ACTIVE", bodies=(), standby=None, active="us-east-1", sales=None, idempotency=None):
    """A worker in us-east-1 of a platform running us-east-1 and sa-east-1."""
    queues = {OWN_QUEUE: list(bodies)}
    if standby is not None:
        queues[STANDBY_QUEUE] = list(standby)
    peers = worker.Peers(Fixed("us-east-1,sa-east-1"), Fixed(active))
    return worker.Worker(FakeSQS(queues), FakeS3(), FakeTable(sales if sales is not None else {SALE["id"]: dict(SALE, status="PENDING"), OTHER["id"]: dict(OTHER, status="PENDING")}),
                         FakeTable(idempotency), Fixed(state), OWN_QUEUE, "receipts-us-east-1",
                         standby_url=STANDBY_QUEUE if standby is not None else None, peers=peers, clock=lambda: 1_000)


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

    def test_passive_region_leaves_the_queues_alone(self):
        w = make(state="PASSIVE", bodies=[json.dumps(SALE)], standby=[json.dumps(OTHER)])
        self.assertEqual(w.poll_once(), 0)
        self.assertEqual((w.sqs.received_from, w.sqs.deleted), ([], []), "the passive copy is the backlog after a failover")

    def test_unknown_state_does_not_consume(self):
        self.assertEqual(make(state="UNKNOWN", bodies=[json.dumps(SALE)]).poll_once(), 0)

    def test_bad_message_stays_for_the_dlq(self):
        w = make(bodies=["not json"])
        w.poll_once()
        self.assertEqual((w.sqs.deleted, w.stats["errors"]), ([], 1))


class Standby(unittest.TestCase):
    """ADR-013: the copies of the other regions' sales, only while another region is out."""

    def test_active_active_processes_only_its_own_sales(self):
        w = make(bodies=[json.dumps(SALE)], standby=[json.dumps(OTHER)], active="us-east-1,sa-east-1")
        self.assertEqual(w.poll_once(), 1)
        self.assertEqual(w.sqs.received_from, [(OWN_QUEUE, 20)], "the standby queue is not even read")
        self.assertEqual(w.sales.items[OTHER["id"]]["status"], "PENDING", "sa-east-1 processes its own sale")

    def test_another_region_out_its_copies_are_processed_here(self):
        w = make(bodies=[json.dumps(SALE)], standby=[json.dumps(OTHER)], active="us-east-1")
        self.assertEqual(w.poll_once(), 2)
        self.assertEqual(w.sqs.received_from, [(STANDBY_QUEUE, 0), (OWN_QUEUE, 0)], "standby first, no long poll while there is work")
        self.assertEqual((w.stats["from_standby"], w.stats["from_own"]), (1, 1))

    def test_unknown_active_regions_counts_as_out(self):
        w = make(standby=[json.dumps(OTHER)], active="UNKNOWN")
        self.assertTrue(w.takes_standby(), "a platform without active-regions is active/passive")
        self.assertEqual(w.poll_once(), 1)

    def test_copy_already_processed_by_its_region_is_a_duplicate(self):
        w = make(standby=[json.dumps(OTHER)], active="us-east-1", idempotency={OTHER["id"]: {"id": OTHER["id"]}})
        w.poll_once()
        self.assertEqual((w.stats["duplicate"], w.sqs.deleted), (1, ["s0"]))

    def test_without_a_standby_queue_nothing_changes(self):
        w = make(bodies=[json.dumps(SALE)], active="us-east-1")
        self.assertFalse(w.takes_standby())
        self.assertEqual(w.poll_once(), 1)


class PeersOut(unittest.TestCase):
    def test_out_is_a_region_not_active(self):
        self.assertTrue(worker.Peers(Fixed("us-east-1,sa-east-1"), Fixed("us-east-1")).out())
        self.assertFalse(worker.Peers(Fixed("us-east-1,sa-east-1"), Fixed("sa-east-1,us-east-1")).out())
        self.assertFalse(worker.Peers(Fixed("us-east-1"), Fixed("us-east-1")).out())


class HeartbeatTable:
    def __init__(self, items=None):
        self.items = dict(items or {})
        self.puts = 0

    def get_item(self, Key):  # noqa: N803
        item = self.items.get(Key["region"])
        return {"Item": item} if item else {}

    def put_item(self, Item):  # noqa: N803
        self.items[Item["region"]] = Item
        self.puts += 1


class Clock:
    def __init__(self, now):
        self.now = now

    def __call__(self):
        return self.now


class Heartbeat(unittest.TestCase):
    """A stopped worker: the other region takes its copies by itself, and lets go when it is back."""

    def setUp(self):
        self.clock = Clock(10_000)
        self.table = HeartbeatTable({"sa-east-1": {"region": "sa-east-1", "at": 10_000}})
        self.heartbeats = worker.Heartbeats(self.table, "us-east-1", clock=self.clock)
        self.peers = worker.Peers(Fixed("us-east-1,sa-east-1"), Fixed("us-east-1,sa-east-1"), self.heartbeats)

    def test_a_beating_region_is_in(self):
        self.clock.now += 59
        self.assertEqual(self.peers.out_regions(), [])

    def test_a_silent_region_is_out_and_back_when_it_beats_again(self):
        self.clock.now += 61
        self.assertEqual(self.peers.out_regions(), ["sa-east-1"])
        self.table.put_item({"region": "sa-east-1", "at": self.clock.now})
        self.assertEqual(self.peers.out_regions(), [])

    def test_a_region_that_never_beat_is_out(self):
        self.table.items.clear()
        self.assertTrue(self.peers.out())

    def test_heartbeats_unreadable_count_as_out(self):
        class Broken:
            def get_item(self, Key):  # noqa: N803
                raise RuntimeError("throttled")
        self.peers.heartbeats = worker.Heartbeats(Broken(), "us-east-1", clock=self.clock)
        self.assertEqual(self.peers.out_regions(), ["UNKNOWN"])

    def test_beats_at_most_every_15_seconds(self):
        self.heartbeats.beat()
        self.clock.now += 10
        self.heartbeats.beat()
        self.clock.now += 5
        self.heartbeats.beat()
        self.assertEqual((self.table.puts, self.table.items["us-east-1"]["at"]), (2, 10_015))

    def test_only_an_active_worker_beats_and_a_stopped_peer_hands_over_its_copies(self):
        w = make(standby=[json.dumps(OTHER)], active="us-east-1,sa-east-1")
        w.peers = self.peers
        self.clock.now += 120  # sa-east-1's worker stopped two minutes ago
        self.assertEqual(w.poll_once(), 1, "the copy of the sale born in sa-east-1 is processed here")
        self.assertIn("us-east-1", self.table.items)

        passive = make(state="PASSIVE", active="us-east-1,sa-east-1")
        passive.peers = worker.Peers(Fixed("us-east-1,sa-east-1"), Fixed("us-east-1"), worker.Heartbeats(HeartbeatTable(), "sa-east-1", clock=self.clock))
        passive.poll_once()
        self.assertEqual(passive.peers.heartbeats.table.puts, 0, "a PASSIVE worker does not consume, so it does not beat")


if __name__ == "__main__":
    unittest.main()
