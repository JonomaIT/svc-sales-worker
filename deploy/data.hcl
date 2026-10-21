# What svc-sales-worker keeps and uses (ADR-011). The data runner builds it in every region of the
# app, apart from the app itself; deploy svc-sales-api's data first (its topic and table must exist).

queues = {
  sales-processing = {
    # svc-sales-api's topic of EVERY region: each region's queue holds every sale, so the region that
    # becomes ACTIVE after a failover already has the backlog.
    subscribe          = ["svc-sales-api/sales-created"]
    visibility_timeout = 60 # longer than processing one batch
    max_receive_count  = 5  # then the dead-letter queue
  }
}

tables = {
  # Sales already processed, by any region (global table); entries expire after 7 days.
  idempotency = { hash_key = "id", ttl_attribute = "expires_at" }
}

buckets = {
  receipts = {} # one receipt per sale, replicated both ways
}

uses = {
  "svc-sales-api/table/sales" = "read-write" # marks each sale PROCESSED
}
