runtime           = "ecs"
name              = "svc-sales-worker"
port              = 8080
health_check_path = "/health"

# Nothing else to set: QUEUE_SALES_PROCESSING, TABLE_IDEMPOTENCY, BUCKET_RECEIPTS and TABLE_SALES
# come from deploy/data.hcl, REGION_STATE_PARAMETER from the platform.
env = {}

slo = {
  availability   = 99.5
  latency_p95_ms = 1000 # from the sale's creation to PROCESSED, in the ACTIVE region
}
