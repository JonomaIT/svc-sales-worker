# svc-sales-worker

Sales processor of the JonomaIT lab: consumes the sales created by [`svc-sales-api`](https://github.com/JonomaIT/svc-sales-api) and processes them **only in the ACTIVE region** (ADR-012). Internal app: no public name, nothing on the ALB.

```
SNS "sales-created" us-east-1 ──┬──► SQS "sales-processing" us-east-1 ──► worker us-east-1 (ACTIVE: consumes)
SNS "sales-created" sa-east-1 ──┘                                          │  1. idempotency? (global table)
         (both topics feed both queues)                                    │  2. receipt → S3 (replicated ⇄)
                                ┌──► SQS "sales-processing" sa-east-1     │  3. sale PROCESSED (svc-sales-api's table)
                                └─── worker sa-east-1 (PASSIVE: waits)     │  4. idempotency mark, delete message
```

## Why it survives a region failure

- **Every region's queue gets every sale** (each queue subscribes to the topic of every region). The PASSIVE region keeps its copy untouched for up to 4 days (`retention_days`).
- **Failover**: the platform puts the other region first in `global/dns-cloudflare` (one PR): the load balancer and the region state flip together. The new ACTIVE worker processes its backlog; the **idempotency table** (global, replicated) skips what the old region already did. Nothing is lost, nothing is processed twice.
- **Every step is safe to repeat**: a crash in the middle means a retry. A message that keeps failing goes to the **dead-letter queue** after 5 attempts (`sales-processing-dlq`).
- If the region state cannot be read, the worker does **not** consume: two regions processing at once is the risk to avoid.

## What the platform gives it

Declared in [`deploy/data.hcl`](deploy/data.hcl) (ADR-011); the data runner builds it in both regions and injects:

| Variable | Value |
|---|---|
| `QUEUE_SALES_PROCESSING` | its queue URL in the region, subscribed to `svc-sales-api/sales-created` of every region |
| `TABLE_IDEMPOTENCY` | `jonomait-prd-svc-sales-worker-idempotency` (TTL `expires_at`, 7 days) |
| `BUCKET_RECEIPTS` | `jonomait-prd-svc-sales-worker-receipts-<region>`, replicated both ways |
| `TABLE_SALES` | svc-sales-api's table, declared in `uses` (read-write) |
| `REGION_STATE_PARAMETER` | the region state (ADR-012) |

| Endpoint | Answer |
|---|---|
| `GET /health` | `ok` |
| `GET /info` | region, region state and counters (`processed`, `duplicate`, `gone`, `errors`, `passive_checks`) |

## Pipeline and deploy

Same as `svc-sales-api` (`build-push`; tests run inside the image build; dependencies pinned with hashes). Deploy order: **svc-sales-api data → this app's data → this app in each region** (its queues subscribe to the API's topics, which must exist): deploy `svc-sales-api` first.

**The pipeline deploys** (`deploy-app` of `pipelines`): on `main` when the `AUTO_DEPLOY` org variable is `true` (the lab is up), or by hand in *Actions → CI → Run workflow* (`destroy` removes the app, data last). It runs, in the production GitHub Environment, this app's data and then the app in each region, with the image of the commit. By hand, the data step:

```bash
cd pipelines/terraform-runner/data
terraform init -backend-config=../../../organization/backends/production.tfbackend -backend-config=key=production/cross-region/data/svc-sales-worker
terraform apply -var-file=../../../svc-sales-worker/deploy/data.hcl \
  -var environment=production -var name=svc-sales-worker -var 'regions=["us-east-1","sa-east-1"]'
```

## Locally

```bash
python -m unittest discover -s tests/unit -v
docker build -t svc-sales-worker .
```
