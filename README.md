# svc-sales-worker

Sales processor of the JonomaIT lab: consumes the sales created by [`svc-sales-api`](https://github.com/JonomaIT/svc-sales-api), in every ACTIVE region. Each region processes the sales born in it, and the copies of the other regions wait in a standby queue for a failover (ADR-012, ADR-013). Internal app: no public name, nothing on the ALB.

```
SNS "sales-created" us-east-1 ──┬──► SQS "sales-processing"         us-east-1 ──► worker us-east-1
                                └──► SQS "sales-processing-standby" sa-east-1     │  1. idempotency? (global table)
SNS "sales-created" sa-east-1 ──┬──► SQS "sales-processing"         sa-east-1 ──► worker sa-east-1
                                └──► SQS "sales-processing-standby" us-east-1     │  2. receipt → S3 (replicated ⇄)
                                                                                  │  3. sale PROCESSED (svc-sales-api's table)
                                                                                  │  4. idempotency mark, delete message
```

| Platform | What each worker consumes |
|---|---|
| **active/active** (both regions ACTIVE) | its own `sales-processing`: each sale is processed where it was created (`processed_region` = `created_region`) |
| **active/passive** | the ACTIVE region: its own queue **and** its standby queue (everything); the PASSIVE region: nothing |
| a region taken out of an active/active platform | the remaining ACTIVE region also takes its standby queue: the copies of the region that is out |

## Why it survives a region failure

- **Every region keeps a copy of every sale** (the queue of each region subscribes to the topic of every region; standby puts the other regions' copies in their own queue). Copies wait up to 4 days (`retention_days`).
- **Failover**: the platform takes the region out in `global/dns-cloudflare` (one PR): the load balancer stops sending it traffic and `active-regions` drops it. The remaining worker then works through its standby queue; the **idempotency table** (global, replicated) skips what the region that went out already did. Nothing is lost, nothing is processed twice.
- **Every step is safe to repeat**: a crash in the middle means a retry. A message that keeps failing goes to the **dead-letter queue** after 5 attempts (`sales-processing-dlq`).
- If the region state cannot be read, the worker does **not** consume: two regions processing at once is the risk to avoid. If `active-regions` cannot be read, it counts the other region as out and takes the standby copies too (the idempotency table skips duplicates).

## What the platform gives it

Declared in [`deploy/data.hcl`](deploy/data.hcl) (ADR-011); the data runner builds it in both regions and injects:

| Variable | Value |
|---|---|
| `QUEUE_SALES_PROCESSING` | its queue URL in the region: the sales created in this region |
| `QUEUE_SALES_PROCESSING_STANDBY` | the standby queue URL in the region: the copies of the sales created in the other regions (`standby = true`) |
| `TABLE_IDEMPOTENCY` | `jonomait-prd-svc-sales-worker-idempotency` (TTL `expires_at`, 7 days) |
| `BUCKET_RECEIPTS` | `jonomait-prd-svc-sales-worker-receipts-<region>`, replicated both ways |
| `TABLE_SALES` | svc-sales-api's table, declared in `uses` (read-write) |
| `REGION_STATE_PARAMETER` | the region state (ADR-012); `regions` and `active-regions` are read next to it (ADR-013) |

| Endpoint | Answer |
|---|---|
| `GET /health` | `ok` |
| `GET /info` | region, region state, `takes_standby` and counters (`processed`, `duplicate`, `gone`, `errors`, `passive_checks`, `from_own`, `from_standby`) |

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
