# Confluent Cloud API notes for this implementation

Verified against current Confluent Cloud documentation (docs.confluent.io)
as of September 2026:

- **Metrics API** base `https://api.telemetry.confluent.cloud`, discovery
  endpoints `/v2/metrics/cloud/descriptors/resources` and
  `/v2/metrics/cloud/descriptors/metrics`, query endpoint
  `/v2/metrics/cloud/query`. Requires an API key that is **resource-scoped
  for resource management** — a Kafka cluster API key will not
  authenticate against this API.
- **Costs API**: `GET https://api.confluent.cloud/billing/v1/costs?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD`.
  Response is a `CostList` with `data[]` entries carrying `amount`,
  `original_amount`, `discount_amount`, `product`, `line_type`,
  `resource.id`, `resource.environment.id`, `start_date`/`end_date`,
  `granularity`. Requires `OrganizationAdmin` or `BillingAdmin` role.
  **Cost data can lag actual usage by up to 72 hours.**
- **Service Quotas API**: `GET https://api.confluent.cloud/service-quota/v1/applied-quotas?scope={scope}`,
  scopes are `organization`, `environment`, `kafka_cluster`, `network`,
  `user_account`, `service_account`. Discovery endpoint:
  `/service-quota/v1/scopes`. Some quotas expose only `applied_limit`, not
  `usage` — this implementation returns `None` for utilization % in that
  case rather than guessing.
- **Environments**: `GET https://api.confluent.cloud/org/v2/environments`.
- **Kafka clusters**: `GET https://api.confluent.cloud/cmk/v2/clusters?environment={environment_id}`.
- **Connectors**: `GET https://api.confluent.cloud/connect/v1/environments/{env}/clusters/{lkc}/connectors`
  and `.../connectors/{name}/status`.

## Costs API granularity (important, verified Sept 2026)

A single `GET /billing/v1/costs?start_date=A&end_date=B` returns one line
item per (product, line_type, resource) **aggregated over the whole
requested range** — each row's `start_date`/`end_date` echo the request and
`amount` is the range total. The `granularity: DAILY` field refers to
Confluent's internal aggregation, NOT one row per calendar day. See the
documented response example at
https://docs.confluent.io/cloud/current/billing/invoices-and-costs.html

Consequence: a ranged call gives a range TOTAL (and cost-by-resource /
cost-by-product), not a per-day breakdown. The dashboard trend therefore uses
one ranged call per month (monthly totals); per-day breakdowns were dropped
because they cost ~one call per day and tripped the rate limit.

Organizations created before May 15, 2024 receive a different Costs API
response format (see legacy-billing docs). Verify against your org.

## Multi-product cost and resource coverage (Sept 2026)

Cost coverage is product-complete automatically: the Costs API `product`
field carries every billed product (KAFKA, CONNECT, FLINK, TABLEFLOW, KSQL,
STREAM_GOVERNANCE, CLUSTER_LINKING, AUDIT_LOG, KAFKA_REST_PROXY,
SUPPORT_CLOUD, ...), and the dashboard's spend-by-product breakdown groups
by it. The exact product enum isn't hardcoded — unknown values are
prettified for display — so new products appear without code changes. Verify
current product/line_type values at
https://docs.confluent.io/cloud/current/billing/overview.html

Resource inventory (health/idle) added beyond Kafka:
- Flink compute pools: `GET /fcpm/v2/compute-pools?environment={env}` (Cloud
  API key). Verified. Fields used: spec.display_name, spec.cloud/region,
  spec.max_cfu, status.phase (PROVISIONING/PROVISIONED/FAILED/DEPROVISIONING),
  status.current_cfu. An idle rule flags a PROVISIONED pool at 0 current CFUs.

Tableflow: costs are captured via the Costs API product=TABLEFLOW breakdown,
but Tableflow has no simple "list compute resources" endpoint — its API is
catalog integrations (`tableflow/v1/catalog-integrations`) and per-topic
enablement — so there is no per-resource inventory panel for it here. Same
applies to ksqlDB (`ksqldbcm/v2/clusters` exists and would be an easy
follow-on inventory collector if needed).

## Costs API date-range limits (verified Sept 2026)

Per Confluent's docs: `start_date` can be up to **one year in the past**, the
**maximum window per call is one month**, `end_date` is exclusive, and cost
data lags up to ~72h (Confluent recommends a start_date at least 72h old for
accuracy). This code queries at most one calendar month per call, so it stays
within the window. A `400 "start date is too far in the past"` for a month
that IS visible in the Console usually means the Costs API's retrievable
window for that org starts later than the Console invoice view (they're
separate backends). The cost-history service treats a 400 as *unavailable* —
shown as a gap in the trend, never a cached $0 — and does not cache it, so it
recovers if the boundary moves. Use `python -m dashboard.diagnose_costs` to
find the earliest month the Costs API actually serves for your org.

## Rate limits (Costs API in particular)

The Costs API rate limit is low, and a burst of calls returns HTTP 429. How
this system keeps the dashboard from tripping it:
- The **scheduler warms the trend cache** each cycle (get_trailing_months_series),
  so the dashboard serves the spend-trend from the DB. Past months cache as
  final, so steady-state is ~1 live call (the current month). Run the scheduler
  (or `python -m dashboard.sync`) so the dashboard rarely calls the Costs
  API live. A cold load with an empty cache still works — it fetches live,
  spaced, behind the progress bar — but can be slow.
- The client retries 429s with exponential backoff honoring `Retry-After`,
  giving up as `RateLimitError` only after `CONFLUENT_API_MAX_RETRIES`
  (default 5). It spaces all requests by `CONFLUENT_API_MIN_INTERVAL` (default
  0.5s) with a thread-safe throttle shared across the dashboard's single reused
  client, so concurrent requests don't burst.
- The scheduler fetches month-to-date costs in a single ranged call, and warms
  the last 12 months of the trend (one ranged call per month, cached).
- List responses may contain `"data": null` — coerced to an empty list rather
  than crashing.

## Metrics API descriptors and query labels (verified Sept 2026)

Two things this code now gets right (they previously caused "metric not
found" / empty results):
- `GET /v2/metrics/cloud/descriptors/metrics` is **paginated** (page_size up to
  1000, follow the cursor / `links.next`); pass `resource_type` (e.g. `kafka`,
  `flink`, `connector`, `ksql`) to scope the returned descriptions to one
  resource type. Reading only the first page made it look like the Kafka metrics
  were missing (the first page happened to hold Flink descriptors) — that was a
  pagination-handling bug on our side, not a missing parameter.
- The Kafka resource label in query `filter`/`group_by` is `resource.kafka.id`
  (resource_type "kafka"), NOT `resource.kafka_cluster.id`.

Confirmed-current Kafka metric names (GA):
`io.confluent.kafka.server/received_bytes`, `.../sent_bytes`,
`.../retained_bytes`, `.../cluster_load_percent`. The collector resolves
these against the account's descriptors and matches by suffix if the
namespace prefix ever changes; if a load metric isn't exposed for a given
cluster type, utilization is skipped but idle-throughput detection (received
+ sent bytes, GA everywhere) still runs.

## Things I was NOT able to verify and you should confirm before production use

- The exact current Metrics API **metric names** (e.g. whether
  `io.confluent.kafka.server/cluster_load_percent` is still the correct
  name and whether it's GA or PREVIEW for your account) — the code
  resolves these dynamically via `/descriptors/metrics` rather than
  trusting the hardcoded fallback list in
  `collectors/metrics_query_builder.py`, specifically to guard against
  this.
- The exact JSON body schema accepted by `POST /v2/metrics/cloud/query`
  (field names like `aggregations`, `filter`, `group_by`, `intervals`) —
  built from Confluent's published examples but not tested against a live
  account as part of this exercise. **Verify against the live API
  reference and a real query before trusting the utilization/idle-detection
  rules.**
- Whether your organization was created before or after May 15, 2024,
  which changes the Costs API response shape per Confluent's docs.
- The exact audit log event schema/topic naming — this is why
  `collectors/audit_log_collector.py` ships disabled by default.
- Connect API v1 response shape for listing connectors (whether it returns
  bare names or objects) — handled defensively but not confirmed against a
  live cluster.

Per my operating instructions as a Confluent Cloud support-facing
assistant: **I'm not certain about these specific behaviors — please
verify against the official Confluent docs and your own account before
relying on this in production.**
