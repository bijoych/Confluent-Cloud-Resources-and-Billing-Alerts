# Confluent Cloud metrics & cost access — pain points

## Background

I'm a Technical Support Engineer on Confluent Cloud. This started with a
recurring customer question: across Intercom we keep getting the same ask —
*how do I generate alerts when my cost or resource usage crosses a threshold?*
Confluent Cloud's built-in notifications cover account, billing, licensing, and
service events, but customers consistently want **configurable** alerts on
their own cost and usage thresholds: a monthly-budget alert, a projected-overage
alert, a quota-nearing alert, a failed-connector or idle-resource alert, and so
on.

To understand what it takes to answer that well — and whether customers could
reasonably build it themselves — I built a small proof of concept: a lightweight
alerting service that pulls from Confluent Cloud's Metrics, Costs, management,
Service Quotas, and Audit Log APIs, evaluates configurable thresholds, and
delivers alerts to Slack, email, and webhooks, with a dashboard for cost trends
and resource health.

While building it I hit a number of friction points accessing metrics and cost
data through the APIs. This document captures them so they can be reviewed,
confirmed, and where appropriate fixed or documented.

## How to read this

Prepared to share with Confluent. Every item is one of two kinds, kept
strictly separate:

- **[DOCUMENTED]** — stated in Confluent's own documentation; the source URL is
  given so it can be checked directly.
- **[OBSERVED]** — reproducible behavior we saw in our own environment during
  this build, with the evidence (log/command output) included. These are NOT
  claimed to be documented; they are raised for Confluent to confirm, fix, or
  document.

Nothing in this document is inference or gap-filling. Items we previously
believed but could not prove are listed at the end under "Removed".

Verified: 2026-09-19. Docs are versioned as `.../cloud/current/...`, so exact
wording may shift over time — the URLs are the source of truth.

---

## A. Documented behaviors (with sources)

### Costs API (`GET /billing/v1/costs`)

1. **[DOCUMENTED]** Query constraints: start date can be up to one year in the
   past; one month is the maximum window between start and end dates; start
   date is inclusive and end date exclusive; cost data can take up to 72 hours
   to become available, and Confluent recommends a start_date at least 72 hours
   in the past.
   - https://docs.confluent.io/cloud/current/billing/invoices-and-costs.html

2. **[DOCUMENTED]** Requires the OrganizationAdmin or BillingAdmin role.
   - https://docs.confluent.io/cloud/current/billing/invoices-and-costs.html

3. **[DOCUMENTED]** A response row reflects "the range of dates from your
   request and total costs" — i.e. a ranged call returns totals aggregated over
   the requested window, with a `granularity: DAILY` field, not one row per
   calendar day. The CLI equivalent is described as "daily aggregated costs".
   - https://docs.confluent.io/cloud/current/billing/invoices-and-costs.html
   - https://docs.confluent.io/cloud/current/billing/legacy-billing.html
   - https://docs.confluent.io/confluent-cli/current/command-reference/billing/cost/confluent_billing_cost_list.html
   - Impact for us: to build a genuine per-day series we had to issue one call
     per day (~28–31/month), which is the root of our rate-limit problems (B1).

4. **[DOCUMENTED]** The Costs API response format differs for organizations
   created before vs on/after May 15, 2024. This is not just cosmetic: in the
   legacy format `resource.environment` is a string and the environment object
   is at the top level (`environment.id`), whereas in the current format the
   environment is an object nested under `resource` (`resource.environment.id`).
   A client written against the documented current format therefore breaks on a
   pre-2024-05-15 org (in our POC, `resource.environment.id` raised
   `'str' object has no attribute ...` and prevented cost-snapshot persistence
   until we handled both shapes). There is no version parameter to opt into a
   single stable shape.
   - https://docs.confluent.io/cloud/current/billing/legacy-billing.html
   - https://docs.confluent.io/cloud/current/billing/invoices-and-costs.html

5. **[DOCUMENTED]** List responses paginate via a `metadata` object
   (`first`/`last`/`prev`/`next`) using an opaque `page_token`.
   - https://docs.confluent.io/cloud/current/billing/legacy-billing.html
   - https://confluent.cloud/api/docs (pagination: page_size / page_token)

### Metrics API (`api.telemetry.confluent.cloud`)

6. **[DOCUMENTED]** Requires an API key whose resource scope is
   "Cloud resource management"; an API key scoped to a Kafka cluster causes an
   authentication error. Authorization requires the MetricsViewer role.
   - https://docs.confluent.io/cloud/current/monitoring/metrics-api.html
   - https://docs.confluent.io/cloud/current/monitoring/monitor-faq.html
   - https://docs.confluent.io/cloud/current/security/access-control/rbac/predefined-rbac-roles.html

7. **[DOCUMENTED]** Query labels are prefixed: resource labels are
   `resource.<resource-type>.<label>`, for example `resource.kafka.id` (Kafka
   clusters), `resource.connector.id`, `resource.compute_pool.id`.
   - https://api.telemetry.confluent.cloud/docs
   - https://docs.confluent.io/cloud/current/client-apps/deprecate-how-to.html (query example uses resource.kafka.id)
   - Note: the label is `resource.kafka.id`, not `resource.kafka_cluster.id`;
     an incorrect label returns empty results with no error.

8. **[DOCUMENTED]** Throttling: "Users who send many requests in quick
   succession or perform too many concurrent operations may be throttled or
   have their request rejected with an error."
   - https://api.telemetry.confluent.cloud/docs

9. **[DOCUMENTED]** Endpoints are paginated (cursor-based; `page_size` 1–1000,
   default 100), including `/descriptors` and `/query`.
   - https://api.telemetry.confluent.cloud/docs
   - https://api.telemetry.confluent.cloud/docs/api.yaml (OpenAPI spec)

10. **[DOCUMENTED]** A metric's lifecycle stage (e.g. preview / generally
    available) is found in the `/descriptors/metrics` response, and while a
    metric is in preview its labels can change in breaking ways without an API
    version change.
    - https://docs.confluent.io/cloud/current/monitoring/monitor-faq.html

11. **[DOCUMENTED]** For alerting, use immutable ID labels (Resource ID,
    Principal ID), not mutable name labels — names are enriched metadata added
    later and are not recommended for alerts.
    - https://docs.confluent.io/cloud/current/monitoring/monitor-faq.html

### Cost allocation / attribution

12. **[DOCUMENTED]** Some products, such as Tableflow, do not emit usage tagged
    with a principal; their full cost appears in the unallocated row. Per-topic
    and user-defined-tag attribution are not supported (cost allocation export).
    - https://docs.confluent.io/cloud/current/billing/cost-allocation.html

13. **[DOCUMENTED]** Cost allocation export: data can lag actual usage by up to
    48 hours; historical data is available only from the date you enabled cost
    allocation; the feature has no SLA during Early Access.
    - https://docs.confluent.io/cloud/current/billing/cost-allocation.html

### Audit logs (creator/ownership source)

14. **[DOCUMENTED]** Audit log records are retained for seven days on an
    independent cluster and are auto-deleted after that; consuming them requires
    an API key specific to the audit log cluster. To keep them longer you must
    replicate/export them yourself.
    - https://docs.confluent.io/cloud/current/monitoring/audit-logging/retain-audit-logs.html
    - https://docs.confluent.io/cloud/current/monitoring/audit-logging/cloud-audit-log-concepts.html
    - https://docs.confluent.io/cloud/current/monitoring/audit-logging/configure.html

### Fragmentation (cross-cutting)

15. **[DOCUMENTED]** The data spans separate API surfaces with different base
    hosts, auth, and pagination conventions: Costs/management APIs on
    `api.confluent.cloud` (metadata-object pagination), the Metrics API on
    `api.telemetry.confluent.cloud` (cursor pagination, throttling, distinct
    label scheme), and Audit Logs consumed as a Kafka topic on a separate
    cluster. Sources: the per-API pages linked above.

---

## B. Observed in our environment (evidence included; NOT claimed as documented)

These are reproducible facts from our own runs. We ask Confluent to confirm
whether each is expected, and to document or fix as appropriate.

### B1. HTTP 429 on the Costs API with a low threshold
A short sequence of monthly/daily `GET /billing/v1/costs` calls returned 429
with multi-second Retry-After values. (Metrics API throttling is documented —
A8 — but we hit this on the billing endpoint.)

Evidence (scheduler log):
```
WARNING collectors.confluent_client: Rate limited by Confluent Cloud API:
  https://api.confluent.cloud/billing/v1/costs — retrying in 53.0s (attempt 1/5)
ConfluentApiError: 429 ... {"errors":[{"status":"429","detail":"Exceeded rate limit"}]}
```
Ask: what is the documented rate limit for `/billing/v1/costs`, and can it be
published so clients can pace correctly?

### B2. Transient 400 "start date is too far in the past" for an in-range month
During the 429 episode, a month ~5 months back returned a 400 "start date is
too far in the past" — yet a clean, spaced sequential run of the same 12 months
(including that month) all succeeded. Documented limit is one year (A1), so the
400 appears erroneous/transient under load.

Evidence:
```
# during load:
ConfluentApiError: 400 ... {"detail":"start date is too far in the past"}  # for 2026-04

# clean sequential probe (one call/day-of-month spaced), same org, same creds:
2026-04   2026-04-01   OK  $1,326.11  (174 line items)
2026-05   2026-05-01   OK  $2,502.67  (205 line items)
... all 12 months returned OK ...
```
Ask: is a 400 "too far in the past" ever expected for a date inside the
one-year window? It looks like a mislabeled throttling/transient error.

### B3. List response body contained `"data": null`
A management list response returned `"data": null` (rather than `[]` or an
absent key), which broke naive iteration.

Evidence (stack trace):
```
File ".../confluent_client.py", in _cloud_get_paginated
    items.extend(payload.get("data", []))
TypeError: 'NoneType' object is not iterable
# response body had "data": null  (endpoint: /cmk/v2/clusters)
```
Ask: is `"data": null` an expected representation for an empty list? If not,
it should return `[]`.

### B4. No creating-principal on resource objects
The `cmk/v2` cluster and `fcpm/v2` compute-pool responses we received exposed
`metadata.created_at` / `updated_at` but no field identifying the principal that
created the resource. The only documented source of "who did what" is the audit
log (A14), which is retained only seven days. This made reliable
"who created this cluster/pool" attribution impractical without standing audit
log ingestion.
Ask: can a creator/owner principal be exposed on resource objects directly?

---

## Removed (could not be proven / was disproven)

Kept out of the record deliberately, so nothing here is inference:

- **"/descriptors/metrics returns a partial set unless resource_type is supplied"**
  — WITHDRAWN. The endpoint is paginated (A9); the Flink-only result we first saw
  was a first-page pagination artifact in our client, not missing data or a
  missing parameter. `resource_type` scopes which metric descriptions are
  returned; it is not required to avoid a partial set.
- **Costs-API-vs-Console retrievable-window mismatch** — DISPROVEN by our own
  probe (B2 evidence): all 12 months were retrievable via the API.
- **Service Quotas expose only limits, not usage** — not re-verified against
  docs this pass; omitted until confirmed.
- **Metrics API ~7-day data retention** — only a third-party source was found,
  not a Confluent doc; omitted until confirmed from Confluent.
- **Connector list returns names vs objects across versions** — handled
  defensively in code but not confirmed in docs; omitted.
