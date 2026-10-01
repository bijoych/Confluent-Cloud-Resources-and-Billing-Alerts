# POC: Confluent Cloud Resource and Billing Alerting System — Design Doc

_This is the original design document this repo implements. Kept verbatim
for reference; see the top-level README for how the implementation maps
to each section._

## Objective

Build a lightweight alerting platform that monitors Confluent Cloud
resources, usage, quotas, and accrued billing, then sends actionable
alerts through Slack, email, or webhooks.

Confluent Cloud already supports notifications for account, billing,
licensing, and service events through email, Slack, Microsoft Teams, and
generic webhooks. The POC extends this with configurable usage and cost
thresholds.

## Proposed architecture

```
Confluent Cloud
   ├── Metrics API
   ├── Costs / Billing API
   ├── Cloud Management APIs
   ├── Service Quotas API
   └── Audit Logs
          ↓
Collection Service
          ↓
Normalizer + Resource/Owner Metadata
          ↓
Rule Evaluation Engine
          ↓
Alert Deduplication and Suppression
          ↓
Slack | Email | Webhook | Dashboard
```

## Alert categories

| Category | Example alerts |
|---|---|
| Resource lifecycle | Cluster, connector, Flink, topic, or environment created/deleted |
| Resource utilization | High throughput, storage growth, connector task growth, idle resources |
| Capacity and quotas | 50%, 90%, and 100% quota utilization |
| Billing | Monthly spend crosses $100, $500, or $1,000 |
| Forecasting | Projected month-end spend exceeds budget |
| Ownership | Resource has no owner, team, cost center, or service account mapping |
| Security and audit | Privilege changes, API key creation, or unusual resource activity |

Confluent Cloud quota notifications natively use 50%, 90%, and 100%
thresholds, with informational, warning, and critical severities
respectively.

## Data sources

- **Metrics API** — throughput, storage, request, response, connection, and
  principal-level usage.
- **Costs API** — accrued and historical costs, billing dimensions,
  products, quantities, and resources.
- **Cloud APIs** — environments, clusters, connectors, Flink resources,
  service accounts, and ownership metadata.
- **Service Quotas API** — configured limits and current usage.
- **Audit Logs** — resource creation, deletion, access, and administrative
  activity.

A resource-management-scoped API key is required for the Metrics API;
cluster-scoped keys are not sufficient.

## Recommended POC rules

### Billing rules
- Alert when month-to-date spend exceeds a configurable dollar threshold.
- Alert at 50%, 80%, and 100% of the monthly budget.
- Alert when projected month-end spend exceeds the budget.
- Alert when daily spend is more than 2x the trailing seven-day average.
- Alert when a resource incurs cost but has negligible traffic for 24 hours.
- Alert when promotional or committed usage approaches its limit.

Billing accrues hourly, while invoices are generated monthly; marketplace
billing data may also be delayed by 6–24 hours. Therefore, the POC clearly
labels cost alerts as accrued usage rather than final invoice values.

### Resource rules
- Cluster utilization exceeds 80% for 15 minutes.
- Storage utilization or retained bytes increases continuously beyond a
  configured rate.
- Connector is failed or repeatedly restarting.
- Resource remains active but has zero throughput for a defined period.
- New resource is created without an owner or cost-center mapping.
- Resource is deleted unexpectedly.

### Cost allocation rules
For shared Dedicated clusters, associate each application with a service
account and use the `principal_id` metric label to attribute usage. This
is more reliable than allocating costs by topic because topics are shared
resources.

## Minimal implementation

### Components
- **Collector**: Scheduled Python or Go service.
- **Storage**: PostgreSQL, BigQuery, or a Confluent Cloud topic plus sink.
- **Rules engine**: YAML-based rules initially; database-backed rules later.
- **Scheduler**: Kubernetes CronJob, Cloud Run Job, or similar.
- **Notifier**: Slack webhook, email, and generic webhook.
- **Dashboard**: Grafana, Looker, or a simple internal web page.
- **Secrets**: Secret Manager or Vault for API credentials.

### Suggested alert payload

```json
{
  "alert_name": "Monthly budget exceeded",
  "severity": "WARNING",
  "organization_id": "org-123",
  "environment_id": "env-456",
  "resource_id": "lkc-789",
  "owner": "platform-team",
  "current_value": 820.50,
  "threshold": 800.00,
  "unit": "USD",
  "observed_at": "2026-09-11T12:00:00Z",
  "dashboard_url": "https://example.internal/costs/org-123"
}
```

Use immutable resource and principal IDs in alert rules instead of mutable
names.

## POC success criteria

- Collect usage and cost data for one organization and two environments.
- Monitor at least Kafka clusters, connectors, and service quotas.
- Support configurable billing thresholds.
- Deliver alerts to Slack and email.
- Prevent duplicate alerts within a configurable cooldown period.
- Attribute shared-cluster usage to service accounts.
- Provide a daily cost and resource summary.
- Demonstrate alert delivery latency, accuracy, and failure recovery.

## Important limitation

The Metrics API is intended for monitoring and capacity planning, not
exact invoice reconciliation, because metrics may exclude protocol
overhead. Final billing reconciliation should use the Costs API or invoice
data.

## Suggested 2-week plan

| Period | Deliverable |
|---|---|
| Days 1–2 | API access, service account, resource inventory, schema |
| Days 3–5 | Metrics and billing collectors |
| Days 6–7 | Rule engine and alert deduplication |
| Days 8–9 | Slack/email integrations and dashboard |
| Days 10–12 | Resource ownership and principal-level allocation |
| Days 13–14 | Failure testing, documentation, and POC demo |

## Recommended starting scope

Start with billing thresholds, quota thresholds, failed connectors, idle
resources, and Slack notifications. This provides useful cost governance
quickly while avoiding the complexity of full invoice reconciliation and
predictive forecasting.
