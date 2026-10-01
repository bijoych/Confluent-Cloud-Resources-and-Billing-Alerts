"""Tests for the metrics collector fixes:
  - /descriptors/metrics is scoped by resource_type
  - queries use the resource.kafka.id label (not resource.kafka_cluster.id)
  - metric names resolve by suffix when the namespace prefix drifts
  - collection skips gracefully when a metric isn't in the account
"""
from collectors import metrics_collector as mc


class FakeClient:
    def __init__(self, names):
        self.names = names
        self.queries = []
        self.describe_calls = []

    def describe_metrics(self, resource_type="kafka"):
        self.describe_calls.append(resource_type)
        return {"data": [{"name": n} for n in self.names]}

    def query_metrics(self, q):
        self.queries.append(q)
        return {"data": [{"resource.kafka.id": "lkc-1", "timestamp": "t", "value": 10.0}]}


KAFKA_NAMES = {
    "io.confluent.kafka.server/received_bytes",
    "io.confluent.kafka.server/sent_bytes",
    "io.confluent.kafka.server/retained_bytes",
    "io.confluent.kafka.server/cluster_load_percent",
}


def test_descriptors_are_scoped_to_kafka():
    c = FakeClient(KAFKA_NAMES)
    names = mc.resolve_available_metric_names(c)
    assert c.describe_calls == ["kafka"]
    assert "io.confluent.kafka.server/received_bytes" in names


def test_query_uses_resource_kafka_id_label():
    c = FakeClient(KAFKA_NAMES)
    mc.collect_cluster_utilization(c, ["lkc-1"], available_metrics=KAFKA_NAMES)
    q = c.queries[-1]
    assert q["filter"]["field"] == "resource.kafka.id"
    assert q["group_by"] == ["resource.kafka.id"]


def test_resolve_metric_exact_and_suffix():
    assert mc._resolve_metric(KAFKA_NAMES, "io.confluent.kafka.server/received_bytes") \
        == "io.confluent.kafka.server/received_bytes"
    drift = {"io.confluent.NEWNS/cluster_load_percent"}
    assert mc._resolve_metric(drift, "io.confluent.kafka.server/cluster_load_percent") \
        == "io.confluent.NEWNS/cluster_load_percent"
    assert mc._resolve_metric({"something/else"}, "x/cluster_load_percent") is None


def test_utilization_skips_when_no_load_metric_but_idle_still_runs():
    no_load = {"io.confluent.kafka.server/received_bytes", "io.confluent.kafka.server/sent_bytes"}
    c = FakeClient(no_load)
    assert mc.collect_cluster_utilization(c, ["lkc-1"], available_metrics=no_load) == {}
    totals = mc.collect_throughput_for_idle_check(c, ["lkc-1"], available_metrics=no_load)
    assert totals["lkc-1"] == 20.0  # received + sent, one point each


def test_empty_cluster_list_makes_no_calls():
    c = FakeClient(KAFKA_NAMES)
    assert mc.collect_cluster_utilization(c, [], available_metrics=KAFKA_NAMES) == {}
    assert c.queries == []
