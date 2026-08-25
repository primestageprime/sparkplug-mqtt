//! What metric each publish path writes on the wire.
//!
//! Three paths build a metric: one metric for an edge node the caller
//! names, one metric for a topic the caller built, and a batch. All three
//! call `payload_helpers::create_metric`, and these tests read what the
//! client actually sends. A path that stops calling that builder, or that
//! writes another datatype, another name or another instant, fails here.

use super::*;

const NAME: &str = "temperature";
const AT: u64 = 1_700_000_000_000;
const VALUE: f64 = 23.5;

/// Read the one metric of the one publish the client sent.
fn metric_sent(requests: &flume::Receiver<rumqttc::Request>) -> crate::Metric {
    let mut metrics = crate::payload::decode_payload(&sent(requests).payload)
        .expect("the client must send a payload that decodes")
        .metrics;

    assert_eq!(metrics.len(), 1, "each path here sends one metric");
    metrics.remove(0)
}

/// What [`SparkplugClient::publish_metric`] puts on the wire.
async fn from_named_edge_node() -> crate::Metric {
    let (client, requests) = wired_publisher();
    client
        .publish_metric(
            "PlantFloor",
            "edge_node_1",
            "pump_3",
            NAME,
            MetricValue::Float(VALUE),
            AT,
        )
        .await
        .expect("the publish must be accepted");

    metric_sent(&requests)
}

/// What [`SparkplugClient::publish_metric_to`] puts on the wire.
async fn from_built_topic() -> crate::Metric {
    let (client, requests) = wired_publisher();
    client
        .publish_metric_to(&ddata_topic(&client), NAME, MetricValue::Float(VALUE), AT)
        .await
        .expect("the publish must be accepted");

    metric_sent(&requests)
}

/// What [`SparkplugClient::publish_metrics_to`] puts on the wire.
async fn from_batch() -> crate::Metric {
    let (client, requests) = wired_publisher();
    client
        .publish_metrics_to(
            &ddata_topic(&client),
            vec![(NAME.to_string(), MetricValue::Float(VALUE), AT)],
        )
        .await
        .expect("the publish must be accepted");

    metric_sent(&requests)
}

#[tokio::test]
async fn every_publish_path_writes_the_same_metric() {
    // The three paths each held their own `Metric` literal. The literals
    // agreed field for field, and this test holds them together now that
    // one builder serves all three.
    let named = from_named_edge_node().await;
    let built = from_built_topic().await;
    let batched = from_batch().await;

    assert_eq!(named, built);
    assert_eq!(named, batched);
}

#[tokio::test]
async fn the_metric_on_the_wire_carries_the_name_datatype_and_instant() {
    // The equality test above passes if every path writes the same wrong
    // metric. This states the fields the wire must carry.
    let metric = from_named_edge_node().await;

    assert_eq!(metric.name.as_deref(), Some(NAME));
    assert_eq!(metric.datatype, Some(crate::DataType::Double.code()));
    assert_eq!(metric.timestamp, Some(AT));
    assert_eq!(
        metric.value,
        Some(crate::payload::payload::metric::Value::DoubleValue(VALUE))
    );
}
