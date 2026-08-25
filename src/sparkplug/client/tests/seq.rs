//! What `seq` the client writes on the wire.
//!
//! These drive the real publish path through a channel the test owns, so
//! they read the count each message actually carries.

use super::*;

/// Read the `seq` of every publish the client has sent so far.
fn seq_counts(requests: &flume::Receiver<rumqttc::Request>) -> Vec<u64> {
    requests
        .try_iter()
        .map(|request| match request {
            rumqttc::Request::Publish(publish) => crate::payload::decode_payload(&publish.payload)
                .expect("the client must send a payload that decodes")
                .seq
                .expect("every Sparkplug payload carries a seq"),
            other => panic!("expected a publish, got {other:?}"),
        })
        .collect()
}

/// Publish one DDATA metric on behalf of one edge node, and expect the
/// client to accept it.
///
/// The metric carries no meaning here. These tests read the `seq` of each
/// message, and the count follows the topic alone.
async fn publish_one(client: &SparkplugClient, group_id: &str, edge_node_id: &str) {
    client
        .publish_metric(
            group_id,
            edge_node_id,
            "pump_3",
            "temperature",
            MetricValue::Float(1.0),
            0,
        )
        .await
        .expect("the publish must be accepted");
}

#[tokio::test]
async fn nbirth_sets_the_count_back_to_zero() {
    // `publish_birth` announces the edge node the client took at connect,
    // so only an edge node client can call it.
    let (client, requests) = wired_edge_node(edge_node_named("PlantFloor", "edge1"));

    publish_one(&client, "PlantFloor", "edge1").await;
    publish_one(&client, "PlantFloor", "edge1").await;

    client
        .publish_birth(DeviceId::new("pump_3").expect("device id"))
        .await
        .expect("the births must be accepted");

    publish_one(&client, "PlantFloor", "edge1").await;

    // Two DDATA, then the NBIRTH and the DBIRTH, then one DDATA. The
    // NBIRTH sets the count back to 0, and each message after it adds 1.
    assert_eq!(seq_counts(&requests), vec![1, 2, 0, 1, 2]);
}

#[tokio::test]
async fn an_nbirth_on_a_caller_built_topic_sets_the_count_back_to_zero() {
    let (client, requests) = wired_publisher();

    publish_one(&client, "PlantFloor", "edge1").await;
    publish_one(&client, "PlantFloor", "edge1").await;

    // The generic publish path takes any topic, so a caller can build the
    // NBIRTH itself. The count reads the message type of the topic, so this
    // route restarts the count the same as `publish_birth` does.
    let nbirth = client.namespace().nbirth(
        GroupId::new("PlantFloor").expect("group id"),
        EdgeNodeId::new("edge1").expect("edge node id"),
    );
    client
        .publish_metric_to(&nbirth, "Node Control/Rebirth", MetricValue::Bool(false), 0)
        .await
        .expect("the NBIRTH must be accepted");

    publish_one(&client, "PlantFloor", "edge1").await;

    assert_eq!(
        seq_counts(&requests),
        vec![1, 2, 0, 1],
        "an NBIRTH through the generic path must set the count back to 0"
    );
}

#[tokio::test]
async fn two_edge_nodes_keep_separate_counts() {
    let (client, requests) = wired_publisher();

    for edge_node_id in ["edge1", "edge2", "edge1", "edge2"] {
        publish_one(&client, "PlantFloor", edge_node_id).await;
    }

    // One client publishes for many edge nodes, and each edge node keeps
    // its own count.
    assert_eq!(seq_counts(&requests), vec![1, 1, 2, 2]);
}

#[tokio::test]
async fn one_edge_node_id_in_two_groups_keeps_separate_counts() {
    let (client, requests) = wired_publisher();

    for group_id in ["PlantFloor", "Boiler", "PlantFloor", "Boiler"] {
        publish_one(&client, group_id, "edge1").await;
    }

    // Sparkplug names an edge node by the pair, so `PlantFloor/edge1` and
    // `Boiler/edge1` are two edge nodes. One count each, or a host
    // application reads a gap in both streams and asks for a rebirth.
    assert_eq!(seq_counts(&requests), vec![1, 1, 2, 2]);
}

#[tokio::test]
async fn a_metric_on_a_host_topic_is_refused() {
    let (client, requests) = wired_publisher();
    let topic = client
        .namespace()
        .state(HostId::new("scada_1").expect("host id"));

    let sent = client
        .publish_metric_to(&topic, "temperature", MetricValue::Float(1.0), 0)
        .await;

    assert!(
        matches!(sent, Err(SparkplugError::InvalidTopic(_))),
        "a host topic names no edge node, so no seq count can stamp the metric"
    );
    assert!(
        seq_counts(&requests).is_empty(),
        "a refused publish must reach no wire"
    );
}

#[tokio::test]
async fn a_birth_and_the_data_for_that_edge_node_share_one_count() {
    // The defect this closes: the births named the edge node of the
    // connection, and the data named the edge node of the call. Where the
    // two differed, they counted apart, and a host application read a gap in
    // both streams. One identity now feeds both.
    let (client, requests) = wired_edge_node(edge_node_named("PlantFloor", "edge1"));

    client
        .publish_birth(DeviceId::new("pump_3").expect("device id"))
        .await
        .expect("the births must be accepted");

    publish_one(&client, "PlantFloor", "edge1").await;

    // The NBIRTH sets the count of `PlantFloor/edge1` back to 0, the DBIRTH
    // beside it carries 1, and the DDATA for that edge node carries 2.
    assert_eq!(
        seq_counts(&requests),
        vec![0, 1, 2],
        "the births and the data of one edge node must draw from one count"
    );
}
