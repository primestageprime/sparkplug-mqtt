//! What the gate of one edge node guarantees.
//!
//! Each test builds a client over a channel that holds one request and
//! fills that slot first, so the next publish stalls where the client waits
//! for room. That wait is the window where a cancelled call used to drop a
//! number, and where two tasks used to reach the broker out of order.

use super::*;

/// Publish one DDATA metric for `edge_node_id` of the group `PlantFloor`.
///
/// The metric carries no meaning here. These tests read the `seq` of each
/// message, and the count follows the topic alone.
async fn publish_for(client: &SparkplugClient, edge_node_id: &str) -> Result<(), SparkplugError> {
    client
        .publish_metric(
            "PlantFloor",
            edge_node_id,
            "pump_3",
            "temperature",
            MetricValue::Float(1.0),
            0,
        )
        .await
}

/// Read the `seq` of the next request the client has sent.
fn seq_of(requests: &flume::Receiver<rumqttc::Request>) -> u64 {
    match requests
        .try_recv()
        .expect("the client must have sent a request")
    {
        rumqttc::Request::Publish(publish) => crate::payload::decode_payload(&publish.payload)
            .expect("the client must send a payload that decodes")
            .seq
            .expect("every Sparkplug payload carries a seq"),
        other => panic!("expected a publish, got {other:?}"),
    }
}

/// Hand the runtime to every other task until each one stalls again.
///
/// The runtime runs one task at a time, so this test task and the spawned
/// publishes never run at once. That is what fixes the interleaving these
/// tests read.
async fn let_the_publishes_run() {
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn two_tasks_that_publish_for_one_edge_node_reach_the_wire_in_order() {
    let (client, requests) = wired(Identity::Publisher, 1);
    let client = Arc::new(client);

    // Fill the one slot the channel holds. This publish names another edge
    // node, so it moves no count of `edge1`.
    publish_for(&client, "edge2")
        .await
        .expect("the first publish must take the slot the channel holds");

    // Both tasks publish for `edge1`. The first takes the gate of that edge
    // node, draws its number, and stalls on the full channel. The second
    // stalls on the gate, so it draws no number yet.
    let publishes: Vec<_> = (0..2)
        .map(|_| {
            let client = Arc::clone(&client);
            tokio::spawn(async move { publish_for(&client, "edge1").await })
        })
        .collect();
    let_the_publishes_run().await;

    // Drain one request at a time. Each drain frees the slot for the task
    // that holds the gate, and that task then releases the gate.
    let filler = seq_of(&requests);
    let_the_publishes_run().await;
    let first = seq_of(&requests);
    let_the_publishes_run().await;
    let second = seq_of(&requests);

    assert_eq!(filler, 1, "another edge node counts alone");
    assert_eq!(
        (first, second),
        (1, 2),
        "two tasks that publish for one edge node must reach the wire in the \
         order they drew their numbers"
    );

    for publish in publishes {
        publish
            .await
            .expect("the publish task must not panic")
            .expect("the client must take both publishes");
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_cancelled_publish_leaves_no_gap_and_no_count() {
    let (client, requests) = wired(Identity::Publisher, 1);

    publish_for(&client, "edge2")
        .await
        .expect("the first publish must take the slot the channel holds");

    // The channel is full, so this publish draws the number of `edge1` and
    // then waits for room. The deadline drops the future, which cancels the
    // call at that wait.
    let cancelled =
        tokio::time::timeout(Duration::from_millis(50), publish_for(&client, "edge1")).await;

    assert!(
        cancelled.is_err(),
        "a full channel must hold the publish open"
    );
    assert_eq!(
        client.unacked(),
        1,
        "the cancelled publish must release the delivery count it took, so \
         only the publish that filled the channel stays outstanding"
    );

    // Free the slot, then publish for `edge1` again.
    assert_eq!(seq_of(&requests), 1, "the publish that filled the channel");
    publish_for(&client, "edge1")
        .await
        .expect("the publish must be accepted");

    assert_eq!(
        seq_of(&requests),
        1,
        "the cancelled publish must leave its number for the next message"
    );
    assert!(
        requests.try_recv().is_err(),
        "the cancelled publish must reach no wire"
    );
}

#[tokio::test]
async fn a_refused_publish_does_not_move_the_count() {
    let (client, requests) = wired(Identity::Publisher, 1);

    // A client with no receiver refuses every publish.
    drop(requests);

    let refused = publish_for(&client, "edge1").await;
    assert!(
        matches!(refused, Err(SparkplugError::Publish(_))),
        "a client with no receiver must refuse the publish, got: {refused:?}"
    );

    let next = client
        .seq
        .reserve(
            GroupId::new("PlantFloor").expect("group id"),
            EdgeNodeId::new("edge1").expect("edge node id"),
        )
        .await;
    assert_eq!(
        *next, 1,
        "a message the client refused must leave its number for the next one"
    );
    assert_eq!(
        client.unacked(),
        0,
        "a message the client refused must hold no later flush open"
    );
}
