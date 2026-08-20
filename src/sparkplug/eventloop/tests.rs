//! Unit tests for [`super`].
//!
//! The classification helpers are tested directly. The loop itself is tested
//! through a scripted [`EventSource`], which is the second adapter that
//! makes the seam real.
//!
//! `rumqttc::ConnectionError` is large, and these helpers hand it back the
//! way the real event loop does, so the size lint has nothing to act on.
#![allow(clippy::result_large_err)]

use std::collections::VecDeque;

use super::*;

// ---------------------------------------------------------------------------
// The scripted adapter
// ---------------------------------------------------------------------------

/// An [`EventSource`] that replays a fixed script, then parks forever.
///
/// Parking rather than ending matches the real loop: `rumqttc` keeps
/// reconnecting, so the loop never runs out of events. A test aborts the
/// task when it has seen what it needs.
struct ScriptedSource(VecDeque<Result<rumqttc::Event, rumqttc::ConnectionError>>);

impl ScriptedSource {
    fn new(
        events: impl IntoIterator<Item = Result<rumqttc::Event, rumqttc::ConnectionError>>,
    ) -> Self {
        Self(events.into_iter().collect())
    }
}

impl EventSource for ScriptedSource {
    async fn poll(&mut self) -> Result<rumqttc::Event, rumqttc::ConnectionError> {
        match self.0.pop_front() {
            Some(event) => event,
            None => std::future::pending().await,
        }
    }
}

fn connack() -> Result<rumqttc::Event, rumqttc::ConnectionError> {
    Ok(rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(
        rumqttc::ConnAck::new(rumqttc::ConnectReturnCode::Success, false),
    )))
}

fn puback() -> Result<rumqttc::Event, rumqttc::ConnectionError> {
    Ok(rumqttc::Event::Incoming(rumqttc::Packet::PubAck(
        rumqttc::PubAck::new(1),
    )))
}

fn poll_error() -> Result<rumqttc::Event, rumqttc::ConnectionError> {
    Err(rumqttc::ConnectionError::RequestsDone)
}

/// Start the loop over a script and hand back what a test needs to watch it.
struct Harness {
    health: tokio::sync::watch::Receiver<Health>,
    ready: oneshot::Receiver<()>,
    task: tokio::task::JoinHandle<()>,
}

impl Harness {
    fn start(
        events: impl IntoIterator<Item = Result<rumqttc::Event, rumqttc::ConnectionError>>,
        delivery: Arc<DeliveryTracker>,
    ) -> Self {
        let (health_tx, health) = tokio::sync::watch::channel(Health::Disconnected);
        let (ready_tx, ready) = oneshot::channel();
        let task = tokio::spawn(run(
            ScriptedSource::new(events),
            delivery,
            health_tx,
            ready_tx,
        ));
        Self {
            health,
            ready,
            task,
        }
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.task.abort();
    }
}

// ---------------------------------------------------------------------------
// The loop
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_first_connack_signals_that_connect_may_return() {
    let mut h = Harness::start([connack()], Arc::new(DeliveryTracker::new()));
    (&mut h.ready)
        .await
        .expect("a ConnAck must release the waiting connect");
}

#[tokio::test(start_paused = true)]
async fn a_poll_error_before_the_first_connack_still_releases_connect() {
    // The error arrives first. The loop must back off and keep polling
    // rather than consume the ready signal or give up.
    let mut h = Harness::start([poll_error(), connack()], Arc::new(DeliveryTracker::new()));
    (&mut h.ready)
        .await
        .expect("an error before the ConnAck must not strand connect");
}

#[tokio::test(start_paused = true)]
async fn the_loop_keeps_polling_after_an_error() {
    let mut h = Harness::start([poll_error(), connack()], Arc::new(DeliveryTracker::new()));

    h.health
        .wait_for(|state| *state == Health::Connected)
        .await
        .expect("the loop must reconnect rather than stop at the error");
}

#[tokio::test]
async fn a_puback_the_loop_polls_clears_the_publish() {
    let delivery = Arc::new(DeliveryTracker::new());
    let _ticket = delivery.record_publish();

    let _h = Harness::start([puback()], delivery.clone());

    delivery
        .flush(Duration::from_secs(1))
        .await
        .expect("the loop must apply the PubAck so the flush drains");
}

#[tokio::test(start_paused = true)]
async fn an_error_the_loop_polls_reports_the_publish_as_lost() {
    let delivery = Arc::new(DeliveryTracker::new());
    let _ticket = delivery.record_publish();

    let mut h = Harness::start([poll_error()], delivery.clone());
    h.health
        .wait_for(|state| *state == Health::Disconnected)
        .await
        .expect("the loop must report the link is down");

    let err = delivery
        .flush(Duration::from_secs(1))
        .await
        .expect_err("a disconnect discards what it had queued");
    assert!(matches!(
        err,
        crate::SparkplugError::PublishLost { count: 1 }
    ));
}

#[tokio::test]
async fn an_unrelated_event_leaves_the_loop_running() {
    let delivery = Arc::new(DeliveryTracker::new());
    let _ticket = delivery.record_publish();

    let mut h = Harness::start(
        [
            Ok(rumqttc::Event::Incoming(rumqttc::Packet::PingResp)),
            connack(),
        ],
        delivery.clone(),
    );

    (&mut h.ready)
        .await
        .expect("a PingResp must not stop the loop");
    assert_eq!(
        delivery.unacked(),
        1,
        "a PingResp must not touch the outstanding count"
    );
}

// ---------------------------------------------------------------------------
// The classification helpers
// ---------------------------------------------------------------------------

#[test]
fn a_puback_clears_one_publish() {
    let tracker = DeliveryTracker::new();
    let _ticket = tracker.record_publish();
    apply_event(&tracker, &puback());
    assert_eq!(tracker.unacked(), 0);
}

#[test]
fn a_poll_error_loses_everything_outstanding() {
    let tracker = DeliveryTracker::new();
    let _ticket = tracker.record_publish();
    apply_event(&tracker, &poll_error());
    assert_eq!(
        tracker.unacked(),
        0,
        "a disconnect clears the outstanding count"
    );
}

#[test]
fn an_incoming_disconnect_loses_everything_outstanding() {
    let tracker = DeliveryTracker::new();
    let _ticket = tracker.record_publish();
    apply_event(
        &tracker,
        &Ok(rumqttc::Event::Incoming(rumqttc::Packet::Disconnect)),
    );
    assert_eq!(tracker.unacked(), 0);
}

#[test]
fn an_unrelated_event_changes_nothing() {
    let tracker = DeliveryTracker::new();
    let _ticket = tracker.record_publish();
    apply_event(
        &tracker,
        &Ok(rumqttc::Event::Incoming(rumqttc::Packet::PingResp)),
    );
    assert_eq!(tracker.unacked(), 1);
}

#[test]
fn connack_reports_connected_and_errors_report_disconnected() {
    let (tx, rx) = tokio::sync::watch::channel(Health::Disconnected);

    apply_health(&tx, &connack());
    assert_eq!(*rx.borrow(), Health::Connected);

    apply_health(&tx, &poll_error());
    assert_eq!(*rx.borrow(), Health::Disconnected);
}

#[test]
fn a_written_disconnect_reports_disconnected() {
    let (tx, rx) = tokio::sync::watch::channel(Health::Connected);
    apply_health(
        &tx,
        &Ok(rumqttc::Event::Outgoing(rumqttc::Outgoing::Disconnect)),
    );
    assert_eq!(*rx.borrow(), Health::Disconnected);
}

#[test]
fn an_unrelated_event_leaves_health_alone() {
    let (tx, rx) = tokio::sync::watch::channel(Health::Connected);
    apply_health(
        &tx,
        &Ok(rumqttc::Event::Incoming(rumqttc::Packet::PingResp)),
    );
    assert_eq!(*rx.borrow(), Health::Connected);
}
