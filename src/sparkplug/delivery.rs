//! Tracks whether published messages reached the broker.
//!
//! `rumqttc` accepts a publish into an in-memory channel and returns
//! immediately. Sparkplug requires `clean_session = true`, so a reconnect
//! discards everything still queued or unacknowledged. This tracker counts
//! what is outstanding and records when a disconnect throws it away, so
//! [`DeliveryTracker::flush`] can tell a caller the truth.

use std::sync::Mutex;
use std::time::Duration;

use tokio::sync::Notify;

use crate::error::SparkplugError;

#[derive(Debug, Default)]
struct State {
    /// QoS 1 publishes enqueued but not yet acknowledged by the broker.
    unacked: u32,
    /// Publishes a disconnect discarded that no flush has reported yet.
    lost: u32,
    /// Every publish that a flush has reported as lost, since the start.
    ///
    /// A flush reads this total when it opens and again when it drains.
    /// The difference is the loss inside its own window, so two flushes
    /// that overlap both learn about the same disconnect.
    reported: u64,
    /// How many disconnects this tracker has recorded.
    ///
    /// Each disconnect clears `unacked`, so a rollback that carries an
    /// older generation refers to a count that no longer exists.
    generation: u64,
}

/// Proof that a publish raised the outstanding count.
///
/// [`DeliveryTracker::record_publish`] returns one, and
/// [`DeliveryTracker::record_publish_failed`] consumes it. The type is
/// neither `Copy` nor `Clone`, so one publish can roll back at most once.
///
/// A bare ticket that a future holds across an `await` is not cancel-safe.
/// Cancellation drops the ticket without a rollback. `unacked` then stays
/// one too high, so every later [`DeliveryTracker::flush`] reports
/// [`SparkplugError::FlushTimeout`] until the next disconnect. The publish
/// path of this crate holds the ticket in a guard instead, and that guard
/// rolls the count back when a cancelled publish drops it.
#[derive(Debug)]
#[must_use = "a ticket must reach `record_publish_failed`, or the caller must release it on purpose"]
pub struct PublishTicket {
    /// The generation the tracker held when it counted this publish.
    generation: u64,
}

/// Counts unacknowledged publishes and records disconnect losses.
#[derive(Debug)]
pub struct DeliveryTracker {
    state: Mutex<State>,
    changed: Notify,
}

impl Default for DeliveryTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl DeliveryTracker {
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Mutex::new(State::default()),
            changed: Notify::new(),
        }
    }

    /// Count one QoS 1 publish before the caller hands it to the client.
    ///
    /// Count first, then publish. A task that shares this client can flush
    /// while the publish is still in flight, and a count that rises
    /// afterwards lets that flush report success for a message the broker
    /// never saw.
    ///
    /// Do not call this for QoS 0. The broker never acknowledges QoS 0, so
    /// a counted message would never clear.
    ///
    /// Keep the returned [`PublishTicket`] and pass it to
    /// [`Self::record_publish_failed`] when the client refuses the publish.
    pub fn record_publish(&self) -> PublishTicket {
        let mut state = self.lock();
        state.unacked += 1;
        PublishTicket {
            generation: state.generation,
        }
    }

    /// Count one QoS 1 publish and hold its rollback in a guard.
    ///
    /// Prefer this to [`Self::record_publish`] wherever the publish crosses
    /// an `await`. The guard rolls the count back when it drops, so a
    /// cancelled publish releases its count the same way a refused one does.
    /// Call [`PublishInFlight::keep`] once the client takes the message.
    pub(crate) fn publish_in_flight(&self) -> PublishInFlight<'_> {
        PublishInFlight {
            delivery: self,
            ticket: Some(self.record_publish()),
        }
    }

    /// Clear one publish that the broker acknowledged.
    pub fn record_ack(&self) {
        self.release_one();
    }

    /// Roll back one count after the client refuses a publish.
    ///
    /// The caller counts the publish before the client sees it, so a
    /// refusal leaves a count for a message that never left. Undo it.
    ///
    /// A disconnect between the two calls already cleared that count, and
    /// any count left belongs to another publish. The ticket carries the
    /// generation, so this rolls back nothing once a disconnect intervenes.
    /// The disconnect then reports a message that never left as lost, which
    /// asks the caller to publish it again. That is the safe direction,
    /// because the refusal already told the caller the same thing.
    pub fn record_publish_failed(&self, ticket: PublishTicket) {
        let mut state = self.lock();
        if ticket.generation == state.generation {
            state.unacked = state.unacked.saturating_sub(1);
            drop(state);
            self.changed.notify_waiters();
        }
    }

    /// Drop one outstanding publish and wake every waiting flush.
    fn release_one(&self) {
        let mut state = self.lock();
        state.unacked = state.unacked.saturating_sub(1);
        drop(state);
        self.changed.notify_waiters();
    }

    /// Record that a disconnect discarded every outstanding publish.
    ///
    /// This is unconditional rather than a guess. Under `clean_session`,
    /// `rumqttc` clears its pending list on the next ConnAck, so a
    /// disconnect with outstanding publishes always loses them.
    pub fn record_disconnect(&self) {
        let mut state = self.lock();
        state.lost += state.unacked;
        state.unacked = 0;
        state.generation += 1;
        drop(state);
        self.changed.notify_waiters();
    }

    /// How many publishes the broker has not acknowledged yet.
    #[must_use]
    pub fn unacked(&self) -> u32 {
        self.lock().unacked
    }

    /// Wait until the broker has acknowledged every publish, then report.
    ///
    /// # Errors
    ///
    /// Returns [`SparkplugError::PublishLost`] when a disconnect discarded
    /// publishes since the last flush, and
    /// [`SparkplugError::FlushTimeout`] when the broker stays silent.
    ///
    /// A successful flush clears the loss record, so each call reports only
    /// what happened since the previous one. Two flushes that overlap in
    /// time both report a disconnect that falls inside both windows. Only
    /// a flush that starts after the disconnect is reported sees a clean
    /// result.
    pub async fn flush(&self, timeout: Duration) -> Result<(), SparkplugError> {
        let opened_at = self.lock().reported;
        let drained = tokio::time::timeout(timeout, self.wait_for_drain()).await;

        let mut state = self.lock();
        if drained.is_err() {
            return Err(SparkplugError::FlushTimeout {
                after: timeout,
                unacked: state.unacked,
            });
        }

        state.reported += u64::from(std::mem::take(&mut state.lost));
        let count = u32::try_from(state.reported - opened_at).unwrap_or(u32::MAX);
        match count {
            0 => Ok(()),
            count => Err(SparkplugError::PublishLost { count }),
        }
    }

    /// Wait until nothing is outstanding.
    ///
    /// Register for the notification before checking the count, so an ack
    /// that lands between the two cannot be missed.
    async fn wait_for_drain(&self) {
        loop {
            let waiter = self.changed.notified();
            if self.lock().unacked == 0 {
                return;
            }
            waiter.await;
        }
    }

    /// A poisoned tracker cannot corrupt anything, so recover the guard.
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// One counted publish that the tracker releases unless the caller keeps
/// it.
///
/// [`DeliveryTracker::publish_in_flight`] counts the publish and returns
/// this guard. [`Self::keep`] leaves the count outstanding, because the
/// client took the message and the broker will answer it. Every other end —
/// a refusal, and the cancellation that drops the publish future — drops
/// the guard, which releases the count through
/// [`DeliveryTracker::record_publish_failed`]. So a message that never left
/// holds no later [`DeliveryTracker::flush`] open.
///
/// The guard carries the ticket, so it obeys the generation rule that
/// [`DeliveryTracker::record_publish_failed`] states: a disconnect between
/// the two calls already cleared the count, and this releases nothing.
#[derive(Debug)]
#[must_use = "the guard releases the count when it drops, so hold it across the publish"]
pub(crate) struct PublishInFlight<'a> {
    delivery: &'a DeliveryTracker,
    /// The ticket of this publish, until one of the two ends takes it.
    ticket: Option<PublishTicket>,
}

impl PublishInFlight<'_> {
    /// Leave the count outstanding, because the client took the message.
    ///
    /// The broker answers a QoS 1 publish with a PubAck, and
    /// [`DeliveryTracker::record_ack`] clears the count then.
    pub(crate) fn keep(mut self) {
        // Release the ticket without a rollback. The count stays.
        self.ticket = None;
    }
}

impl Drop for PublishInFlight<'_> {
    fn drop(&mut self) {
        if let Some(ticket) = self.ticket.take() {
            self.delivery.record_publish_failed(ticket);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_publish_is_unacked_until_the_broker_answers() {
        let tracker = DeliveryTracker::new();
        let _ticket = tracker.record_publish();
        let _ticket = tracker.record_publish();
        assert_eq!(tracker.unacked(), 2);

        tracker.record_ack();
        assert_eq!(tracker.unacked(), 1);
    }

    #[test]
    fn an_ack_with_nothing_outstanding_does_not_underflow() {
        let tracker = DeliveryTracker::new();
        tracker.record_ack();
        assert_eq!(tracker.unacked(), 0);
    }

    #[test]
    fn a_disconnect_with_nothing_outstanding_loses_nothing() {
        let tracker = DeliveryTracker::new();
        tracker.record_disconnect();
        assert_eq!(tracker.unacked(), 0);
    }

    #[tokio::test]
    async fn flush_succeeds_once_every_publish_is_acked() {
        let tracker = DeliveryTracker::new();
        let _ticket = tracker.record_publish();
        tracker.record_ack();
        tracker
            .flush(Duration::from_millis(50))
            .await
            .expect("a fully acked client should flush clean");
    }

    #[tokio::test]
    async fn flush_reports_publishes_lost_to_a_disconnect() {
        let tracker = DeliveryTracker::new();
        let _ticket = tracker.record_publish();
        let _ticket = tracker.record_publish();
        tracker.record_disconnect();

        let err = tracker
            .flush(Duration::from_millis(50))
            .await
            .expect_err("a disconnect with outstanding publishes must fail the flush");
        assert!(matches!(err, SparkplugError::PublishLost { count: 2 }));
    }

    #[tokio::test]
    async fn flush_clears_the_loss_so_the_next_flush_is_clean() {
        let tracker = DeliveryTracker::new();
        let _ticket = tracker.record_publish();
        tracker.record_disconnect();
        let _ = tracker.flush(Duration::from_millis(50)).await;

        tracker
            .flush(Duration::from_millis(50))
            .await
            .expect("an old loss must not poison later flushes");
    }

    #[test]
    fn a_refused_publish_rolls_the_count_back() {
        let tracker = DeliveryTracker::new();
        let ticket = tracker.record_publish();
        tracker.record_publish_failed(ticket);
        assert_eq!(
            tracker.unacked(),
            0,
            "a message the client refused must not hold a flush open"
        );
    }

    #[test]
    fn a_dropped_guard_releases_the_count_and_a_kept_one_does_not() {
        let tracker = DeliveryTracker::new();
        drop(tracker.publish_in_flight());
        assert_eq!(
            tracker.unacked(),
            0,
            "a cancelled publish must not hold a flush open"
        );

        tracker.publish_in_flight().keep();
        assert_eq!(
            tracker.unacked(),
            1,
            "a publish the client took stays outstanding until the broker answers"
        );
    }

    #[test]
    fn a_refusal_after_a_disconnect_rolls_back_nothing() {
        let tracker = DeliveryTracker::new();
        let ticket = tracker.record_publish();
        tracker.record_disconnect();
        let _ticket = tracker.record_publish();
        tracker.record_publish_failed(ticket);
        assert_eq!(
            tracker.unacked(),
            1,
            "the disconnect already cleared that count, so the rollback must \
             leave the publish that came after it outstanding"
        );
    }

    #[tokio::test]
    async fn a_refusal_after_a_disconnect_still_reports_the_loss() {
        let tracker = DeliveryTracker::new();
        let ticket = tracker.record_publish();
        tracker.record_disconnect();
        tracker.record_publish_failed(ticket);

        let err = tracker
            .flush(Duration::from_millis(50))
            .await
            .expect_err("the disconnect must still report what it discarded");
        assert!(matches!(err, SparkplugError::PublishLost { count: 1 }));
    }

    #[tokio::test]
    async fn two_overlapping_flushes_both_learn_about_the_same_loss() {
        let tracker = std::sync::Arc::new(DeliveryTracker::new());
        let _ticket = tracker.record_publish();

        let flushes: Vec<_> = (0..2)
            .map(|_| {
                let tracker = tracker.clone();
                tokio::spawn(async move { tracker.flush(Duration::from_secs(1)).await })
            })
            .collect();

        // Let both flushes open their window before the disconnect lands.
        tokio::time::sleep(Duration::from_millis(20)).await;
        tracker.record_disconnect();

        for flush in flushes {
            let err = flush
                .await
                .expect("the flush task must not panic")
                .expect_err("every flush that overlaps the disconnect must report the loss");
            assert!(matches!(err, SparkplugError::PublishLost { count: 1 }));
        }
    }

    #[tokio::test]
    async fn a_flush_that_starts_after_the_report_stays_clean() {
        let tracker = DeliveryTracker::new();
        let _ticket = tracker.record_publish();
        tracker.record_disconnect();
        let _ = tracker.flush(Duration::from_millis(50)).await;

        tracker
            .flush(Duration::from_millis(50))
            .await
            .expect("a loss reported before this flush opened is not its loss");
    }

    #[tokio::test]
    async fn flush_times_out_while_the_broker_stays_silent() {
        let tracker = DeliveryTracker::new();
        let _ticket = tracker.record_publish();

        let err = tracker
            .flush(Duration::from_millis(20))
            .await
            .expect_err("an unanswered publish must time out");
        assert!(matches!(
            err,
            SparkplugError::FlushTimeout { unacked: 1, .. }
        ));
    }
}
