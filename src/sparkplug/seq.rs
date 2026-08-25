//! Counts the `seq` number that each edge node's messages carry.
//!
//! The specification gives one counter to each edge node. An NBIRTH sets
//! that counter to 0. Every message after it adds 1, and the count wraps
//! from 255 back to 0, because `seq` holds one byte. A host application
//! reads the count to find a message it did not receive.
//!
//! Sparkplug names an edge node by the pair `group_id/edge_node_id`, so the
//! map keys on the pair. The same node id in two groups names two edge
//! nodes, and each one counts alone. A map keyed on the node id alone would
//! draw both streams from one count, and a host application would read a
//! gap in each of them.
//!
//! One client publishes for many edge nodes, so the map holds one count for
//! each pair. Nothing removes an entry. The map grows by one `String` and
//! one gate for each distinct pair the client publishes for, and it frees
//! every entry when the client drops.
//!
//! # The gate
//!
//! Each count sits behind its own [`tokio::sync::Mutex`], which this module
//! calls a gate. A publish takes the gate of its edge node, draws the
//! number, hands the message to the client, and releases the gate. So one
//! publish for one edge node runs at a time, and the number a message
//! carries and the order it reaches the client agree. Publishes for
//! different edge nodes take different gates and do not wait for each
//! other — see ADR-0005.
//!
//! The map itself sits behind a [`std::sync::Mutex`], and a caller holds
//! that guard only long enough to clone the gate of one edge node. Holding
//! it across an `await` would make the publish future not `Send`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::OwnedMutexGuard;

use super::topic::ids::{EdgeNodeId, GroupId};

/// The count of one edge node, and the gate that guards it.
type SeqGate = Arc<tokio::sync::Mutex<u8>>;

/// One `seq` count for each edge node a client publishes for.
#[derive(Debug, Default)]
pub(crate) struct SeqCounters {
    gates: Mutex<HashMap<String, SeqGate>>,
}

impl SeqCounters {
    /// Build a set of counts that holds no edge node yet.
    ///
    /// This reads no runtime, so a client builds it in a plain function.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Take the gate of the edge node `group`/`node` and set its count back
    /// to 0.
    ///
    /// An NBIRTH calls this. The specification gives the first message after
    /// an NBIRTH the count 0.
    pub(crate) async fn reserve_reset(
        &self,
        group: GroupId<'_>,
        node: EdgeNodeId<'_>,
    ) -> SeqReservation {
        self.reserve_with(group, node, |_| 0).await
    }

    /// Take the gate of the edge node `group`/`node` and add 1 to its count.
    ///
    /// The count wraps from 255 to 0. An edge node this has not seen holds
    /// 0, so its first message reads 1.
    pub(crate) async fn reserve(&self, group: GroupId<'_>, node: EdgeNodeId<'_>) -> SeqReservation {
        self.reserve_with(group, node, |current| current.wrapping_add(1))
            .await
    }

    /// Take the gate of the edge node `group`/`node`, write a new count,
    /// and hold the gate in the reservation.
    ///
    /// `step` reads the count the edge node holds, and reads 0 for an edge
    /// node the map does not hold yet.
    ///
    /// The identifiers are [`GroupId`] and [`EdgeNodeId`] rather than `&str`
    /// per ADR-0002. Both have the same shape and sit beside each other at
    /// the call sites, so a positional swap would be silent.
    async fn reserve_with(
        &self,
        group: GroupId<'_>,
        node: EdgeNodeId<'_>,
        step: impl FnOnce(u8) -> u8,
    ) -> SeqReservation {
        // `gate_of` releases the map guard before this line, so nothing
        // holds a `std::sync` guard across the `await` below.
        let mut count = self.gate_of(group, node).lock_owned().await;
        let previous = *count;
        *count = step(previous);
        SeqReservation {
            count,
            restore_to: Some(previous),
        }
    }

    /// Clone the gate of the edge node `group`/`node`, and add the edge node
    /// when the map does not hold it yet.
    ///
    /// The map owns its keys, so this copies the text of the pair once, when
    /// it adds the edge node. The guard drops when this returns, which is
    /// what keeps it clear of the `await` in [`Self::reserve_with`].
    fn gate_of(&self, group: GroupId<'_>, node: EdgeNodeId<'_>) -> SeqGate {
        self.lock().entry(key_of(group, node)).or_default().clone()
    }

    /// A poisoned map cannot corrupt anything, so recover the guard.
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, SeqGate>> {
        self.gates
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// One number drawn for one edge node, and the gate that holds it.
///
/// The reservation derefs to the number the next message carries.
/// [`Self::commit`] keeps that number, because the client took the message.
/// Every other end — a refusal, and the cancellation that drops the publish
/// future — drops the reservation, which writes the previous number back
/// under the gate. So the number a message never carried stays available to
/// the message after it, and a host application reads no gap.
///
/// The reservation holds the gate until it drops, so no other publish for
/// that edge node can draw a number while this one waits for the client.
#[derive(Debug)]
#[must_use = "the reservation holds the gate of one edge node, so a publish must hold it across the publish call"]
pub(crate) struct SeqReservation {
    /// The count of the edge node, and the gate the publish holds.
    count: OwnedMutexGuard<u8>,
    /// The count to write back when the message does not reach the client.
    /// [`SeqReservation::commit`] clears it.
    restore_to: Option<u8>,
}

impl SeqReservation {
    /// Add 1 to the number this reservation holds, and return the new one.
    ///
    /// [`crate::SparkplugClient::publish_birth`] calls this between
    /// the NBIRTH and the DBIRTH. One reservation covers both messages, so
    /// the pair carries 0 then 1 and no other publish for that edge node
    /// draws a number between them. The reservation still restores the count
    /// the edge node held before the NBIRTH, so a DBIRTH the client refuses
    /// leaves the pair with no effect on the count.
    pub(crate) fn next(&mut self) -> u8 {
        *self.count = self.count.wrapping_add(1);
        *self.count
    }

    /// Keep the number, because the client took the message.
    pub(crate) fn commit(mut self) {
        self.restore_to = None;
    }
}

impl std::ops::Deref for SeqReservation {
    type Target = u8;

    fn deref(&self) -> &Self::Target {
        &self.count
    }
}

impl Drop for SeqReservation {
    /// Write the previous count back, unless the publish committed it.
    ///
    /// This runs before the gate releases, because the guard is a field of
    /// this struct. So no other publish for that edge node reads the number
    /// of a message that never left.
    fn drop(&mut self) {
        if let Some(previous) = self.restore_to.take() {
            *self.count = previous;
        }
    }
}

/// Name one edge node with one string.
///
/// Neither identifier can hold `/`, because each type checks its segment
/// when a caller builds it. One `/` therefore separates the two parts, and
/// no two different pairs write the same key.
fn key_of(group: GroupId<'_>, node: EdgeNodeId<'_>) -> String {
    format!("{group}/{node}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn group(id: &str) -> GroupId<'_> {
        GroupId::new(id).expect("the test uses a usable group id")
    }

    fn node(id: &str) -> EdgeNodeId<'_> {
        EdgeNodeId::new(id).expect("the test uses a usable edge node id")
    }

    /// Draw one number and keep it, the way a publish the client takes does.
    async fn drawn(counters: &SeqCounters, group: GroupId<'_>, node: EdgeNodeId<'_>) -> u8 {
        let reservation = counters.reserve(group, node).await;
        let count = *reservation;
        reservation.commit();
        count
    }

    /// Set one count back to 0 and keep it, the way an NBIRTH does.
    async fn restarted(counters: &SeqCounters, group: GroupId<'_>, node: EdgeNodeId<'_>) -> u8 {
        let reservation = counters.reserve_reset(group, node).await;
        let count = *reservation;
        reservation.commit();
        count
    }

    #[tokio::test]
    async fn the_first_message_of_an_unseen_edge_node_reads_one() {
        let counters = SeqCounters::new();
        assert_eq!(
            drawn(&counters, group("PlantFloor"), node("edge1")).await,
            1
        );
    }

    #[tokio::test]
    async fn each_message_adds_one() {
        let counters = SeqCounters::new();
        let mut counts = Vec::new();
        for _ in 0..3 {
            counts.push(drawn(&counters, group("PlantFloor"), node("edge1")).await);
        }
        assert_eq!(counts, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn a_reset_starts_the_count_at_zero() {
        let counters = SeqCounters::new();
        let _ = drawn(&counters, group("PlantFloor"), node("edge1")).await;
        assert_eq!(
            restarted(&counters, group("PlantFloor"), node("edge1")).await,
            0
        );
        assert_eq!(
            drawn(&counters, group("PlantFloor"), node("edge1")).await,
            1,
            "the message after an NBIRTH reads 1"
        );
    }

    #[tokio::test]
    async fn each_edge_node_holds_its_own_count() {
        let counters = SeqCounters::new();
        let _ = drawn(&counters, group("PlantFloor"), node("edge1")).await;
        let _ = drawn(&counters, group("PlantFloor"), node("edge1")).await;

        assert_eq!(
            drawn(&counters, group("PlantFloor"), node("edge2")).await,
            1,
            "a second edge node keeps its own count"
        );
        assert_eq!(
            drawn(&counters, group("PlantFloor"), node("edge1")).await,
            3
        );
    }

    #[tokio::test]
    async fn the_same_node_id_in_two_groups_counts_apart() {
        // Sparkplug names an edge node by the pair. `PlantFloor/edge1` and
        // `Boiler/edge1` are two edge nodes, so one count each.
        let counters = SeqCounters::new();
        let _ = drawn(&counters, group("PlantFloor"), node("edge1")).await;
        let _ = drawn(&counters, group("PlantFloor"), node("edge1")).await;

        assert_eq!(
            drawn(&counters, group("Boiler"), node("edge1")).await,
            1,
            "a node id in another group names another edge node"
        );
        assert_eq!(
            drawn(&counters, group("PlantFloor"), node("edge1")).await,
            3
        );
    }

    #[tokio::test]
    async fn a_reset_moves_one_edge_node_only() {
        let counters = SeqCounters::new();
        let _ = drawn(&counters, group("PlantFloor"), node("edge1")).await;
        let _ = drawn(&counters, group("PlantFloor"), node("edge2")).await;
        let _ = drawn(&counters, group("Boiler"), node("edge1")).await;
        let _ = restarted(&counters, group("PlantFloor"), node("edge1")).await;

        assert_eq!(
            drawn(&counters, group("PlantFloor"), node("edge2")).await,
            2,
            "an NBIRTH from one edge node must not restart another"
        );
        assert_eq!(
            drawn(&counters, group("Boiler"), node("edge1")).await,
            2,
            "an NBIRTH in one group must not restart the same node id in another"
        );
    }

    #[tokio::test]
    async fn the_count_wraps_from_255_to_zero_without_panicking() {
        let counters = SeqCounters::new();
        assert_eq!(
            restarted(&counters, group("PlantFloor"), node("edge1")).await,
            0
        );

        let mut last = 0;
        for _ in 0..256 {
            last = drawn(&counters, group("PlantFloor"), node("edge1")).await;
        }

        assert_eq!(
            last, 0,
            "the count holds one byte, so it wraps from 255 to 0"
        );
    }

    #[tokio::test]
    async fn a_dropped_reservation_leaves_its_number_for_the_next_message() {
        let counters = SeqCounters::new();
        let _ = drawn(&counters, group("PlantFloor"), node("edge1")).await;

        drop(counters.reserve(group("PlantFloor"), node("edge1")).await);

        assert_eq!(
            drawn(&counters, group("PlantFloor"), node("edge1")).await,
            2,
            "a message that never left must leave its number for the next one"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn one_edge_node_does_not_wait_behind_another() {
        // The gate guards one count, not the map. A publish that waits for
        // the client holds the gate of its own edge node, and a publish for
        // another edge node must still draw its number.
        let counters = SeqCounters::new();
        let held = counters.reserve(group("PlantFloor"), node("edge1")).await;

        let other = tokio::time::timeout(
            Duration::from_millis(50),
            counters.reserve(group("PlantFloor"), node("edge2")),
        )
        .await;

        assert!(
            other.is_ok(),
            "each edge node holds its own gate, so one slow publish must not \
             stop a publish for another edge node"
        );
        drop(held);
    }
}
