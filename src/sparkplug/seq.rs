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
//! one byte for each distinct pair the client publishes for, and it frees
//! every entry when the client drops.

use std::collections::HashMap;
use std::sync::Mutex;

use super::topic::ids::{EdgeNodeId, GroupId};

/// One `seq` count for each edge node a client publishes for.
#[derive(Debug, Default)]
pub(crate) struct SeqCounters {
    counters: Mutex<HashMap<String, u8>>,
}

impl SeqCounters {
    /// Build a set of counts that holds no edge node yet.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Set the count of the edge node `group`/`node` back to 0, and return
    /// it.
    ///
    /// An NBIRTH calls this. The specification gives the first message after
    /// an NBIRTH the count 0.
    pub(crate) fn reset(&self, group: GroupId<'_>, node: EdgeNodeId<'_>) -> u8 {
        self.store(group, node, |_| 0)
    }

    /// Add 1 to the count of the edge node `group`/`node`, and return the
    /// new count.
    ///
    /// The count wraps from 255 to 0. An edge node this has not seen holds
    /// 0, so its first message reads 1.
    pub(crate) fn next(&self, group: GroupId<'_>, node: EdgeNodeId<'_>) -> u8 {
        self.store(group, node, |current| current.wrapping_add(1))
    }

    /// Write a new count for the edge node `group`/`node`, and return it.
    ///
    /// `step` reads the count the edge node holds, and reads 0 for an edge
    /// node the map does not hold yet. The map owns its keys, so this copies
    /// the text of the pair once, when it adds the edge node.
    ///
    /// The identifiers are [`GroupId`] and [`EdgeNodeId`] rather than `&str`
    /// per ADR-0002. Both have the same shape and sit beside each other at
    /// the call sites, so a positional swap would be silent.
    fn store(&self, group: GroupId<'_>, node: EdgeNodeId<'_>, step: impl FnOnce(u8) -> u8) -> u8 {
        let key = key_of(group, node);
        let mut counters = self.lock();
        match counters.get_mut(key.as_str()) {
            Some(current) => {
                *current = step(*current);
                *current
            }
            None => {
                let started = step(0);
                counters.insert(key, started);
                started
            }
        }
    }

    /// A poisoned map cannot corrupt anything, so recover the guard.
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, u8>> {
        self.counters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
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

    fn group(id: &str) -> GroupId<'_> {
        GroupId::new(id).expect("the test uses a usable group id")
    }

    fn node(id: &str) -> EdgeNodeId<'_> {
        EdgeNodeId::new(id).expect("the test uses a usable edge node id")
    }

    #[test]
    fn the_first_message_of_an_unseen_edge_node_reads_one() {
        let counters = SeqCounters::new();
        assert_eq!(counters.next(group("PlantFloor"), node("edge1")), 1);
    }

    #[test]
    fn each_message_adds_one() {
        let counters = SeqCounters::new();
        let counts: Vec<u8> = (0..3)
            .map(|_| counters.next(group("PlantFloor"), node("edge1")))
            .collect();
        assert_eq!(counts, vec![1, 2, 3]);
    }

    #[test]
    fn a_reset_starts_the_count_at_zero() {
        let counters = SeqCounters::new();
        let _ = counters.next(group("PlantFloor"), node("edge1"));
        assert_eq!(counters.reset(group("PlantFloor"), node("edge1")), 0);
        assert_eq!(
            counters.next(group("PlantFloor"), node("edge1")),
            1,
            "the message after an NBIRTH reads 1"
        );
    }

    #[test]
    fn each_edge_node_holds_its_own_count() {
        let counters = SeqCounters::new();
        let _ = counters.next(group("PlantFloor"), node("edge1"));
        let _ = counters.next(group("PlantFloor"), node("edge1"));

        assert_eq!(
            counters.next(group("PlantFloor"), node("edge2")),
            1,
            "a second edge node keeps its own count"
        );
        assert_eq!(counters.next(group("PlantFloor"), node("edge1")), 3);
    }

    #[test]
    fn the_same_node_id_in_two_groups_counts_apart() {
        // Sparkplug names an edge node by the pair. `PlantFloor/edge1` and
        // `Boiler/edge1` are two edge nodes, so one count each.
        let counters = SeqCounters::new();
        let _ = counters.next(group("PlantFloor"), node("edge1"));
        let _ = counters.next(group("PlantFloor"), node("edge1"));

        assert_eq!(
            counters.next(group("Boiler"), node("edge1")),
            1,
            "a node id in another group names another edge node"
        );
        assert_eq!(counters.next(group("PlantFloor"), node("edge1")), 3);
    }

    #[test]
    fn a_reset_moves_one_edge_node_only() {
        let counters = SeqCounters::new();
        let _ = counters.next(group("PlantFloor"), node("edge1"));
        let _ = counters.next(group("PlantFloor"), node("edge2"));
        let _ = counters.next(group("Boiler"), node("edge1"));
        let _ = counters.reset(group("PlantFloor"), node("edge1"));

        assert_eq!(
            counters.next(group("PlantFloor"), node("edge2")),
            2,
            "an NBIRTH from one edge node must not restart another"
        );
        assert_eq!(
            counters.next(group("Boiler"), node("edge1")),
            2,
            "an NBIRTH in one group must not restart the same node id in another"
        );
    }

    #[test]
    fn the_count_wraps_from_255_to_zero_without_panicking() {
        let counters = SeqCounters::new();
        assert_eq!(counters.reset(group("PlantFloor"), node("edge1")), 0);

        let last = (0..256)
            .map(|_| counters.next(group("PlantFloor"), node("edge1")))
            .last()
            .expect("256 steps produce 256 counts");

        assert_eq!(
            last, 0,
            "the count holds one byte, so it wraps from 255 to 0"
        );
    }
}
