//! Validated identifiers for the segments of a Sparkplug topic.
//!
//! Each type wraps a borrowed string that has been checked as one topic
//! segment. They exist to stop a positional swap: a group, an edge node and
//! a device are all `&str`, sit next to each other in a call, and mean
//! entirely different things. Passing the wrong one no longer compiles.
//!
//! The types borrow rather than own, so wrapping costs nothing. Build the
//! loop-invariant ones once and reuse them across a batch.

use std::fmt;
use std::sync::Arc;

use crate::error::SparkplugError;

/// Whether a string can stand as one topic segment.
///
/// [`super::Namespace`] checks its own segment the same way.
pub(super) fn is_usable_segment(value: &str) -> bool {
    !value.is_empty() && !value.contains(['/', '+', '#'])
}

macro_rules! topic_id {
    ($name:ident, $label:literal, $summary:literal, $example:literal) => {
        #[doc = $summary]
        ///
        /// Wraps a checked topic segment. Build one with `new`, which
        /// rejects an empty string and any string containing `/`, `+` or
        /// `#`.
        ///
        /// # Examples
        ///
        /// ```
        #[doc = concat!("use sparkplug_mqtt::", stringify!($name), ";")]
        ///
        #[doc = concat!("let id = ", stringify!($name), "::new(", stringify!($example), ")?;")]
        #[doc = concat!("assert_eq!(id.as_str(), ", stringify!($example), ");")]
        #[doc = concat!("assert!(", stringify!($name), "::new(\"a/b\").is_err());")]
        /// # Ok::<(), sparkplug_mqtt::SparkplugError>(())
        /// ```
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub struct $name<'a>(&'a str);

        impl<'a> $name<'a> {
            #[doc = concat!("Check `value` and wrap it as a ", $label, ".")]
            ///
            /// # Errors
            ///
            /// Returns [`SparkplugError::InvalidTopic`] when the string
            /// cannot stand as a topic segment. The message names which
            /// identifier failed.
            pub fn new(value: &'a str) -> Result<Self, SparkplugError> {
                match is_usable_segment(value) {
                    true => Ok(Self(value)),
                    false => Err(SparkplugError::InvalidTopic(format!(
                        "{} {value:?} must be non-empty and free of '/', '+' and '#'",
                        $label
                    ))),
                }
            }

            /// Wrap a segment that a caller checked already.
            ///
            /// A topic stores the text of a segment that entered through
            /// `new` or through the parser, which checks the same way. The
            /// accessors on a topic hand that text back as its own type, and
            /// this is the wrap they use. ADR-0002 checks a value once, where
            /// it enters its type, so a second check here can never fail and
            /// would force a caller to handle an error that cannot happen.
            ///
            /// Call this only with a segment a check has passed. The
            /// `debug_assert` states that rule to the compiler. A debug
            /// build panics on a segment that skipped the check, and a
            /// release build drops the test. `super::super::seq` joins a
            /// group and an edge node with `/` to key its counts, and that
            /// key names one edge node only while no segment holds a `/`.
            ///
            /// The visibility stops at the topic module, which holds every
            /// caller: the accessors on a topic, and
            /// [`OwnedEdgeNode`]. Outside that wall a caller reaches a
            /// segment through `new` or through the parser, so the
            /// `debug_assert` guards a rule the module keeps rather than a
            /// rule the crate hopes for.
            pub(in crate::sparkplug::topic) fn wrap_checked(value: &'a str) -> Self {
                debug_assert!(is_usable_segment(value));
                Self(value)
            }

            /// The identifier as it appears in a topic.
            #[must_use]
            pub fn as_str(&self) -> &'a str {
                self.0
            }
        }

        impl<'a> TryFrom<&'a str> for $name<'a> {
            type Error = SparkplugError;

            fn try_from(value: &'a str) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl AsRef<str> for $name<'_> {
            fn as_ref(&self) -> &str {
                self.0
            }
        }

        impl fmt::Display for $name<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.0)
            }
        }
    };
}

topic_id!(
    GroupId,
    "group id",
    "A named collection of edge nodes that share a bus.",
    "PlantFloor"
);
topic_id!(
    EdgeNodeId,
    "edge node id",
    "A Sparkplug entity that owns one MQTT session.",
    "edge_node_1"
);
topic_id!(
    DeviceId,
    "device id",
    "An asset that reports metrics through an edge node.",
    "pump_3"
);
topic_id!(
    HostId,
    "host id",
    "A host application, named by a STATE topic.",
    "scada_1"
);

/// The pair that names one Sparkplug edge node: a group and an edge node id.
///
/// Sparkplug names an edge node by both segments, so the same edge node id
/// in two groups names two edge nodes. The pair travels together to stop a
/// caller from taking one segment from one place and the other from
/// another.
///
/// A client takes this at connect and keeps it. The client id names the MQTT
/// connection and has no part in it.
///
/// # Examples
///
/// ```
/// use sparkplug_mqtt::{EdgeNode, EdgeNodeId, GroupId};
///
/// let edge_node = EdgeNode::new(GroupId::new("PlantFloor")?, EdgeNodeId::new("edge_node_1")?);
/// assert_eq!(edge_node.group().as_str(), "PlantFloor");
/// assert_eq!(edge_node.edge_node_id().as_str(), "edge_node_1");
/// # Ok::<(), sparkplug_mqtt::SparkplugError>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EdgeNode<'a> {
    group: GroupId<'a>,
    edge_node_id: EdgeNodeId<'a>,
}

impl<'a> EdgeNode<'a> {
    /// Name one edge node by its group and its edge node id.
    ///
    /// Both arguments are checked already, so this cannot fail.
    #[must_use]
    pub fn new(group: GroupId<'a>, edge_node_id: EdgeNodeId<'a>) -> Self {
        Self {
            group,
            edge_node_id,
        }
    }

    /// The group the edge node belongs to.
    #[must_use]
    pub fn group(&self) -> GroupId<'a> {
        self.group
    }

    /// The edge node id inside that group.
    #[must_use]
    pub fn edge_node_id(&self) -> EdgeNodeId<'a> {
        self.edge_node_id
    }
}

/// The same checked pair, owned, for a holder that outlives the strings it
/// was built from.
///
/// [`EdgeNode`] borrows, so a client that keeps its identity for the length
/// of a session cannot store it. This type stores the two segments as
/// `Arc<str>` instead.
///
/// The fields are private and this module holds the only constructor, which
/// is `From<EdgeNode<'_>>`. So the two strings inside always came from
/// [`GroupId::new`] and [`EdgeNodeId::new`], or from the parser, which
/// checks the same way. No caller can build one from a string that skipped
/// the check, which is what ADR-0004 rests on: a client in the edge node
/// role holds this type, so that role cannot exist beside an unusable
/// segment.
///
/// `From<&OwnedEdgeNode>` hands an [`EdgeNode`] back. It rewraps rather than
/// checks a second time, on the rule ADR-0002 states: check a value once,
/// where it enters its type.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct OwnedEdgeNode {
    group: Arc<str>,
    edge_node_id: Arc<str>,
}

impl From<EdgeNode<'_>> for OwnedEdgeNode {
    fn from(edge_node: EdgeNode<'_>) -> Self {
        Self {
            group: Arc::from(edge_node.group().as_str()),
            edge_node_id: Arc::from(edge_node.edge_node_id().as_str()),
        }
    }
}

impl<'a> From<&'a OwnedEdgeNode> for EdgeNode<'a> {
    fn from(owned: &'a OwnedEdgeNode) -> Self {
        Self::new(
            GroupId::wrap_checked(&owned.group),
            EdgeNodeId::wrap_checked(&owned.edge_node_id),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_usable_segment_is_accepted() {
        assert_eq!(GroupId::new("PlantFloor").unwrap().as_str(), "PlantFloor");
        assert_eq!(EdgeNodeId::new("edge-1").unwrap().as_str(), "edge-1");
        assert_eq!(DeviceId::new("pump_3").unwrap().as_str(), "pump_3");
        assert_eq!(HostId::new("scada_1").unwrap().as_str(), "scada_1");
    }

    #[test]
    fn an_unusable_segment_is_rejected() {
        for bad in ["", "a/b", "a+b", "a#b"] {
            assert!(GroupId::new(bad).is_err(), "group id {bad:?}");
            assert!(EdgeNodeId::new(bad).is_err(), "edge node id {bad:?}");
            assert!(DeviceId::new(bad).is_err(), "device id {bad:?}");
            assert!(HostId::new(bad).is_err(), "host id {bad:?}");
        }
    }

    #[test]
    fn the_error_names_which_identifier_failed() {
        let message = DeviceId::new("pump/3").unwrap_err().to_string();
        assert!(
            message.contains("device id"),
            "the error must name the identifier, got: {message}"
        );
        let message = EdgeNodeId::new("").unwrap_err().to_string();
        assert!(
            message.contains("edge node id"),
            "the error must name the identifier, got: {message}"
        );
    }

    #[test]
    fn an_identifier_borrows_rather_than_allocates() {
        let owned = String::from("PlantFloor");
        let id = GroupId::new(&owned).unwrap();
        assert!(
            std::ptr::eq(id.as_str(), owned.as_str()),
            "the identifier must borrow its text, not copy it"
        );
    }

    #[test]
    fn try_from_and_display_agree_with_new() {
        let id = GroupId::try_from("PlantFloor").unwrap();
        assert_eq!(id, GroupId::new("PlantFloor").unwrap());
        assert_eq!(id.to_string(), "PlantFloor");
        assert_eq!(id.as_ref(), "PlantFloor");
    }

    #[test]
    fn an_edge_node_keeps_its_two_segments_apart() {
        let edge_node = EdgeNode::new(
            GroupId::new("PlantFloor").unwrap(),
            EdgeNodeId::new("edge_node_1").unwrap(),
        );
        assert_eq!(edge_node.group().as_str(), "PlantFloor");
        assert_eq!(edge_node.edge_node_id().as_str(), "edge_node_1");
    }

    #[test]
    fn one_edge_node_id_in_two_groups_names_two_edge_nodes() {
        let edge_node_id = EdgeNodeId::new("edge1").unwrap();
        let plant = EdgeNode::new(GroupId::new("PlantFloor").unwrap(), edge_node_id);
        let boiler = EdgeNode::new(GroupId::new("Boiler").unwrap(), edge_node_id);
        assert_ne!(
            plant, boiler,
            "Sparkplug names an edge node by the pair, so the group must count"
        );
    }

    #[test]
    fn an_owned_edge_node_hands_back_the_pair_it_took() {
        let borrowed = EdgeNode::new(
            GroupId::new("PlantFloor").unwrap(),
            EdgeNodeId::new("edge_node_1").unwrap(),
        );
        let owned = OwnedEdgeNode::from(borrowed);

        assert_eq!(
            EdgeNode::from(&owned),
            borrowed,
            "the owned pair must name the same edge node as the pair it took"
        );
    }

    #[test]
    fn an_owned_edge_node_outlives_the_strings_it_was_built_from() {
        let owned = {
            let group = String::from("PlantFloor");
            let edge_node_id = String::from("edge_node_1");
            OwnedEdgeNode::from(EdgeNode::new(
                GroupId::new(&group).unwrap(),
                EdgeNodeId::new(&edge_node_id).unwrap(),
            ))
        };

        assert_eq!(EdgeNode::from(&owned).group().as_str(), "PlantFloor");
        assert_eq!(
            EdgeNode::from(&owned).edge_node_id().as_str(),
            "edge_node_1"
        );
    }
}
