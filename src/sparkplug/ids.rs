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

use crate::error::SparkplugError;

/// Whether a string can stand as one topic segment.
///
/// [`super::topic::Namespace`] checks its own segment the same way.
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
}
