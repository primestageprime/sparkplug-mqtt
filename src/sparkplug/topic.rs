//! The Sparkplug topic namespace, in both directions.
//!
//! Every topic this crate publishes to, and every topic it reads, passes
//! through here. [`Namespace`] builds topics and parses them. A
//! [`SparkplugTopic`] that exists is well-formed, so the accessors can say
//! honestly which identities a topic carries.

use std::fmt;
use std::sync::Arc;

use crate::error::SparkplugError;

/// The checked identifiers that stand as the segments of a topic.
pub(crate) mod ids;

use self::ids::{DeviceId, EdgeNodeId, GroupId, HostId, is_usable_segment};
use super::types::{MessageType, Shape};

/// The root segment of every Sparkplug topic, which names the protocol
/// version.
///
/// Build one at connect time and keep it. It owns every topic constructor
/// and the parser, so the namespace string is written down once.
///
/// # Examples
///
/// ```
/// use sparkplug_mqtt::Namespace;
///
/// use sparkplug_mqtt::{GroupId, EdgeNodeId, DeviceId};
///
/// let ns = Namespace::sparkplug_b();
/// let group = GroupId::new("PlantFloor")?;
/// let node = EdgeNodeId::new("edge_node_1")?;
/// let device = DeviceId::new("pump_3")?;
///
/// // Checked once, so building the topic cannot fail.
/// let topic = ns.ddata(group, node, device);
/// assert_eq!(topic.to_string(), "spBv1.0/PlantFloor/DDATA/edge_node_1/pump_3");
/// # Ok::<(), sparkplug_mqtt::SparkplugError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Namespace(Arc<str>);

impl Default for Namespace {
    fn default() -> Self {
        Self::sparkplug_b()
    }
}

impl Namespace {
    /// The standard Sparkplug B namespace, `spBv1.0`.
    #[must_use]
    pub fn sparkplug_b() -> Self {
        Self(Arc::from("spBv1.0"))
    }

    /// Build a namespace from a configured version string.
    ///
    /// # Errors
    ///
    /// Returns [`SparkplugError::InvalidNamespace`] when the string is empty
    /// or contains `/`, `+` or `#`. Such a namespace would produce topics no
    /// broker accepts.
    pub fn new(version: &str) -> Result<Self, SparkplugError> {
        match is_usable_segment(version) {
            true => Ok(Self(Arc::from(version))),
            false => Err(SparkplugError::InvalidNamespace(version.to_string())),
        }
    }

    /// The namespace as it appears in a topic.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    // -- node-shaped constructors ------------------------------------------

    /// Build an NBIRTH topic: `{namespace}/{group}/NBIRTH/{node}`.
    #[must_use]
    pub fn nbirth(&self, group_id: GroupId<'_>, node_id: EdgeNodeId<'_>) -> SparkplugTopic {
        self.node_topic(MessageType::NBIRTH, group_id, node_id)
    }

    /// Build an NDATA topic: `{namespace}/{group}/NDATA/{node}`.
    #[must_use]
    pub fn ndata(&self, group_id: GroupId<'_>, node_id: EdgeNodeId<'_>) -> SparkplugTopic {
        self.node_topic(MessageType::NDATA, group_id, node_id)
    }

    /// Build an NDEATH topic: `{namespace}/{group}/NDEATH/{node}`.
    #[must_use]
    pub fn ndeath(&self, group_id: GroupId<'_>, node_id: EdgeNodeId<'_>) -> SparkplugTopic {
        self.node_topic(MessageType::NDEATH, group_id, node_id)
    }

    /// Build an NCMD topic: `{namespace}/{group}/NCMD/{node}`.
    #[must_use]
    pub fn ncmd(&self, group_id: GroupId<'_>, node_id: EdgeNodeId<'_>) -> SparkplugTopic {
        self.node_topic(MessageType::NCMD, group_id, node_id)
    }

    // -- device-shaped constructors ----------------------------------------

    /// Build a DBIRTH topic: `{namespace}/{group}/DBIRTH/{node}/{device}`.
    #[must_use]
    pub fn dbirth(
        &self,
        group_id: GroupId<'_>,
        node_id: EdgeNodeId<'_>,
        device_id: DeviceId<'_>,
    ) -> SparkplugTopic {
        self.device_topic(MessageType::DBIRTH, group_id, node_id, device_id)
    }

    /// Build a DDATA topic: `{namespace}/{group}/DDATA/{node}/{device}`.
    ///
    /// This cannot fail. Every identifier was checked when its type was
    /// built, so there is nothing left to reject.
    #[must_use]
    pub fn ddata(
        &self,
        group_id: GroupId<'_>,
        node_id: EdgeNodeId<'_>,
        device_id: DeviceId<'_>,
    ) -> SparkplugTopic {
        self.device_topic(MessageType::DDATA, group_id, node_id, device_id)
    }

    /// Build a DDEATH topic: `{namespace}/{group}/DDEATH/{node}/{device}`.
    #[must_use]
    pub fn ddeath(
        &self,
        group_id: GroupId<'_>,
        node_id: EdgeNodeId<'_>,
        device_id: DeviceId<'_>,
    ) -> SparkplugTopic {
        self.device_topic(MessageType::DDEATH, group_id, node_id, device_id)
    }

    /// Build a DCMD topic: `{namespace}/{group}/DCMD/{node}/{device}`.
    #[must_use]
    pub fn dcmd(
        &self,
        group_id: GroupId<'_>,
        node_id: EdgeNodeId<'_>,
        device_id: DeviceId<'_>,
    ) -> SparkplugTopic {
        self.device_topic(MessageType::DCMD, group_id, node_id, device_id)
    }

    // -- host-shaped constructor -------------------------------------------

    /// Build a STATE topic: `{namespace}/STATE/{host}`.
    #[must_use]
    pub fn state(&self, host_id: HostId<'_>) -> SparkplugTopic {
        SparkplugTopic {
            namespace: self.0.clone(),
            message_type: MessageType::STATE,
            address: Address::Host {
                host_id: host_id.as_str().to_string(),
            },
        }
    }

    // -- parsing -----------------------------------------------------------

    /// Parse a topic string published on this namespace.
    ///
    /// Parsing is strict. A topic whose segments do not match the shape its
    /// message type requires is rejected rather than reinterpreted, so a
    /// topic that survives is safe to read through the accessors.
    ///
    /// # Errors
    ///
    /// Returns [`SparkplugError::InvalidTopic`] when the topic does not
    /// belong to this namespace or has too few segments to address anything.
    /// Returns [`SparkplugError::UnknownMessageType`] when the message type
    /// segment names no Sparkplug message. Returns
    /// [`SparkplugError::WrongTopicShape`] when the message type is known but
    /// the segment layout does not match it — a publisher addressed that
    /// message wrongly.
    ///
    /// # Examples
    ///
    /// ```
    /// use sparkplug_mqtt::{Namespace, MessageType, Shape};
    ///
    /// let ns = Namespace::sparkplug_b();
    ///
    /// let t = ns.parse("spBv1.0/PlantFloor/DDATA/edge_node_1/pump_3")?;
    /// assert_eq!(t.message_type(), MessageType::DDATA);
    /// assert_eq!(t.device_id().map(|d| d.as_str()), Some("pump_3"));
    ///
    /// // A host topic carries no group and no edge node.
    /// let t = ns.parse("spBv1.0/STATE/scada_1")?;
    /// assert_eq!(t.shape(), Shape::Host);
    /// assert_eq!(t.host_id().map(|h| h.as_str()), Some("scada_1"));
    /// assert_eq!(t.group_id(), None);
    ///
    /// // An NBIRTH is node-shaped, so a device segment is a publisher error.
    /// assert!(ns.parse("spBv1.0/PlantFloor/NBIRTH/edge_node_1/pump_3").is_err());
    /// # Ok::<(), sparkplug_mqtt::SparkplugError>(())
    /// ```
    pub fn parse(&self, topic: &str) -> Result<SparkplugTopic, SparkplugError> {
        let parts: Vec<&str> = topic.split('/').collect();

        if parts.first() != Some(&self.as_str()) {
            return Err(SparkplugError::InvalidTopic(topic.to_string()));
        }

        // STATE carries its message type in the second segment, not the third.
        if parts.get(1) == Some(&MessageType::STATE.as_str()) {
            return match parts.len() {
                3 => Ok(self.state(HostId::new(parts[2])?)),
                _ => Err(wrong_shape(topic, MessageType::STATE)),
            };
        }

        let Some(raw_type) = parts.get(2) else {
            return Err(SparkplugError::InvalidTopic(topic.to_string()));
        };
        let message_type: MessageType = raw_type.parse()?;

        match (message_type.shape(), parts.len()) {
            (Shape::Node, 4) => Ok(self.node_topic(
                message_type,
                GroupId::new(parts[1])?,
                EdgeNodeId::new(parts[3])?,
            )),
            (Shape::Device, 5) => Ok(self.device_topic(
                message_type,
                GroupId::new(parts[1])?,
                EdgeNodeId::new(parts[3])?,
                DeviceId::new(parts[4])?,
            )),
            _ => Err(wrong_shape(topic, message_type)),
        }
    }

    // -- private -----------------------------------------------------------

    fn node_topic(
        &self,
        message_type: MessageType,
        group_id: GroupId<'_>,
        node_id: EdgeNodeId<'_>,
    ) -> SparkplugTopic {
        SparkplugTopic {
            namespace: self.0.clone(),
            message_type,
            address: Address::Node {
                group_id: group_id.as_str().to_string(),
                node_id: node_id.as_str().to_string(),
            },
        }
    }

    fn device_topic(
        &self,
        message_type: MessageType,
        group_id: GroupId<'_>,
        node_id: EdgeNodeId<'_>,
        device_id: DeviceId<'_>,
    ) -> SparkplugTopic {
        SparkplugTopic {
            namespace: self.0.clone(),
            message_type,
            address: Address::Device {
                group_id: group_id.as_str().to_string(),
                node_id: node_id.as_str().to_string(),
                device_id: device_id.as_str().to_string(),
            },
        }
    }
}

/// Which identities a topic carries, and their values.
///
/// Private, so a caller cannot build a shape that contradicts the message
/// type. Read it through the accessors on [`SparkplugTopic`].
#[derive(Debug, Clone, PartialEq, Eq)]
enum Address {
    Node {
        group_id: String,
        node_id: String,
    },
    Device {
        group_id: String,
        node_id: String,
        device_id: String,
    },
    Host {
        host_id: String,
    },
}

/// A well-formed Sparkplug topic.
///
/// Build one through [`Namespace`], or parse one with [`Namespace::parse`].
/// There is no other way to make one, so holding a value of this type means
/// the topic is addressable.
///
/// The identity accessors return `Option` because the shape decides which
/// identities exist. A host topic has no group and no edge node; a node topic
/// has no device. Each one returns the identifier's own type, because the
/// segment passed its check when the topic was built. A caller passes the
/// value straight to another topic constructor, and handles no error that
/// cannot happen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SparkplugTopic {
    namespace: Arc<str>,
    message_type: MessageType,
    address: Address,
}

impl SparkplugTopic {
    /// Which segment layout this topic uses.
    #[must_use]
    pub fn shape(&self) -> Shape {
        self.message_type.shape()
    }

    /// What this message means in the session lifecycle.
    #[must_use]
    pub fn message_type(&self) -> MessageType {
        self.message_type
    }

    /// The namespace this topic belongs to.
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// The group, on node and device topics. `None` on a host topic.
    ///
    /// Read the text with [`GroupId::as_str`].
    #[must_use]
    pub fn group_id(&self) -> Option<GroupId<'_>> {
        match &self.address {
            Address::Node { group_id, .. } | Address::Device { group_id, .. } => {
                Some(GroupId::wrap_checked(group_id))
            }
            Address::Host { .. } => None,
        }
    }

    /// The edge node, on node and device topics. `None` on a host topic.
    ///
    /// Read the text with [`EdgeNodeId::as_str`].
    #[must_use]
    pub fn node_id(&self) -> Option<EdgeNodeId<'_>> {
        match &self.address {
            Address::Node { node_id, .. } | Address::Device { node_id, .. } => {
                Some(EdgeNodeId::wrap_checked(node_id))
            }
            Address::Host { .. } => None,
        }
    }

    /// The device, on device topics only.
    ///
    /// Read the text with [`DeviceId::as_str`].
    #[must_use]
    pub fn device_id(&self) -> Option<DeviceId<'_>> {
        match &self.address {
            Address::Device { device_id, .. } => Some(DeviceId::wrap_checked(device_id)),
            Address::Node { .. } | Address::Host { .. } => None,
        }
    }

    /// The host application, on host topics only.
    ///
    /// Read the text with [`HostId::as_str`].
    #[must_use]
    pub fn host_id(&self) -> Option<HostId<'_>> {
        match &self.address {
            Address::Host { host_id } => Some(HostId::wrap_checked(host_id)),
            Address::Node { .. } | Address::Device { .. } => None,
        }
    }
}

impl fmt::Display for SparkplugTopic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ns = &self.namespace;
        let mt = self.message_type;
        match &self.address {
            Address::Node { group_id, node_id } => {
                write!(f, "{ns}/{group_id}/{mt}/{node_id}")
            }
            Address::Device {
                group_id,
                node_id,
                device_id,
            } => write!(f, "{ns}/{group_id}/{mt}/{node_id}/{device_id}"),
            Address::Host { host_id } => write!(f, "{ns}/{mt}/{host_id}"),
        }
    }
}

fn wrong_shape(topic: &str, message_type: MessageType) -> SparkplugError {
    SparkplugError::WrongTopicShape {
        topic: topic.to_string(),
        message_type,
    }
}

#[cfg(test)]
mod tests;
