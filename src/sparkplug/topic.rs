use crate::error::SparkplugError;

use super::types::{MessageType, SparkplugTopic};

/// Parse a SparkplugB topic string into structured components.
///
/// The SparkPlug B topic namespace is:
/// ```text
/// spBv1.0/<group_id>/<message_type>/<edge_node_id>[/<device_id>]
/// ```
///
/// # Examples
/// ```
/// use sparkplug_mqtt::{parse_sparkplug_topic, MessageType};
///
/// // Node-level message (no device)
/// let t = parse_sparkplug_topic("spBv1.0/PlantFloor/NBIRTH/edge_node_1").unwrap();
/// assert_eq!(t.group_id, "PlantFloor");
/// assert_eq!(t.message_type, MessageType::NBIRTH);
/// assert_eq!(t.node_id, "edge_node_1");
/// assert_eq!(t.device_id, None);
///
/// // Device-level message
/// let t = parse_sparkplug_topic("spBv1.0/PlantFloor/DDATA/edge_node_1/pump_3").unwrap();
/// assert_eq!(t.message_type, MessageType::DDATA);
/// assert_eq!(t.node_id, "edge_node_1");
/// assert_eq!(t.device_id, Some("pump_3".to_string()));
/// ```
pub fn parse_sparkplug_topic(topic: &str) -> Result<SparkplugTopic, SparkplugError> {
    let parts: Vec<&str> = topic.split('/').collect();

    // Must have at least 4 parts and start with spBv1.0
    if parts.len() < 4 || parts[0] != "spBv1.0" {
        return Err(SparkplugError::InvalidTopic(topic.to_string()));
    }

    let message_type: MessageType = parts[2].parse()?;
    let group_id = parts[1].to_string();
    let node_id = parts[3].to_string();
    let device_id = parts.get(4).map(|s| (*s).to_string());

    Ok(SparkplugTopic {
        group_id,
        node_id,
        device_id,
        message_type,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_node_level_topic() {
        let t = parse_sparkplug_topic("spBv1.0/Group1/NBIRTH/node1")
            .expect("should parse node-level topic");
        assert_eq!(t.group_id, "Group1");
        assert_eq!(t.message_type, MessageType::NBIRTH);
        assert_eq!(t.node_id, "node1");
        assert_eq!(t.device_id, None);
    }

    #[test]
    fn parse_device_level_topic() {
        let t = parse_sparkplug_topic("spBv1.0/Group1/DDATA/node1/device1")
            .expect("should parse device-level topic");
        assert_eq!(t.group_id, "Group1");
        assert_eq!(t.message_type, MessageType::DDATA);
        assert_eq!(t.node_id, "node1");
        assert_eq!(t.device_id, Some("device1".to_string()));
    }

    #[test]
    fn parse_topic_missing_version_prefix() {
        let err = parse_sparkplug_topic("v2.0/Group1/NBIRTH/node1")
            .expect_err("should reject bad prefix");
        assert!(matches!(err, SparkplugError::InvalidTopic(_)));
    }

    #[test]
    fn parse_topic_too_few_segments() {
        let err =
            parse_sparkplug_topic("spBv1.0/Group1").expect_err("should reject too-few segments");
        assert!(matches!(err, SparkplugError::InvalidTopic(_)));
    }

    #[test]
    fn parse_topic_unknown_message_type() {
        let err = parse_sparkplug_topic("spBv1.0/Group1/XDATA/node1")
            .expect_err("should reject unknown type");
        assert!(matches!(err, SparkplugError::UnknownMessageType(_)));
    }
}
