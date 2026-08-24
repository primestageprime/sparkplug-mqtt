//! Unit tests for [`super`].
//!
//! They live in their own file so `topic.rs` stays under the module size
//! limit. Segment validation is tested in `ids.rs`, which now owns it.

use super::*;

fn ns() -> Namespace {
    Namespace::sparkplug_b()
}

fn group() -> GroupId<'static> {
    GroupId::new("G").expect("G is a usable group id")
}

fn node() -> EdgeNodeId<'static> {
    EdgeNodeId::new("n").expect("n is a usable edge node id")
}

fn device() -> DeviceId<'static> {
    DeviceId::new("d").expect("d is a usable device id")
}

/// Every message type, built through its own constructor.
///
/// None of these calls returns a `Result`. The identifiers were checked when
/// their types were built, so there is nothing left for a constructor to
/// reject.
fn one_of_each() -> Vec<(MessageType, SparkplugTopic)> {
    let ns = ns();
    vec![
        (MessageType::NBIRTH, ns.nbirth(group(), node())),
        (MessageType::NDATA, ns.ndata(group(), node())),
        (MessageType::NDEATH, ns.ndeath(group(), node())),
        (MessageType::NCMD, ns.ncmd(group(), node())),
        (MessageType::DBIRTH, ns.dbirth(group(), node(), device())),
        (MessageType::DDATA, ns.ddata(group(), node(), device())),
        (MessageType::DDEATH, ns.ddeath(group(), node(), device())),
        (MessageType::DCMD, ns.dcmd(group(), node(), device())),
        (
            MessageType::STATE,
            ns.state(HostId::new("host").expect("host is a usable host id")),
        ),
    ]
}

// ---------------------------------------------------------------------------
// Round trip
// ---------------------------------------------------------------------------

#[test]
fn every_message_type_survives_a_round_trip() {
    for (message_type, topic) in one_of_each() {
        let rendered = topic.to_string();
        let reparsed = ns().parse(&rendered).unwrap_or_else(|e| {
            panic!("{message_type} rendered as {rendered}, which failed to parse: {e}")
        });
        assert_eq!(
            reparsed, topic,
            "{message_type} did not survive a round trip"
        );
        assert_eq!(reparsed.to_string(), rendered);
    }
}

#[test]
fn constructors_render_the_documented_segment_order() {
    let ns = ns();
    let plant = GroupId::new("PlantFloor").unwrap();
    let edge = EdgeNodeId::new("edge_node_1").unwrap();
    let pump = DeviceId::new("pump_3").unwrap();

    assert_eq!(
        ns.ddata(plant, edge, pump).to_string(),
        "spBv1.0/PlantFloor/DDATA/edge_node_1/pump_3"
    );
    assert_eq!(
        ns.nbirth(plant, edge).to_string(),
        "spBv1.0/PlantFloor/NBIRTH/edge_node_1"
    );
    assert_eq!(
        ns.state(HostId::new("scada_1").unwrap()).to_string(),
        "spBv1.0/STATE/scada_1"
    );
}

#[test]
fn a_data_topic_keeps_the_edge_node_and_device_apart() {
    // Deployments usually set them equal, but the topic must carry both
    // independently — and the types stop a caller swapping them.
    let topic = ns().ddata(
        GroupId::new("Stax").unwrap(),
        EdgeNodeId::new("edge-node-A").unwrap(),
        DeviceId::new("device-1").unwrap(),
    );
    assert_eq!(topic.to_string(), "spBv1.0/Stax/DDATA/edge-node-A/device-1");
    assert_eq!(
        topic.node_id(),
        Some(EdgeNodeId::new("edge-node-A").unwrap())
    );
    assert_eq!(topic.device_id(), Some(DeviceId::new("device-1").unwrap()));
}

// ---------------------------------------------------------------------------
// Accessors
// ---------------------------------------------------------------------------

#[test]
fn a_node_topic_carries_no_device() {
    let topic = ns().nbirth(group(), node());
    assert_eq!(topic.shape(), Shape::Node);
    assert_eq!(topic.group_id(), Some(group()));
    assert_eq!(topic.node_id(), Some(node()));
    assert_eq!(topic.device_id(), None);
    assert_eq!(topic.host_id(), None);
}

#[test]
fn a_device_topic_carries_every_identity_but_the_host() {
    let topic = ns().ddata(group(), node(), device());
    assert_eq!(topic.shape(), Shape::Device);
    assert_eq!(topic.group_id(), Some(group()));
    assert_eq!(topic.node_id(), Some(node()));
    assert_eq!(topic.device_id(), Some(device()));
    assert_eq!(topic.host_id(), None);
}

#[test]
fn a_host_topic_carries_only_the_host() {
    let topic = ns().state(HostId::new("scada_1").unwrap());
    assert_eq!(topic.shape(), Shape::Host);
    assert_eq!(topic.host_id(), Some(HostId::new("scada_1").unwrap()));
    assert_eq!(topic.group_id(), None);
    assert_eq!(topic.node_id(), None);
    assert_eq!(topic.device_id(), None);
}

#[test]
fn a_topic_reports_the_namespace_it_belongs_to() {
    let ns = Namespace::new("spBv9.9").unwrap();
    let topic = ns.ndata(group(), node());
    assert_eq!(topic.namespace(), "spBv9.9");
    assert_eq!(topic.to_string(), "spBv9.9/G/NDATA/n");
}

// ---------------------------------------------------------------------------
// Parsing — rejection
// ---------------------------------------------------------------------------

#[test]
fn parse_rejects_another_namespace() {
    let err = ns()
        .parse("v2.0/G/NBIRTH/n")
        .expect_err("a foreign namespace is not ours to read");
    assert!(matches!(err, SparkplugError::InvalidTopic(_)));
}

#[test]
fn parse_rejects_too_few_segments() {
    for topic in ["spBv1.0", "spBv1.0/G"] {
        let err = ns().parse(topic).unwrap_err();
        assert!(
            matches!(err, SparkplugError::InvalidTopic(_)),
            "{topic} addresses nothing"
        );
    }
}

#[test]
fn parse_rejects_an_unknown_message_type() {
    let err = ns().parse("spBv1.0/G/XDATA/n").unwrap_err();
    assert!(matches!(err, SparkplugError::UnknownMessageType(_)));
}

#[test]
fn parse_rejects_a_device_segment_on_a_node_message() {
    let err = ns()
        .parse("spBv1.0/G/NBIRTH/n/d")
        .expect_err("an NBIRTH is node-shaped");
    assert!(matches!(
        err,
        SparkplugError::WrongTopicShape {
            message_type: MessageType::NBIRTH,
            ..
        }
    ));
}

#[test]
fn parse_rejects_a_device_message_with_no_device() {
    let err = ns()
        .parse("spBv1.0/G/DDATA/n")
        .expect_err("a DDATA must name its device");
    assert!(matches!(
        err,
        SparkplugError::WrongTopicShape {
            message_type: MessageType::DDATA,
            ..
        }
    ));
}

#[test]
fn parse_rejects_a_state_topic_of_the_wrong_length() {
    for topic in ["spBv1.0/STATE", "spBv1.0/STATE/host/extra"] {
        let err = ns().parse(topic).unwrap_err();
        assert!(
            matches!(
                err,
                SparkplugError::WrongTopicShape {
                    message_type: MessageType::STATE,
                    ..
                }
            ),
            "{topic} is not host-shaped"
        );
    }
}

#[test]
fn parse_rejects_a_state_message_addressed_like_a_node() {
    // STATE names its type in the second segment. A group there means the
    // publisher used the wrong shape.
    let err = ns().parse("spBv1.0/G/STATE/n").unwrap_err();
    assert!(matches!(
        err,
        SparkplugError::WrongTopicShape {
            message_type: MessageType::STATE,
            ..
        }
    ));
}

#[test]
fn parse_rejects_an_empty_segment() {
    let err = ns().parse("spBv1.0//NBIRTH/n").unwrap_err();
    assert!(matches!(err, SparkplugError::InvalidTopic(_)));
}

// ---------------------------------------------------------------------------
// Namespace
// ---------------------------------------------------------------------------

#[test]
fn the_standard_namespace_is_spbv1() {
    assert_eq!(Namespace::sparkplug_b().as_str(), "spBv1.0");
    assert_eq!(Namespace::default(), Namespace::sparkplug_b());
}

#[test]
fn namespace_rejects_a_string_no_broker_would_accept() {
    for bad in ["", "a/b", "a+b", "a#b"] {
        let err = Namespace::new(bad).unwrap_err();
        assert!(
            matches!(err, SparkplugError::InvalidNamespace(_)),
            "namespace {bad:?} must be rejected"
        );
    }
}

#[test]
fn a_namespace_does_not_parse_another_namespaces_topics() {
    let ours = Namespace::new("spBv9.9").unwrap();
    assert!(ours.parse("spBv1.0/G/NBIRTH/n").is_err());
    assert!(ours.parse("spBv9.9/G/NBIRTH/n").is_ok());
}
