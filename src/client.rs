//! MQTT connection setup.
//!
//! This module owns the options a Sparkplug connection needs and hands back
//! the two `rumqttc` halves. [`crate::SparkplugClient`] drives them for you.
//! Call [`mqtt_parts`] directly only when you run your own event loop.

use rumqttc::Transport;
use rumqttc::{AsyncClient, EventLoop, MqttOptions, TlsConfiguration};
use rustls::ClientConfig;
use std::sync::Arc;
use std::time::Duration;

use crate::error::SparkplugError;

/// Creates a TLS transport using system root certificates.
/// Use this for connecting to brokers over TLS (port 8883).
///
/// # Errors
///
/// Returns `SparkplugError::Connection` if no trusted root certificates are found.
pub fn tls_transport() -> Result<Transport, SparkplugError> {
    let result = rustls_native_certs::load_native_certs();
    for e in &result.errors {
        tracing::warn!("error loading native certificates: {e}");
    }

    let mut root_store = rustls::RootCertStore::empty();
    for cert in result.certs {
        if let Err(e) = root_store.add(cert) {
            tracing::warn!("skipping invalid system certificate: {e}");
        }
    }
    if root_store.is_empty() {
        return Err(SparkplugError::Connection(
            "no trusted root certificates found".to_owned(),
        ));
    }

    let client_config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    Ok(Transport::Tls(TlsConfiguration::Rustls(Arc::new(
        client_config,
    ))))
}

/// What one MQTT connection needs.
///
/// This names the connection alone. It carries no Sparkplug identity: a
/// client takes the edge node it speaks for at connect, through
/// [`crate::SparkplugClient::connect_as_edge_node`], and a publisher client
/// takes the edge node of each publish per call.
#[derive(Clone)]
pub struct MqttConfig {
    pub broker_url: String,
    pub broker_port: u16,
    pub transport: Transport,
    pub username: String,
    pub password: String,
    /// The MQTT-level identifier of this connection.
    ///
    /// A broker treats two connections that share a client id as a
    /// takeover, so give each connection of one process its own value. Leave
    /// this `None` to take a generated one — see [`generate_client_id`].
    pub client_id: Option<String>,
    pub version: String,
}

impl std::fmt::Debug for MqttConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MqttConfig")
            .field("broker_url", &self.broker_url)
            .field("broker_port", &self.broker_port)
            .field("username", &self.username)
            .field("password", &"[REDACTED]")
            .field("client_id", &self.client_id)
            .field("version", &self.version)
            .finish_non_exhaustive()
    }
}

/// Generate a client id in the format `{version}_{random 32-bit hex}`.
///
/// [`mqtt_options`] calls this when [`MqttConfig::client_id`] is `None`. A
/// broker treats two connections that share a client id as a takeover, and
/// it disconnects the earlier session, which the event loop then reconnects.
/// The suffix draws one of 2^32 values to keep the connections of one
/// process apart. `version` is the same string for every caller, so the
/// suffix carries all of the difference. Set
/// [`MqttConfig::client_id`] to name a connection yourself.
///
/// A client id names an MQTT connection only. It names no edge node.
#[must_use]
pub fn generate_client_id(version: &str) -> String {
    format!("{version}_{:08x}", rand::random::<u32>())
}

/// Build the MQTT options a Sparkplug connection needs.
///
/// `clean_session` is `true` because Sparkplug requires it. That setting is
/// load-bearing: it is why a reconnect discards queued publishes, which is
/// why [`crate::SparkplugClient::flush`] exists. Do not change it.
///
/// Split out from [`mqtt_parts`] so a test can read the settings back
/// without opening a socket.
#[must_use]
pub fn mqtt_options(config: &MqttConfig) -> MqttOptions {
    let client_id = config
        .client_id
        .clone()
        .unwrap_or_else(|| generate_client_id(&config.version));
    let mut options = MqttOptions::new(client_id, &config.broker_url, config.broker_port);

    options.set_credentials(&config.username, &config.password);
    options.set_keep_alive(Duration::from_secs(15));
    options.set_clean_session(true);
    options.set_max_packet_size(10 * 1024 * 1024, 10 * 1024 * 1024);
    options.set_inflight(100);
    options.set_manual_acks(false);
    options.set_pending_throttle(Duration::from_millis(100));
    options.set_transport(config.transport.clone());

    options
}

/// Build the two halves of an MQTT connection.
///
/// The caller owns both: publish through the [`AsyncClient`], and poll the
/// [`EventLoop`] so the connection makes progress. Neither half works
/// without the other.
///
/// Prefer [`crate::SparkplugClient::connect`], which drives the event loop
/// for you and tracks delivery. Use this only when you run your own loop.
#[must_use]
pub fn mqtt_parts(config: &MqttConfig) -> (AsyncClient, EventLoop) {
    AsyncClient::new(mqtt_options(config), 10)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> MqttConfig {
        MqttConfig {
            broker_url: "localhost".to_owned(),
            broker_port: 1883,
            transport: Transport::Tcp,
            username: String::new(),
            password: String::new(),
            client_id: None,
            version: "spBv1.0".to_owned(),
        }
    }

    #[test]
    fn mqtt_config_construction() {
        let config = config();
        assert_eq!(config.broker_port, 1883);
        assert_eq!(config.version, "spBv1.0");
    }

    #[test]
    fn client_id_format() {
        let id = generate_client_id("spBv1.0");
        assert!(id.starts_with("spBv1.0_"));
        let suffix: &str = id
            .rsplit('_')
            .next()
            .expect("client ID should contain underscores");
        assert_eq!(suffix.len(), 8, "the suffix holds a 32-bit value in hex");
        assert!(
            u32::from_str_radix(suffix, 16).is_ok(),
            "the suffix must read as hex, got {suffix:?}"
        );
    }

    #[test]
    fn mqtt_config_debug_redacts_password() {
        let mut config = config();
        config.username = "admin".to_owned();
        config.password = "super_secret_password".to_owned();

        let debug_output = format!("{config:?}");
        assert!(
            debug_output.contains("[REDACTED]"),
            "password should be redacted in Debug output"
        );
        assert!(
            !debug_output.contains("super_secret_password"),
            "actual password must not appear in Debug output"
        );
        assert!(
            debug_output.contains("localhost"),
            "other fields should be visible"
        );
    }

    #[test]
    fn sparkplug_requires_a_clean_session() {
        assert!(
            mqtt_options(&config()).clean_session(),
            "Sparkplug requires clean_session = true; flush reports the \
             publishes a reconnect discards because of it"
        );
    }

    #[test]
    fn the_options_carry_the_generated_client_id() {
        let options = mqtt_options(&config());
        assert!(options.client_id().starts_with("spBv1.0_"));
    }

    #[test]
    fn the_options_carry_the_client_id_the_caller_gives() {
        let config = MqttConfig {
            client_id: Some("stax-publisher-7".to_owned()),
            ..config()
        };
        assert_eq!(
            mqtt_options(&config).client_id(),
            "stax-publisher-7",
            "a caller that names its connection must reach the broker under \
             that name"
        );
    }
}
