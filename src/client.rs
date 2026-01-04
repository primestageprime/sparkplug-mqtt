use rumqttc::Transport;
use rumqttc::{AsyncClient, EventLoop, MqttOptions, QoS, TlsConfiguration};
use rustls::ClientConfig;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

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

    Ok(Transport::Tls(TlsConfiguration::Rustls(Arc::new(client_config))))
}

pub type MessageCallback = Box<dyn Fn(String, Vec<u8>) + Send + Sync>;

#[derive(Clone)]
pub struct MqttConfig {
    pub broker_url: String,
    pub broker_port: u16,
    pub transport: Transport,
    pub username: String,
    pub password: String,
    pub group_id: String, // default group id
    pub node_id: String,
    pub version: String,
}

impl std::fmt::Debug for MqttConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MqttConfig")
            .field("broker_url", &self.broker_url)
            .field("broker_port", &self.broker_port)
            .field("username", &self.username)
            .field("password", &"[REDACTED]")
            .field("group_id", &self.group_id)
            .field("node_id", &self.node_id)
            .field("version", &self.version)
            .finish_non_exhaustive()
    }
}

/// Generate a client ID in the format `{version}_{group}_{node}_{random 1000–9999}`.
#[must_use]
pub fn generate_client_id(config: &MqttConfig) -> String {
    format!(
        "{}_{}_{}_{}",
        &config.version,
        &config.group_id,
        &config.node_id,
        rand::random::<u16>() % 9000 + 1000
    )
}

/// Maximum consecutive event-loop errors before escalating from warn to error.
const MAX_CONSECUTIVE_ERRORS: u32 = 5;

#[derive(Clone)]
pub struct MqttClient {
    client: Arc<AsyncClient>,
    config: Arc<MqttConfig>,
    message_callback: Arc<Mutex<Option<MessageCallback>>>,
}

#[derive(Clone)]
pub struct MqttClientManager {
    clients: Arc<Mutex<HashMap<String, Arc<MqttClient>>>>,
    base_config: MqttConfig,
}

impl MqttClientManager {
    pub fn new(base_config: MqttConfig) -> Self {
        Self {
            clients: Arc::new(Mutex::new(HashMap::new())),
            base_config,
        }
    }

    pub async fn get_or_create_client(
        &self,
        group_id: &str,
    ) -> Result<Arc<MqttClient>, SparkplugError> {
        let mut clients = self.clients.lock().await;

        if let Some(client) = clients.get(group_id) {
            return Ok(client.clone());
        }

        // Create new client for this group
        let mut config = self.base_config.clone();
        config.group_id = group_id.to_string();

        let (client, mut eventloop) = MqttClient::new(config);
        let client = Arc::new(client);

        // Clone group_id for both the task and the HashMap insertion
        let group_id_for_task = group_id.to_string();
        let group_id_for_map = group_id.to_string();

        // Create a channel to signal when the client is connected
        let (tx, rx) = tokio::sync::oneshot::channel();
        let tx = Arc::new(tokio::sync::Mutex::new(Some(tx)));

        // Spawn event loop for this client
        let tx_clone = tx.clone();
        let client_for_messages = client.clone();
        let handle = tokio::spawn(async move {
            tracing::info!("Starting MQTT event loop for group {}", group_id_for_task);
            let mut consecutive_errors: u32 = 0;
            loop {
                match eventloop.poll().await {
                    Ok(rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(_))) => {
                        tracing::info!(
                            "MQTT client connected for group {}",
                            group_id_for_task
                        );
                        consecutive_errors = 0;
                        // Signal that we're connected
                        if let Some(tx) = tx_clone.lock().await.take() {
                            let _ = tx.send(());
                        }
                    }
                    Ok(rumqttc::Event::Incoming(rumqttc::Packet::Disconnect)) => {
                        tracing::warn!(
                            "MQTT client disconnected for group {}",
                            group_id_for_task
                        );
                    }
                    Ok(rumqttc::Event::Incoming(rumqttc::Packet::Publish(publish))) => {
                        consecutive_errors = 0;
                        tracing::debug!(
                            "Received MQTT message on topic: {} ({} bytes)",
                            publish.topic,
                            publish.payload.len()
                        );

                        // Call the message callback if set
                        if let Some(ref callback) =
                            *client_for_messages.message_callback.lock().await
                        {
                            callback(publish.topic, publish.payload.to_vec());
                        }
                    }
                    Ok(_) => {
                        consecutive_errors = 0;
                        continue;
                    }
                    Err(e) => {
                        consecutive_errors += 1;
                        if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                            tracing::error!(
                                "MQTT event loop error for group {} ({} consecutive): {}",
                                group_id_for_task,
                                consecutive_errors,
                                e
                            );
                        } else {
                            tracing::warn!(
                                "MQTT event loop error for group {} ({} consecutive): {}",
                                group_id_for_task,
                                consecutive_errors,
                                e
                            );
                        }
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                }
            }
        });

        // Wait for initial connection with timeout
        let timeout_duration = Duration::from_secs(5);
        match tokio::time::timeout(timeout_duration, rx).await {
            Ok(_) => {
                tracing::info!(
                    "MQTT client connected successfully for group {}",
                    group_id
                );
            }
            Err(_) => {
                handle.abort();
                return Err(SparkplugError::ConnectionTimeout(timeout_duration));
            }
        }

        clients.insert(group_id_for_map, client.clone());
        Ok(client)
    }

    pub async fn remove_client(&self, group_id: &str) {
        let mut clients = self.clients.lock().await;
        clients.remove(group_id);
    }
}

impl MqttClient {
    pub fn new(config: MqttConfig) -> (Self, EventLoop) {
        let client_id = generate_client_id(&config);
        let mut mqtt_options =
            MqttOptions::new(client_id, &config.broker_url, config.broker_port);

        mqtt_options.set_credentials(&config.username, &config.password);

        // Set MQTT options for better reliability
        mqtt_options.set_keep_alive(Duration::from_secs(15));
        mqtt_options.set_clean_session(true);
        mqtt_options.set_max_packet_size(10 * 1024 * 1024, 10 * 1024 * 1024);
        mqtt_options.set_inflight(100);
        mqtt_options.set_manual_acks(false);
        mqtt_options.set_pending_throttle(Duration::from_millis(100));

        // Set up transport — rumqttc accepts any Transport variant directly
        mqtt_options.set_transport(config.transport.clone());

        let (client, eventloop) = AsyncClient::new(mqtt_options, 10);
        let client = Arc::new(client);
        let config = Arc::new(config);
        let message_callback = Arc::new(Mutex::new(None));

        (
            Self {
                client,
                config,
                message_callback,
            },
            eventloop,
        )
    }

    pub async fn subscribe(&self, topic: &str) -> Result<(), SparkplugError> {
        self.client.subscribe(topic, QoS::AtLeastOnce).await?;
        Ok(())
    }

    pub async fn publish(&self, topic: &str, payload: &[u8]) -> Result<(), SparkplugError> {
        self.client
            .publish(topic, QoS::AtLeastOnce, false, payload.to_vec())
            .await?;
        Ok(())
    }

    #[must_use]
    pub fn version(&self) -> &str {
        &self.config.version
    }

    #[must_use]
    pub fn node_id(&self) -> &str {
        &self.config.node_id
    }

    #[must_use]
    pub fn config(&self) -> &MqttConfig {
        &self.config
    }

    pub async fn set_message_callback(&self, callback: MessageCallback) {
        *self.message_callback.lock().await = Some(callback);
    }

    /// Consume this client wrapper and return the underlying `AsyncClient`.
    #[must_use]
    pub fn into_async_client(self) -> rumqttc::AsyncClient {
        (*self.client).clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mqtt_config_construction() {
        let config = MqttConfig {
            broker_url: "localhost".to_owned(),
            broker_port: 1883,
            transport: Transport::Tcp,
            username: String::new(),
            password: String::new(),
            group_id: "test".to_owned(),
            node_id: "node1".to_owned(),
            version: "spBv1.0".to_owned(),
        };
        assert_eq!(config.broker_port, 1883);
        assert_eq!(config.version, "spBv1.0");
    }

    #[test]
    fn client_id_format() {
        let config = MqttConfig {
            broker_url: "localhost".to_owned(),
            broker_port: 1883,
            transport: Transport::Tcp,
            username: String::new(),
            password: String::new(),
            group_id: "MyGroup".to_owned(),
            node_id: "node1".to_owned(),
            version: "spBv1.0".to_owned(),
        };
        let id = generate_client_id(&config);
        assert!(id.starts_with("spBv1.0_MyGroup_node1_"));
        let suffix: &str = id.rsplit('_').next().expect("client ID should contain underscores");
        let num: u16 = suffix.parse().expect("suffix should be a number");
        assert!((1000..=9999).contains(&num));
    }

    #[test]
    fn mqtt_config_debug_redacts_password() {
        let config = MqttConfig {
            broker_url: "localhost".to_owned(),
            broker_port: 1883,
            transport: Transport::Tcp,
            username: "admin".to_owned(),
            password: "super_secret_password".to_owned(),
            group_id: "test".to_owned(),
            node_id: "node1".to_owned(),
            version: "spBv1.0".to_owned(),
        };
        let debug_output = format!("{:?}", config);
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
}
