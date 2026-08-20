//! What [`crate::SparkplugClient::shutdown`] learns from its DISCONNECT.
//!
//! Split from `client.rs` so that file stays under the module size limit.

use std::time::Duration;

use crate::error::SparkplugError;

use crate::sparkplug::Health;

/// Wait until the event loop reports the link is down, or `deadline` passes.
///
/// The loop sets [`Health::Disconnected`] once it writes the DISCONNECT
/// packet, so this waits on the packet rather than on a fixed delay.
/// Returns `false` when the deadline arrives first, which means the packet
/// is unconfirmed. A link that is already down reports `true` at once,
/// because a dead link needs no DISCONNECT.
pub(super) async fn wait_for_disconnect(
    mut health: tokio::sync::watch::Receiver<Health>,
    deadline: tokio::time::Instant,
) -> bool {
    let down = health.wait_for(|state| *state == Health::Disconnected);
    tokio::time::timeout_at(deadline, down)
        .await
        .is_ok_and(|state| state.is_ok())
}

/// What [`crate::SparkplugClient::shutdown`] learns from its DISCONNECT attempt.
#[derive(Debug)]
pub(super) enum Disconnected {
    /// The event loop wrote the DISCONNECT packet.
    Written,
    /// The deadline arrived before the event loop confirmed the packet.
    Unconfirmed,
    /// The client refused the request, so no packet can go out.
    Refused(rumqttc::ClientError),
}

/// Read the DISCONNECT request, then wait for the event loop to write it.
///
/// `requested` holds `Err` when the deadline beat the request, `Ok(Err)`
/// when the client refused it, and `Ok(Ok)` when the request went through.
pub(super) async fn disconnect_outcome(
    requested: Result<Result<(), rumqttc::ClientError>, tokio::time::error::Elapsed>,
    health: tokio::sync::watch::Receiver<Health>,
    deadline: tokio::time::Instant,
) -> Disconnected {
    match requested {
        Err(_elapsed) => Disconnected::Unconfirmed,
        Ok(Err(error)) => {
            tracing::warn!(error = %error, "the client refused the DISCONNECT request");
            Disconnected::Refused(error)
        }
        Ok(Ok(())) => match wait_for_disconnect(health, deadline).await {
            true => Disconnected::Written,
            false => Disconnected::Unconfirmed,
        },
    }
}

/// Choose what [`crate::SparkplugClient::shutdown`] reports.
///
/// A lost publish outranks a failed DISCONNECT, because the caller must
/// publish those messages again. A refusal outranks the deadline, because
/// it names the cause instead of the symptom.
pub(super) fn shutdown_outcome(
    flushed: Result<(), SparkplugError>,
    disconnected: Disconnected,
    timeout: Duration,
) -> Result<(), SparkplugError> {
    match (flushed, disconnected) {
        (Err(error), _) => Err(error),
        (Ok(()), Disconnected::Written) => Ok(()),
        (Ok(()), Disconnected::Refused(error)) => Err(SparkplugError::Disconnect(error)),
        (Ok(()), Disconnected::Unconfirmed) => {
            Err(SparkplugError::ShutdownTimeout { after: timeout })
        }
    }
}
