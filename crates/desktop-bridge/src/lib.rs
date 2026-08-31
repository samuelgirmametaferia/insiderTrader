//! Native control-plane bridge for the authenticated engine command service.
//!
//! Terminal and unattended local clients use the same Unix transport;
//! this crate contains no trading logic and cannot bypass engine validation.

#![forbid(unsafe_code)]

use std::sync::Arc;

use insider_engine::EngineCommandService;
use insider_ipc::{CommandResponse, UnixServerError, UnixSocketServer};

/// Bridge transport or command-dispatch failure.
#[cfg(unix)]
#[derive(Debug)]
pub enum BridgeError {
    /// Unix socket transport failed.
    Transport(UnixServerError),
}

/// Bounded native bridge serving one authenticated engine service.
#[cfg(unix)]
pub struct ControlPlaneBridge {
    server: UnixSocketServer,
    service: Arc<EngineCommandService>,
}

#[cfg(unix)]
impl ControlPlaneBridge {
    /// Binds an owner-only Unix socket for local terminal clients.
    ///
    /// # Errors
    /// Returns [`BridgeError::Transport`] when the socket cannot be bound.
    pub fn bind(
        path: impl AsRef<std::path::Path>,
        service: Arc<EngineCommandService>,
        max_payload_bytes: usize,
    ) -> Result<Self, BridgeError> {
        let server =
            UnixSocketServer::bind(path, max_payload_bytes).map_err(BridgeError::Transport)?;
        Ok(Self { server, service })
    }

    /// Accepts one client and serves it until EOF or a bounded protocol error.
    /// The caller's supervisor controls restart/backoff and graceful shutdown.
    ///
    /// # Errors
    /// Returns [`BridgeError::Transport`] for socket/framing/dispatch failures.
    pub fn serve_next(&self) -> Result<(), BridgeError> {
        self.server
            .serve_next(|command| {
                self.service
                    .dispatch(command)
                    .map_err(|error| format!("dispatch: {error:?}"))
            })
            .map_err(BridgeError::Transport)
    }
}

/// Converts a command response into an owned payload for native clients.
#[must_use]
pub fn response_parts(response: CommandResponse) -> (u64, Vec<u8>) {
    (response.state_version, response.payload)
}
