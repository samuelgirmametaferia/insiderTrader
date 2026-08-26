//! Bounded length-prefixed IPC framing for local and future remote transports.

#![forbid(unsafe_code)]

/// Framing failures.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FrameError {
    /// The configured frame limit is invalid.
    InvalidLimit,
    /// The frame exceeds the configured maximum.
    TooLarge(usize),
    /// The input is shorter than a complete frame.
    Incomplete,
    /// The declared length does not match the supplied bytes.
    LengthMismatch {
        /// Payload length encoded in the prefix.
        declared: usize,
        /// Number of payload bytes supplied.
        actual: usize,
    },
}

/// Typed local-bridge message kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MessageKind {
    /// Client request requiring one response.
    Request,
    /// Server response correlated to one request.
    Response,
    /// Unsolicited runtime event.
    Event,
}

/// Validated message envelope carried inside a frame.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Envelope {
    /// Protocol version negotiated by both ends.
    pub protocol_version: u16,
    /// Message kind.
    pub kind: MessageKind,
    /// Correlation ID; events may use a stable event ID.
    pub request_id: String,
    /// Trace ID used to reconstruct decisions.
    pub trace_id: String,
    /// Opaque versioned payload bytes.
    pub payload: Vec<u8>,
}

const MAX_CORRELATION_ID_BYTES: usize = 256;
const MAX_ACTOR_BYTES: usize = 128;

impl Envelope {
    /// Validates envelope metadata and payload bound.
    ///
    /// # Errors
    /// Returns `FrameError` when metadata is blank or payload exceeds the limit.
    pub fn validate(&self, max_payload_bytes: usize) -> Result<(), FrameError> {
        if self.protocol_version == 0
            || self.request_id.trim().is_empty()
            || self.trace_id.trim().is_empty()
        {
            return Err(FrameError::Incomplete);
        }
        if self.request_id.len() > MAX_CORRELATION_ID_BYTES
            || self.trace_id.len() > MAX_CORRELATION_ID_BYTES
        {
            return Err(FrameError::TooLarge(
                self.request_id.len().max(self.trace_id.len()),
            ));
        }
        if self.payload.len() > max_payload_bytes {
            return Err(FrameError::TooLarge(self.payload.len()));
        }
        Ok(())
    }
}

/// Command metadata carried over the control-plane IPC boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandEnvelope {
    /// Durable command identity used for idempotency/audit.
    pub command_id: String,
    /// Trace identity propagated through the runtime.
    pub trace_id: String,
    /// Authenticated local session/actor identity.
    pub actor: String,
    /// Wall-clock issue timestamp supplied by the client.
    pub issued_wall_ns: u64,
    /// Optimistic-concurrency version expected by the command.
    pub expected_state_version: u64,
    /// Stable retry key; duplicate keys must return the original result.
    pub idempotency_key: String,
    /// Versioned command payload.
    pub payload: Vec<u8>,
}

/// Command authorization failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AuthorizationError {
    /// Actor/session identity is blank or unknown.
    UnknownActor,
    /// Capability name is blank.
    InvalidCapability,
    /// Actor exists but lacks the requested capability.
    Denied,
}

/// In-memory least-privilege capability policy for the local command bridge.
///
/// A production composition root can populate this policy from the authenticated
/// session manager. The UI receives no capability secrets; it only receives the
/// typed denial result.
#[derive(Clone, Debug, Default)]
pub struct CapabilityPolicy {
    grants: std::collections::BTreeMap<String, std::collections::BTreeSet<String>>,
}

impl CapabilityPolicy {
    /// Creates an empty deny-by-default policy.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Grants one named capability to an actor.
    ///
    /// # Errors
    /// Returns [`AuthorizationError`] when either identity is blank.
    pub fn grant(&mut self, actor: &str, capability: &str) -> Result<(), AuthorizationError> {
        if actor.trim().is_empty() {
            return Err(AuthorizationError::UnknownActor);
        }
        if capability.trim().is_empty() {
            return Err(AuthorizationError::InvalidCapability);
        }
        self.grants
            .entry(actor.to_owned())
            .or_default()
            .insert(capability.to_owned());
        Ok(())
    }

    /// Revokes one capability; absent grants are harmless and idempotent.
    pub fn revoke(&mut self, actor: &str, capability: &str) {
        if let Some(capabilities) = self.grants.get_mut(actor) {
            capabilities.remove(capability);
            if capabilities.is_empty() {
                self.grants.remove(actor);
            }
        }
    }

    /// Authorizes a command actor for one capability.
    ///
    /// # Errors
    /// Returns [`AuthorizationError::Denied`] unless an exact grant exists.
    pub fn authorize(&self, actor: &str, capability: &str) -> Result<(), AuthorizationError> {
        if actor.trim().is_empty() {
            return Err(AuthorizationError::UnknownActor);
        }
        if capability.trim().is_empty() {
            return Err(AuthorizationError::InvalidCapability);
        }
        if self
            .grants
            .get(actor)
            .is_some_and(|capabilities| capabilities.contains(capability))
        {
            Ok(())
        } else {
            Err(AuthorizationError::Denied)
        }
    }
}

impl CommandEnvelope {
    /// Validates command identity and payload bounds.
    ///
    /// # Errors
    /// Returns [`FrameError`] when required metadata is blank or exceeds a
    /// configured bound.
    pub fn validate(&self, max_payload_bytes: usize) -> Result<(), FrameError> {
        for value in [
            &self.command_id,
            &self.trace_id,
            &self.actor,
            &self.idempotency_key,
        ] {
            if value.trim().is_empty() {
                return Err(FrameError::Incomplete);
            }
        }
        if self.command_id.len() > MAX_CORRELATION_ID_BYTES
            || self.trace_id.len() > MAX_CORRELATION_ID_BYTES
            || self.idempotency_key.len() > MAX_CORRELATION_ID_BYTES
            || self.actor.len() > MAX_ACTOR_BYTES
        {
            return Err(FrameError::TooLarge(
                self.command_id
                    .len()
                    .max(self.trace_id.len())
                    .max(self.idempotency_key.len())
                    .max(self.actor.len()),
            ));
        }
        if self.payload.len() > max_payload_bytes {
            return Err(FrameError::TooLarge(self.payload.len()));
        }
        if self.issued_wall_ns == 0 {
            return Err(FrameError::Incomplete);
        }
        Ok(())
    }
}

/// Deterministic bounded codec for command envelopes inside a framed socket.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommandCodec {
    max_payload_bytes: usize,
}

impl CommandCodec {
    /// Creates a codec with a bounded command payload.
    #[must_use]
    pub const fn new(max_payload_bytes: usize) -> Option<Self> {
        if max_payload_bytes > 0 && max_payload_bytes <= u32::MAX as usize {
            Some(Self { max_payload_bytes })
        } else {
            None
        }
    }

    /// Encodes one command envelope without allocation based on untrusted lengths.
    ///
    /// # Errors
    /// Returns [`FrameError`] when validation fails or a field exceeds its
    /// fixed-width wire length.
    pub fn encode(&self, command: &CommandEnvelope) -> Result<Vec<u8>, FrameError> {
        command.validate(self.max_payload_bytes)?;
        let mut output = Vec::with_capacity(command.payload.len().saturating_add(64));
        put_string(&mut output, &command.command_id)?;
        put_string(&mut output, &command.trace_id)?;
        put_string(&mut output, &command.actor)?;
        output.extend_from_slice(&command.issued_wall_ns.to_le_bytes());
        output.extend_from_slice(&command.expected_state_version.to_le_bytes());
        put_string(&mut output, &command.idempotency_key)?;
        let payload_len = u32::try_from(command.payload.len())
            .map_err(|_| FrameError::TooLarge(command.payload.len()))?;
        output.extend_from_slice(&payload_len.to_le_bytes());
        output.extend_from_slice(&command.payload);
        Ok(output)
    }

    /// Decodes one complete command envelope and rejects trailing bytes.
    ///
    /// # Errors
    /// Returns [`FrameError`] for malformed lengths, missing fields, or an
    /// oversized payload.
    pub fn decode(&self, bytes: &[u8]) -> Result<CommandEnvelope, FrameError> {
        let mut cursor = 0_usize;
        let command = CommandEnvelope {
            command_id: take_string(bytes, &mut cursor, MAX_CORRELATION_ID_BYTES)?,
            trace_id: take_string(bytes, &mut cursor, MAX_CORRELATION_ID_BYTES)?,
            actor: take_string(bytes, &mut cursor, MAX_ACTOR_BYTES)?,
            issued_wall_ns: take_u64(bytes, &mut cursor)?,
            expected_state_version: take_u64(bytes, &mut cursor)?,
            idempotency_key: take_string(bytes, &mut cursor, MAX_CORRELATION_ID_BYTES)?,
            payload: take_bytes(bytes, &mut cursor, self.max_payload_bytes)?,
        };
        if cursor != bytes.len() {
            return Err(FrameError::LengthMismatch {
                declared: cursor,
                actual: bytes.len(),
            });
        }
        command.validate(self.max_payload_bytes)?;
        Ok(command)
    }
}

/// Result returned by an authorized command handler.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandResponse {
    /// State version observed after the handler completed.
    pub state_version: u64,
    /// Versioned response payload.
    pub payload: Vec<u8>,
}

/// Deterministic codec for a command response body.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResponseCodec {
    max_payload_bytes: usize,
}

impl ResponseCodec {
    /// Creates a response codec with a bounded payload.
    #[must_use]
    pub const fn new(max_payload_bytes: usize) -> Option<Self> {
        if max_payload_bytes > 0 && max_payload_bytes <= u32::MAX as usize {
            Some(Self { max_payload_bytes })
        } else {
            None
        }
    }

    /// Encodes the committed state version and response payload.
    ///
    /// # Errors
    /// Returns [`FrameError::TooLarge`] when the response exceeds the bound.
    pub fn encode(&self, response: &CommandResponse) -> Result<Vec<u8>, FrameError> {
        if response.payload.len() > self.max_payload_bytes {
            return Err(FrameError::TooLarge(response.payload.len()));
        }
        let length = u32::try_from(response.payload.len())
            .map_err(|_| FrameError::TooLarge(response.payload.len()))?;
        let mut output = Vec::with_capacity(response.payload.len().saturating_add(12));
        output.extend_from_slice(&response.state_version.to_le_bytes());
        output.extend_from_slice(&length.to_le_bytes());
        output.extend_from_slice(&response.payload);
        Ok(output)
    }

    /// Decodes exactly one response body.
    ///
    /// # Errors
    /// Returns [`FrameError`] for malformed lengths or oversized payloads.
    pub fn decode(&self, bytes: &[u8]) -> Result<CommandResponse, FrameError> {
        if bytes.len() < 12 {
            return Err(FrameError::Incomplete);
        }
        let state_version = u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]);
        let declared = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]) as usize;
        if declared > self.max_payload_bytes {
            return Err(FrameError::TooLarge(declared));
        }
        let actual = bytes.len().saturating_sub(12);
        if declared != actual {
            return Err(FrameError::LengthMismatch { declared, actual });
        }
        Ok(CommandResponse {
            state_version,
            payload: bytes[12..].to_vec(),
        })
    }
}

/// Dispatch failure at the authenticated command boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DispatchError {
    /// The command envelope failed structural validation.
    Invalid(FrameError),
    /// The actor lacks the requested capability.
    Unauthorized(AuthorizationError),
    /// The command was issued against an old optimistic-concurrency version.
    StaleState {
        /// Version supplied by the caller.
        expected: u64,
        /// Authoritative version observed by the dispatcher.
        current: u64,
    },
    /// An idempotency key was reused with a different command body.
    IdempotencyConflict,
    /// The handler rejected the command.
    Handler(String),
}

#[derive(Clone, Debug)]
struct CachedCommand {
    command: CommandEnvelope,
    response: CommandResponse,
}

/// Bounded, authorization-aware command dispatcher for local or remote transports.
///
/// The dispatcher is deliberately transport-agnostic: a Unix socket, Tauri
/// bridge, or future remote server can decode the same [`CommandEnvelope`] and
/// call this boundary. Successful responses are cached by actor/idempotency key,
/// so retries return the original result without invoking a broker twice.
#[derive(Clone, Debug)]
pub struct CommandDispatcher {
    policy: CapabilityPolicy,
    cache_capacity: usize,
    cache: std::collections::BTreeMap<String, CachedCommand>,
}

impl CommandDispatcher {
    /// Creates a dispatcher with a bounded response cache.
    #[must_use]
    pub fn new(policy: CapabilityPolicy, cache_capacity: usize) -> Option<Self> {
        (cache_capacity > 0).then(|| Self {
            policy,
            cache_capacity,
            cache: std::collections::BTreeMap::new(),
        })
    }

    /// Dispatches one command after validation, authorization, and version checks.
    ///
    /// `current_version` must come from the authoritative runtime immediately
    /// before invoking the handler. The handler returns the committed version
    /// and response payload only after its durable mutation succeeds.
    ///
    /// # Errors
    /// Returns a typed error when validation, authorization, optimistic
    /// concurrency, idempotency, or the handler itself rejects the command.
    pub fn dispatch<F>(
        &mut self,
        command: CommandEnvelope,
        capability: &str,
        current_version: u64,
        handler: F,
    ) -> Result<CommandResponse, DispatchError>
    where
        F: FnOnce(&CommandEnvelope) -> Result<CommandResponse, String>,
    {
        command
            .validate(16 * 1024 * 1024)
            .map_err(DispatchError::Invalid)?;
        self.policy
            .authorize(&command.actor, capability)
            .map_err(DispatchError::Unauthorized)?;
        let cache_key = format!("{}\u{1f}{}", command.actor, command.idempotency_key);
        if let Some(cached) = self.cache.get(&cache_key) {
            if cached.command != command {
                return Err(DispatchError::IdempotencyConflict);
            }
            return Ok(cached.response.clone());
        }
        if command.expected_state_version != current_version {
            return Err(DispatchError::StaleState {
                expected: command.expected_state_version,
                current: current_version,
            });
        }
        let response = handler(&command).map_err(DispatchError::Handler)?;
        if response.state_version < current_version {
            return Err(DispatchError::Handler(
                "handler returned a regressed state version".to_owned(),
            ));
        }
        if self.cache.len() >= self.cache_capacity
            && let Some(oldest) = self.cache.keys().next().cloned()
        {
            self.cache.remove(&oldest);
        }
        self.cache.insert(
            cache_key,
            CachedCommand {
                command,
                response: response.clone(),
            },
        );
        Ok(response)
    }

    /// Returns the number of retained idempotent command results.
    #[must_use]
    pub fn cached_results(&self) -> usize {
        self.cache.len()
    }
}

fn put_string(output: &mut Vec<u8>, value: &str) -> Result<(), FrameError> {
    let length = u16::try_from(value.len()).map_err(|_| FrameError::TooLarge(value.len()))?;
    output.extend_from_slice(&length.to_le_bytes());
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn take_bytes(bytes: &[u8], cursor: &mut usize, maximum: usize) -> Result<Vec<u8>, FrameError> {
    let length = take_u32(bytes, cursor)? as usize;
    if length > maximum {
        return Err(FrameError::TooLarge(length));
    }
    let end = cursor
        .checked_add(length)
        .ok_or(FrameError::TooLarge(length))?;
    let value = bytes.get(*cursor..end).ok_or(FrameError::Incomplete)?;
    *cursor = end;
    Ok(value.to_vec())
}

fn take_string(bytes: &[u8], cursor: &mut usize, maximum: usize) -> Result<String, FrameError> {
    let length = take_u16(bytes, cursor)? as usize;
    if length > maximum {
        return Err(FrameError::TooLarge(length));
    }
    let end = cursor
        .checked_add(length)
        .ok_or(FrameError::TooLarge(length))?;
    let value = bytes.get(*cursor..end).ok_or(FrameError::Incomplete)?;
    *cursor = end;
    String::from_utf8(value.to_vec()).map_err(|_| FrameError::Incomplete)
}

fn take_u16(bytes: &[u8], cursor: &mut usize) -> Result<u16, FrameError> {
    let end = cursor.checked_add(2).ok_or(FrameError::Incomplete)?;
    let value = bytes.get(*cursor..end).ok_or(FrameError::Incomplete)?;
    *cursor = end;
    Ok(u16::from_le_bytes([value[0], value[1]]))
}

fn take_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32, FrameError> {
    let end = cursor.checked_add(4).ok_or(FrameError::Incomplete)?;
    let value = bytes.get(*cursor..end).ok_or(FrameError::Incomplete)?;
    *cursor = end;
    Ok(u32::from_le_bytes([value[0], value[1], value[2], value[3]]))
}

fn take_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64, FrameError> {
    let end = cursor.checked_add(8).ok_or(FrameError::Incomplete)?;
    let value = bytes.get(*cursor..end).ok_or(FrameError::Incomplete)?;
    *cursor = end;
    Ok(u64::from_le_bytes([
        value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7],
    ]))
}

/// Correlation tracker preventing duplicate or unsolicited responses.
pub struct RequestTracker {
    capacity: usize,
    outstanding: std::collections::BTreeSet<String>,
}

impl Default for RequestTracker {
    fn default() -> Self {
        Self {
            capacity: 1024,
            outstanding: std::collections::BTreeSet::new(),
        }
    }
}

impl RequestTracker {
    /// Creates a tracker with a hard upper bound on in-flight requests.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Option<Self> {
        (capacity > 0).then(|| Self {
            capacity,
            outstanding: std::collections::BTreeSet::new(),
        })
    }

    /// Opens a request ID, returning false if it is already outstanding.
    pub fn open(&mut self, request_id: impl Into<String>) -> bool {
        let request_id = request_id.into();
        if request_id.trim().is_empty() {
            return false;
        }
        if self.outstanding.len() >= self.capacity {
            return false;
        }
        self.outstanding.insert(request_id)
    }

    /// Completes a request ID, returning false if it was unknown/already complete.
    pub fn complete(&mut self, request_id: &str) -> bool {
        self.outstanding.remove(request_id)
    }

    /// Returns the number of in-flight requests.
    #[must_use]
    pub fn len(&self) -> usize {
        self.outstanding.len()
    }

    /// Returns whether there are no in-flight requests.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.outstanding.is_empty()
    }
}

/// A codec for one complete command/event frame at a time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrameCodec {
    max_frame_bytes: usize,
}

impl FrameCodec {
    /// Creates a codec with a non-zero maximum payload size.
    #[must_use]
    pub const fn new(max_frame_bytes: usize) -> Option<Self> {
        if max_frame_bytes > 0 && max_frame_bytes <= u32::MAX as usize {
            Some(Self { max_frame_bytes })
        } else {
            None
        }
    }

    /// Encodes a payload with a four-byte little-endian length prefix.
    ///
    /// # Errors
    /// Returns [`FrameError::TooLarge`] if the payload exceeds the configured limit.
    pub fn encode(&self, payload: &[u8]) -> Result<Vec<u8>, FrameError> {
        if payload.len() > self.max_frame_bytes {
            return Err(FrameError::TooLarge(payload.len()));
        }
        let length =
            u32::try_from(payload.len()).map_err(|_| FrameError::TooLarge(payload.len()))?;
        let mut frame = Vec::with_capacity(payload.len().saturating_add(4));
        frame.extend_from_slice(&length.to_le_bytes());
        frame.extend_from_slice(payload);
        Ok(frame)
    }

    /// Decodes exactly one frame, rejecting trailing bytes and malformed lengths.
    ///
    /// # Errors
    /// Returns a typed error for incomplete, oversized, or length-mismatched data.
    pub fn decode<'a>(&self, frame: &'a [u8]) -> Result<&'a [u8], FrameError> {
        if frame.len() < 4 {
            return Err(FrameError::Incomplete);
        }
        let declared = u32::from_le_bytes([frame[0], frame[1], frame[2], frame[3]]) as usize;
        if declared > self.max_frame_bytes {
            return Err(FrameError::TooLarge(declared));
        }
        let actual = frame.len().saturating_sub(4);
        if declared != actual {
            return Err(FrameError::LengthMismatch { declared, actual });
        }
        Ok(&frame[4..])
    }
}

/// Errors returned by the Unix-domain control-plane server.
#[cfg(unix)]
#[derive(Debug)]
pub enum UnixServerError {
    /// The operating-system socket operation failed.
    Io(std::io::Error),
    /// A bounded frame or command could not be decoded.
    Frame(FrameError),
    /// The command handler rejected the request.
    Handler(String),
}

/// Errors returned by a bounded Unix-domain command client.
#[cfg(unix)]
#[derive(Debug)]
pub enum UnixClientError {
    /// The operating-system socket or stream operation failed.
    Io(std::io::Error),
    /// A command or response frame violated the configured bounds.
    Frame(FrameError),
    /// The command or response body was malformed.
    Protocol(FrameError),
}

#[cfg(unix)]
impl From<std::io::Error> for UnixClientError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

#[cfg(unix)]
impl From<FrameError> for UnixClientError {
    fn from(error: FrameError) -> Self {
        Self::Frame(error)
    }
}

#[cfg(unix)]
impl From<std::io::Error> for UnixServerError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

#[cfg(unix)]
impl From<FrameError> for UnixServerError {
    fn from(error: FrameError) -> Self {
        Self::Frame(error)
    }
}

/// Bounded Unix-domain socket transport for the command contract.
///
/// The server intentionally handles one accepted connection at a time. The
/// engine's supervisor can run `serve_next` on a bounded worker and apply its
/// own admission policy; this transport never allocates based on an untrusted
/// length and never exposes credential material to clients.
#[cfg(unix)]
pub struct UnixSocketServer {
    listener: std::os::unix::net::UnixListener,
    command_codec: CommandCodec,
    response_codec: ResponseCodec,
    frame_codec: FrameCodec,
}

/// Maximum number of request/response exchanges served before a client
/// connection is closed. Clients can reconnect, but cannot monopolize the
/// single bounded server accept loop indefinitely.
const MAX_REQUESTS_PER_CONNECTION: usize = 256;
const CONNECTION_IO_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

#[cfg(unix)]
impl UnixSocketServer {
    /// Binds a local socket and applies owner-only permissions.
    ///
    /// The path must not already exist; callers should remove a stale socket
    /// only after verifying that no engine process owns it.
    ///
    /// # Errors
    /// Returns [`UnixServerError::Io`] for bind or permission failures.
    pub fn bind(
        path: impl AsRef<std::path::Path>,
        max_payload_bytes: usize,
    ) -> Result<Self, UnixServerError> {
        use std::os::unix::fs::{FileTypeExt, PermissionsExt};
        let path = path.as_ref();
        if let Ok(metadata) = std::fs::symlink_metadata(path) {
            if !metadata.file_type().is_socket() {
                return Err(UnixServerError::Io(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    "IPC path exists and is not a Unix socket",
                )));
            }
            // A live listener accepts a connection; never unlink it. A failed
            // connect means the pathname is stale after an unclean shutdown,
            // which is the only case this bind operation repairs.
            match std::os::unix::net::UnixStream::connect(path) {
                Ok(_) => {
                    return Err(UnixServerError::Io(std::io::Error::new(
                        std::io::ErrorKind::AddrInUse,
                        "IPC socket is already served by another process",
                    )));
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
                    ) =>
                {
                    std::fs::remove_file(path)?;
                }
                Err(error) => return Err(UnixServerError::Io(error)),
            }
        }
        let listener = std::os::unix::net::UnixListener::bind(path)?;
        let mut permissions = std::fs::metadata(path)
            .map_err(UnixServerError::Io)?
            .permissions();
        permissions.set_mode(0o600);
        std::fs::set_permissions(path, permissions).map_err(UnixServerError::Io)?;
        let command_codec = CommandCodec::new(max_payload_bytes).ok_or(FrameError::InvalidLimit)?;
        let response_codec =
            ResponseCodec::new(max_payload_bytes).ok_or(FrameError::InvalidLimit)?;
        let frame_codec = FrameCodec::new(max_payload_bytes.saturating_add(64))
            .ok_or(FrameError::InvalidLimit)?;
        Ok(Self {
            listener,
            command_codec,
            response_codec,
            frame_codec,
        })
    }

    /// Accepts one client and serves requests until clean EOF or a protocol
    /// error. The callback is responsible for capability authorization and
    /// durable dispatch through [`CommandDispatcher`].
    ///
    /// # Errors
    /// Returns a transport, framing, or handler error.
    pub fn serve_next<F>(&self, mut handler: F) -> Result<(), UnixServerError>
    where
        F: FnMut(CommandEnvelope) -> Result<CommandResponse, String>,
    {
        use std::io::{Read, Write};
        let (mut stream, _) = self.listener.accept()?;
        stream.set_read_timeout(Some(CONNECTION_IO_TIMEOUT))?;
        stream.set_write_timeout(Some(CONNECTION_IO_TIMEOUT))?;
        for _ in 0..MAX_REQUESTS_PER_CONNECTION {
            let mut prefix = [0_u8; 4];
            match stream.read_exact(&mut prefix) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
                Err(error) => return Err(UnixServerError::Io(error)),
            }
            let declared = u32::from_le_bytes(prefix) as usize;
            if declared > self.frame_codec.max_frame_bytes {
                return Err(UnixServerError::Frame(FrameError::TooLarge(declared)));
            }
            let mut body = vec![0_u8; declared];
            stream.read_exact(&mut body)?;
            let mut frame = Vec::with_capacity(declared.saturating_add(4));
            frame.extend_from_slice(&prefix);
            frame.extend_from_slice(&body);
            let payload = self.frame_codec.decode(&frame)?;
            let command = self.command_codec.decode(payload)?;
            let response = handler(command).map_err(UnixServerError::Handler)?;
            let encoded = self.response_codec.encode(&response)?;
            let frame = self.frame_codec.encode(&encoded)?;
            stream.write_all(&frame)?;
            stream.flush()?;
        }
        Ok(())
    }
}

/// Bounded Unix-domain command client for desktop/native adapters.
#[cfg(unix)]
pub struct UnixSocketClient {
    path: std::path::PathBuf,
    command_codec: CommandCodec,
    response_codec: ResponseCodec,
    frame_codec: FrameCodec,
}

#[cfg(unix)]
impl UnixSocketClient {
    /// Creates a client with the same maximum payload bound as the server.
    ///
    /// # Errors
    /// Returns [`FrameError::InvalidLimit`] when the bound is zero or exceeds
    /// the protocol's representable frame size.
    pub fn new(
        path: impl AsRef<std::path::Path>,
        max_payload_bytes: usize,
    ) -> Result<Self, FrameError> {
        let command_codec = CommandCodec::new(max_payload_bytes).ok_or(FrameError::InvalidLimit)?;
        let response_codec =
            ResponseCodec::new(max_payload_bytes).ok_or(FrameError::InvalidLimit)?;
        let frame_codec = FrameCodec::new(max_payload_bytes.saturating_add(64))
            .ok_or(FrameError::InvalidLimit)?;
        Ok(Self {
            path: path.as_ref().to_path_buf(),
            command_codec,
            response_codec,
            frame_codec,
        })
    }

    /// Sends one command over a fresh owner-authenticated local connection.
    /// A fresh connection keeps command boundaries explicit and prevents a
    /// partially written request from contaminating a later idempotency key.
    ///
    /// # Errors
    /// Returns [`UnixClientError`] for connection, framing, or protocol errors.
    pub fn request(&self, command: &CommandEnvelope) -> Result<CommandResponse, UnixClientError> {
        use std::io::{Read, Write};
        let mut stream = std::os::unix::net::UnixStream::connect(&self.path)?;
        let encoded = self
            .command_codec
            .encode(command)
            .map_err(UnixClientError::Protocol)?;
        let frame = self.frame_codec.encode(&encoded)?;
        stream.write_all(&frame)?;
        stream.flush()?;
        let mut prefix = [0_u8; 4];
        stream.read_exact(&mut prefix)?;
        let declared = u32::from_le_bytes(prefix) as usize;
        if declared > self.frame_codec.max_frame_bytes {
            return Err(UnixClientError::Frame(FrameError::TooLarge(declared)));
        }
        let mut body = vec![0_u8; declared];
        stream.read_exact(&mut body)?;
        let mut response_frame = Vec::with_capacity(declared.saturating_add(4));
        response_frame.extend_from_slice(&prefix);
        response_frame.extend_from_slice(&body);
        let response_body = self.frame_codec.decode(&response_frame)?;
        self.response_codec
            .decode(response_body)
            .map_err(UnixClientError::Protocol)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AuthorizationError, CapabilityPolicy, CommandCodec, CommandDispatcher, CommandEnvelope,
        CommandResponse, DispatchError, Envelope, FrameCodec, FrameError, MessageKind,
        RequestTracker, UnixSocketServer,
    };

    #[test]
    fn framing_round_trip_and_limits_are_explicit() {
        let Some(codec) = FrameCodec::new(4) else {
            return;
        };
        let encoded = codec.encode(b"ping").ok();
        assert_eq!(
            encoded
                .as_deref()
                .and_then(|frame| codec.decode(frame).ok()),
            Some(&b"ping"[..])
        );
        assert_eq!(codec.encode(b"large"), Err(FrameError::TooLarge(5)));
        assert_eq!(
            codec.decode(&[4, 0, 0, 0, 1]),
            Err(FrameError::LengthMismatch {
                declared: 4,
                actual: 1
            })
        );
        assert_eq!(codec.decode(&[1, 0]), Err(FrameError::Incomplete));
    }

    #[test]
    fn envelopes_and_correlations_are_bounded_and_idempotent() {
        let envelope = Envelope {
            protocol_version: 1,
            kind: MessageKind::Request,
            request_id: "req-1".into(),
            trace_id: "trace-1".into(),
            payload: vec![1, 2],
        };
        assert!(envelope.validate(2).is_ok());
        assert_eq!(envelope.validate(1), Err(FrameError::TooLarge(2)));
        let mut tracker = RequestTracker::default();
        assert!(tracker.open("req-1"));
        assert!(!tracker.open("req-1"));
        assert!(!tracker.complete("unknown"));
        assert!(tracker.complete("req-1"));
        assert!(tracker.is_empty());
        let _ = MessageKind::Event;
    }

    #[test]
    fn correlation_capacity_and_metadata_bounds_are_enforced() {
        let Some(mut tracker) = RequestTracker::with_capacity(1) else {
            return;
        };
        assert!(tracker.open("first"));
        assert!(!tracker.open("second"));
        let oversized = Envelope {
            protocol_version: 1,
            kind: MessageKind::Request,
            request_id: "x".repeat(257),
            trace_id: "trace".into(),
            payload: Vec::new(),
        };
        assert_eq!(oversized.validate(1), Err(FrameError::TooLarge(257)));
    }

    #[test]
    fn command_codec_round_trips_metadata_and_rejects_truncation() {
        let Some(codec) = CommandCodec::new(16) else {
            return;
        };
        let command = CommandEnvelope {
            command_id: "cmd-1".into(),
            trace_id: "trace-1".into(),
            actor: "ui-session-1".into(),
            issued_wall_ns: 42,
            expected_state_version: 7,
            idempotency_key: "idem-1".into(),
            payload: vec![1, 2, 3],
        };
        assert!(command.validate(16).is_ok());
        assert_eq!(
            CommandEnvelope {
                issued_wall_ns: 0,
                ..command.clone()
            }
            .validate(16),
            Err(FrameError::Incomplete)
        );
        let encoded = codec.encode(&command).ok();
        assert_eq!(
            encoded
                .as_deref()
                .and_then(|bytes| codec.decode(bytes).ok()),
            Some(command.clone())
        );
        let Some(mut truncated) = encoded else {
            return;
        };
        truncated.pop();
        assert_eq!(codec.decode(&truncated), Err(FrameError::Incomplete));
        assert_eq!(
            codec.encode(&CommandEnvelope {
                payload: vec![0; 17],
                ..command
            }),
            Err(FrameError::TooLarge(17))
        );
    }

    #[test]
    fn capability_policy_is_deny_by_default_and_revocation_is_immediate() {
        let mut policy = CapabilityPolicy::new();
        assert_eq!(
            policy.authorize("session-1", "submit_order"),
            Err(AuthorizationError::Denied)
        );
        assert!(policy.grant("session-1", "submit_order").is_ok());
        assert!(policy.authorize("session-1", "submit_order").is_ok());
        policy.revoke("session-1", "submit_order");
        assert_eq!(
            policy.authorize("session-1", "submit_order"),
            Err(AuthorizationError::Denied)
        );
    }

    #[test]
    fn dispatcher_authorizes_checks_version_and_replays_idempotently() {
        let mut policy = CapabilityPolicy::new();
        assert!(policy.grant("session-1", "submit_order").is_ok());
        let Some(mut dispatcher) = CommandDispatcher::new(policy, 2) else {
            return;
        };
        let command = CommandEnvelope {
            command_id: "cmd-1".into(),
            trace_id: "trace-1".into(),
            actor: "session-1".into(),
            issued_wall_ns: 1,
            expected_state_version: 4,
            idempotency_key: "intent-1".into(),
            payload: vec![7],
        };
        let mut invocations = 0;
        let first = dispatcher.dispatch(command.clone(), "submit_order", 4, |_| {
            invocations += 1;
            Ok(CommandResponse {
                state_version: 5,
                payload: vec![9],
            })
        });
        assert_eq!(first.ok().map(|response| response.payload), Some(vec![9]));
        let second = dispatcher.dispatch(command.clone(), "submit_order", 5, |_| {
            invocations += 1;
            Ok(CommandResponse {
                state_version: 6,
                payload: vec![8],
            })
        });
        assert_eq!(second.ok().map(|response| response.payload), Some(vec![9]));
        assert_eq!(invocations, 1);
        let mut conflicting = command;
        conflicting.payload = vec![8];
        assert_eq!(
            dispatcher.dispatch(conflicting, "submit_order", 5, |_| unreachable!()),
            Err(DispatchError::IdempotencyConflict)
        );
    }

    #[test]
    fn dispatcher_rejects_stale_and_unauthorized_commands_before_handler() {
        let policy = CapabilityPolicy::new();
        let Some(mut dispatcher) = CommandDispatcher::new(policy, 1) else {
            return;
        };
        let command = CommandEnvelope {
            command_id: "cmd-2".into(),
            trace_id: "trace-2".into(),
            actor: "session-2".into(),
            issued_wall_ns: 1,
            expected_state_version: 1,
            idempotency_key: "intent-2".into(),
            payload: Vec::new(),
        };
        assert!(matches!(
            dispatcher.dispatch(command.clone(), "submit_order", 1, |_| unreachable!()),
            Err(DispatchError::Unauthorized(AuthorizationError::Denied))
        ));
        let mut policy = CapabilityPolicy::new();
        assert!(policy.grant("session-2", "submit_order").is_ok());
        let Some(mut dispatcher) = CommandDispatcher::new(policy, 1) else {
            return;
        };
        assert_eq!(
            dispatcher.dispatch(command, "submit_order", 2, |_| unreachable!()),
            Err(DispatchError::StaleState {
                expected: 1,
                current: 2
            })
        );
    }

    #[cfg(unix)]
    #[test]
    fn unix_server_reclaims_only_a_stale_socket_path() {
        use std::os::unix::net::UnixListener;

        let Ok(elapsed) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) else {
            return;
        };
        let path = std::env::temp_dir().join(format!(
            "insidertrader-ipc-stale-{}-{}",
            std::process::id(),
            elapsed.as_nanos()
        ));
        let Ok(stale) = UnixListener::bind(&path) else {
            return;
        };
        drop(stale);
        let server = UnixSocketServer::bind(&path, 1_024);
        assert!(server.is_ok());
        drop(server);
        let _ = std::fs::remove_file(path);
    }
}
