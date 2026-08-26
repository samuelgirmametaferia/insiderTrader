//! Provider-neutral LLM contracts and trading-output validation.

#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::io::Read;

/// Stable subsystem identifier used by startup diagnostics.
pub const SUBSYSTEM_ID: &str = "llm_core";
const MAX_INPUT_BYTES: usize = 4 * 1024 * 1024;
const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const MAX_PROMPT_FIELD_BYTES: usize = 512;
const MAX_PROMPT_SCHEMA_BYTES: usize = 256 * 1024;
const MAX_PROMPT_TOOLS: usize = 128;

/// Immutable, versioned prompt/template artifact metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PromptRecord {
    /// Stable prompt identity.
    pub prompt_id: String,
    /// Immutable semantic version selected by runtime configuration.
    pub version: String,
    /// Human-readable purpose/task class.
    pub purpose: String,
    /// Input schema identifier or canonical schema document.
    pub input_schema: String,
    /// Output schema identifier or canonical schema document.
    pub output_schema: String,
    /// Tool names permitted for this prompt, in sorted order.
    pub allowed_tools: Vec<String>,
    /// Recommended task class.
    pub task_class: String,
    /// Required provider capabilities.
    pub required_capabilities: Capabilities,
    /// SHA-256 of the canonical prompt artifact bytes.
    pub artifact_hash: String,
    /// Fixture-suite identifier used to validate this prompt.
    pub fixture_suite: String,
}

impl PromptRecord {
    /// Computes the content hash for a prompt artifact.
    #[must_use]
    pub fn content_hash(prompt_id: &str, version: &str, template: &str) -> String {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(b"insidertrader-prompt-v1\0");
        for value in [prompt_id, version, template] {
            hasher.update((value.len() as u64).to_le_bytes());
            hasher.update(value.as_bytes());
        }
        format!("{:x}", hasher.finalize())
    }

    /// Validates a journal-restored record without exposing internal rules.
    ///
    /// # Errors
    /// Returns [`LlmError::SchemaViolation`] when persisted metadata is invalid.
    pub fn validate_for_replay(&self) -> Result<(), LlmError> {
        self.validate()
    }

    fn validate(&self) -> Result<(), LlmError> {
        for (name, value) in [
            ("prompt_id", self.prompt_id.as_str()),
            ("version", self.version.as_str()),
            ("purpose", self.purpose.as_str()),
            ("input_schema", self.input_schema.as_str()),
            ("output_schema", self.output_schema.as_str()),
            ("task_class", self.task_class.as_str()),
            ("artifact_hash", self.artifact_hash.as_str()),
            ("fixture_suite", self.fixture_suite.as_str()),
        ] {
            if value.trim().is_empty() || value.len() > MAX_PROMPT_FIELD_BYTES {
                return Err(LlmError::SchemaViolation(format!("invalid prompt {name}")));
            }
        }
        if self.input_schema.len() > MAX_PROMPT_SCHEMA_BYTES
            || self.output_schema.len() > MAX_PROMPT_SCHEMA_BYTES
            || self.allowed_tools.len() > MAX_PROMPT_TOOLS
            || self.allowed_tools.windows(2).any(|pair| pair[0] >= pair[1])
            || self
                .allowed_tools
                .iter()
                .any(|tool| tool.trim().is_empty() || tool.len() > MAX_PROMPT_FIELD_BYTES)
            || self.artifact_hash.len() != 64
            || !self
                .artifact_hash
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(LlmError::SchemaViolation(
                "prompt metadata exceeds bounds or is not canonical".into(),
            ));
        }
        Ok(())
    }
}

/// Immutable prompt registry used to prevent unversioned prompt drift.
#[derive(Clone, Default)]
pub struct PromptRegistry {
    records: BTreeMap<(String, String), PromptRecord>,
}

impl PromptRegistry {
    /// Creates an empty prompt registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers one immutable prompt record.
    ///
    /// # Errors
    /// Returns [`LlmError::SchemaViolation`] for invalid metadata or a duplicate
    /// `(prompt_id, version)` identity.
    pub fn register(&mut self, record: PromptRecord) -> Result<(), LlmError> {
        record.validate()?;
        let key = (record.prompt_id.clone(), record.version.clone());
        if self.records.contains_key(&key) {
            return Err(LlmError::SchemaViolation(
                "prompt version already registered".into(),
            ));
        }
        self.records.insert(key, record);
        Ok(())
    }

    /// Resolves an exact prompt version; `latest` is intentionally unsupported.
    #[must_use]
    pub fn get(&self, prompt_id: &str, version: &str) -> Option<&PromptRecord> {
        if version == "latest" {
            return None;
        }
        self.records
            .get(&(prompt_id.to_owned(), version.to_owned()))
    }

    /// Returns all records in deterministic identity order.
    #[must_use]
    pub fn records(&self) -> Vec<PromptRecord> {
        self.records.values().cloned().collect()
    }
}

/// Protocol endpoint preference.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Endpoint {
    /// OpenAI-compatible Responses endpoint.
    Responses,
    /// OpenAI-compatible Chat Completions endpoint.
    ChatCompletions,
}

/// Provider capability declaration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct Capabilities {
    /// Responses API is available.
    pub responses: bool,
    /// Chat Completions is available.
    pub chat_completions: bool,
    /// Streaming is available.
    pub streaming: bool,
    /// Schema-constrained JSON is available.
    pub json_schema: bool,
    /// Tool calls are available.
    pub tools: bool,
}

/// Explicit request budget and reproducibility metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Request {
    /// Stable trace identifier.
    pub trace_id: String,
    /// Versioned prompt identifier.
    pub prompt_version: String,
    /// Model identifier.
    pub model: String,
    /// Request task class.
    pub task: String,
    /// Serialized context hash.
    pub context_hash: String,
    /// Fully assembled prompt/context sent to the provider.
    pub input: String,
    /// Maximum output tokens.
    pub max_output_tokens: u32,
    /// Preferred endpoint.
    pub endpoint: Endpoint,
}

impl Request {
    /// Validates required reproducibility and budget fields.
    ///
    /// # Errors
    /// Returns [`LlmError`] if an identity field is blank or the token budget is zero.
    pub fn validate(&self) -> Result<(), LlmError> {
        for (name, value) in [
            ("trace_id", self.trace_id.as_str()),
            ("prompt_version", self.prompt_version.as_str()),
            ("model", self.model.as_str()),
            ("task", self.task.as_str()),
            ("context_hash", self.context_hash.as_str()),
            ("input", self.input.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(LlmError::EmptyField(name));
            }
        }
        if self.prompt_version == "latest" {
            return Err(LlmError::SchemaViolation(
                "LLM requests must pin an exact prompt version".into(),
            ));
        }
        if self.max_output_tokens == 0 {
            return Err(LlmError::ZeroBudget);
        }
        if self.input.len() > MAX_INPUT_BYTES {
            return Err(LlmError::Provider("LLM input exceeds bounded size".into()));
        }
        Ok(())
    }
}

/// Provider result failure category.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LlmError {
    /// Required request field is blank.
    EmptyField(&'static str),
    /// No output budget was provided.
    ZeroBudget,
    /// Transport/provider failure.
    Provider(String),
    /// Provider rejected the configured credential or actor.
    Authentication(String),
    /// Provider rate limit, optionally with a server-advertised delay.
    RateLimited {
        /// Optional provider-advertised retry delay.
        retry_after_ms: Option<u64>,
    },
    /// Provider or transport deadline expired.
    Timeout,
    /// Streaming ended before a completion marker was received.
    InterruptedStream,
    /// Provider returned a refusal or policy stop instead of an answer.
    Refusal(String),
    /// Provider response was syntactically invalid JSON.
    MalformedOutput(String),
    /// JSON shape did not match the requested schema.
    SchemaViolation(String),
    /// JSON shape was valid but failed domain validation.
    SemanticValidation(String),
    /// Output was not valid for the requested trading action.
    InvalidAction(String),
}

/// Authorization class for a tool. Read-only tools may inspect authoritative
/// state; action tools require an explicit caller authorization boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ToolPermission {
    /// Tool cannot mutate trading or configuration state.
    ReadOnly,
    /// Tool may request a state-changing action and must be explicitly enabled.
    Action,
}

/// Bounded description of one authoritative tool exposed to a model.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolSpec {
    /// Finite protocol name.
    pub name: String,
    /// Maximum serialized argument bytes.
    pub max_input_bytes: usize,
    /// Maximum serialized result bytes.
    pub max_output_bytes: usize,
    /// Authorization class enforced by the registry.
    pub permission: ToolPermission,
    /// Maximum wall-clock execution time for one invocation.
    pub deadline_ms: u64,
}

/// One trace-correlated tool invocation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolRequest {
    /// Request trace propagated from the LLM completion.
    pub trace_id: String,
    /// Registered tool name.
    pub name: String,
    /// Serialized, schema-validated arguments.
    pub input: String,
}

/// One authoritative tool result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolResponse {
    /// Trace returned by the tool implementation.
    pub trace_id: String,
    /// Tool name returned by the implementation.
    pub name: String,
    /// Serialized structured result.
    pub output: String,
}

/// Tool implementation boundary. Handlers must query authoritative runtime
/// state rather than rely on model-supplied prices, quantities, or positions.
pub trait ToolHandler: Send + Sync {
    /// Returns the immutable finite tool specification.
    fn spec(&self) -> &ToolSpec;
    /// Executes one bounded request.
    ///
    /// # Errors
    /// Returns a classified LLM error when arguments or authoritative state
    /// cannot be processed.
    fn invoke(&self, request: &ToolRequest) -> Result<ToolResponse, LlmError>;
}

/// Registry enforcing finite names, bounds, and trace correlation for tools.
#[derive(Default)]
pub struct ToolRegistry {
    handlers: std::collections::BTreeMap<String, Box<dyn ToolHandler>>,
    allow_actions: bool,
}

impl ToolRegistry {
    /// Creates an empty deny-by-default registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            handlers: std::collections::BTreeMap::new(),
            allow_actions: false,
        }
    }

    /// Enables action tools for a caller that has already passed its policy
    /// and live-safety checks. Read-only tools remain available by default.
    pub fn authorize_actions(&mut self) {
        self.allow_actions = true;
    }

    /// Registers one tool exactly once after validating its bounds.
    ///
    /// # Errors
    /// Returns [`LlmError::SchemaViolation`] for invalid or duplicate specs.
    pub fn register(&mut self, handler: Box<dyn ToolHandler>) -> Result<(), LlmError> {
        let spec = handler.spec();
        if spec.name.trim().is_empty()
            || spec.max_input_bytes == 0
            || spec.max_output_bytes == 0
            || spec.deadline_ms == 0
            || self.handlers.contains_key(&spec.name)
        {
            return Err(LlmError::SchemaViolation(
                "invalid or duplicate tool spec".into(),
            ));
        }
        self.handlers.insert(spec.name.clone(), handler);
        Ok(())
    }

    /// Returns registered tool specifications in deterministic name order.
    #[must_use]
    pub fn specs(&self) -> Vec<ToolSpec> {
        self.handlers
            .values()
            .map(|handler| handler.spec().clone())
            .collect()
    }

    /// Invokes one registered tool with bounded input and trace checking.
    ///
    /// # Errors
    /// Returns a schema, provider, or trace error; unregistered tools are
    /// denied without invoking any handler.
    pub fn invoke(&self, request: &ToolRequest) -> Result<ToolResponse, LlmError> {
        if request.trace_id.trim().is_empty() || request.name.trim().is_empty() {
            return Err(LlmError::EmptyField("tool request identity"));
        }
        let Some(handler) = self.handlers.get(&request.name) else {
            return Err(LlmError::SchemaViolation("tool is not registered".into()));
        };
        let spec = handler.spec();
        if spec.permission == ToolPermission::Action && !self.allow_actions {
            return Err(LlmError::InvalidAction(
                "action tool requires explicit authorization".into(),
            ));
        }
        if request.input.len() > spec.max_input_bytes {
            return Err(LlmError::SchemaViolation("tool input exceeds bound".into()));
        }
        let started = std::time::Instant::now();
        let response = handler.invoke(request)?;
        if started.elapsed() > std::time::Duration::from_millis(spec.deadline_ms) {
            return Err(LlmError::Timeout);
        }
        if response.trace_id != request.trace_id || response.name != request.name {
            return Err(LlmError::SchemaViolation(
                "tool response correlation mismatch".into(),
            ));
        }
        if response.output.len() > spec.max_output_bytes {
            return Err(LlmError::SchemaViolation(
                "tool output exceeds bound".into(),
            ));
        }
        Ok(response)
    }
}

/// Non-streaming provider response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Response {
    /// Provider request trace.
    pub trace_id: String,
    /// Returned text or structured JSON.
    pub content: String,
    /// Provider stop reason.
    pub finish_reason: String,
}

/// Durable cache entry used to pin historical LLM output for replay.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CachedResponseSnapshot {
    /// Semantic request key generated from all request-defining fields.
    pub key: String,
    /// Insertion timestamp in the caller's wall-clock domain.
    pub inserted_ms: i64,
    /// Original provider response, including trace correlation.
    pub response: Response,
}

/// Stream item emitted by a provider adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StreamItem {
    /// Incremental text delta.
    Delta(String),
    /// Stream completed with a finish reason.
    Done(String),
}

/// Bounded HTTP request passed to an injected provider transport.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpRequest {
    /// HTTP method, currently `POST` for completion calls.
    pub method: String,
    /// Fully resolved endpoint URL.
    pub url: String,
    /// Request headers; secrets are supplied only to the transport boundary.
    pub headers: Vec<(String, String)>,
    /// UTF-8 JSON request body.
    pub body: Vec<u8>,
}

/// HTTP response returned by an injected provider transport.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpResponse {
    /// HTTP status code.
    pub status: u16,
    /// Response headers, including optional retry metadata.
    pub headers: Vec<(String, String)>,
    /// Raw response bytes.
    pub body: Vec<u8>,
}

/// Transport failure classification used by provider adapters.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransportError {
    /// Connection or socket failure.
    Connection(String),
    /// Deadline expired before a complete response.
    Timeout,
    /// Request was cancelled by the caller.
    Cancelled,
}

/// Injected transport boundary. Implementations may use reqwest, a local
/// inference socket, or a fake server without changing this crate.
pub trait HttpTransport: Send + Sync {
    /// Sends one bounded request.
    ///
    /// # Errors
    /// Returns a classified transport failure when the request cannot produce
    /// a complete response.
    fn send(&self, request: HttpRequest) -> Result<HttpResponse, TransportError>;
}

/// Production HTTPS transport using a bounded blocking client.
///
/// The provider API is synchronous at this crate boundary, so callers must
/// run this transport on a control-plane worker; it must never execute on the
/// market-data hot path.
pub struct ReqwestTransport {
    client: reqwest::blocking::Client,
}

impl ReqwestTransport {
    /// Creates a TLS-validating client with one finite request timeout.
    ///
    /// # Errors
    /// Returns a transport construction error when the timeout is invalid or
    /// the TLS client cannot be initialized.
    pub fn new(timeout_ms: u64) -> Result<Self, TransportError> {
        if timeout_ms == 0 {
            return Err(TransportError::Timeout);
        }
        let client = reqwest::blocking::Client::builder()
            .connect_timeout(std::time::Duration::from_millis(timeout_ms.min(120_000)))
            .timeout(std::time::Duration::from_millis(timeout_ms.min(120_000)))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| TransportError::Connection(format!("client: {error}")))?;
        Ok(Self { client })
    }
}

impl HttpTransport for ReqwestTransport {
    fn send(&self, request: HttpRequest) -> Result<HttpResponse, TransportError> {
        if request.method != "POST" || request.url.trim().is_empty() {
            return Err(TransportError::Connection("invalid HTTP request".into()));
        }
        let mut builder = self.client.post(&request.url).body(request.body);
        for (name, value) in request.headers {
            builder = builder.header(name, value);
        }
        let response = builder.send().map_err(|error| {
            if error.is_timeout() {
                TransportError::Timeout
            } else if error.is_request() || error.is_connect() {
                TransportError::Connection(error.to_string())
            } else {
                TransportError::Connection("HTTP transport failure".into())
            }
        })?;
        if response
            .content_length()
            .is_some_and(|length| length > MAX_RESPONSE_BYTES as u64)
        {
            return Err(TransportError::Connection(
                "HTTP response exceeds bounded LLM response size".into(),
            ));
        }
        let status = response.status().as_u16();
        let headers = response
            .headers()
            .iter()
            .filter_map(|(name, value)| {
                value
                    .to_str()
                    .ok()
                    .map(|value| (name.as_str().to_owned(), value.to_owned()))
            })
            .collect();
        let body = read_bounded_response(response)?;
        Ok(HttpResponse {
            status,
            headers,
            body,
        })
    }
}

fn read_bounded_response(reader: impl Read) -> Result<Vec<u8>, TransportError> {
    let mut body = Vec::new();
    reader
        .take((MAX_RESPONSE_BYTES as u64).saturating_add(1))
        .read_to_end(&mut body)
        .map_err(|_| TransportError::Connection("HTTP response body could not be read".into()))?;
    if body.len() > MAX_RESPONSE_BYTES {
        return Err(TransportError::Connection(
            "HTTP response exceeds bounded LLM response size".into(),
        ));
    }
    Ok(body)
}

/// OpenAI-compatible provider adapter for Responses and Chat Completions.
///
/// The adapter owns endpoint translation and strict response/SSE extraction;
/// credentials never enter [`Request`] or response traces. Network policy,
/// TLS, cancellation, and connection pooling remain in the injected transport.
pub struct OpenAiCompatibleProvider<T> {
    transport: T,
    base_url: String,
    api_key: String,
    capabilities: Capabilities,
}

impl<T> OpenAiCompatibleProvider<T> {
    /// Creates a provider with a configured base URL and secret key.
    ///
    /// # Errors
    /// Returns [`LlmError::EmptyField`] when the base URL or key is blank.
    pub fn new(
        transport: T,
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        capabilities: Capabilities,
    ) -> Result<Self, LlmError> {
        let base_url = base_url.into().trim_end_matches('/').to_owned();
        let api_key = api_key.into();
        if base_url.trim().is_empty() {
            return Err(LlmError::EmptyField("base_url"));
        }
        if api_key.trim().is_empty() {
            return Err(LlmError::EmptyField("api_key"));
        }
        Ok(Self {
            transport,
            base_url,
            api_key,
            capabilities,
        })
    }

    fn path(endpoint: Endpoint) -> &'static str {
        match endpoint {
            Endpoint::Responses => "/responses",
            Endpoint::ChatCompletions => "/chat/completions",
        }
    }

    fn alternate(endpoint: Endpoint) -> Endpoint {
        match endpoint {
            Endpoint::Responses => Endpoint::ChatCompletions,
            Endpoint::ChatCompletions => Endpoint::Responses,
        }
    }

    fn request(&self, request: &Request) -> HttpRequest {
        let body = match request.endpoint {
            Endpoint::Responses => format!(
                "{{\"model\":{},\"input\":{},\"max_output_tokens\":{}}}",
                json_string(&request.model),
                json_string(&request.input),
                request.max_output_tokens
            ),
            Endpoint::ChatCompletions => format!(
                "{{\"model\":{},\"messages\":[{{\"role\":\"user\",\"content\":{}}}],\"max_tokens\":{}}}",
                json_string(&request.model),
                json_string(&request.input),
                request.max_output_tokens
            ),
        };
        HttpRequest {
            method: "POST".into(),
            url: format!("{}{}", self.base_url, Self::path(request.endpoint)),
            headers: vec![
                ("authorization".into(), format!("Bearer {}", self.api_key)),
                ("content-type".into(), "application/json".into()),
                ("accept".into(), "application/json".into()),
            ],
            body: body.into_bytes(),
        }
    }

    fn classify(response: &HttpResponse) -> Result<(), LlmError> {
        match response.status {
            200..=299 => Ok(()),
            401 | 403 => Err(LlmError::Authentication(
                "provider authentication failed".into(),
            )),
            408 => Err(LlmError::Timeout),
            429 => Err(LlmError::RateLimited {
                retry_after_ms: retry_after_ms(response),
            }),
            500..=599 => Err(LlmError::Provider(format!(
                "provider server failure: {}",
                response.status
            ))),
            status => Err(LlmError::Provider(format!(
                "provider rejected request: {status}"
            ))),
        }
    }

    fn send(&self, request: &Request) -> Result<HttpResponse, LlmError>
    where
        T: HttpTransport,
    {
        let response = self
            .transport
            .send(self.request(request))
            .map_err(|error| match error {
                TransportError::Timeout => LlmError::Timeout,
                TransportError::Cancelled => LlmError::InterruptedStream,
                TransportError::Connection(message) => LlmError::Provider(message),
            })?;
        if response.body.len() > MAX_RESPONSE_BYTES {
            return Err(LlmError::Provider(
                "provider response exceeds bounded size".into(),
            ));
        }
        Ok(response)
    }
}

fn retry_after_ms(response: &HttpResponse) -> Option<u64> {
    response.headers.iter().find_map(|(name, value)| {
        if name.eq_ignore_ascii_case("retry-after-ms") {
            return value.trim().parse::<u64>().ok();
        }
        if name.eq_ignore_ascii_case("retry-after")
            && let Ok(seconds) = value.trim().parse::<u64>()
        {
            return seconds.checked_mul(1_000);
        }
        None
    })
}

impl<T> Provider for OpenAiCompatibleProvider<T>
where
    T: HttpTransport,
{
    fn manifest(&self) -> Option<insider_provider_core::ProviderManifest> {
        let mut capabilities = Vec::new();
        if self.capabilities.responses {
            capabilities.push("responses".into());
        }
        if self.capabilities.chat_completions {
            capabilities.push("chat_completions".into());
        }
        if self.capabilities.json_schema {
            capabilities.push("json_schema".into());
        }
        if self.capabilities.tools {
            capabilities.push("tools".into());
        }
        Some(insider_provider_core::ProviderManifest {
            provider_id: "openai-compatible".into(),
            kind: insider_provider_core::ProviderKind::Llm,
            schema_version: "openai-compatible-v1".into(),
            base_url: self.base_url.clone(),
            auth: insider_provider_core::AuthMethod::ApiKey,
            capabilities,
            retry: insider_provider_core::RetryPolicy {
                max_retries: 2,
                initial_backoff_ms: 100,
                max_backoff_ms: 2_000,
                honor_retry_after: true,
            },
            timeout: insider_provider_core::TimeoutPolicy {
                connect_timeout_ms: 2_000,
                request_timeout_ms: 30_000,
                max_parallel_requests: 8,
                max_requests: 120,
                window_ms: 60_000,
            },
            health_probe: Some("/models".into()),
            streaming: self.capabilities.streaming,
        })
    }

    fn capabilities(&self) -> Capabilities {
        self.capabilities
    }

    fn complete(&self, request: &Request) -> Result<Response, LlmError> {
        request.validate()?;
        let mut response = self.send(request)?;
        if matches!(response.status, 404 | 405) {
            let alternate = Self::alternate(request.endpoint);
            let supported = match alternate {
                Endpoint::Responses => self.capabilities.responses,
                Endpoint::ChatCompletions => self.capabilities.chat_completions,
            };
            if supported {
                let mut alternate_request = request.clone();
                alternate_request.endpoint = alternate;
                response = self.send(&alternate_request)?;
            }
        }
        Self::classify(&response)?;
        let body = String::from_utf8(response.body)
            .map_err(|_| LlmError::MalformedOutput("provider response is not UTF-8".into()))?;
        if let Some(refusal) = extract_json_string(&body, "refusal") {
            return Err(LlmError::Refusal(refusal));
        }
        let content = extract_json_string(&body, "output_text")
            .or_else(|| extract_json_string(&body, "content"))
            .ok_or_else(|| LlmError::MalformedOutput("provider content field missing".into()))?;
        Ok(Response {
            trace_id: request.trace_id.clone(),
            content,
            finish_reason: extract_json_string(&body, "finish_reason")
                .unwrap_or_else(|| "stop".into()),
        })
    }

    fn stream(&self, request: &Request) -> Result<Vec<StreamItem>, LlmError> {
        request.validate()?;
        if !self.capabilities.streaming {
            return Err(LlmError::Provider(
                "provider does not support streaming".into(),
            ));
        }
        let mut response = self.send(request)?;
        if matches!(response.status, 404 | 405) {
            let alternate = Self::alternate(request.endpoint);
            let supported = match alternate {
                Endpoint::Responses => self.capabilities.responses,
                Endpoint::ChatCompletions => self.capabilities.chat_completions,
            };
            if supported {
                let mut alternate_request = request.clone();
                alternate_request.endpoint = alternate;
                response = self.send(&alternate_request)?;
            }
        }
        Self::classify(&response)?;
        parse_sse(&response.body)
    }
}

fn json_string(value: &str) -> String {
    let mut output = String::with_capacity(value.len().saturating_add(2));
    output.push('"');
    for character in value.chars() {
        match character {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            character if character.is_control() => output.push('?'),
            character => output.push(character),
        }
    }
    output.push('"');
    output
}

fn extract_json_string(input: &str, field: &str) -> Option<String> {
    let marker = format!("\"{field}\"");
    let start = input.find(&marker)?.saturating_add(marker.len());
    let bytes = input.as_bytes();
    let mut cursor = start;
    while bytes.get(cursor).is_some_and(u8::is_ascii_whitespace) || bytes.get(cursor) == Some(&b':')
    {
        cursor += 1;
    }
    if bytes.get(cursor) != Some(&b'\"') {
        return None;
    }
    cursor += 1;
    let mut output = String::new();
    while let Some(byte) = bytes.get(cursor).copied() {
        cursor += 1;
        match byte {
            b'\"' => return Some(output),
            b'\\' => {
                let escaped = bytes.get(cursor).copied()?;
                cursor += 1;
                output.push(match escaped {
                    b'\"' => '"',
                    b'\\' => '\\',
                    b'/' => '/',
                    b'n' => '\n',
                    b'r' => '\r',
                    b't' => '\t',
                    _ => return None,
                });
            }
            byte if byte.is_ascii() => output.push(byte as char),
            _ => return None,
        }
    }
    None
}

fn parse_sse(bytes: &[u8]) -> Result<Vec<StreamItem>, LlmError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| LlmError::MalformedOutput("stream is not UTF-8".into()))?;
    let mut items = Vec::new();
    for line in text.lines() {
        let Some(data) = line.strip_prefix("data:") else {
            continue;
        };
        let data = data.trim();
        if data == "[DONE]" {
            items.push(StreamItem::Done("stop".into()));
            continue;
        }
        if let Some(delta) =
            extract_json_string(data, "delta").or_else(|| extract_json_string(data, "text"))
        {
            items.push(StreamItem::Delta(delta));
        }
        if let Some(reason) = extract_json_string(data, "finish_reason") {
            items.push(StreamItem::Done(reason));
        }
    }
    if items.is_empty() {
        return Err(LlmError::MalformedOutput("stream contained no data".into()));
    }
    if !items.iter().any(|item| matches!(item, StreamItem::Done(_))) {
        return Err(LlmError::InterruptedStream);
    }
    Ok(items)
}

/// Provider adapter boundary. HTTP, local inference, retries, and auth stay outside the core.
pub trait Provider: Send + Sync {
    /// Returns the provider manifest when the adapter exposes one.
    fn manifest(&self) -> Option<insider_provider_core::ProviderManifest> {
        None
    }

    /// Returns capabilities discovered during provider health probing.
    fn capabilities(&self) -> Capabilities;
    /// Performs one bounded non-streaming request.
    ///
    /// # Errors
    /// Returns [`LlmError`] for provider, transport, or request failures.
    fn complete(&self, request: &Request) -> Result<Response, LlmError>;
    /// Performs one bounded streaming request and returns all stream items in order.
    ///
    /// # Errors
    /// Returns [`LlmError`] for provider, transport, or request failures.
    fn stream(&self, request: &Request) -> Result<Vec<StreamItem>, LlmError>;
}

/// Provider-neutral endpoint router with bounded fallback behavior.
pub struct ProviderRouter {
    primary: Box<dyn Provider>,
    fallback: Option<Box<dyn Provider>>,
    preferred: Endpoint,
}

/// Bounded retry policy for control-plane provider calls.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetryPolicy {
    /// Maximum total attempts, including the first call.
    pub max_attempts: u8,
    /// Initial delay before retrying a transient failure.
    pub base_delay_ms: u64,
    /// Maximum delay imposed by this policy or a provider hint.
    pub max_delay_ms: u64,
}

impl RetryPolicy {
    /// Returns whether the policy has safe, finite bounds.
    #[must_use]
    pub const fn is_valid(self) -> bool {
        self.max_attempts > 0 && self.base_delay_ms > 0 && self.max_delay_ms >= self.base_delay_ms
    }
}

#[derive(Clone, Debug)]
struct CachedResponse {
    inserted_ms: i64,
    response: Response,
}

/// Bounded semantic response cache for non-trading or explicitly cacheable
/// tasks. The key includes every request field that can change model output.
#[derive(Clone, Debug)]
pub struct ResponseCache {
    capacity: usize,
    ttl_ms: i64,
    entries: std::collections::BTreeMap<String, CachedResponse>,
}

impl ResponseCache {
    /// Creates a finite cache. A zero capacity or TTL is rejected.
    #[must_use]
    pub fn new(capacity: usize, ttl_ms: i64) -> Option<Self> {
        (capacity > 0 && ttl_ms > 0).then(|| Self {
            capacity,
            ttl_ms,
            entries: std::collections::BTreeMap::new(),
        })
    }

    /// Returns a fresh cached response for an equivalent semantic request.
    pub fn get(&mut self, request: &Request, now_ms: i64) -> Option<Response> {
        let key = semantic_key(request);
        let entry = self.entries.get(&key)?;
        if now_ms.saturating_sub(entry.inserted_ms) < self.ttl_ms {
            return Some(entry.response.clone());
        }
        self.entries.remove(&key);
        None
    }

    /// Inserts a successful response and evicts the oldest entry at capacity.
    pub fn insert(&mut self, request: &Request, now_ms: i64, response: Response) {
        let key = semantic_key(request);
        if self.entries.len() >= self.capacity
            && !self.entries.contains_key(&key)
            && let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(entry_key, entry)| (entry.inserted_ms, entry_key.as_str()))
                .map(|(entry_key, _)| entry_key.clone())
        {
            self.entries.remove(&oldest);
        }
        self.entries.insert(
            key,
            CachedResponse {
                inserted_ms: now_ms,
                response,
            },
        );
    }

    /// Returns the number of retained responses.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns whether the cache contains no retained responses.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Exports retained entries in deterministic semantic-key order.
    #[must_use]
    pub fn snapshot(&self) -> Vec<CachedResponseSnapshot> {
        self.entries
            .iter()
            .map(|(key, entry)| CachedResponseSnapshot {
                key: key.clone(),
                inserted_ms: entry.inserted_ms,
                response: entry.response.clone(),
            })
            .collect()
    }

    /// Restores pinned responses, rejecting malformed or oversized snapshots.
    ///
    /// # Errors
    /// Returns [`LlmError::SchemaViolation`] if an entry has blank identity,
    /// blank response metadata, duplicates, or exceeds cache bounds.
    pub fn restore_snapshot(
        &mut self,
        entries: Vec<CachedResponseSnapshot>,
    ) -> Result<(), LlmError> {
        if entries.len() > self.capacity {
            return Err(LlmError::SchemaViolation(
                "cache snapshot exceeds capacity".into(),
            ));
        }
        let mut restored = std::collections::BTreeMap::new();
        for entry in entries {
            if entry.key.trim().is_empty()
                || entry.response.trace_id.trim().is_empty()
                || entry.response.content.is_empty()
                || entry.response.finish_reason.trim().is_empty()
                || restored.contains_key(&entry.key)
            {
                return Err(LlmError::SchemaViolation(
                    "invalid cache snapshot entry".into(),
                ));
            }
            restored.insert(
                entry.key,
                CachedResponse {
                    inserted_ms: entry.inserted_ms,
                    response: entry.response,
                },
            );
        }
        self.entries = restored;
        Ok(())
    }
}

fn semantic_key(request: &Request) -> String {
    let canonical = format!(
        "{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}",
        request.prompt_version,
        request.model,
        request.task,
        request.context_hash,
        request.input,
        request.max_output_tokens,
        match request.endpoint {
            Endpoint::Responses => "responses",
            Endpoint::ChatCompletions => "chat_completions",
        },
    );
    let mut hash = 2_166_136_261_u64;
    for byte in canonical.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(1_099_511_628_211);
    }
    format!("{hash:016x}")
}

impl ProviderRouter {
    /// Creates a router. The fallback is used only when the selected primary
    /// endpoint is unavailable or returns a provider error.
    #[must_use]
    pub fn new(
        primary: Box<dyn Provider>,
        fallback: Option<Box<dyn Provider>>,
        preferred: Endpoint,
    ) -> Self {
        Self {
            primary,
            fallback,
            preferred,
        }
    }

    /// Returns the union of capabilities available through the route.
    #[must_use]
    pub fn capabilities(&self) -> Capabilities {
        let primary = self.primary.capabilities();
        let Some(fallback) = self.fallback.as_ref() else {
            return primary;
        };
        let secondary = fallback.capabilities();
        Capabilities {
            responses: primary.responses || secondary.responses,
            chat_completions: primary.chat_completions || secondary.chat_completions,
            streaming: primary.streaming || secondary.streaming,
            json_schema: primary.json_schema || secondary.json_schema,
            tools: primary.tools || secondary.tools,
        }
    }

    fn endpoint_available(capabilities: Capabilities, endpoint: Endpoint) -> bool {
        match endpoint {
            Endpoint::Responses => capabilities.responses,
            Endpoint::ChatCompletions => capabilities.chat_completions,
        }
    }

    fn request_for(
        &self,
        request: &Request,
        capabilities: Capabilities,
    ) -> Result<Request, LlmError> {
        let preferred = if Self::endpoint_available(capabilities, self.preferred) {
            self.preferred
        } else {
            match self.preferred {
                Endpoint::Responses => Endpoint::ChatCompletions,
                Endpoint::ChatCompletions => Endpoint::Responses,
            }
        };
        if !Self::endpoint_available(capabilities, preferred) {
            return Err(LlmError::Provider(String::from(
                "no compatible LLM endpoint",
            )));
        }
        let mut routed = request.clone();
        routed.endpoint = preferred;
        Ok(routed)
    }

    fn should_fallback(error: &LlmError) -> bool {
        matches!(
            error,
            LlmError::Provider(_)
                | LlmError::RateLimited { .. }
                | LlmError::Timeout
                | LlmError::Refusal(_)
        )
    }

    /// Completes a request through the primary provider, then one fallback.
    ///
    /// # Errors
    /// Returns request validation, endpoint, or provider failures. At most two
    /// provider calls occur, preventing unbounded retry storms.
    pub fn complete(&self, request: &Request) -> Result<Response, LlmError> {
        request.validate()?;
        let routed = self.request_for(request, self.primary.capabilities())?;
        match self.primary.complete(&routed) {
            Ok(response) => Ok(response),
            Err(primary_error) => {
                if !Self::should_fallback(&primary_error) {
                    return Err(primary_error);
                }
                let Some(fallback) = self.fallback.as_ref() else {
                    return Err(primary_error);
                };
                let fallback_request = self.request_for(request, fallback.capabilities())?;
                fallback.complete(&fallback_request)
            }
        }
    }

    /// Completes a request using a bounded semantic cache for successful output.
    ///
    /// The cache is caller-owned so it can be scoped per task/account and
    /// persisted or disabled by policy. Errors are never cached.
    ///
    /// # Errors
    /// Returns the same validation or provider error as [`Self::complete`].
    pub fn complete_cached(
        &self,
        request: &Request,
        cache: &mut ResponseCache,
        now_ms: i64,
    ) -> Result<Response, LlmError> {
        request.validate()?;
        if let Some(mut response) = cache.get(request, now_ms) {
            response.trace_id.clone_from(&request.trace_id);
            return Ok(response);
        }
        let response = self.complete(request)?;
        cache.insert(request, now_ms, response.clone());
        Ok(response)
    }

    /// Completes a request with bounded retries for transient failures.
    ///
    /// Authentication, refusal, malformed output, schema, and semantic errors
    /// are returned immediately. Delays use provider `Retry-After` when
    /// present, otherwise exponential backoff with deterministic trace-derived
    /// jitter, bounded by `policy.max_delay_ms`.
    ///
    /// # Errors
    /// Returns [`LlmError`] when the policy is invalid or all attempts fail.
    pub fn complete_with_retry(
        &self,
        request: &Request,
        policy: RetryPolicy,
    ) -> Result<Response, LlmError> {
        if !policy.is_valid() {
            return Err(LlmError::Provider("invalid retry policy".into()));
        }
        let mut attempt = 0_u8;
        loop {
            attempt = attempt.saturating_add(1);
            match self.complete(request) {
                Ok(response) => return Ok(response),
                Err(error) if attempt < policy.max_attempts && Self::retryable(&error) => {
                    let exponent = u32::from(attempt.saturating_sub(1)).min(16);
                    let exponential = policy
                        .base_delay_ms
                        .saturating_mul(1_u64 << exponent)
                        .min(policy.max_delay_ms);
                    let hinted = match error {
                        LlmError::RateLimited {
                            retry_after_ms: Some(delay),
                        } => delay.min(policy.max_delay_ms),
                        _ => exponential,
                    };
                    let jitter = trace_jitter_ms(&request.trace_id, hinted);
                    std::thread::sleep(std::time::Duration::from_millis(
                        hinted.saturating_add(jitter).min(policy.max_delay_ms),
                    ));
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn retryable(error: &LlmError) -> bool {
        matches!(
            error,
            LlmError::Provider(_) | LlmError::RateLimited { .. } | LlmError::Timeout
        )
    }

    /// Streams through the primary provider, then one fallback on failure.
    ///
    /// # Errors
    /// Returns request validation, endpoint, or provider failures.
    pub fn stream(&self, request: &Request) -> Result<Vec<StreamItem>, LlmError> {
        request.validate()?;
        if !self.capabilities().streaming {
            return Err(LlmError::Provider(String::from(
                "no streaming-capable LLM provider",
            )));
        }
        let routed = self.request_for(request, self.primary.capabilities())?;
        match self.primary.stream(&routed) {
            Ok(items) => Ok(items),
            Err(primary_error) => {
                if !Self::should_fallback(&primary_error) {
                    return Err(primary_error);
                }
                let Some(fallback) = self.fallback.as_ref() else {
                    return Err(primary_error);
                };
                let fallback_request = self.request_for(request, fallback.capabilities())?;
                fallback.stream(&fallback_request)
            }
        }
    }

    /// Completes and validates one trading-relevant autonomous action.
    ///
    /// The complete response is buffered before parsing. A response with a
    /// mismatched trace cannot be accepted, preventing cross-request output
    /// confusion when a provider or gateway behaves incorrectly.
    ///
    /// # Errors
    /// Returns provider, trace, syntax, schema, or semantic validation errors.
    pub fn complete_action(&self, request: &Request) -> Result<AutonomousAction, LlmError> {
        let response = self.complete(request)?;
        if response.trace_id != request.trace_id {
            return Err(LlmError::SchemaViolation(
                "response trace_id does not match request".into(),
            ));
        }
        parse_autonomous_action(&response.content)
    }
}

fn trace_jitter_ms(trace_id: &str, upper_bound: u64) -> u64 {
    if upper_bound == 0 {
        return 0;
    }
    let mut hash = 2_166_136_261_u64;
    for byte in trace_id.bytes() {
        hash = hash.rotate_left(5) ^ u64::from(byte);
    }
    (hash % upper_bound.saturating_add(1)).min(upper_bound / 4)
}

/// Typed finite vocabulary for autonomous decisions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActionType {
    /// Execute an existing validated proposal.
    ExecuteProposal,
    /// Execute an existing proposal at a scale.
    ExecuteProposalScaled,
    /// Ignore an existing proposal.
    IgnoreProposal,
    /// Pause a strategy.
    PauseStrategy,
    /// Resume a strategy.
    ResumeStrategy,
    /// Ask for fresh analysis.
    RequestReanalysis,
    /// Add an instrument to watch.
    AddToWatch,
    /// Remove an instrument from watch.
    RemoveFromWatch,
    /// Reduce autonomy.
    ReduceAutonomy,
    /// Explicitly do nothing.
    NoAction,
}

/// Schema-validated autonomous action.
#[derive(Clone, Debug, PartialEq)]
pub struct AutonomousAction {
    /// Finite action type.
    pub action_type: ActionType,
    /// Existing proposal identifier when required by action type.
    pub proposal_id: Option<String>,
    /// Scale for scaled execution, constrained to `[0, 1]`.
    pub scale: Option<f64>,
    /// Stable reason codes.
    pub reason_codes: Vec<String>,
}

impl AutonomousAction {
    /// Validates action-specific fields before the action reaches trading services.
    ///
    /// # Errors
    /// Returns [`LlmError::InvalidAction`] for missing IDs, invalid scales, or empty reasons.
    pub fn validate(&self) -> Result<(), LlmError> {
        if matches!(
            self.action_type,
            ActionType::ExecuteProposal | ActionType::ExecuteProposalScaled
        ) && self.proposal_id.as_deref().is_none_or(str::is_empty)
        {
            return Err(LlmError::InvalidAction("proposal_id required".into()));
        }
        if self.action_type == ActionType::ExecuteProposalScaled
            && !self
                .scale
                .is_some_and(|scale| scale.is_finite() && (0.0..=1.0).contains(&scale))
        {
            return Err(LlmError::InvalidAction(
                "scaled execution requires scale in [0,1]".into(),
            ));
        }
        if self
            .reason_codes
            .iter()
            .any(|reason| reason.trim().is_empty())
        {
            return Err(LlmError::InvalidAction(
                "reason codes must be non-empty".into(),
            ));
        }
        Ok(())
    }
}

/// Parses one complete autonomous action object from provider JSON.
///
/// The parser is deliberately strict: one object is required, unknown or
/// duplicate fields are rejected, and no trailing content is accepted. This
/// keeps streamed text display-only until the complete action is validated.
///
/// # Errors
/// Returns [`LlmError::MalformedOutput`], [`LlmError::SchemaViolation`], or
/// [`LlmError::SemanticValidation`] before an action can reach autonomy.
pub fn parse_autonomous_action(input: &str) -> Result<AutonomousAction, LlmError> {
    let mut parser = JsonParser::new(input);
    parser.object_action()
}

struct JsonParser<'a> {
    input: &'a [u8],
    cursor: usize,
}

impl<'a> JsonParser<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            input: input.as_bytes(),
            cursor: 0,
        }
    }

    fn error(message: impl Into<String>) -> LlmError {
        LlmError::MalformedOutput(message.into())
    }

    fn schema(message: impl Into<String>) -> LlmError {
        LlmError::SchemaViolation(message.into())
    }

    fn whitespace(&mut self) {
        while self
            .input
            .get(self.cursor)
            .is_some_and(u8::is_ascii_whitespace)
        {
            self.cursor += 1;
        }
    }

    fn byte(&self) -> Option<u8> {
        self.input.get(self.cursor).copied()
    }

    fn consume(&mut self, expected: u8) -> Result<(), LlmError> {
        self.whitespace();
        if self.byte() == Some(expected) {
            self.cursor += 1;
            Ok(())
        } else {
            Err(Self::error(format!(
                "expected '{}': offset {}",
                expected as char, self.cursor
            )))
        }
    }

    fn string(&mut self) -> Result<String, LlmError> {
        self.whitespace();
        self.consume(b'"')?;
        let mut output = String::new();
        loop {
            let Some(byte) = self.byte() else {
                return Err(Self::error("unterminated string"));
            };
            self.cursor += 1;
            match byte {
                b'"' => return Ok(output),
                b'\\' => {
                    let Some(escaped) = self.byte() else {
                        return Err(Self::error("unterminated escape"));
                    };
                    self.cursor += 1;
                    let character = match escaped {
                        b'"' => '"',
                        b'\\' => '\\',
                        b'/' => '/',
                        b'b' => '\u{0008}',
                        b'f' => '\u{000c}',
                        b'n' => '\n',
                        b'r' => '\r',
                        b't' => '\t',
                        _ => return Err(Self::error("unsupported string escape")),
                    };
                    output.push(character);
                }
                0..=0x1f => return Err(Self::error("control byte in string")),
                byte if byte.is_ascii() => output.push(byte as char),
                _ => return Err(Self::error("non-ASCII JSON requires escaped UTF-8")),
            }
        }
    }

    fn number(&mut self) -> Result<f64, LlmError> {
        self.whitespace();
        let start = self.cursor;
        while self.byte().is_some_and(|byte| {
            byte.is_ascii_digit() || matches!(byte, b'-' | b'+' | b'.' | b'e' | b'E')
        }) {
            self.cursor += 1;
        }
        if start == self.cursor {
            return Err(Self::error("number required"));
        }
        let text = std::str::from_utf8(&self.input[start..self.cursor])
            .map_err(|_| Self::error("invalid number bytes"))?;
        text.parse::<f64>()
            .map_err(|_| Self::error("invalid number"))
    }

    fn null(&mut self) -> Result<(), LlmError> {
        self.whitespace();
        if self.input.get(self.cursor..self.cursor.saturating_add(4)) == Some(b"null") {
            self.cursor += 4;
            Ok(())
        } else {
            Err(Self::error("null required"))
        }
    }

    fn object_action(&mut self) -> Result<AutonomousAction, LlmError> {
        self.consume(b'{')?;
        let mut action_type = None;
        let mut proposal_id = None;
        let mut scale = None;
        let mut reasons = None;
        let mut seen = std::collections::BTreeSet::new();
        self.whitespace();
        if self.byte() != Some(b'}') {
            loop {
                let key = self.string()?;
                if !seen.insert(key.clone()) {
                    return Err(Self::schema(format!("duplicate field: {key}")));
                }
                self.consume(b':')?;
                match key.as_str() {
                    "type" => action_type = Some(self.string()?),
                    "proposal_id" => {
                        self.whitespace();
                        proposal_id = if self.byte() == Some(b'n') {
                            self.null()?;
                            Some(None)
                        } else {
                            Some(Some(self.string()?))
                        };
                    }
                    "scale" => {
                        self.whitespace();
                        scale = if self.byte() == Some(b'n') {
                            self.null()?;
                            Some(None)
                        } else {
                            Some(Some(self.number()?))
                        };
                    }
                    "reason_codes" => {
                        self.consume(b'[')?;
                        let mut values = Vec::new();
                        self.whitespace();
                        if self.byte() != Some(b']') {
                            loop {
                                values.push(self.string()?);
                                self.whitespace();
                                if self.byte() == Some(b']') {
                                    break;
                                }
                                self.consume(b',')?;
                            }
                        }
                        self.consume(b']')?;
                        reasons = Some(values);
                    }
                    _ => return Err(Self::schema(format!("unknown field: {key}"))),
                }
                self.whitespace();
                if self.byte() == Some(b'}') {
                    break;
                }
                self.consume(b',')?;
            }
        }
        self.consume(b'}')?;
        self.whitespace();
        if self.cursor != self.input.len() {
            return Err(Self::error("trailing JSON content"));
        }
        let action_type = action_type.ok_or_else(|| Self::schema("type is required"))?;
        let action_type = match action_type.as_str() {
            "EXECUTE_PROPOSAL" => ActionType::ExecuteProposal,
            "EXECUTE_PROPOSAL_SCALED" => ActionType::ExecuteProposalScaled,
            "IGNORE_PROPOSAL" => ActionType::IgnoreProposal,
            "PAUSE_STRATEGY" => ActionType::PauseStrategy,
            "RESUME_STRATEGY" => ActionType::ResumeStrategy,
            "REQUEST_REANALYSIS" => ActionType::RequestReanalysis,
            "ADD_TO_WATCH" => ActionType::AddToWatch,
            "REMOVE_FROM_WATCH" => ActionType::RemoveFromWatch,
            "REDUCE_AUTONOMY" => ActionType::ReduceAutonomy,
            "NO_ACTION" => ActionType::NoAction,
            _ => return Err(Self::schema("unsupported action type")),
        };
        let action = AutonomousAction {
            action_type,
            proposal_id: proposal_id.flatten(),
            scale: scale.flatten(),
            reason_codes: reasons.ok_or_else(|| Self::schema("reason_codes is required"))?,
        };
        action
            .validate()
            .map_err(|error| LlmError::SemanticValidation(format!("{error:?}")))?;
        Ok(action)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ActionType, AutonomousAction, Capabilities, Endpoint, HttpRequest, HttpResponse,
        HttpTransport, LlmError, MAX_RESPONSE_BYTES, OpenAiCompatibleProvider, Provider, Request,
        SUBSYSTEM_ID, StreamItem, ToolHandler, ToolPermission, ToolRegistry, ToolRequest,
        ToolResponse, ToolSpec, parse_autonomous_action, read_bounded_response,
    };

    #[test]
    fn subsystem_id_is_non_empty_and_ascii() {
        assert!(!SUBSYSTEM_ID.is_empty());
        assert!(SUBSYSTEM_ID.is_ascii());
    }

    #[test]
    fn llm_response_reader_enforces_the_transport_bound() {
        assert!(
            read_bounded_response(std::io::Cursor::new(vec![0_u8; MAX_RESPONSE_BYTES + 1]))
                .is_err()
        );
        assert_eq!(
            read_bounded_response(std::io::Cursor::new(vec![1_u8; MAX_RESPONSE_BYTES]))
                .ok()
                .map(|body| body.len()),
            Some(MAX_RESPONSE_BYTES)
        );
    }

    #[test]
    fn request_and_autonomous_action_are_strictly_validated() {
        let request = Request {
            trace_id: "trace-1".into(),
            prompt_version: "prompt-v1".into(),
            model: "model".into(),
            task: "AUTONOMOUS_PLAN".into(),
            context_hash: "hash".into(),
            input: "choose no action".into(),
            max_output_tokens: 128,
            endpoint: Endpoint::Responses,
        };
        assert!(request.validate().is_ok());
        let action = AutonomousAction {
            action_type: ActionType::ExecuteProposalScaled,
            proposal_id: Some("proposal-1".into()),
            scale: Some(0.5),
            reason_codes: vec!["agreement".into()],
        };
        assert!(action.validate().is_ok());
        assert!(
            AutonomousAction {
                scale: Some(2.0),
                ..action
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn structured_action_parser_rejects_unknown_duplicate_and_trailing_fields() {
        let parsed = parse_autonomous_action(
            r#"{"type":"EXECUTE_PROPOSAL_SCALED","proposal_id":"p-1","scale":0.5,"reason_codes":["agreement"]}"#,
        );
        assert!(parsed.is_ok_and(|action| {
            action.action_type == ActionType::ExecuteProposalScaled
                && action.proposal_id.as_deref() == Some("p-1")
                && action.scale == Some(0.5)
        }));
        assert!(matches!(
            parse_autonomous_action(r#"{"type":"NO_ACTION","reason_codes":[],"extra":1}"#),
            Err(LlmError::SchemaViolation(_))
        ));
        assert!(matches!(
            parse_autonomous_action(r#"{"type":"NO_ACTION","type":"NO_ACTION","reason_codes":[]}"#),
            Err(LlmError::SchemaViolation(_))
        ));
        assert!(matches!(
            parse_autonomous_action(r#"{"type":"NO_ACTION","reason_codes":[]} trailing"#),
            Err(LlmError::MalformedOutput(_))
        ));
    }

    #[derive(Clone)]
    struct FakeTransport {
        response: HttpResponse,
    }

    impl HttpTransport for FakeTransport {
        fn send(&self, request: HttpRequest) -> Result<HttpResponse, super::TransportError> {
            assert_eq!(request.method, "POST");
            assert!(request.url.ends_with("/responses"));
            assert!(String::from_utf8_lossy(&request.body).contains("choose no action"));
            Ok(self.response.clone())
        }
    }

    #[derive(Clone)]
    struct StatusTransport {
        status: u16,
        headers: Vec<(String, String)>,
    }

    impl HttpTransport for StatusTransport {
        fn send(&self, _request: HttpRequest) -> Result<HttpResponse, super::TransportError> {
            Ok(HttpResponse {
                status: self.status,
                headers: self.headers.clone(),
                body: Vec::new(),
            })
        }
    }

    fn provider_request() -> Request {
        Request {
            trace_id: "trace-provider".into(),
            prompt_version: "prompt-v1".into(),
            model: "configured-model".into(),
            task: "NEWS_SUMMARY".into(),
            context_hash: "ctx".into(),
            input: "choose no action".into(),
            max_output_tokens: 32,
            endpoint: Endpoint::Responses,
        }
    }

    #[test]
    fn openai_adapter_classifies_rate_limit_auth_and_server_failures() {
        let capabilities = Capabilities {
            responses: true,
            chat_completions: false,
            streaming: false,
            json_schema: false,
            tools: false,
        };
        let limited = OpenAiCompatibleProvider::new(
            StatusTransport {
                status: 429,
                headers: vec![("Retry-After".into(), "3".into())],
            },
            "https://llm.example/v1",
            "secret",
            capabilities,
        );
        let limited = limited.ok();
        assert!(limited.is_some());
        let Some(limited) = limited else { return };
        assert!(matches!(
            limited.complete(&provider_request()),
            Err(LlmError::RateLimited {
                retry_after_ms: Some(3_000)
            })
        ));

        for (status, expected) in [(401, "authentication"), (503, "server failure")] {
            let provider = OpenAiCompatibleProvider::new(
                StatusTransport {
                    status,
                    headers: Vec::new(),
                },
                "https://llm.example/v1",
                "secret",
                capabilities,
            );
            let provider = provider.ok();
            assert!(provider.is_some());
            let Some(provider) = provider else { continue };
            let result = provider.complete(&provider_request());
            assert!(result.is_err(), "status {status} unexpectedly succeeded");
            let Err(error) = result else { continue };
            let rendered = format!("{error:?}").to_lowercase();
            assert!(rendered.contains(expected), "{rendered}");
        }
    }

    #[test]
    fn openai_compatible_adapter_translates_complete_and_sse_streams() {
        let response = HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: br#"{"output_text":"hello","finish_reason":"stop"}"#.to_vec(),
        };
        let capabilities = Capabilities {
            responses: true,
            chat_completions: false,
            streaming: true,
            json_schema: true,
            tools: false,
        };
        let provider = OpenAiCompatibleProvider::new(
            FakeTransport {
                response: response.clone(),
            },
            "https://llm.example/v1/",
            "secret",
            capabilities,
        );
        assert!(provider.is_ok());
        if let Ok(provider) = provider {
            let complete = provider.complete(&provider_request()).ok();
            assert_eq!(complete.map(|value| value.content), Some("hello".into()));
        }
        let stream_provider = OpenAiCompatibleProvider::new(
            FakeTransport {
                response: HttpResponse {
                    status: 200,
                    headers: Vec::new(),
                    body:
                        b"data: {\"delta\":\"hel\"}\n\ndata: {\"delta\":\"lo\"}\n\ndata: [DONE]\n\n"
                            .to_vec(),
                },
            },
            "https://llm.example/v1",
            "secret",
            capabilities,
        );
        assert_eq!(
            stream_provider
                .ok()
                .and_then(|provider| provider.stream(&provider_request()).ok()),
            Some(vec![
                StreamItem::Delta("hel".into()),
                StreamItem::Delta("lo".into()),
                StreamItem::Done("stop".into()),
            ])
        );
    }

    #[test]
    fn semantic_cache_reuses_equivalent_context_but_expires_and_bounds_entries() {
        let Some(mut cache) = super::ResponseCache::new(1, 100) else {
            return;
        };
        let first = provider_request();
        cache.insert(
            &first,
            1_000,
            super::Response {
                trace_id: first.trace_id.clone(),
                content: "cached".into(),
                finish_reason: "stop".into(),
            },
        );
        let mut equivalent = first.clone();
        equivalent.trace_id = "new-trace".into();
        assert_eq!(
            cache.get(&equivalent, 1_050).map(|value| value.content),
            Some("cached".into())
        );
        assert!(cache.get(&equivalent, 1_100).is_none());
        assert!(cache.is_empty());
    }

    #[test]
    fn prompt_registry_requires_exact_immutable_versions() {
        let hash = super::PromptRecord::content_hash("analyst", "1.2.0", "prompt body");
        let record = super::PromptRecord {
            prompt_id: "analyst".into(),
            version: "1.2.0".into(),
            purpose: "chart analysis".into(),
            input_schema: "chart-context-v1".into(),
            output_schema: "analysis-v1".into(),
            allowed_tools: vec!["get_market_snapshot".into(), "get_news".into()],
            task_class: "CHART_CONTEXT".into(),
            required_capabilities: super::Capabilities {
                responses: true,
                chat_completions: false,
                streaming: true,
                json_schema: true,
                tools: true,
            },
            artifact_hash: hash,
            fixture_suite: "analyst-v1".into(),
        };
        let mut registry = super::PromptRegistry::new();
        assert!(registry.register(record.clone()).is_ok());
        assert_eq!(registry.get("analyst", "1.2.0"), Some(&record));
        assert!(registry.get("analyst", "latest").is_none());
        assert!(registry.register(record).is_err());
    }

    struct TestTool {
        spec: ToolSpec,
        delay_ms: u64,
    }

    impl ToolHandler for TestTool {
        fn spec(&self) -> &ToolSpec {
            &self.spec
        }

        fn invoke(&self, request: &ToolRequest) -> Result<ToolResponse, LlmError> {
            std::thread::sleep(std::time::Duration::from_millis(self.delay_ms));
            Ok(ToolResponse {
                trace_id: request.trace_id.clone(),
                name: request.name.clone(),
                output: "{}".into(),
            })
        }
    }

    fn test_spec(name: &str, permission: ToolPermission, deadline_ms: u64) -> ToolSpec {
        ToolSpec {
            name: name.into(),
            max_input_bytes: 16,
            max_output_bytes: 16,
            permission,
            deadline_ms,
        }
    }

    #[test]
    fn tool_registry_denies_actions_and_enforces_deadlines() {
        let mut registry = ToolRegistry::new();
        assert!(
            registry
                .register(Box::new(TestTool {
                    spec: test_spec("mutate", ToolPermission::Action, 100),
                    delay_ms: 0,
                }))
                .is_ok()
        );
        let request = ToolRequest {
            trace_id: "trace".into(),
            name: "mutate".into(),
            input: "{}".into(),
        };
        assert!(matches!(
            registry.invoke(&request),
            Err(LlmError::InvalidAction(_))
        ));

        let mut slow = ToolRegistry::new();
        assert!(
            slow.register(Box::new(TestTool {
                spec: test_spec("slow", ToolPermission::ReadOnly, 1),
                delay_ms: 5,
            }))
            .is_ok()
        );
        let request = ToolRequest {
            trace_id: "trace".into(),
            name: "slow".into(),
            input: "{}".into(),
        };
        assert!(matches!(slow.invoke(&request), Err(LlmError::Timeout)));
    }
}
