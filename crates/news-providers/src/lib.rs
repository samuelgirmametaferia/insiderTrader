//! HTTP provider adapters for normalized news ingestion.
//!
//! The adapters own URL construction, authentication headers, response-size
//! limits, status classification, and provider-specific JSON mapping. HTTP is
//! injected so the engine can use a production client, deterministic replay,
//! or a fault-injection transport without changing news-core.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io::Read;
use std::sync::Mutex;

use insider_news_core::{CursorProvider, NewsItem, ProviderBatch, RetryClass};
use insider_provider_core::{
    AuthMethod, ProviderKind, ProviderManifest, RetryPolicy, TimeoutPolicy,
};

const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const MAX_PAGE_SIZE: usize = 100;
const MAX_TIMEOUT_MS: u64 = 120_000;

fn valid_https_base_url(value: &str) -> bool {
    let authority = value
        .strip_prefix("https://")
        .and_then(|rest| rest.split(['/', '?', '#']).next())
        .unwrap_or_default();
    value.len() <= 2_048
        && !value.chars().any(char::is_whitespace)
        && !authority.is_empty()
        && !authority.contains('@')
        && reqwest::Url::parse(value).is_ok_and(|url| {
            url.scheme() == "https"
                && url.host_str().is_some_and(|host| !host.is_empty())
                && url.username().is_empty()
                && url.password().is_none()
        })
}

fn news_manifest(
    provider_id: &str,
    base_url: &str,
    auth: AuthMethod,
    capabilities: Vec<String>,
) -> ProviderManifest {
    ProviderManifest {
        provider_id: provider_id.into(),
        kind: ProviderKind::News,
        schema_version: "news-item-v1".into(),
        base_url: base_url.into(),
        auth,
        capabilities,
        retry: RetryPolicy {
            max_retries: 3,
            initial_backoff_ms: 100,
            max_backoff_ms: 5_000,
            honor_retry_after: true,
        },
        timeout: TimeoutPolicy {
            connect_timeout_ms: 2_000,
            request_timeout_ms: 30_000,
            max_parallel_requests: 4,
            max_requests: 60,
            window_ms: 60_000,
        },
        health_probe: None,
        streaming: false,
    }
}

/// Production HTTPS transport used by `NewsAPI`, Yahoo Finance, and RSS
/// adapters. Provider-specific parsing remains above this boundary so a
/// transport failure cannot be mistaken for an empty feed.
pub struct ReqwestHttpTransport {
    client: reqwest::blocking::Client,
}

impl ReqwestHttpTransport {
    /// Builds a TLS-only client with bounded connect and request timeouts.
    ///
    /// # Errors
    /// Returns a diagnostic when the requested timeout is zero, too large, or
    /// the underlying TLS client cannot be constructed.
    pub fn new(timeout_ms: u64) -> Result<Self, String> {
        if timeout_ms == 0 || timeout_ms > MAX_TIMEOUT_MS {
            return Err("news HTTP timeout is outside bounds".into());
        }
        let timeout = std::time::Duration::from_millis(timeout_ms);
        let client = reqwest::blocking::Client::builder()
            .connect_timeout(timeout)
            .timeout(timeout)
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| format!("news HTTP client: {error}"))?;
        Ok(Self { client })
    }
}

impl HttpTransport for ReqwestHttpTransport {
    fn send(&self, request: HttpRequest) -> Result<HttpResponse, String> {
        if request.method != "GET" {
            return Err("news HTTP transport only permits GET".into());
        }
        if !request.url.starts_with("https://") {
            return Err("news HTTP transport requires HTTPS".into());
        }
        let mut builder = self.client.get(&request.url);
        for (name, value) in &request.headers {
            builder = builder.header(name, value);
        }
        let response = builder
            .send()
            .map_err(|error| format!("transport: {error}"))?;
        if response
            .content_length()
            .is_some_and(|length| length > MAX_RESPONSE_BYTES as u64)
        {
            return Err("provider response exceeds bound".into());
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
            .collect::<BTreeMap<_, _>>();
        let body = read_bounded_body(response)?;
        Ok(HttpResponse {
            status,
            headers,
            body,
        })
    }
}

fn read_bounded_body(reader: impl Read) -> Result<Vec<u8>, String> {
    let mut body = Vec::new();
    reader
        .take((MAX_RESPONSE_BYTES as u64).saturating_add(1))
        .read_to_end(&mut body)
        .map_err(|error| format!("transport body: {error}"))?;
    if body.len() > MAX_RESPONSE_BYTES {
        return Err("provider response exceeds bound".into());
    }
    Ok(body)
}

/// HTTP request passed to an injected transport.
#[derive(Clone, Eq, PartialEq)]
pub struct HttpRequest {
    /// HTTP method.
    pub method: String,
    /// Fully encoded URL without secrets.
    pub url: String,
    /// Request headers.
    pub headers: BTreeMap<String, String>,
}

impl fmt::Debug for HttpRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpRequest")
            .field("method", &self.method)
            .field("url", &self.url)
            .field("header_names", &self.headers.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// HTTP response returned by an injected transport.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpResponse {
    /// Status code.
    pub status: u16,
    /// Response headers.
    pub headers: BTreeMap<String, String>,
    /// Bounded response body.
    pub body: Vec<u8>,
}

/// Transport boundary for provider adapters.
pub trait HttpTransport: Send + Sync {
    /// Performs one request.
    ///
    /// # Errors
    /// Returns a transport diagnostic when the request cannot be completed.
    fn send(&self, request: HttpRequest) -> Result<HttpResponse, String>;
}

/// Provider request failure classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderFailure {
    /// Credentials or authorization are invalid.
    Authentication,
    /// The provider asked the caller to wait.
    RateLimited {
        /// Provider-advertised delay before retrying.
        retry_after_ms: u64,
    },
    /// A bounded upstream/server failure.
    Transient,
    /// The provider response was malformed or unsupported.
    Permanent,
}

/// Adapter error preserving retry-relevant classification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderError {
    /// Failure class.
    pub class: ProviderFailure,
    /// Safe diagnostic without credentials or response bodies.
    pub message: String,
}

/// Converts this crate's redacted provider diagnostic into the news-core retry
/// vocabulary. The worker never retries authentication or schema failures.
#[must_use]
pub fn classify_provider_error(message: &str) -> RetryClass {
    if message.contains("RateLimited") {
        let retry_after_ms = message
            .split("retry_after_ms: ")
            .nth(1)
            .and_then(|value| value.split('}').next())
            .and_then(|value| value.trim().parse::<u64>().ok())
            .unwrap_or(1_000);
        RetryClass::RateLimited { retry_after_ms }
    } else if message.contains("Transient") || message.contains("transport:") {
        RetryClass::Transient
    } else {
        RetryClass::Permanent
    }
}

/// NewsAPI-compatible `/v2/everything` adapter.
pub struct NewsApiProvider<T> {
    transport: T,
    base_url: String,
    api_key: String,
    query: String,
    page_size: usize,
}

/// NewsAPI-compatible `/v2/top-headlines` adapter.
///
/// The endpoint has a different filter contract from `everything`; keeping a
/// separate type prevents accidentally sending a query as a country code or
/// silently changing pagination semantics for an existing deployment.
pub struct NewsApiTopHeadlinesProvider<T> {
    transport: T,
    base_url: String,
    api_key: String,
    country: Option<String>,
    category: Option<String>,
    sources: Option<String>,
    page_size: usize,
}

impl<T: HttpTransport> NewsApiTopHeadlinesProvider<T> {
    /// Constructs a top-headlines adapter with `NewsAPI`'s bounded filters.
    ///
    /// # Errors
    /// Returns [`ProviderError`] when the endpoint, key, filters, or page size
    /// violates the provider contract.
    pub fn from_secret(
        transport: T,
        base_url: impl Into<String>,
        api_key: String,
        country: Option<String>,
        category: Option<String>,
        sources: Option<String>,
        page_size: usize,
    ) -> Result<Self, ProviderError> {
        let base_url = base_url.into().trim_end_matches('/').to_owned();
        let country = country.map(|value| value.trim().to_lowercase());
        let category = category.map(|value| value.trim().to_lowercase());
        let sources = sources.map(|value| value.trim().to_owned());
        let valid_country = country.as_deref().is_none_or(|value| {
            value.len() == 2 && value.bytes().all(|byte| byte.is_ascii_lowercase())
        });
        let valid_category = category.as_deref().is_none_or(|value| {
            matches!(
                value,
                "business"
                    | "entertainment"
                    | "general"
                    | "health"
                    | "science"
                    | "sports"
                    | "technology"
            )
        });
        let valid_sources = sources.as_deref().is_none_or(|value| {
            !value.is_empty()
                && value.len() <= 512
                && value.split(',').all(|item| !item.trim().is_empty())
        });
        let page_size = page_size.min(MAX_PAGE_SIZE);
        if !valid_https_base_url(&base_url)
            || api_key.trim().is_empty()
            || (country.is_none() && category.is_none() && sources.is_none())
            || !valid_country
            || !valid_category
            || !valid_sources
            || page_size == 0
        {
            return Err(ProviderError {
                class: ProviderFailure::Permanent,
                message: "invalid NewsAPI top-headlines configuration".into(),
            });
        }
        Ok(Self {
            transport,
            base_url,
            api_key,
            country,
            category,
            sources,
            page_size,
        })
    }

    fn request(&self, page: usize, now_ms: i64) -> Result<ProviderBatch, ProviderError> {
        let mut filters = Vec::new();
        if let Some(country) = &self.country {
            filters.push(format!("country={}", encode(country)));
        }
        if let Some(category) = &self.category {
            filters.push(format!("category={}", encode(category)));
        }
        if let Some(sources) = &self.sources {
            filters.push(format!("sources={}", encode(sources)));
        }
        filters.push(format!("pageSize={}", self.page_size));
        filters.push(format!("page={}", page.max(1)));
        let url = format!("{}/v2/top-headlines?{}", self.base_url, filters.join("&"));
        let mut headers = BTreeMap::new();
        headers.insert("accept".into(), "application/json".into());
        headers.insert("x-api-key".into(), self.api_key.clone());
        let response = self
            .transport
            .send(HttpRequest {
                method: "GET".into(),
                url,
                headers,
            })
            .map_err(|message| ProviderError {
                class: ProviderFailure::Transient,
                message,
            })?;
        classify_status(&response)?;
        parse_newsapi_with_symbols(&response.body, page, &[], now_ms).map_err(|message| {
            ProviderError {
                class: ProviderFailure::Permanent,
                message,
            }
        })
    }
}

impl<T: HttpTransport> CursorProvider for NewsApiTopHeadlinesProvider<T> {
    fn manifest(&self) -> Option<ProviderManifest> {
        Some(news_manifest(
            "newsapi_top_headlines",
            &self.base_url,
            AuthMethod::ApiKey,
            vec!["headlines".into(), "pagination".into()],
        ))
    }

    fn provider_id(&self) -> &'static str {
        "newsapi_top_headlines"
    }

    fn fetch_page(&self, cursor: Option<&str>, now_ms: i64) -> Result<ProviderBatch, String> {
        let page = cursor
            .unwrap_or("1")
            .parse::<usize>()
            .map_err(|_| "invalid NewsAPI top-headlines cursor".to_owned())?;
        self.request(page, now_ms)
            .map_err(|error| format!("{:?}: {}", error.class, error.message))
    }
}

impl<T: HttpTransport> NewsApiProvider<T> {
    /// Constructs an adapter. The key is held only in memory and sent as a
    /// header; it is never included in the URL or normalized records.
    ///
    /// # Errors
    /// Returns [`ProviderError`] for an invalid HTTPS endpoint, empty secret,
    /// empty query, or zero page size.
    pub fn new(
        transport: T,
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        query: impl Into<String>,
        page_size: usize,
    ) -> Result<Self, ProviderError> {
        let base_url = base_url.into().trim_end_matches('/').to_owned();
        let api_key = api_key.into();
        let query = query.into();
        let page_size = page_size.min(MAX_PAGE_SIZE);
        if !valid_https_base_url(&base_url)
            || query.trim().is_empty()
            || api_key.trim().is_empty()
            || page_size == 0
        {
            return Err(ProviderError {
                class: ProviderFailure::Permanent,
                message: "invalid NewsAPI configuration".into(),
            });
        }
        Ok(Self {
            transport,
            base_url,
            api_key,
            query,
            page_size,
        })
    }

    /// Constructs an adapter from an owned runtime secret.
    ///
    /// # Errors
    /// Returns [`ProviderError`] for invalid configuration.
    pub fn from_secret(
        transport: T,
        base_url: impl Into<String>,
        api_key: String,
        query: impl Into<String>,
        page_size: usize,
    ) -> Result<Self, ProviderError> {
        let base_url = base_url.into().trim_end_matches('/').to_owned();
        let query = query.into();
        let page_size = page_size.min(MAX_PAGE_SIZE);
        if !valid_https_base_url(&base_url)
            || query.trim().is_empty()
            || api_key.trim().is_empty()
            || page_size == 0
        {
            return Err(ProviderError {
                class: ProviderFailure::Permanent,
                message: "invalid NewsAPI configuration".into(),
            });
        }
        Ok(Self {
            transport,
            base_url,
            api_key,
            query,
            page_size,
        })
    }

    fn request(&self, page: usize, now_ms: i64) -> Result<ProviderBatch, ProviderError> {
        let url = format!(
            "{}/v2/everything?q={}&pageSize={}&page={}&sortBy=publishedAt",
            self.base_url,
            encode(&self.query),
            self.page_size,
            page.max(1)
        );
        let mut headers = BTreeMap::new();
        headers.insert("accept".into(), "application/json".into());
        headers.insert("x-api-key".into(), self.api_key.clone());
        let response = self
            .transport
            .send(HttpRequest {
                method: "GET".into(),
                url,
                headers,
            })
            .map_err(|message| ProviderError {
                class: ProviderFailure::Transient,
                message,
            })?;
        classify_status(&response)?;
        if response.body.len() > MAX_RESPONSE_BYTES {
            return Err(ProviderError {
                class: ProviderFailure::Permanent,
                message: "provider response exceeds bound".into(),
            });
        }
        parse_newsapi(&response.body, page, self.query.as_str(), now_ms).map_err(|message| {
            ProviderError {
                class: ProviderFailure::Permanent,
                message,
            }
        })
    }
}

impl<T: HttpTransport> CursorProvider for NewsApiProvider<T> {
    fn manifest(&self) -> Option<ProviderManifest> {
        Some(news_manifest(
            "newsapi",
            &self.base_url,
            AuthMethod::ApiKey,
            vec!["article_search".into(), "pagination".into()],
        ))
    }

    fn provider_id(&self) -> &'static str {
        "newsapi"
    }

    fn fetch_page(&self, cursor: Option<&str>, now_ms: i64) -> Result<ProviderBatch, String> {
        let page = cursor
            .unwrap_or("1")
            .parse::<usize>()
            .map_err(|_| "invalid NewsAPI cursor".to_owned())?;
        self.request(page, now_ms)
            .map_err(|error| format!("{:?}: {}", error.class, error.message))
    }
}

/// Yahoo Finance search/news adapter. It is intentionally separate from the
/// market-data provider so Yahoo failure cannot affect chart state.
pub struct YahooFinanceNewsProvider<T> {
    transport: T,
    base_url: String,
    query: String,
}

impl<T: HttpTransport> YahooFinanceNewsProvider<T> {
    /// Creates a Yahoo adapter with a symbol/company search query.
    ///
    /// # Errors
    /// Returns [`ProviderError`] when the endpoint or query is invalid.
    pub fn new(
        transport: T,
        base_url: impl Into<String>,
        query: impl Into<String>,
    ) -> Result<Self, ProviderError> {
        let base_url = base_url.into().trim_end_matches('/').to_owned();
        let query = query.into();
        if !valid_https_base_url(&base_url) || query.trim().is_empty() {
            return Err(ProviderError {
                class: ProviderFailure::Permanent,
                message: "invalid Yahoo configuration".into(),
            });
        }
        Ok(Self {
            transport,
            base_url,
            query,
        })
    }
}

impl<T: HttpTransport> CursorProvider for YahooFinanceNewsProvider<T> {
    fn manifest(&self) -> Option<ProviderManifest> {
        Some(news_manifest(
            "yahoo_finance",
            &self.base_url,
            AuthMethod::None,
            vec!["search_news".into()],
        ))
    }

    fn provider_id(&self) -> &'static str {
        "yahoo_finance"
    }

    fn fetch_page(&self, _cursor: Option<&str>, now_ms: i64) -> Result<ProviderBatch, String> {
        let url = format!(
            "{}/v1/finance/search?q={}&quotesCount=0&newsCount=25",
            self.base_url,
            encode(&self.query)
        );
        let mut headers = BTreeMap::new();
        headers.insert("accept".into(), "application/json".into());
        let response = self
            .transport
            .send(HttpRequest {
                method: "GET".into(),
                url,
                headers,
            })
            .map_err(|message| format!("transport: {message}"))?;
        classify_status(&response).map_err(|error| error.message)?;
        if response.body.len() > MAX_RESPONSE_BYTES {
            return Err("provider response exceeds bound".into());
        }
        let items = parse_yahoo(&response.body, self.query.as_str(), now_ms)?;
        Ok(ProviderBatch {
            items,
            next_cursor: None,
        })
    }
}

/// RSS 2.0 and Atom feed adapter with conditional-request support.
pub struct RssProvider<T> {
    transport: T,
    feed_url: String,
    source_name: String,
    symbols: BTreeSet<String>,
    etag: Mutex<Option<String>>,
}

impl<T: HttpTransport> RssProvider<T> {
    /// Creates a feed adapter. Feed URLs must use HTTPS and symbols are
    /// normalized before being attached to every emitted item.
    ///
    /// # Errors
    /// Returns [`ProviderError`] when the URL, source name, or symbol set is
    /// invalid.
    pub fn new(
        transport: T,
        feed_url: impl Into<String>,
        source_name: impl Into<String>,
        symbols: impl IntoIterator<Item = String>,
    ) -> Result<Self, ProviderError> {
        let feed_url = feed_url.into().trim_end_matches('/').to_owned();
        let source_name = source_name.into().trim().to_owned();
        let symbols = symbols
            .into_iter()
            .map(|symbol| symbol.trim().to_uppercase())
            .filter(|symbol| !symbol.is_empty())
            .collect::<BTreeSet<_>>();
        if !valid_https_base_url(&feed_url) || source_name.is_empty() {
            return Err(ProviderError {
                class: ProviderFailure::Permanent,
                message: "invalid RSS configuration".into(),
            });
        }
        Ok(Self {
            transport,
            feed_url,
            source_name,
            symbols,
            etag: Mutex::new(None),
        })
    }
}

impl<T: HttpTransport> CursorProvider for RssProvider<T> {
    fn manifest(&self) -> Option<ProviderManifest> {
        Some(news_manifest(
            "rss",
            &self.feed_url,
            AuthMethod::None,
            vec!["rss".into(), "conditional_requests".into()],
        ))
    }

    fn provider_id(&self) -> &'static str {
        "rss"
    }

    fn fetch_page(&self, _cursor: Option<&str>, now_ms: i64) -> Result<ProviderBatch, String> {
        let mut headers = BTreeMap::new();
        headers.insert(
            "accept".into(),
            "application/rss+xml, application/atom+xml, application/xml".into(),
        );
        if let Ok(etag) = self.etag.lock()
            && let Some(value) = etag.as_deref()
        {
            headers.insert("if-none-match".into(), value.to_owned());
        }
        let response = self
            .transport
            .send(HttpRequest {
                method: "GET".into(),
                url: self.feed_url.clone(),
                headers,
            })
            .map_err(|message| format!("transport: {message}"))?;
        if response.status == 304 {
            return Ok(ProviderBatch {
                items: Vec::new(),
                next_cursor: None,
            });
        }
        classify_status(&response).map_err(|error| error.message)?;
        if response.body.len() > MAX_RESPONSE_BYTES {
            return Err("provider response exceeds bound".into());
        }
        let items = parse_rss(&response.body, &self.source_name, &self.symbols, now_ms)?;
        if let Some(value) = response.headers.get("etag")
            && let Ok(mut etag) = self.etag.lock()
        {
            *etag = Some(value.clone());
        }
        Ok(ProviderBatch {
            items,
            next_cursor: None,
        })
    }
}

fn classify_status(response: &HttpResponse) -> Result<(), ProviderError> {
    match response.status {
        200..=299 => Ok(()),
        401 | 403 => Err(ProviderError {
            class: ProviderFailure::Authentication,
            message: "provider authentication rejected".into(),
        }),
        429 => Err(ProviderError {
            class: ProviderFailure::RateLimited {
                retry_after_ms: response
                    .headers
                    .get("retry-after")
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(1_000),
            },
            message: "provider rate limit".into(),
        }),
        500..=599 => Err(ProviderError {
            class: ProviderFailure::Transient,
            message: format!("provider server status {}", response.status),
        }),
        status => Err(ProviderError {
            class: ProviderFailure::Permanent,
            message: format!("provider status {status}"),
        }),
    }
}

fn encode(value: &str) -> String {
    value
        .bytes()
        .flat_map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' => vec![char::from(byte)],
            b' ' => vec!['+'],
            other => format!("%{other:02X}").chars().collect(),
        })
        .collect()
}

fn parse_rss(
    bytes: &[u8],
    source_name: &str,
    symbols: &BTreeSet<String>,
    received_at_ms: i64,
) -> Result<Vec<NewsItem>, String> {
    let text = std::str::from_utf8(bytes).map_err(|_| "feed response is not UTF-8")?;
    let blocks = if text.contains("<entry") {
        blocks_between(text, "<entry", "</entry>")
    } else {
        blocks_between(text, "<item", "</item>")
    };
    let mut items = Vec::with_capacity(blocks.len().min(1_000));
    for block in blocks.into_iter().take(1_000) {
        let title = tag_text(block, &["title"]).ok_or("feed item title missing")?;
        let url = tag_text(block, &["link", "id"])
            .or_else(|| tag_attribute(block, "link", "href"))
            .ok_or("feed item URL missing")?;
        let summary = tag_text(block, &["description", "summary", "content"]);
        let published = tag_text(block, &["pubDate", "published", "updated"])
            .and_then(|value| parse_rfc3339_ms(&value));
        let content = format!("{title}\n{}", summary.as_deref().unwrap_or_default());
        items.push(NewsItem {
            id: hash(&url),
            provider: "rss".into(),
            canonical_url: url,
            source_name: source_name.to_owned(),
            title,
            summary_text: summary,
            published_at_ms: published,
            received_at_ms,
            symbols: symbols.clone(),
            content_hash: hash(&content),
        });
    }
    Ok(items)
}

fn blocks_between<'a>(text: &'a str, start_tag: &str, end_tag: &str) -> Vec<&'a str> {
    let mut blocks = Vec::new();
    let mut cursor = 0;
    while let Some(relative_start) = text[cursor..].find(start_tag) {
        let start = cursor + relative_start;
        let content_start = text[start..]
            .find('>')
            .map_or(start + start_tag.len(), |offset| start + offset + 1);
        let Some(relative_end) = text[content_start..].find(end_tag) else {
            break;
        };
        let end = content_start + relative_end;
        blocks.push(&text[content_start..end]);
        cursor = end + end_tag.len();
        if blocks.len() >= 1_000 {
            break;
        }
    }
    blocks
}

fn tag_text(block: &str, tags: &[&str]) -> Option<String> {
    tags.iter().find_map(|tag| {
        let open = format!("<{tag}");
        let start = block.find(&open)?;
        let content_start = block[start..].find('>')? + start + 1;
        let end = block[content_start..].find(&format!("</{tag}>"))? + content_start;
        let value = strip_markup(&block[content_start..end]);
        (!value.is_empty()).then_some(value)
    })
}

fn tag_attribute(block: &str, tag: &str, attribute: &str) -> Option<String> {
    let start = block.find(&format!("<{tag}"))?;
    let end = block[start..].find('>')? + start;
    let opening = &block[start..end];
    let needle = format!("{attribute}=\"");
    let value_start = opening.find(&needle)? + needle.len();
    let value_end = opening[value_start..].find('"')? + value_start;
    Some(decode_entities(&opening[value_start..value_end]))
}

fn strip_markup(value: &str) -> String {
    let mut output = String::new();
    let mut in_tag = false;
    for character in value.chars() {
        match character {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => output.push(character),
            _ => {}
        }
    }
    decode_entities(&output)
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn decode_entities(value: &str) -> String {
    value
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
}

// The upstream schemas are intentionally parsed through a small bounded JSON
// reader below rather than making provider-specific serde types part of the
// domain crates. Unknown fields are ignored; required identity fields fail.
#[derive(Clone, Debug)]
enum Json {
    Null,
    Bool,
    Number(f64),
    String(String),
    Array(Vec<Json>),
    Object(BTreeMap<String, Json>),
}

fn parse_json(bytes: &[u8]) -> Result<Json, String> {
    let text = std::str::from_utf8(bytes).map_err(|_| "provider response is not UTF-8")?;
    let mut parser = Parser {
        text,
        offset: 0,
        depth: 0,
    };
    let value = parser.value()?;
    parser.ws();
    if parser.offset != text.len() {
        return Err("trailing JSON data".into());
    }
    Ok(value)
}

struct Parser<'a> {
    text: &'a str,
    offset: usize,
    depth: usize,
}
impl Parser<'_> {
    fn ws(&mut self) {
        while self
            .text
            .as_bytes()
            .get(self.offset)
            .is_some_and(u8::is_ascii_whitespace)
        {
            self.offset += 1;
        }
    }
    fn value(&mut self) -> Result<Json, String> {
        self.ws();
        if self.depth > 32 {
            return Err("JSON nesting exceeds bound".into());
        }
        let byte = *self
            .text
            .as_bytes()
            .get(self.offset)
            .ok_or("unexpected JSON EOF")?;
        match byte {
            b'n' => {
                self.expect("null")?;
                Ok(Json::Null)
            }
            b't' => {
                self.expect("true")?;
                Ok(Json::Bool)
            }
            b'f' => {
                self.expect("false")?;
                Ok(Json::Bool)
            }
            b'"' => Ok(Json::String(self.string()?)),
            b'[' => self.array(),
            b'{' => self.object(),
            b'-' | b'0'..=b'9' => self.number(),
            _ => Err("invalid JSON value".into()),
        }
    }
    fn expect(&mut self, value: &str) -> Result<(), String> {
        if self.text.get(self.offset..self.offset + value.len()) == Some(value) {
            self.offset += value.len();
            Ok(())
        } else {
            Err("invalid JSON literal".into())
        }
    }
    fn string(&mut self) -> Result<String, String> {
        self.offset += 1;
        let mut output = String::new();
        while let Some(byte) = self.text.as_bytes().get(self.offset).copied() {
            match byte {
                b'"' => {
                    self.offset += 1;
                    return Ok(output);
                }
                b'\\' => {
                    self.offset += 1;
                    let escaped = *self
                        .text
                        .as_bytes()
                        .get(self.offset)
                        .ok_or("truncated JSON escape")?;
                    self.offset += 1;
                    if escaped == b'u' {
                        let hex = self
                            .text
                            .as_bytes()
                            .get(self.offset..self.offset.saturating_add(4))
                            .ok_or("truncated unicode escape")?;
                        let code = hex
                            .iter()
                            .try_fold(0_u32, |value, digit| {
                                value.checked_mul(16)?.checked_add(match digit {
                                    b'0'..=b'9' => u32::from(*digit - b'0'),
                                    b'a'..=b'f' => u32::from(*digit - b'a' + 10),
                                    b'A'..=b'F' => u32::from(*digit - b'A' + 10),
                                    _ => return None,
                                })
                            })
                            .ok_or("invalid unicode escape")?;
                        self.offset += 4;
                        output.push(char::from_u32(code).ok_or("invalid unicode scalar")?);
                    } else {
                        output.push(match escaped {
                            b'"' => '"',
                            b'\\' => '\\',
                            b'/' => '/',
                            b'n' => '\n',
                            b'r' => '\r',
                            b't' => '\t',
                            _ => return Err("unsupported JSON escape".into()),
                        });
                    }
                }
                byte if byte.is_ascii_control() => {
                    return Err("control character in JSON string".into());
                }
                _ => {
                    let character = self
                        .text
                        .get(self.offset..)
                        .and_then(|remaining| remaining.chars().next())
                        .ok_or("invalid UTF-8 JSON string")?;
                    self.offset += character.len_utf8();
                    output.push(character);
                }
            }
        }
        Err("unterminated JSON string".into())
    }
    fn array(&mut self) -> Result<Json, String> {
        self.offset += 1;
        self.depth += 1;
        let mut values = Vec::new();
        self.ws();
        if self.text.as_bytes().get(self.offset) == Some(&b']') {
            self.offset += 1;
            self.depth -= 1;
            return Ok(Json::Array(values));
        }
        loop {
            if values.len() >= 1_000 {
                return Err("JSON array exceeds bound".into());
            }
            values.push(self.value()?);
            self.ws();
            match self.text.as_bytes().get(self.offset) {
                Some(b',') => self.offset += 1,
                Some(b']') => {
                    self.offset += 1;
                    self.depth -= 1;
                    return Ok(Json::Array(values));
                }
                _ => return Err("invalid JSON array".into()),
            }
        }
    }
    fn object(&mut self) -> Result<Json, String> {
        self.offset += 1;
        self.depth += 1;
        let mut values = BTreeMap::new();
        self.ws();
        if self.text.as_bytes().get(self.offset) == Some(&b'}') {
            self.offset += 1;
            self.depth -= 1;
            return Ok(Json::Object(values));
        }
        loop {
            if values.len() >= 256 {
                return Err("JSON object exceeds bound".into());
            }
            self.ws();
            let key = self.string()?;
            self.ws();
            if self.text.as_bytes().get(self.offset) != Some(&b':') {
                return Err("invalid JSON object".into());
            }
            self.offset += 1;
            values.insert(key, self.value()?);
            self.ws();
            match self.text.as_bytes().get(self.offset) {
                Some(b',') => self.offset += 1,
                Some(b'}') => {
                    self.offset += 1;
                    self.depth -= 1;
                    return Ok(Json::Object(values));
                }
                _ => return Err("invalid JSON object".into()),
            }
        }
    }
    fn number(&mut self) -> Result<Json, String> {
        let start = self.offset;
        while self
            .text
            .as_bytes()
            .get(self.offset)
            .is_some_and(|byte| byte.is_ascii_digit() || b"+-.eE".contains(byte))
        {
            self.offset += 1;
        }
        let value = self.text[start..self.offset]
            .parse::<f64>()
            .map_err(|_| "invalid JSON number")?;
        Ok(Json::Number(value))
    }
}

fn field<'a>(value: &'a Json, key: &str) -> Option<&'a Json> {
    match value {
        Json::Object(object) => object.get(key),
        _ => None,
    }
}
fn string_field(value: &Json, key: &str) -> Option<String> {
    field(value, key).and_then(|value| match value {
        Json::String(text) => Some(text.clone()),
        _ => None,
    })
}
fn number_field(value: &Json, key: &str) -> Option<f64> {
    field(value, key).and_then(|value| match value {
        Json::Number(number) if number.is_finite() => Some(*number),
        _ => None,
    })
}
fn hash(text: &str) -> String {
    let mut value = 0xcbf2_9ce4_8422_2325_u64;
    for byte in text.as_bytes() {
        value ^= u64::from(*byte);
        value = value.wrapping_mul(0x0100_0000_01b3);
    }
    format!("{value:016x}")
}

fn parse_newsapi(
    bytes: &[u8],
    page: usize,
    query: &str,
    received_at_ms: i64,
) -> Result<ProviderBatch, String> {
    let symbols = query
        .split_whitespace()
        .map(str::to_uppercase)
        .collect::<Vec<_>>();
    parse_newsapi_with_symbols(bytes, page, &symbols, received_at_ms)
}

fn parse_newsapi_with_symbols(
    bytes: &[u8],
    page: usize,
    symbols: &[String],
    received_at_ms: i64,
) -> Result<ProviderBatch, String> {
    let root = parse_json(bytes)?;
    let articles = field(&root, "articles")
        .and_then(|value| match value {
            Json::Array(items) => Some(items),
            _ => None,
        })
        .ok_or("NewsAPI articles array missing")?;
    let mut items = Vec::new();
    for article in articles {
        let url = string_field(article, "url").ok_or("NewsAPI article URL missing")?;
        let title = string_field(article, "title").ok_or("NewsAPI article title missing")?;
        let source = field(article, "source")
            .and_then(|value| string_field(value, "name"))
            .unwrap_or_else(|| "unknown".into());
        let summary = string_field(article, "description");
        let published =
            string_field(article, "publishedAt").and_then(|value| parse_rfc3339_ms(&value));
        let content = format!("{title}\n{}", summary.as_deref().unwrap_or_default());
        items.push(NewsItem {
            id: hash(&url),
            provider: "newsapi".into(),
            canonical_url: url,
            source_name: source,
            title,
            summary_text: summary,
            published_at_ms: published,
            received_at_ms,
            symbols: symbols.iter().cloned().collect(),
            content_hash: hash(&content),
        });
    }
    let next_cursor = (items.len() == MAX_PAGE_SIZE.min(articles.len()))
        .then(|| (page.saturating_add(1)).to_string());
    Ok(ProviderBatch { items, next_cursor })
}

#[allow(clippy::cast_possible_truncation)]
fn parse_yahoo(bytes: &[u8], query: &str, received_at_ms: i64) -> Result<Vec<NewsItem>, String> {
    let root = parse_json(bytes)?;
    let news = field(&root, "news")
        .and_then(|value| match value {
            Json::Array(items) => Some(items),
            _ => None,
        })
        .ok_or("Yahoo news array missing")?;
    Ok(news
        .iter()
        .filter_map(|article| {
            let url = string_field(article, "link")?;
            let title = string_field(article, "title")?;
            let source = string_field(article, "publisher").unwrap_or_else(|| "unknown".into());
            let published = number_field(article, "providerPublishTime")
                .and_then(|seconds| i64::try_from(seconds as i128).ok())
                .and_then(|seconds| seconds.checked_mul(1_000));
            Some(NewsItem {
                id: string_field(article, "uuid").unwrap_or_else(|| hash(&url)),
                provider: "yahoo_finance".into(),
                canonical_url: url.clone(),
                source_name: source,
                title: title.clone(),
                summary_text: None,
                published_at_ms: published,
                received_at_ms,
                symbols: [query.to_uppercase()].into_iter().collect(),
                content_hash: hash(&title),
            })
        })
        .collect())
}

fn parse_rfc3339_ms(value: &str) -> Option<i64> {
    let bytes = value.as_bytes();
    if bytes.len() < 20
        || bytes.get(4) != Some(&b'-')
        || bytes.get(7) != Some(&b'-')
        || bytes.get(10) != Some(&b'T')
    {
        return None;
    }
    let number =
        |start: usize, length: usize| value.get(start..start + length)?.parse::<i64>().ok();
    let year = number(0, 4)?;
    let month = number(5, 2)?;
    let day = number(8, 2)?;
    let hour = number(11, 2)?;
    let minute = number(14, 2)?;
    let second = number(17, 2)?;
    if !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || hour > 23
        || minute > 59
        || second > 60
    {
        return None;
    }
    let days = (year - 1970) * 365 + (year - 1969) / 4 - (year - 1901) / 100
        + (year - 1900) / 400
        + (367 * month - 362) / 12
        - if month <= 2 {
            0
        } else if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 {
            1
        } else {
            2
        }
        + day
        - 1;
    days.checked_mul(86_400_000)?
        .checked_add(hour * 3_600_000 + minute * 60_000 + second * 1_000)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        HttpRequest, HttpResponse, HttpTransport, MAX_RESPONSE_BYTES, NewsApiProvider, RssProvider,
        read_bounded_body, valid_https_base_url,
    };
    use insider_news_core::CursorProvider;

    #[test]
    fn provider_base_urls_require_https_authority_without_credentials() {
        assert!(valid_https_base_url("https://news.example/v2"));
        assert!(!valid_https_base_url("http://news.example/v2"));
        assert!(!valid_https_base_url("https:///missing-authority"));
        assert!(!valid_https_base_url(
            "https://user:password@news.example/v2"
        ));
        assert!(!valid_https_base_url("https://news.example/has whitespace"));
        let oversized = format!("https://news.example/{}", "a".repeat(2_040));
        assert!(!valid_https_base_url(&oversized));
    }

    struct FixtureTransport;

    impl HttpTransport for FixtureTransport {
        fn send(&self, request: HttpRequest) -> Result<HttpResponse, String> {
            assert!(request.url.contains("q=AAPL"));
            assert!(!request.url.contains("secret"));
            assert_eq!(
                request.headers.get("x-api-key").map(String::as_str),
                Some("secret")
            );
            Ok(HttpResponse {
                status: 200,
                headers: BTreeMap::new(),
                body: r#"{"articles":[{"source":{"name":"Wire"},"url":"https://example.test/a","title":"AAPL rises ★","description":"summary","publishedAt":"2026-08-25T12:00:00Z"}]}"#.as_bytes().to_vec(),
            })
        }
    }

    struct RssFixture;

    impl HttpTransport for RssFixture {
        fn send(&self, request: HttpRequest) -> Result<HttpResponse, String> {
            assert_eq!(request.url, "https://feed.example.test/rss");
            Ok(HttpResponse {
                status: 200,
                headers: BTreeMap::from([(String::from("etag"), String::from("v1"))]),
                body: br"<rss><channel><item><title> AAPL &amp; rises </title><link>https://example.test/item</link><description><![CDATA[<b>summary</b>]]></description><pubDate>2026-08-25T12:00:00Z</pubDate></item></channel></rss>".to_vec(),
            })
        }
    }

    #[test]
    fn newsapi_adapter_builds_bounded_request_and_normalizes_article() {
        let provider = NewsApiProvider::new(
            FixtureTransport,
            "https://news.example.test",
            "secret",
            "AAPL",
            10,
        )
        .ok();
        let Some(provider) = provider else { return };
        let batch = provider.fetch_page(None, 1234).ok();
        let Some(batch) = batch else { return };
        assert_eq!(batch.items.len(), 1);
        assert_eq!(batch.items[0].provider, "newsapi");
        assert_eq!(batch.items[0].received_at_ms, 1234);
        assert_eq!(
            batch.items[0].symbols.iter().next().map(String::as_str),
            Some("AAPL")
        );
    }

    #[test]
    fn rss_adapter_normalizes_markup_and_caches_etag() {
        let provider = RssProvider::new(
            RssFixture,
            "https://feed.example.test/rss",
            "Wire",
            [String::from("aapl")],
        )
        .ok();
        let Some(provider) = provider else { return };
        let batch = provider.fetch_page(None, 99).ok();
        let Some(batch) = batch else { return };
        assert_eq!(batch.items[0].title, "AAPL & rises");
        assert_eq!(batch.items[0].source_name, "Wire");
        assert_eq!(batch.items[0].received_at_ms, 99);
    }

    #[test]
    fn news_http_body_reader_rejects_oversized_streams() {
        let oversized = vec![0_u8; MAX_RESPONSE_BYTES + 1];
        assert!(read_bounded_body(std::io::Cursor::new(oversized)).is_err());
        let bounded = vec![1_u8; MAX_RESPONSE_BYTES];
        assert_eq!(
            read_bounded_body(std::io::Cursor::new(bounded))
                .ok()
                .map(|body| body.len()),
            Some(MAX_RESPONSE_BYTES)
        );
    }

    #[test]
    fn request_debug_redacts_header_values() {
        let request = HttpRequest {
            method: "GET".into(),
            url: "https://news.example.test".into(),
            headers: BTreeMap::from([(String::from("x-api-key"), String::from("secret"))]),
        };
        let rendered = format!("{request:?}");
        assert!(rendered.contains("x-api-key"));
        assert!(!rendered.contains("secret"));
    }
}
