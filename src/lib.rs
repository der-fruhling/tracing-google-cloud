//! This is a tracing-subcriber [Layer][tracing_subscriber::Layer] that
//! implements Google Cloud's [Structured Logging] format.
//!
//! Features:
//! - [OpenTelemetry] Trace integration (with `opentelemetry` feature flag)
//! - Support for HTTP requests via log entry fields
//! - Support for populating operation structures into log entries from
//!   the parent [Span][tracing::Span]\(s).
//!
//! ```
//! # const PROJECT_ID: &str = "my-project";
//! # use tracing_subscriber::layer::SubscriberExt;
//! # use tracing_subscriber::util::SubscriberInitExt;
//! tracing_subscriber::registry()
//!     .with(tracing_opentelemetry::layer())
//!     .with(tracing_google_cloud::builder()
//!         .with_project_id(PROJECT_ID) // required with opentelemetry feature
//!         .with_writer(std::io::stdout())
//!         .build())
//!     .init();
//! ```
//!
//! ## HTTP request fields
//! ```
//! # const PROJECT_ID: &str = "my-project";
//! # use tracing_subscriber::layer::SubscriberExt;
//! # use tracing_subscriber::util::SubscriberInitExt;
//! # tracing_subscriber::registry()
//! #     .with(tracing_opentelemetry::layer())
//! #     .with(tracing_google_cloud::builder()
//! #         .with_project_id(PROJECT_ID) // required with opentelemetry feature
//! #         .with_writer(std::io::stdout())
//! #         .build())
//! #     .init();
//! tracing::info!(
//!     http.request_method = "POST",
//!     http.request_url = "/",
//!     http.request_size = 420,
//!     http.status = 200,
//!     http.response_size = 1024,
//!     http.user_agent = "Fire fox",
//!     http.remote_ip = "127.0.0.1",
//!     http.server_ip = "127.0.0.2",
//!     http.referer = "127.0.0.2",
//!     http.latency_ns = 32000000,
//!     http.cache_lookup = false,
//!     http.cache_hit = false,
//!     http.cache_validated_with_origin_server = false,
//!     http.cache_fill_bytes = 200,
//!     http.protocol = "HTTP/4",
//! )
//! ```
//!
//! ## Using operations
//! See [SpanExt::operation] and [Operation].
//!
//! ```
//! # const PROJECT_ID: &str = "my-project";
//! # use tracing_subscriber::layer::SubscriberExt;
//! # use tracing_subscriber::util::SubscriberInitExt;
//! # tracing_subscriber::registry()
//! #     .with(tracing_opentelemetry::layer())
//! #     .with(tracing_google_cloud::builder()
//! #         .with_project_id(PROJECT_ID) // required with opentelemetry feature
//! #         .with_writer(std::io::stdout())
//! #         .build())
//! #     .init();
//! use tracing_google_cloud::{OperationInfo, SpanExt};
//!
//! let span = tracing::info_span!("long_operation");
//! let operation = span.start_operation(
//!     "unique-id",
//!     Some("github.com/der-fruhling/tracing-google-cloud")
//! );
//!
//! span.in_scope(|| {
//!     // First log entry with an operation automatically has the "first" field set
//!     tracing::info!("First log");
//!
//!     tracing::info!("Something in the middle");
//!
//!     // Call Operation::end() to cause the next log entry to have "last" set.
//!     // You can also call this before the first entry if you wish.
//!     operation.end();
//!     tracing::info!("End of the operation");
//! })
//! ```
//!
//! [OpenTelemetry]: https://docs.rs/tracing-opentelemetry
//! [Structured Logging]: https://docs.cloud.google.com/logging/docs/structured-logging#structured_logging_special_fields

use base64::Engine;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize, Serializer};
use std::any::TypeId;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::str::FromStr;
use std::sync::{Arc, LazyLock, Mutex, RwLock};
use tracing::field::Field;
use tracing::span::{Attributes, Record};
use tracing::{Event, Id, Level, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{field, Layer as TracingLayer};

#[cfg(feature = "opentelemetry")]
use {
    opentelemetry::TraceId,
    tracing_opentelemetry::OtelData
};

#[derive(Serialize, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Severity {
    #[default] Default,
    Debug,
    Info,
    Notice,
    Warning,
    Error,
    Critical,
    Alert,
    Emergency,
}

impl Display for Severity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Severity::Default => "DEFAULT",
            Severity::Debug => "DEBUG",
            Severity::Info => "INFO",
            Severity::Notice => "NOTICE",
            Severity::Warning => "WARNING",
            Severity::Error => "ERROR",
            Severity::Critical => "CRITICAL",
            Severity::Alert => "ALERT",
            Severity::Emergency => "EMERGENCY",
        })
    }
}

#[derive(Debug)]
pub struct InvalidSeverity(Box<str>);

impl Display for InvalidSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid severity: {:?}", self.0)
    }
}

impl std::error::Error for InvalidSeverity {}

impl FromStr for Severity {
    type Err = InvalidSeverity;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "DEFAULT" => Ok(Severity::Default),
            "DEBUG" => Ok(Severity::Debug),
            "INFO" => Ok(Severity::Info),
            "NOTICE" => Ok(Severity::Notice),
            "WARNING" => Ok(Severity::Warning),
            "ERROR" => Ok(Severity::Error),
            "CRITICAL" => Ok(Severity::Critical),
            "ALERT" => Ok(Severity::Alert),
            "EMERGENCY" => Ok(Severity::Emergency),
            other => Err(InvalidSeverity(other.into())),
        }
    }
}

/// This is not a Google Cloud thing, just trying to include span fields
/// where they might otherwise be missing.
#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct SpanExposition {
    #[serde(rename = "@trace", skip_serializing_if = "Option::is_none")]
    trace_id: Option<Box<str>>,
    #[serde(rename = "@id")]
    id: Box<str>,
    #[serde(flatten)]
    fields: HashMap<Cow<'static, str>, serde_json::Value>
}

#[derive(Serialize, Default)]
#[serde(rename_all = "camelCase")]
struct LogEntry<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    severity: Option<Severity>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<Box<str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    http_request: Option<HttpRequestInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    time: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "logging.googleapis.com/insertId")]
    insert_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "logging.googleapis.com/labels")]
    labels: Option<HashMap<Cow<'static, str>, serde_json::Value>>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "logging.googleapis.com/operation")]
    operation: Option<OperationDetail>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "logging.googleapis.com/sourceLocation")]
    source_location: Option<SourceLocation<'a>>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "logging.googleapis.com/spanId")]
    span_id: Option<Box<str>>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "logging.googleapis.com/trace")]
    trace: Option<Box<str>>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "logging.googleapis.com/trace_sampled")]
    trace_sampled: Option<bool>,

    #[serde(skip_serializing_if = "Vec::is_empty", rename = "@spans")]
    x_spans: Vec<Arc<SpanExposition>>,
    #[serde(skip_serializing_if = "HashMap::is_empty", rename = "@effective_fields")]
    x_fields: HashMap<Cow<'static, str>, serde_json::Value>,

    #[serde(flatten)]
    extra: HashMap<Cow<'static, str>, serde_json::Value>,
}

fn duration_serializer<S: Serializer>(dur: &Option<Duration>, ser: S) -> Result<S::Ok, S::Error> {
    ser.serialize_str(&format!("{:.5}s", dur.unwrap_or_else(|| unreachable!("this should never be serialized if None")).as_seconds_f32()))
}

#[derive(Serialize, Default, Clone)]
#[serde(rename_all = "camelCase")]
struct HttpRequestInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    request_method: Option<Box<str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    request_url: Option<Box<str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    request_size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    response_size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    user_agent: Option<Box<str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    remote_ip: Option<Box<str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    server_ip: Option<Box<str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    referer: Option<Box<str>>,
    #[serde(skip_serializing_if = "Option::is_none", serialize_with = "duration_serializer")]
    latency: Option<Duration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_lookup: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_hit: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_validated_with_origin_server: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_fill_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    protocol: Option<Box<str>>,
}

#[derive(Deserialize, Serialize, Default, Clone)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct OperationInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Box<str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub producer: Option<Box<str>>,
}

impl OperationInfo {
    pub fn new(id: impl AsRef<str>, producer: Option<impl AsRef<str>>) -> Self {
        let mut v = Self::default();
        v.id = Some(id.as_ref().into());
        v.producer = producer.map(|v| v.as_ref().into());
        v
    }
}

#[derive(Serialize, Default)]
#[serde(rename_all = "camelCase")]
struct OperationDetail {
    #[serde(skip_serializing_if = "Option::is_none", flatten)]
    pub info: Option<OperationInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last: Option<bool>,
}

#[derive(Serialize, Default)]
#[serde(rename_all = "camelCase")]
struct SourceLocation<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub function: Option<Box<str>>,
}

#[derive(Clone)]
struct OperationData {
    info: OperationInfo,
    first: bool,
    last: bool,
}

impl OperationData {
    pub fn new(info: OperationInfo) -> Self {
        Self {
            info,
            first: true,
            last: false,
        }
    }
}

pub struct Layer<W: std::io::Write> {
    writer: Mutex<W>,
    #[cfg_attr(not(feature = "opentelemetry"), allow(unused))]
    project_id: Box<str>,
    operations: Mutex<HashMap<Id, Arc<Mutex<OperationData>>>>,
    expositions: RwLock<HashMap<Id, Arc<SpanExposition>>>
}

impl<W: std::io::Write> Layer<W> {
    fn new(writer: W, project_id: impl AsRef<str>) -> Self {
        Self {
            writer: writer.into(),
            project_id: project_id.as_ref().into(),
            operations: HashMap::new().into(),
            expositions: HashMap::new().into()
        }
    }
}

impl<W: std::io::Write + 'static, S: Subscriber + for<'a> LookupSpan<'a>> TracingLayer<S> for Layer<W> {
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, #[allow(unused)] ctx: Context<'_, S>) {
        if let Some(mut expositions) = self.expositions.write().ok() {
            let span_id = format!("{:016x}", id.into_u64());

            let mut exposition = SpanExposition {
                trace_id: None,
                id: span_id.into(),
                fields: HashMap::new()
            };

            #[cfg(feature = "opentelemetry")]
            self.try_find_trace_id(id, ctx, &mut exposition);
            attrs.record(&mut exposition.visit());

            expositions.insert(id.clone(), Arc::new(exposition));
        }
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, #[allow(unused)] ctx: Context<'_, S>) {
        if let Some(mut expositions) = self.expositions.write().ok() {
            let arc = expositions.get_mut(id).unwrap();
            let mut exposition = (**arc).clone();

            if exposition.trace_id.is_none() {
                #[cfg(feature = "opentelemetry")]
                self.try_find_trace_id(id, ctx, &mut exposition);
            }

            values.record(&mut exposition.visit());
            expositions.insert(id.clone(), Arc::new(exposition));
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let mut log = LogEntry {
            severity: match *event.metadata().level() {
                Level::TRACE | Level::DEBUG => Some(Severity::Debug),
                Level::INFO => Some(Severity::Info),
                Level::WARN => Some(Severity::Warning),
                Level::ERROR => Some(Severity::Error),
            },
            time: Some(Utc::now()),
            source_location: Some(SourceLocation {
                file: event.metadata().file(),
                line: event.metadata().line().map(|v| v as u64),
                function: None
            }),
            ..LogEntry::default()
        };

        let mut span_ref = ctx.event_span(event);

        let mut ops = self.operations.lock().ok();
        let expositions = self.expositions.read().ok();
        let mut looking_for_function = true;

        while let Some(span) = span_ref {
            let drop_ops = if let Some(op) = ops.as_ref().and_then(|v| v.get(&span.id())) && log.operation.is_none() {
                if let Ok(mut op_m) = op.lock() {
                    log.operation = Some(OperationDetail {
                        info: Some(op_m.info.clone()),
                        first: if op_m.first {
                            op_m.first = false;
                            Some(true)
                        } else { None },
                        last: if op_m.last {
                            op_m.last = false;
                            Some(true)
                        } else { None },
                    });

                    true
                } else {
                    false
                }
            } else {
                false
            };

            if drop_ops {
                // drop mutex guard early to avoid unnecessary terribleness
                ops.take();
            }

            let span_id = format!("{:016x}", span.id().into_u64());
            if log.span_id.is_none() {
                log.span_id = Some(span_id.clone().into())
            }

            #[cfg(feature = "opentelemetry")]
            if let Some(otel) = span.extensions().get::<OtelData>() {
                if let Some(trace) = otel.trace_id() {
                    if log.trace.is_none() && trace != TraceId::INVALID {
                        log.trace = Some(format!("projects/{}/traces/{:032x}", self.project_id, trace).into());
                    }
                }
            }

            if let Some(exposition) = expositions.as_ref().and_then(|v| v.get(&span.id())).cloned() {
                for (k, v) in &exposition.fields {
                    if !log.x_fields.contains_key(k) {
                        log.x_fields.insert(k.clone(), v.clone());
                    }
                }

                if looking_for_function {
                    if let Some(serde_json::Value::String(s)) = exposition.fields.get("function") {
                        if let Some(src) = log.source_location.as_mut() && src.function.is_none() {
                            src.function = Some(s.as_str().into());
                            looking_for_function = false;
                        }
                    }
                }

                log.x_spans.push(exposition);
            }

            span_ref = span.parent();
        }

        event.record(&mut log);

        let _ = writeln!(self.writer.lock().unwrap(), "{}", serde_json::to_string(&log).unwrap());
    }

    fn on_close(&self, id: Id, _ctx: Context<'_, S>) {
        if let Some(mut ops) = self.operations.lock().ok() {
            ops.remove(&id);
        }

        if let Some(mut expositions) = self.expositions.write().ok() {
            expositions.remove(&id);
        }
    }

    fn on_id_change(&self, old: &Id, new: &Id, _ctx: Context<'_, S>) {
        if let Some(mut ops) = self.operations.lock().ok() {
            if let Some(data) = ops.remove(old) {
                ops.insert(new.clone(), data);
            }
        }
    }

    unsafe fn downcast_raw(&self, id: TypeId) -> Option<*const ()> {
        if id == TypeId::of::<Self>() {
            Some(self as *const _ as *const ())
        } else if id == TypeId::of::<Mutex<HashMap<Id, Arc<Mutex<OperationData>>>>>() {
            let access = &self.operations;
            Some(access as *const _ as *const ())
        } else {
            None
        }
    }
}

#[cfg(feature = "opentelemetry")]
impl<W: std::io::Write> Layer<W> {
    fn try_find_trace_id<S: Subscriber + for<'a> LookupSpan<'a>>(&self, id: &Id, ctx: Context<'_, S>, exposition: &mut SpanExposition) {
        if let Some(span) = ctx.span(id) {
            if let Some(otel) = span.extensions().get::<OtelData>() {
                if let Some(trace) = otel.trace_id() {
                    exposition.trace_id = Some(format!("projects/{}/traces/{:032x}", self.project_id, trace).into());
                }
            }
        }
    }
}

static B64: LazyLock<base64::engine::GeneralPurpose> = LazyLock::new(|| {
    base64::engine::GeneralPurpose::new(&base64::alphabet::STANDARD, Default::default())
});

impl<'a> LogEntry<'a> {
    fn record(&mut self, field: &Field, value: impl Into<serde_json::Value>) {
        if field.name().starts_with("labels.") {
            self.labels.get_or_insert(HashMap::new()).insert(
                field.name()[7..].into(),
                value.into()
            );
        } else {
            self.extra.insert(field.name().into(), value.into());
        }
    }
}

impl<'a> field::Visit for LogEntry<'a> {
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.record(field, value);
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        if value >= 0 {
            self.record_u64(field, value as u64);
        } else {
            self.record(field, value);
        }
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            name if name.starts_with("http.") => {
                let http = self.http_request.get_or_insert_default();
                match &name[5..] {
                    "request_size" => http.request_size = Some(value),
                    "status" if value < 65536 => http.status = Some(value as u16),
                    "response_size" => http.response_size = Some(value),
                    "cache_fill_bytes" => http.cache_fill_bytes = Some(value),
                    "latency_ns" => http.latency = Some(Duration::nanoseconds(value as i64)),
                    "latency_ms" => http.latency = Some(Duration::milliseconds(value as i64)),
                    "latency_sec" => http.latency = Some(Duration::seconds(value as i64)),
                    _ => self.record(field, value)
                }
            },
            _ => self.record(field, value)
        }
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        match field.name() {
            name if name.starts_with("http.") => {
                let http = self.http_request.get_or_insert_default();
                match &name[5..] {
                    "cache_lookup" => http.cache_lookup = Some(value),
                    "cache_hit" => http.cache_hit = Some(value),
                    "cache_validated_with_origin_server" => http.cache_validated_with_origin_server = Some(value),
                    _ => self.record(field, value)
                }
            },
            _ => self.record(field, value)
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "message" => self.message = Some(value.into()),
            "severity" => self.severity = value.parse().ok(),
            name if name.starts_with("http.") => {
                let http = self.http_request.get_or_insert_default();
                match &name[5..] {
                    "request_method" => http.request_method = Some(value.into()),
                    "request_url" => http.request_url = Some(value.into()),
                    "user_agent" => http.user_agent = Some(value.into()),
                    "remote_ip" => http.remote_ip = Some(value.into()),
                    "server_ip" => http.server_ip = Some(value.into()),
                    "referer" => http.referer = Some(value.into()),
                    "protocol" => http.protocol = Some(value.into()),
                    _ => self.record(field, value)
                }
            },
            _ => self.record(field, value)
        }
    }

    fn record_bytes(&mut self, field: &Field, value: &[u8]) {
        self.record(field, B64.encode(value));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        self.record_str(field, &format!("{:?}", value));
    }
}

trait Exposition {
    fn record(&mut self, field: &Field, value: impl Into<serde_json::Value>);

    fn visit(&'_ mut self) -> Visit<'_, Self> {
        Visit(self)
    }
}

impl Exposition for SpanExposition {
    fn record(&mut self, field: &Field, value: impl Into<serde_json::Value>) {
        self.fields.record(field, value);
    }
}

impl Exposition for HashMap<Cow<'static, str>, serde_json::Value> {
    fn record(&mut self, field: &Field, value: impl Into<serde_json::Value>) {
        let mut s = Cow::Borrowed(field.name());

        if s.len() >= 1 && s.starts_with('@') {
            s.to_mut().insert(1, '@');
        }

        self.insert(s.into(), value.into());
    }
}

struct Visit<'a, T: Exposition + ?Sized>(&'a mut T);

impl<T: Exposition + ?Sized> field::Visit for Visit<'_, T> {
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.0.record(field, value);
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.0.record(field, value);
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.record(field, value);
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.0.record(field, value);
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.record(field, value);
    }

    fn record_bytes(&mut self, field: &Field, value: &[u8]) {
        self.0.record(field, B64.encode(value));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        self.record_str(field, &format!("{:?}", value));
    }
}

/// Represents a potentially long-running operation that can be used by
/// Google Cloud to group log entries.
///
/// Created automatically by [SpanExt::operation].
pub struct Operation(Option<Arc<Mutex<OperationData>>>);

impl Operation {
    /// Updates the details within this operation. You should really only call
    /// this once.
    ///
    /// After initializing, the first event will have the operation attached
    /// with the `first` attribute set to `true`. Subsequent events will not
    /// have this attribute.
    ///
    /// To indicate that you are done operating, see [Operation::end].
    pub fn init(&self, info: OperationInfo) -> &Self {
        if let Some(mut data) = self.0.as_ref().and_then(|v| v.lock().ok()) {
            data.info = info;
        }

        self
    }

    /// Imports operation info that has already been started. The only
    /// difference between this method and [Operation::init] is the first event
    /// will _not_ have a `first` attribute.
    pub fn import(&self, info: OperationInfo) -> &Self {
        if let Some(mut data) = self.0.as_ref().and_then(|v| v.lock().ok()) {
            data.info = info;
            data.first = false;
        }

        self
    }

    /// Returns a copy of this operations info that can be passed to other
    /// programs. [OperationInfo] implements [Serialize] and [Deserialize] to
    /// allow passing them safely.
    pub fn export(&self) -> OperationInfo {
        if let Some(data) = self.0.as_ref().and_then(|v| v.lock().ok()) {
            data.info.clone()
        } else {
            OperationInfo::default()
        }
    }

    /// Marks this operation as finished. **You must send an event after calling
    /// this if you really mean it.**
    ///
    /// The next event after calling this method will have the `last` attribute
    /// set to `true`, indicating that the event is the final in the chain
    /// making up the operation.
    pub fn end(self) {
        if let Some(mut data) = self.0.as_ref().and_then(|v| v.lock().ok()) {
            data.last = true;
        }
    }
}

pub trait SpanExt {
    /// Retrieves an [Operation] associated with this [Span][tracing::Span].
    /// If one does not already exist, it will be created automatically.
    ///
    /// This method is safe to call when this crate's [Layer] is not available,
    /// e.g. in a development environment. In this case, the returned
    /// [Operation] will not perform any actions.
    fn operation(&'_ self) -> Operation;

    /// A simple wrapper over [Operation::init] to make creating them a bit
    /// nicer.
    ///
    /// ```
    /// # use tracing_google_cloud::SpanExt;
    /// # let span = tracing::info_span!("span");
    /// let op = span.start_operation("id", Some("provider"));
    /// ```
    ///
    /// The above is equivalent to:
    /// ```
    /// # use tracing_google_cloud::{SpanExt, OperationInfo};
    /// # let span = tracing::info_span!("span");
    /// let op = span.operation();
    /// op.init(OperationInfo::new("id", Some("provider")));
    /// ```
    fn start_operation(&'_ self, id: impl AsRef<str>, producer: Option<impl AsRef<str>>) -> Operation {
        let op = self.operation();
        op.init(OperationInfo::new(id, producer));
        op
    }
}

impl SpanExt for tracing::Span {
    fn operation(&'_ self) -> Operation {
        Operation(self.with_subscriber(|(id, subscriber)| {
            if let Some(operations) = subscriber.downcast_ref::<Mutex<HashMap<Id, Arc<Mutex<OperationData>>>>>() {
                operations.lock().ok().map(|mut v| {
                    v.entry(id.clone()).or_insert_with(|| Arc::new(Mutex::new(OperationData::new(OperationInfo::default())))).clone()
                })
            } else { None }
        }).and_then(|v| v))
    }
}

pub struct LayerBuilder<ProjectId = (), W: std::io::Write = std::io::Stdout> {
    project_id: ProjectId,
    writer: W
}

impl<W: std::io::Write> LayerBuilder<(), W> {
    /// Sets the project's ID for use by the [Layer]. This is mainly useful for
    /// including trace IDs, where, depending on the writer you're using,
    /// Google Cloud may not be able to infer the correct project ID. To
    /// (hopefully) get around this if it is ever a problem, this crate always
    /// writes traces in the full format, `projects/<...>/traces/<...>`.
    ///
    /// As such, the project ID is required.
    ///
    /// If the `opentelemetry` feature is disabled, it is allowed to construct
    /// the layer without calling this function.
    pub fn with_project_id<T: AsRef<str>>(self, text: T) -> LayerBuilder<T, W> {
        LayerBuilder {
            project_id: text,
            writer: self.writer
        }
    }
}

impl<ProjectId, W: std::io::Write> LayerBuilder<ProjectId, W> {
    /// Sets the writer which logs will be written to.
    /// The default is [std::io::stdout()].
    pub fn with_writer<N: std::io::Write>(self, writer: N) -> LayerBuilder<ProjectId, N> {
        LayerBuilder {
            project_id: self.project_id,
            writer
        }
    }
}

trait ProjectIdTrait {
    fn string(&self) -> &str;
}

#[cfg(not(feature = "opentelemetry"))]
impl<T> ProjectIdTrait for T {
    fn string(&self) -> &str {
        ""
    }
}

#[cfg(feature = "opentelemetry")]
impl<T: AsRef<str>> ProjectIdTrait for T {
    fn string(&self) -> &str {
        self.as_ref()
    }
}

#[allow(private_bounds)]
impl<ProjectId: ProjectIdTrait, W: std::io::Write> LayerBuilder<ProjectId, W> {
    pub fn build(self) -> Layer<W> {
        Layer::new(self.writer, self.project_id.string())
    }
}

pub fn builder() -> LayerBuilder {
    LayerBuilder {
        project_id: (),
        writer: std::io::stdout()
    }
}
