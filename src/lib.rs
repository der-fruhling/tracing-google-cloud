use std::any::{Any, TypeId};
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::rc::Rc;
use std::str::FromStr;
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, RwLock};
use base64::Engine;
use chrono::{DateTime, Duration, Utc};
use opentelemetry::TraceId;
use serde::{Serialize, Serializer};
use serde::ser::Error;
use tracing::{Event, Id, Level, Metadata, Subscriber};
use tracing::field::Field;
use tracing::span::{Attributes, Record};
use tracing_opentelemetry::OtelData;
use tracing_subscriber::{field, Layer};
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;

#[derive(Serialize, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum GcpSeverity {
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

impl Display for GcpSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            GcpSeverity::Default => "DEFAULT",
            GcpSeverity::Debug => "DEBUG",
            GcpSeverity::Info => "INFO",
            GcpSeverity::Notice => "NOTICE",
            GcpSeverity::Warning => "WARNING",
            GcpSeverity::Error => "ERROR",
            GcpSeverity::Critical => "CRITICAL",
            GcpSeverity::Alert => "ALERT",
            GcpSeverity::Emergency => "EMERGENCY",
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

impl FromStr for GcpSeverity {
    type Err = InvalidSeverity;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "DEFAULT" => Ok(GcpSeverity::Default),
            "DEBUG" => Ok(GcpSeverity::Debug),
            "INFO" => Ok(GcpSeverity::Info),
            "NOTICE" => Ok(GcpSeverity::Notice),
            "WARNING" => Ok(GcpSeverity::Warning),
            "ERROR" => Ok(GcpSeverity::Error),
            "CRITICAL" => Ok(GcpSeverity::Critical),
            "ALERT" => Ok(GcpSeverity::Alert),
            "EMERGENCY" => Ok(GcpSeverity::Emergency),
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
struct GcpStructuredLog<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    severity: Option<GcpSeverity>,
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
pub struct HttpRequestInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_method: Option<Box<str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_url: Option<Box<str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_agent: Option<Box<str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remote_ip: Option<Box<str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_ip: Option<Box<str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub referer: Option<Box<str>>,
    #[serde(skip_serializing_if = "Option::is_none", serialize_with = "duration_serializer")]
    pub latency: Option<Duration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_lookup: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_hit: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_validated_with_origin_server: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_fill_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol: Option<Box<str>>,
}

#[derive(Serialize, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct OperationInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Box<str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub producer: Option<Box<str>>,
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

pub struct GcpLayer<W: std::io::Write> {
    writer: W,
    project_id: Box<str>,
    operations: Mutex<HashMap<Id, Arc<Mutex<OperationData>>>>,
    expositions: RwLock<HashMap<Id, Arc<SpanExposition>>>
}

trait LayerAccess: Any {
    fn operations(&self) -> &Mutex<HashMap<Id, Arc<Mutex<OperationData>>>>;
}

impl<W: std::io::Write + 'static> LayerAccess for GcpLayer<W> {
    fn operations(&self) -> &Mutex<HashMap<Id, Arc<Mutex<OperationData>>>> {
        &self.operations
    }
}

impl<W: std::io::Write> GcpLayer<W> {
    pub fn new(writer: W, project_id: impl AsRef<str>) -> Self {
        Self {
            writer,
            project_id: project_id.as_ref().into(),
            operations: HashMap::new().into(),
            expositions: HashMap::new().into()
        }
    }
}

impl<W: std::io::Write + 'static, S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for GcpLayer<W> {
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        if let Some(mut expositions) = self.expositions.write().ok() {
            let span_id = format!("{:016x}", id.into_u64());

            let mut exposition = SpanExposition {
                trace_id: None,
                id: span_id.into(),
                fields: HashMap::new()
            };

            self.try_find_trace_id(id, ctx, &mut exposition);
            attrs.record(&mut exposition.visit());

            expositions.insert(id.clone(), Arc::new(exposition));
        }
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        if let Some(mut expositions) = self.expositions.write().ok() {
            let arc = expositions.get_mut(id).unwrap();
            let mut exposition = (**arc).clone();

            if exposition.trace_id.is_none() {
                self.try_find_trace_id(id, ctx, &mut exposition);
            }

            values.record(&mut exposition.visit());
            expositions.insert(id.clone(), Arc::new(exposition));
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let mut log = GcpStructuredLog {
            severity: match *event.metadata().level() {
                Level::TRACE | Level::DEBUG => Some(GcpSeverity::Debug),
                Level::INFO => Some(GcpSeverity::Info),
                Level::WARN => Some(GcpSeverity::Warning),
                Level::ERROR => Some(GcpSeverity::Error),
            },
            time: Some(Utc::now()),
            source_location: Some(SourceLocation {
                file: event.metadata().file(),
                line: event.metadata().line().map(|v| v as u64),
                function: None
            }),
            ..GcpStructuredLog::default()
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
            let mut trace_id = None::<String>;
            if log.span_id.is_none() {
                log.span_id = Some(span_id.clone().into())
            }

            if let Some(otel) = span.extensions().get::<OtelData>() {
                if let Some(trace) = otel.trace_id() {
                    trace_id = Some(format!("projects/{}/traces/{:032x}", self.project_id, trace));
                    if log.trace.is_none() && trace != TraceId::INVALID {
                        log.trace = Some(trace_id.clone().unwrap().into());
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
                        }
                    }
                }

                log.x_spans.push(exposition);
            }

            span_ref = span.parent();
        }

        event.record(&mut log);

        println!("{}", serde_json::to_string(&log).unwrap());
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

impl<W: std::io::Write> GcpLayer<W> {
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

impl<'a> GcpStructuredLog<'a> {
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

impl<'a> field::Visit for GcpStructuredLog<'a> {
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

pub struct Operation(Option<Arc<Mutex<OperationData>>>);

impl Operation {
    pub fn update(&self, info: OperationInfo) -> &Self {
        if let Some(mut data) = self.0.as_ref().and_then(|v| v.lock().ok()) {
            data.info = info;
        }

        self
    }

    pub fn end(self) {
        if let Some(mut data) = self.0.as_ref().and_then(|v| v.lock().ok()) {
            data.last = true;
        }
    }
}

pub trait SpanExt {
    fn operation(&'_ self) -> Operation;
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
    pub fn with_project_id<T: AsRef<str>>(self, text: T) -> LayerBuilder<T, W> {
        LayerBuilder {
            project_id: text,
            writer: self.writer
        }
    }
}

impl<ProjectId, W: std::io::Write> LayerBuilder<ProjectId, W> {
    pub fn with_writer<N: std::io::Write>(self, writer: N) -> LayerBuilder<ProjectId, N> {
        LayerBuilder {
            project_id: self.project_id,
            writer
        }
    }
}

impl<ProjectId: AsRef<str>, W: std::io::Write> LayerBuilder<ProjectId, W> {
    pub fn build(self) -> GcpLayer<W> {
        GcpLayer::new(self.writer, self.project_id)
    }
}

pub fn builder() -> LayerBuilder {
    LayerBuilder {
        project_id: (),
        writer: std::io::stdout()
    }
}
