# tracing-google-cloud

This is a tracing-subcriber [Layer] that
implements Google Cloud's [Structured Logging] format.
Features:
- [OpenTelemetry] Trace integration (with `opentelemetry` feature flag)
- Support for HTTP requests via log entry fields
- Support for populating operation structures into log entries from
  the parent [Span]\(s).

[OpenTelemetry]: https://docs.rs/tracing-opentelemetry
[Structured Logging]: https://docs.cloud.google.com/logging/docs/structured-logging#structured_logging_special_fields
[Span]: https://docs.rs/tracing/latest/tracing/struct.Span.html
[Layer]: https://docs.rs/tracing-subscriber/latest/tracing_subscriber/layer/trait.Layer.html


## Usage

Install via:
```shell
# with tracing-opentelemetry support:
cargo add tracing-google-cloud -F opentelemetry

# but it's not required!
cargo add tracing-google-cloud # <- omits trace ids
```

Add to a tracing-subscriber stack after `tracing-opentelemetry`'s layer, if you're using that: 
```rust
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

fn main() {
    tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer())
        .with(tracing_google_cloud::builder()
            .with_project_id(PROJECT_ID) // required with opentelemetry feature
            .with_writer(std::io::stdout())
            .build())
        .init();
}
```
