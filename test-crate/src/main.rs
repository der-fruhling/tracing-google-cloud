use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_google_cloud::{OperationInfo, SpanExt};

fn main() {
    tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer())
        .with(tracing_google_cloud::builder()
            .with_project_id("amphoreus")
            .with_writer(std::io::stdout())
            .build())
        .init();

    tracing::info!("Hello world!");

    tracing::info!(
        http.request_method = "POST",
        http.request_url = "/",
        http.request_size = 420,
        http.status = 200,
        http.response_size = 1024,
        http.user_agent = "Fire fox",
        http.remote_ip = "127.0.0.1",
        http.server_ip = "127.0.0.2",
        http.referer = "127.0.0.2",
        http.latency_ns = 32000000,
        http.cache_lookup = false,
        http.cache_hit = false,
        http.cache_validated_with_origin_server = false,
        http.cache_fill_bytes = 200,
        http.protocol = "HTTP/4",

        labels.miaw = "yes"
    );

    let span = tracing::info_span!("test", test = 1);

    let operation = span.operation();
    operation.update(OperationInfo {
        id: Some("operation".into()),
        producer: Some("amphoreus.info".into())
    });

    span.in_scope(|| {
        tracing::info!("Hello world again!");

        tracing::info_span!("test2", miaw = "yes", test = 2).in_scope(|| {
            tracing::info!("Hello world again again!");
        });

        operation.end();
        tracing::info!("End of operation");
    });
}
