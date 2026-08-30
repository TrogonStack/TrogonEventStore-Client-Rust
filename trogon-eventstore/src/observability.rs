mod generated;

use generated::CLIENT_SPAN_KIND;
pub(crate) use generated::{
    DB_COLLECTION_NAME, DB_OPERATION_NAME, DB_SYSTEM_NAME, ERROR_TYPE,
    TROGON_EVENTSTORE_BATCH_CORRELATION_ID,
};
use opentelemetry::trace::{FutureExt, Status, TraceContextExt, Tracer};
use opentelemetry::{Context, InstrumentationScope, KeyValue, global};
use std::borrow::Cow;
use std::future::Future;

pub(crate) use generated::operation;

#[cfg(test)]
pub(crate) static TEST_GLOBALS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Clone, Copy)]
pub(crate) struct InstrumentationScopeIdentity {
    pub(crate) name: &'static str,
    pub(crate) version: &'static str,
}

pub(crate) const INSTRUMENTATION_SCOPE: InstrumentationScopeIdentity =
    InstrumentationScopeIdentity {
        name: env!("CARGO_PKG_NAME"),
        version: env!("CARGO_PKG_VERSION"),
    };

#[derive(Clone, Copy)]
pub(crate) struct ClientOperation {
    operation_name: &'static str,
}

impl ClientOperation {
    pub(super) const fn new(operation_name: &'static str) -> Self {
        Self { operation_name }
    }

    pub(crate) fn on_collection(self, collection_name: impl Into<String>) -> ClientSpan {
        ClientSpan {
            operation: self,
            collection_name: Some(collection_name.into()),
        }
    }

    pub(crate) fn on_stream(self, stream_name: &bytes::Bytes) -> ClientSpan {
        self.on_collection(String::from_utf8_lossy(stream_name).into_owned())
    }

    pub(crate) const fn span_name(self) -> &'static str {
        self.operation_name
    }
}

pub(crate) struct ClientSpan {
    operation: ClientOperation,
    collection_name: Option<String>,
}

impl ClientSpan {
    fn span_name(&self) -> Cow<'static, str> {
        match &self.collection_name {
            Some(collection_name) => Cow::Owned(format!(
                "{} {}",
                self.operation.span_name(),
                collection_name
            )),
            None => Cow::Borrowed(self.operation.span_name()),
        }
    }
}

impl From<ClientOperation> for ClientSpan {
    fn from(operation: ClientOperation) -> Self {
        Self {
            operation,
            collection_name: None,
        }
    }
}

fn instrumentation_scope() -> InstrumentationScope {
    InstrumentationScope::builder(INSTRUMENTATION_SCOPE.name)
        .with_version(INSTRUMENTATION_SCOPE.version)
        .build()
}

fn start_client_operation(operation: impl Into<ClientSpan>) -> Context {
    let operation = operation.into();
    let tracer = global::tracer_with_scope(instrumentation_scope());
    let mut attributes = vec![
        KeyValue::new(DB_SYSTEM_NAME, "trogoneventstore"),
        KeyValue::new(DB_OPERATION_NAME, operation.operation.span_name()),
    ];
    if let Some(collection_name) = &operation.collection_name {
        attributes.push(KeyValue::new(DB_COLLECTION_NAME, collection_name.clone()));
    }

    let span = tracer
        .span_builder(operation.span_name())
        .with_kind(CLIENT_SPAN_KIND)
        .with_attributes(attributes)
        .start(&tracer);

    Context::current_with_span(span)
}

fn error_type(error: &crate::Error) -> &'static str {
    match error {
        crate::Error::ServerError(_) => "server_error",
        crate::Error::NotLeaderException(_) => "not_leader",
        crate::Error::ConnectionClosed => "connection_closed",
        crate::Error::Grpc { .. } => "grpc",
        crate::Error::GrpcConnectionError(_) => "grpc_connection",
        crate::Error::InternalParsingError(_) => "internal_parsing",
        crate::Error::AccessDenied => "access_denied",
        crate::Error::ResourceAlreadyExists => "resource_already_exists",
        crate::Error::ResourceNotFound => "resource_not_found",
        crate::Error::ResourceDeleted => "resource_deleted",
        crate::Error::UnsupportedFeature => "unsupported_feature",
        crate::Error::InternalClientError => "internal_client",
        crate::Error::DeadlineExceeded => "deadline_exceeded",
        crate::Error::InitializationError(_) => "initialization",
        crate::Error::IllegalStateError(_) => "illegal_state",
        crate::Error::WrongExpectedVersion { .. } => "wrong_expected_version",
    }
}

pub(crate) async fn client_operation<T, F>(
    operation: impl Into<ClientSpan>,
    future: F,
) -> crate::Result<T>
where
    F: Future<Output = crate::Result<T>>,
{
    let context = start_client_operation(operation);
    let result = future.with_context(context.clone()).await;

    if let Err(error) = &result {
        let error_type = error_type(error);
        context
            .span()
            .set_attribute(KeyValue::new(ERROR_TYPE, error_type));
        context.span().set_status(Status::error(error_type));
    }
    context.span().end();

    result
}

pub(crate) async fn infallible_client_operation<T, F>(
    operation: impl Into<ClientSpan>,
    future: F,
) -> T
where
    F: Future<Output = T>,
{
    let context = start_client_operation(operation);
    let output = future.with_context(context.clone()).await;
    context.span().end();

    output
}

#[cfg(test)]
mod tests {
    use super::{
        DB_COLLECTION_NAME, DB_OPERATION_NAME, DB_SYSTEM_NAME, ERROR_TYPE, INSTRUMENTATION_SCOPE,
        client_operation, operation::APPEND_TO_STREAM,
    };
    use opentelemetry::Context;
    use opentelemetry::global;
    use opentelemetry::trace::{SpanKind, Status, TraceContextExt, noop::NoopTracerProvider};
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};

    #[tokio::test]
    async fn client_operation_emits_a_semantic_database_client_span() {
        let _guard = super::TEST_GLOBALS.lock().await;
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        global::set_tracer_provider(provider.clone());

        let current_span_id = client_operation(APPEND_TO_STREAM.on_collection("orders"), async {
            Ok::<_, crate::Error>(Context::current().span().span_context().span_id())
        })
        .await
        .unwrap();
        provider.force_flush().unwrap();

        let spans = exporter.get_finished_spans().unwrap();
        let span = spans
            .iter()
            .find(|span| span.name == "append_to_stream orders")
            .expect("client span");

        assert_eq!(span.span_kind, SpanKind::Client);
        assert_eq!(span.span_context.span_id(), current_span_id);
        assert_eq!(span.status, Status::Unset);
        assert_eq!(
            span.instrumentation_scope.name(),
            INSTRUMENTATION_SCOPE.name
        );
        assert_eq!(
            span.instrumentation_scope.version(),
            Some(INSTRUMENTATION_SCOPE.version)
        );
        assert_eq!(attribute(span, DB_SYSTEM_NAME), Some("trogoneventstore"));
        assert_eq!(attribute(span, DB_OPERATION_NAME), Some("append_to_stream"));
        assert_eq!(attribute(span, DB_COLLECTION_NAME), Some("orders"));
        assert_eq!(attribute(span, ERROR_TYPE), None);

        global::set_tracer_provider(NoopTracerProvider::new());
    }

    #[tokio::test]
    async fn client_operation_records_a_bounded_error_status() {
        let _guard = super::TEST_GLOBALS.lock().await;
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        global::set_tracer_provider(provider.clone());

        const ERROR_DETAIL: &str = "variable backend detail";
        let result = client_operation::<(), _>(APPEND_TO_STREAM, async {
            Err(crate::Error::ServerError(ERROR_DETAIL.to_owned()))
        })
        .await;
        assert!(result.is_err());
        provider.force_flush().unwrap();

        let spans = exporter.get_finished_spans().unwrap();
        let span = spans
            .iter()
            .find(|span| span.name == APPEND_TO_STREAM.span_name())
            .expect("client span");

        assert_eq!(span.status, Status::error("server_error"));
        if let Status::Error { description } = &span.status {
            assert!(!description.contains(ERROR_DETAIL));
        }
        assert_eq!(attribute(span, ERROR_TYPE), Some("server_error"));

        global::set_tracer_provider(NoopTracerProvider::new());
    }

    fn attribute<'a>(span: &'a opentelemetry_sdk::trace::SpanData, key: &str) -> Option<&'a str> {
        span.attributes
            .iter()
            .find(|attribute| attribute.key.as_str() == key)
            .and_then(|attribute| match &attribute.value {
                opentelemetry::Value::String(value) => Some(value.as_ref()),
                _ => None,
            })
    }
}
