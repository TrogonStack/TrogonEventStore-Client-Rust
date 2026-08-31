mod generated;

use bytes::Bytes;
use generated::{CLIENT_SPAN_KIND, RECEIVE_SPAN_KIND};
pub(crate) use generated::{
    DB_COLLECTION_NAME, DB_OPERATION_NAME, DB_SYSTEM_NAME, ERROR_TYPE,
    MESSAGING_CONSUMER_GROUP_NAME, MESSAGING_DESTINATION_NAME, MESSAGING_MESSAGE_ID,
    MESSAGING_OPERATION_NAME, MESSAGING_OPERATION_TYPE, MESSAGING_SYSTEM,
    TROGON_EVENTSTORE_BATCH_CORRELATION_ID, TROGON_EVENTSTORE_EVENT_TYPE,
};
use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::trace::{FutureExt, Link, Span as _, Status, TraceContextExt, Tracer};
use opentelemetry::{Context, InstrumentationScope, KeyValue, global};
use serde_json::{Map, Value};
use std::borrow::Cow;
use std::future::Future;
use std::time::SystemTime;

use crate::{EventData, ResolvedEvent};

pub(crate) use generated::operation;

const TRACE_PARENT: &str = "traceparent";
const TRACE_STATE: &str = "tracestate";
const RECEIVE_OPERATION: &str = "receive";

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

struct EventMetadataCarrier(Map<String, Value>);

impl Injector for EventMetadataCarrier {
    fn set(&mut self, key: &str, value: String) {
        if !is_persisted_trace_field(key) {
            return;
        }

        self.0
            .retain(|existing, _| !existing.eq_ignore_ascii_case(key));
        self.0.insert(key.to_owned(), Value::String(value));
    }
}

fn is_persisted_trace_field(key: &str) -> bool {
    key.eq_ignore_ascii_case(TRACE_PARENT) || key.eq_ignore_ascii_case(TRACE_STATE)
}

impl Extractor for EventMetadataCarrier {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.iter().find_map(|(existing, value)| {
            existing
                .eq_ignore_ascii_case(key)
                .then(|| value.as_str())
                .flatten()
        })
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(String::as_str).collect()
    }
}

pub(crate) fn inject_event_context(mut event: EventData, context: &Context) -> EventData {
    if !context.span().span_context().is_valid() {
        return event;
    }

    let mut propagation_fields = EventMetadataCarrier(Map::new());
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(context, &mut propagation_fields)
    });
    if propagation_fields.0.is_empty() {
        return event;
    }

    let metadata = match event.custom_metadata.as_deref() {
        None | Some([]) => Map::new(),
        Some(metadata) => match serde_json::from_slice::<Value>(metadata) {
            Ok(Value::Object(metadata)) => metadata,
            _ => return event,
        },
    };

    let mut carrier = EventMetadataCarrier(metadata);
    for (key, value) in propagation_fields.0 {
        if let Value::String(value) = value {
            carrier.set(&key, value);
        }
    }

    if let Ok(metadata) = serde_json::to_vec(&Value::Object(carrier.0)) {
        event.custom_metadata = Some(Bytes::from(metadata));
    }

    event
}

fn extract_event_context(metadata: &[u8]) -> Context {
    if metadata.is_empty() {
        return Context::new();
    }

    let Ok(Value::Object(metadata)) = serde_json::from_slice(metadata) else {
        return Context::new();
    };

    let carrier = EventMetadataCarrier(metadata);
    global::get_text_map_propagator(|propagator| propagator.extract(&carrier))
}

pub(crate) struct SubscriptionReceive {
    parent: Context,
    started_at: SystemTime,
}

impl SubscriptionReceive {
    pub(crate) fn start() -> Self {
        Self {
            parent: Context::current(),
            started_at: SystemTime::now(),
        }
    }

    pub(crate) fn complete(
        self,
        consumer_group_name: Option<&str>,
        resolved_event: &ResolvedEvent,
    ) {
        let Some(delivered_event) = resolved_event
            .event
            .as_ref()
            .or(resolved_event.link.as_ref())
        else {
            return;
        };
        let original_event = resolved_event.get_original_event();
        let destination = original_event.stream_id();
        let mut attributes = vec![
            KeyValue::new(MESSAGING_SYSTEM, "trogoneventstore"),
            KeyValue::new(MESSAGING_OPERATION_NAME, RECEIVE_OPERATION),
            KeyValue::new(MESSAGING_OPERATION_TYPE, RECEIVE_OPERATION),
            KeyValue::new(MESSAGING_DESTINATION_NAME, destination.to_owned()),
            KeyValue::new(MESSAGING_MESSAGE_ID, original_event.id.to_string()),
            KeyValue::new(
                TROGON_EVENTSTORE_EVENT_TYPE,
                delivered_event.event_type.clone(),
            ),
        ];
        if let Some(consumer_group_name) = consumer_group_name {
            attributes.push(KeyValue::new(
                MESSAGING_CONSUMER_GROUP_NAME,
                consumer_group_name.to_owned(),
            ));
        }

        let message_context = extract_event_context(&delivered_event.custom_metadata);
        let message_span_context = message_context.span().span_context().clone();
        let tracer = global::tracer_with_scope(instrumentation_scope());
        let mut builder = tracer
            .span_builder(format!("receive {destination}"))
            .with_kind(RECEIVE_SPAN_KIND)
            .with_start_time(self.started_at)
            .with_attributes(attributes);
        if message_span_context.is_valid() {
            builder = builder.with_links(vec![Link::with_context(message_span_context)]);
        }

        let mut span = builder.start_with_context(&tracer, &self.parent);
        span.end();
    }
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
        MESSAGING_CONSUMER_GROUP_NAME, MESSAGING_DESTINATION_NAME, MESSAGING_MESSAGE_ID,
        MESSAGING_OPERATION_NAME, MESSAGING_OPERATION_TYPE, MESSAGING_SYSTEM, SubscriptionReceive,
        TROGON_EVENTSTORE_EVENT_TYPE, client_operation, extract_event_context,
        inject_event_context, operation::APPEND_TO_STREAM,
    };
    use bytes::Bytes;
    use chrono::Utc;
    use opentelemetry::baggage::BaggageExt;
    use opentelemetry::global;
    use opentelemetry::propagation::TextMapCompositePropagator;
    use opentelemetry::trace::{
        SpanContext, SpanId, SpanKind, Status, TraceContextExt, TraceFlags, TraceId, TraceState,
        noop::NoopTracerProvider,
    };
    use opentelemetry::{Context, KeyValue};
    use opentelemetry_sdk::propagation::{BaggagePropagator, TraceContextPropagator};
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
    use serde_json::json;

    use crate::{EventData, Position, RecordedEvent, ResolvedEvent};

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

    #[tokio::test]
    async fn event_context_round_trips_without_replacing_custom_metadata() {
        let _guard = super::TEST_GLOBALS.lock().await;
        global::set_text_map_propagator(TextMapCompositePropagator::new(vec![
            Box::new(TraceContextPropagator::new()),
            Box::new(BaggagePropagator::new()),
        ]));
        let context = remote_context().with_baggage([KeyValue::new("tenant", "secret")]);
        let event = EventData::json("order-created", &json!({"orderId": "42"}))
            .unwrap()
            .metadata_as_json(&json!({
                "tenant": "north",
                "TraceParent": "00-00000000000000000000000000000001-0000000000000001-00"
            }))
            .unwrap();

        let event = inject_event_context(event, &context);
        let metadata: serde_json::Value =
            serde_json::from_slice(event.custom_metadata.as_ref().unwrap()).unwrap();
        let object = metadata.as_object().unwrap();

        assert_eq!(object.get("tenant"), Some(&json!("north")));
        assert!(!object.keys().any(|key| key.eq_ignore_ascii_case("baggage")));
        assert_eq!(
            object
                .keys()
                .filter(|key| key.eq_ignore_ascii_case("traceparent"))
                .count(),
            1
        );
        let extracted = extract_event_context(event.custom_metadata.as_ref().unwrap());
        assert_eq!(
            extracted.span().span_context().trace_id(),
            context.span().span_context().trace_id()
        );
        assert_eq!(
            extracted.span().span_context().span_id(),
            context.span().span_context().span_id()
        );

        reset_propagator();
    }

    #[tokio::test]
    async fn event_context_injection_preserves_unsupported_metadata_and_empty_contexts() {
        let _guard = super::TEST_GLOBALS.lock().await;
        global::set_text_map_propagator(TraceContextPropagator::new());
        for unsupported in [
            b"not-json".as_slice(),
            b"[]".as_slice(),
            br#""scalar""#.as_slice(),
            b"42".as_slice(),
        ] {
            let original = Bytes::copy_from_slice(unsupported);
            let event = EventData::binary("binary", Bytes::new()).metadata(original.clone());
            let event = inject_event_context(event, &remote_context());
            assert_eq!(event.custom_metadata.as_ref(), Some(&original));
        }

        let original = Bytes::from_static(br#"{"tenant":"north"}"#);
        let event = EventData::binary("binary", Bytes::new()).metadata(original.clone());
        let event = inject_event_context(event, &Context::new());
        assert_eq!(event.custom_metadata.as_ref(), Some(&original));

        reset_propagator();
    }

    #[tokio::test]
    async fn subscription_event_emits_a_receive_span_with_ambient_parent_and_message_link() {
        let _guard = super::TEST_GLOBALS.lock().await;
        global::set_text_map_propagator(TraceContextPropagator::new());
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        global::set_tracer_provider(provider.clone());
        let message_context = remote_context();
        let ambient_context = alternate_remote_context();
        let event = inject_event_context(
            EventData::json("order-created", &json!({"orderId": "42"})).unwrap(),
            &message_context,
        );
        let recorded = recorded_event(event.custom_metadata.unwrap());
        let message_id = recorded.id.to_string();
        let resolved = ResolvedEvent {
            event: Some(recorded),
            link: None,
            commit_position: None,
        };

        let _ambient = ambient_context.clone().attach();
        let receive = SubscriptionReceive::start();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        receive.complete(Some("billing"), &resolved);
        provider.force_flush().unwrap();

        let spans = exporter.get_finished_spans().unwrap();
        let span = spans
            .iter()
            .find(|span| span.name == "receive orders")
            .expect("receive span");
        assert_eq!(span.span_kind, SpanKind::Client);
        assert!(
            span.end_time.duration_since(span.start_time).unwrap()
                >= std::time::Duration::from_millis(10)
        );
        assert_eq!(
            span.parent_span_id,
            ambient_context.span().span_context().span_id()
        );
        assert_eq!(span.links.links.len(), 1);
        assert_eq!(
            span.links.links[0].span_context,
            message_context.span().span_context().clone()
        );
        assert_eq!(attribute(span, MESSAGING_SYSTEM), Some("trogoneventstore"));
        assert_eq!(attribute(span, MESSAGING_OPERATION_NAME), Some("receive"));
        assert_eq!(attribute(span, MESSAGING_OPERATION_TYPE), Some("receive"));
        assert_eq!(attribute(span, MESSAGING_DESTINATION_NAME), Some("orders"));
        assert_eq!(
            attribute(span, MESSAGING_MESSAGE_ID),
            Some(message_id.as_str())
        );
        assert_eq!(
            attribute(span, MESSAGING_CONSUMER_GROUP_NAME),
            Some("billing")
        );
        assert_eq!(
            attribute(span, TROGON_EVENTSTORE_EVENT_TYPE),
            Some("order-created")
        );

        global::set_tracer_provider(NoopTracerProvider::new());
        reset_propagator();
    }

    #[tokio::test]
    async fn subscription_event_emits_receive_telemetry_without_persisted_context() {
        let _guard = super::TEST_GLOBALS.lock().await;
        global::set_text_map_propagator(TraceContextPropagator::new());
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        global::set_tracer_provider(provider.clone());
        let ambient_context = alternate_remote_context();
        let resolved = ResolvedEvent {
            event: Some(recorded_event(Bytes::new())),
            link: None,
            commit_position: None,
        };

        let _ambient = ambient_context.clone().attach();
        SubscriptionReceive::start().complete(None, &resolved);
        provider.force_flush().unwrap();

        let spans = exporter.get_finished_spans().unwrap();
        let span = spans
            .iter()
            .find(|span| span.name == "receive orders")
            .expect("receive span");
        assert_eq!(span.span_kind, SpanKind::Client);
        assert_eq!(
            span.parent_span_id,
            ambient_context.span().span_context().span_id()
        );
        assert!(span.links.links.is_empty());

        global::set_tracer_provider(NoopTracerProvider::new());
        reset_propagator();
    }

    #[tokio::test]
    async fn resolved_subscription_event_uses_delivered_event_type_with_original_identity() {
        let _guard = super::TEST_GLOBALS.lock().await;
        global::set_text_map_propagator(TraceContextPropagator::new());
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        global::set_tracer_provider(provider.clone());
        let message_context = remote_context();
        let event = inject_event_context(
            EventData::json("order-created", &json!({"orderId": "42"})).unwrap(),
            &message_context,
        );
        let mut link = recorded_event(Bytes::new());
        link.stream_id_raw = Bytes::from_static(b"$ce-orders");
        link.event_type = "$>".to_owned();
        let link_id = link.id.to_string();
        let resolved = ResolvedEvent {
            event: Some(recorded_event(event.custom_metadata.unwrap())),
            link: Some(link),
            commit_position: None,
        };

        SubscriptionReceive::start().complete(None, &resolved);
        provider.force_flush().unwrap();

        let spans = exporter.get_finished_spans().unwrap();
        let span = spans
            .iter()
            .find(|span| span.name == "receive $ce-orders")
            .expect("receive span");
        assert_eq!(
            attribute(span, MESSAGING_DESTINATION_NAME),
            Some("$ce-orders")
        );
        assert_eq!(
            attribute(span, MESSAGING_MESSAGE_ID),
            Some(link_id.as_str())
        );
        assert_eq!(
            attribute(span, TROGON_EVENTSTORE_EVENT_TYPE),
            Some("order-created")
        );

        global::set_tracer_provider(NoopTracerProvider::new());
        reset_propagator();
    }

    #[tokio::test]
    async fn link_only_subscription_event_emits_receive_telemetry_from_the_link() {
        let _guard = super::TEST_GLOBALS.lock().await;
        global::set_text_map_propagator(TraceContextPropagator::new());
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        global::set_tracer_provider(provider.clone());
        let message_context = remote_context();
        let event = inject_event_context(
            EventData::json("$>", &json!({"link": "0@orders"})).unwrap(),
            &message_context,
        );
        let mut link = recorded_event(event.custom_metadata.unwrap());
        link.stream_id_raw = Bytes::from_static(b"$ce-orders");
        link.event_type = "$>".to_owned();
        let link_id = link.id.to_string();
        let resolved = ResolvedEvent {
            event: None,
            link: Some(link),
            commit_position: None,
        };

        SubscriptionReceive::start().complete(None, &resolved);
        provider.force_flush().unwrap();

        let spans = exporter.get_finished_spans().unwrap();
        let span = spans
            .iter()
            .find(|span| span.name == "receive $ce-orders")
            .expect("receive span");
        assert_eq!(
            attribute(span, MESSAGING_DESTINATION_NAME),
            Some("$ce-orders")
        );
        assert_eq!(
            attribute(span, MESSAGING_MESSAGE_ID),
            Some(link_id.as_str())
        );
        assert_eq!(attribute(span, TROGON_EVENTSTORE_EVENT_TYPE), Some("$>"));
        assert_eq!(span.links.links.len(), 1);
        assert_eq!(
            span.links.links[0].span_context,
            message_context.span().span_context().clone()
        );

        global::set_tracer_provider(NoopTracerProvider::new());
        reset_propagator();
    }

    fn remote_context() -> Context {
        remote_context_with("58406520a006649127e371903a2de979", "58406520a0066491")
    }

    fn alternate_remote_context() -> Context {
        remote_context_with("68406520a006649127e371903a2de978", "68406520a0066492")
    }

    fn remote_context_with(trace_id: &str, span_id: &str) -> Context {
        Context::new().with_remote_span_context(SpanContext::new(
            TraceId::from_hex(trace_id).unwrap(),
            SpanId::from_hex(span_id).unwrap(),
            TraceFlags::SAMPLED,
            true,
            TraceState::default(),
        ))
    }

    fn recorded_event(custom_metadata: Bytes) -> RecordedEvent {
        RecordedEvent {
            stream_id_raw: Bytes::from_static(b"orders"),
            id: uuid::Uuid::new_v4(),
            revision: 0,
            event_type: "order-created".to_owned(),
            data: Bytes::new(),
            metadata: Default::default(),
            custom_metadata,
            is_json: true,
            position: Position::start(),
            created: Utc::now(),
        }
    }

    fn reset_propagator() {
        global::set_text_map_propagator(TextMapCompositePropagator::new(Vec::new()));
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
