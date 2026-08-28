use crate::options::CommonOperationOptions;
use crate::{Authentication, ClientSettings, Credentials, NodePreference};
use base64::Engine;
use opentelemetry::Context;
use opentelemetry::global;
use opentelemetry::propagation::Injector;
use std::borrow::Cow;
use tonic::metadata::{Ascii, MetadataKey, MetadataMap, MetadataValue};

struct MetadataInjector<'a>(&'a mut MetadataMap);

impl Injector for MetadataInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        let Ok(key) = MetadataKey::<Ascii>::from_bytes(key.as_bytes()) else {
            tracing::warn!(key, "propagator produced an invalid gRPC metadata key");
            return;
        };
        let Ok(value) = MetadataValue::<Ascii>::try_from(value.as_str()) else {
            tracing::warn!(
                key = key.as_str(),
                "propagator produced an invalid gRPC metadata value"
            );
            return;
        };

        self.0.insert(key, value);
    }
}

pub(crate) fn build_request_metadata(
    settings: &ClientSettings,
    options: &CommonOperationOptions,
) -> tonic::metadata::MetadataMap
where
{
    let mut metadata = tonic::metadata::MetadataMap::new();
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&Context::current(), &mut MetadataInjector(&mut metadata));
    });
    let authentication: Option<Cow<'_, Authentication>> = options
        .authentication
        .as_ref()
        .map(Cow::Borrowed)
        .or_else(|| {
            settings
                .default_authenticated_user()
                .as_ref()
                .map(|c| Cow::Owned(Authentication::Basic(c.clone())))
        });

    if let Some(header_value) = authentication
        .as_deref()
        .and_then(build_authorization_header)
    {
        metadata.insert("authorization", header_value);
    }

    if options.requires_leader || settings.node_preference() == NodePreference::Leader {
        let header_value = MetadataValue::try_from("true").expect("valid metadata header value");
        metadata.insert("requires-leader", header_value);
    }

    if let Some(conn_name) = settings.connection_name.as_ref() {
        let header_value =
            MetadataValue::try_from(conn_name.as_str()).expect("valid metadata header value");
        metadata.insert("connection-name", header_value);
    }

    metadata
}

fn build_authorization_header(
    auth: &Authentication,
) -> Option<tonic::metadata::MetadataValue<tonic::metadata::Ascii>> {
    use tonic::metadata::MetadataValue;

    let header = match auth {
        Authentication::Basic(Credentials { login, password }) => {
            let login = String::from_utf8_lossy(login);
            let password = String::from_utf8_lossy(password);
            let encoded =
                base64::engine::general_purpose::STANDARD.encode(format!("{}:{}", login, password));
            format!("Basic {}", encoded)
        }
        Authentication::Bearer(token) => {
            let token = String::from_utf8_lossy(token);
            format!("Bearer {}", token)
        }
    };

    match MetadataValue::try_from(header.as_str()) {
        Ok(value) => Some(value),
        Err(_) => {
            tracing::warn!(
                auth_kind = auth.kind(),
                "authentication value contains characters that are not valid in a gRPC metadata header; the Authorization header will be omitted"
            );
            None
        }
    }
}

#[cfg(test)]
mod auth_tests {
    use super::*;
    use crate::AppendToStreamOptions;
    use crate::observability::{client_operation, operation};
    use crate::options::Options;
    use opentelemetry::global;
    use opentelemetry::trace::noop::{NoopTextMapPropagator, NoopTracerProvider};
    use opentelemetry_sdk::propagation::TraceContextPropagator;
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};

    fn settings_from(connection_string: &str) -> ClientSettings {
        connection_string
            .parse::<ClientSettings>()
            .expect("valid connection string")
    }

    #[test]
    fn basic_authentication_produces_base64_basic_header() {
        let auth = Authentication::basic("admin", "changeit");
        let header = build_authorization_header(&auth).expect("ASCII header");
        assert_eq!(header.to_str().unwrap(), "Basic YWRtaW46Y2hhbmdlaXQ=");
    }

    #[test]
    fn bearer_authentication_produces_bearer_header_verbatim() {
        let auth = Authentication::bearer("abc.def.ghi");
        let header = build_authorization_header(&auth).expect("ASCII header");
        assert_eq!(header.to_str().unwrap(), "Bearer abc.def.ghi");
    }

    #[test]
    fn basic_authentication_with_special_chars_encodes_correctly() {
        let auth = Authentication::basic("user@example.com", "p@ss:word");
        let header = build_authorization_header(&auth).expect("ASCII header");
        assert_eq!(
            header.to_str().unwrap(),
            "Basic dXNlckBleGFtcGxlLmNvbTpwQHNzOndvcmQ="
        );
    }

    #[test]
    fn build_request_metadata_skips_bearer_token_with_invalid_chars() {
        let settings = settings_from("esdb://localhost:2113?tls=false");
        let options =
            AppendToStreamOptions::default().authenticated(Authentication::bearer("token\nleak"));
        let metadata = build_request_metadata(&settings, options.common_operation_options());
        assert!(metadata.get("authorization").is_none());
    }

    #[test]
    fn no_auth_anywhere_produces_no_authorization_header() {
        let settings = settings_from("esdb://localhost:2113?tls=false");
        let options = AppendToStreamOptions::default();
        let metadata = build_request_metadata(&settings, options.common_operation_options());

        assert!(metadata.get("authorization").is_none());
    }

    #[test]
    fn default_user_from_connection_string_falls_through_as_basic() {
        let settings = settings_from("esdb://admin:changeit@localhost:2113?tls=false");
        let options = AppendToStreamOptions::default();
        let metadata = build_request_metadata(&settings, options.common_operation_options());

        assert_eq!(
            metadata.get("authorization").unwrap().to_str().unwrap(),
            "Basic YWRtaW46Y2hhbmdlaXQ="
        );
    }

    #[test]
    fn per_call_bearer_overrides_default_user() {
        let settings = settings_from("esdb://admin:changeit@localhost:2113?tls=false");
        let options =
            AppendToStreamOptions::default().authenticated(Authentication::bearer("call-token"));
        let metadata = build_request_metadata(&settings, options.common_operation_options());

        assert_eq!(
            metadata.get("authorization").unwrap().to_str().unwrap(),
            "Bearer call-token"
        );
    }

    #[tokio::test]
    async fn build_request_metadata_injects_the_current_client_context() {
        let _guard = crate::observability::TEST_GLOBALS.lock().await;
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        global::set_tracer_provider(provider.clone());
        global::set_text_map_propagator(TraceContextPropagator::new());
        let settings = settings_from("esdb://localhost:2113?tls=false");
        let options = AppendToStreamOptions::default();

        let metadata = client_operation(operation::APPEND_TO_STREAM, async {
            Ok(build_request_metadata(
                &settings,
                options.common_operation_options(),
            ))
        })
        .await
        .unwrap();
        provider.force_flush().unwrap();
        let spans = exporter.get_finished_spans().unwrap();
        let span = spans
            .iter()
            .find(|span| span.name == operation::APPEND_TO_STREAM.span_name())
            .expect("client span");

        assert_eq!(
            metadata.get("traceparent").unwrap().to_str().unwrap(),
            format!(
                "00-{}-{}-01",
                span.span_context.trace_id(),
                span.span_context.span_id()
            )
        );
        global::set_tracer_provider(NoopTracerProvider::new());
        global::set_text_map_propagator(NoopTextMapPropagator::new());
    }

    #[test]
    fn authenticated_builder_accepts_credentials_directly() {
        let settings = settings_from("esdb://localhost:2113?tls=false");
        let options =
            AppendToStreamOptions::default().authenticated(Credentials::new("alice", "secret"));
        let metadata = build_request_metadata(&settings, options.common_operation_options());

        assert_eq!(
            metadata.get("authorization").unwrap().to_str().unwrap(),
            "Basic YWxpY2U6c2VjcmV0"
        );
    }
}
