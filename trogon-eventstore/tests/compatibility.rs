use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use opentelemetry::propagation::TextMapCompositePropagator;
use opentelemetry::trace::{FutureExt, TraceContextExt, Tracer};
use opentelemetry::{Context, global};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::propagation::{BaggagePropagator, TraceContextPropagator};
use opentelemetry_sdk::trace::SdkTracerProvider;
use serde::{Deserialize, Serialize};
use trogon_eventstore::{
    AppendToStreamOptions, Client, EventData, PersistentSubscriptionEvent,
    PersistentSubscriptionOptions, RecordedEvent, StreamPosition, StreamState,
    SubscribeToStreamOptions, SubscriptionEvent,
};

const EVENT_TYPE: &str = "trogon-compatibility";
const PRODUCER: &str = "rust";
const SERVICE_NAME: &str = "trogon-eventstore-client-rust";
const TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct CompatibilityPayload {
    producer: String,
    run_id: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CompatibilityResult {
    command: String,
    stream: String,
    group: Option<String>,
    producer: String,
    run_id: String,
    event_id: Option<uuid::Uuid>,
}

struct CompatibilityOptions {
    command: CompatibilityCommand,
    uri: String,
    run_id: RunId,
    stream: StreamName,
    group: Option<GroupName>,
    ready_file: Option<ReadyFile>,
    otlp_endpoint: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CompatibilityCommand {
    Write,
    BatchWrite,
    Read,
    Subscribe,
    CreatePersistentSubscription,
    ConsumePersistentSubscription,
}

struct StreamName(String);
struct GroupName(String);
struct RunId(String);
struct ReadyFile(PathBuf);

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires an external server and OTLP collector"]
async fn cross_client_compatibility() -> eyre::Result<()> {
    let options = CompatibilityOptions::load()?;
    let provider = configure_telemetry(options.otlp_endpoint.clone())?;
    let tracer = global::tracer(SERVICE_NAME);
    let root = tracer.start(format!("compatibility {}", options.command.as_str()));
    let context = Context::current_with_span(root);

    let result =
        tokio::time::timeout(TIMEOUT, execute(options).with_context(context.clone())).await;
    context.span().end();
    provider.force_flush()?;

    let result = result.map_err(|_| eyre::eyre!("compatibility command timed out"))??;
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}

async fn execute(options: CompatibilityOptions) -> eyre::Result<CompatibilityResult> {
    let settings = options.uri.parse()?;
    let client = Client::new(settings)?;

    match options.command {
        CompatibilityCommand::Write => write(&client, options).await,
        CompatibilityCommand::BatchWrite => batch_write(&client, options).await,
        CompatibilityCommand::Read => read(&client, options).await,
        CompatibilityCommand::Subscribe => subscribe(&client, options).await,
        CompatibilityCommand::CreatePersistentSubscription => {
            create_persistent_subscription(&client, options).await
        }
        CompatibilityCommand::ConsumePersistentSubscription => {
            consume_persistent_subscription(&client, options).await
        }
    }
}

async fn write(
    client: &Client,
    options: CompatibilityOptions,
) -> eyre::Result<CompatibilityResult> {
    let (payload, event_id, event) = compatibility_event(&options)?;
    let append_options = AppendToStreamOptions::default().stream_state(StreamState::NoStream);
    client
        .append_to_stream(options.stream.as_str(), &append_options, event)
        .await?;

    Ok(options.result(payload, Some(event_id)))
}

async fn batch_write(
    client: &Client,
    options: CompatibilityOptions,
) -> eyre::Result<CompatibilityResult> {
    let (payload, event_id, event) = compatibility_event(&options)?;
    let batch = client.batch_append(&Default::default()).await?;
    batch
        .append_to_stream(options.stream.as_str(), StreamState::NoStream, vec![event])
        .await?;

    Ok(options.result(payload, Some(event_id)))
}

fn compatibility_event(
    options: &CompatibilityOptions,
) -> eyre::Result<(CompatibilityPayload, uuid::Uuid, EventData)> {
    let payload = CompatibilityPayload {
        producer: PRODUCER.to_owned(),
        run_id: options.run_id.as_str().to_owned(),
    };
    let event_id = uuid::Uuid::new_v4();
    let event = EventData::json(EVENT_TYPE, &payload)?.id(event_id);

    Ok((payload, event_id, event))
}

async fn read(client: &Client, options: CompatibilityOptions) -> eyre::Result<CompatibilityResult> {
    let mut read = client
        .read_stream(options.stream.as_str(), &Default::default())
        .await?;

    while let Some(event) = read.next().await? {
        if let Some(payload) =
            matching_payload(event.get_original_event(), options.run_id.as_str())?
        {
            return Ok(options.result(payload, Some(event.get_original_event().id)));
        }
    }

    Err(missing_event(&options))
}

async fn subscribe(
    client: &Client,
    options: CompatibilityOptions,
) -> eyre::Result<CompatibilityResult> {
    let subscription_options =
        SubscribeToStreamOptions::default().start_from(StreamPosition::Start);
    let mut subscription = client
        .subscribe_to_stream(options.stream.as_str(), &subscription_options)
        .await;
    loop {
        if let SubscriptionEvent::Confirmed(_) = subscription.next_subscription_event().await? {
            break;
        }
    }
    options.signal_ready()?;

    loop {
        let event = subscription.next().await?;
        if let Some(payload) =
            matching_payload(event.get_original_event(), options.run_id.as_str())?
        {
            return Ok(options.result(payload, Some(event.get_original_event().id)));
        }
    }
}

async fn create_persistent_subscription(
    client: &Client,
    options: CompatibilityOptions,
) -> eyre::Result<CompatibilityResult> {
    let group = options.required_group()?;
    let subscription_options =
        PersistentSubscriptionOptions::default().start_from(StreamPosition::Start);
    client
        .create_persistent_subscription(options.stream.as_str(), group, &subscription_options)
        .await?;
    let payload = CompatibilityPayload {
        producer: PRODUCER.to_owned(),
        run_id: options.run_id.as_str().to_owned(),
    };

    Ok(options.result(payload, None))
}

async fn consume_persistent_subscription(
    client: &Client,
    options: CompatibilityOptions,
) -> eyre::Result<CompatibilityResult> {
    let group = options.required_group()?;
    let mut subscription = client
        .subscribe_to_persistent_subscription(options.stream.as_str(), group, &Default::default())
        .await?;

    loop {
        if let PersistentSubscriptionEvent::Confirmed(_) =
            subscription.next_subscription_event().await?
        {
            break;
        }
    }
    options.signal_ready()?;

    loop {
        match subscription.next_subscription_event().await? {
            PersistentSubscriptionEvent::EventAppeared { event, .. } => {
                subscription.ack(&event).await?;
                if let Some(payload) =
                    matching_payload(event.get_original_event(), options.run_id.as_str())?
                {
                    return Ok(options.result(payload, Some(event.get_original_event().id)));
                }
            }
            PersistentSubscriptionEvent::Confirmed(_) => {}
        }
    }
}

fn matching_payload(
    event: &RecordedEvent,
    run_id: &str,
) -> eyre::Result<Option<CompatibilityPayload>> {
    if event.event_type != EVENT_TYPE {
        return Ok(None);
    }

    let payload: CompatibilityPayload = event.as_json()?;
    if payload.run_id != run_id {
        return Ok(None);
    }
    if payload.producer.trim().is_empty() {
        return Err(eyre::eyre!("compatibility event producer is required"));
    }

    Ok(Some(payload))
}

fn missing_event(options: &CompatibilityOptions) -> eyre::Report {
    eyre::eyre!(
        "stream {} does not contain a {EVENT_TYPE} event for run {}",
        options.stream.as_str(),
        options.run_id.as_str()
    )
}

impl CompatibilityOptions {
    fn load() -> eyre::Result<Self> {
        Self::load_with(|name| std::env::var(name).ok())
    }

    fn load_with(read_environment: impl Fn(&str) -> Option<String>) -> eyre::Result<Self> {
        let command = CompatibilityCommand::parse(&required_value(
            &read_environment,
            "TROGON_EVENTSTORE_COMMAND",
        )?)?;
        let group = command
            .requires_group()
            .then(|| required_value(&read_environment, "TROGON_EVENTSTORE_GROUP").map(GroupName))
            .transpose()?;
        let ready_file = optional_value(&read_environment, "TROGON_EVENTSTORE_READY_FILE")
            .map(|value| ReadyFile(PathBuf::from(value)));

        Ok(Self {
            command,
            uri: required_value(&read_environment, "TROGON_EVENTSTORE_URI")?,
            run_id: RunId(required_value(
                &read_environment,
                "TROGON_EVENTSTORE_RUN_ID",
            )?),
            stream: StreamName(required_value(
                &read_environment,
                "TROGON_EVENTSTORE_STREAM",
            )?),
            group,
            ready_file,
            otlp_endpoint: required_value(&read_environment, "OTEL_EXPORTER_OTLP_ENDPOINT")?,
        })
    }

    fn required_group(&self) -> eyre::Result<&str> {
        self.group.as_ref().map(GroupName::as_str).ok_or_else(|| {
            eyre::eyre!(
                "TROGON_EVENTSTORE_GROUP is required for {}",
                self.command.as_str()
            )
        })
    }

    fn signal_ready(&self) -> eyre::Result<()> {
        if let Some(ready_file) = &self.ready_file {
            ready_file.signal()?;
        }

        Ok(())
    }

    fn result(
        self,
        payload: CompatibilityPayload,
        event_id: Option<uuid::Uuid>,
    ) -> CompatibilityResult {
        let CompatibilityOptions {
            command,
            stream,
            group,
            ..
        } = self;
        CompatibilityResult {
            command: command.as_str().to_owned(),
            stream: stream.0,
            group: group.map(|group| group.0),
            producer: payload.producer,
            run_id: payload.run_id,
            event_id,
        }
    }
}

impl CompatibilityCommand {
    fn parse(value: &str) -> eyre::Result<Self> {
        match value {
            "write" => Ok(Self::Write),
            "batch-write" => Ok(Self::BatchWrite),
            "read" => Ok(Self::Read),
            "subscribe" => Ok(Self::Subscribe),
            "create-persistent-subscription" => Ok(Self::CreatePersistentSubscription),
            "consume-persistent-subscription" => Ok(Self::ConsumePersistentSubscription),
            _ => Err(eyre::eyre!("unsupported compatibility command {value}")),
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Write => "write",
            Self::BatchWrite => "batch-write",
            Self::Read => "read",
            Self::Subscribe => "subscribe",
            Self::CreatePersistentSubscription => "create-persistent-subscription",
            Self::ConsumePersistentSubscription => "consume-persistent-subscription",
        }
    }

    const fn requires_group(self) -> bool {
        matches!(
            self,
            Self::CreatePersistentSubscription | Self::ConsumePersistentSubscription
        )
    }
}

impl StreamName {
    fn as_str(&self) -> &str {
        &self.0
    }
}

impl GroupName {
    fn as_str(&self) -> &str {
        &self.0
    }
}

impl RunId {
    fn as_str(&self) -> &str {
        &self.0
    }
}

impl ReadyFile {
    fn signal(&self) -> eyre::Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&self.0)?;
        file.write_all(b"ready\n")?;
        Ok(())
    }

    #[cfg(test)]
    fn path(&self) -> &Path {
        &self.0
    }
}

fn configure_telemetry(endpoint: String) -> eyre::Result<SdkTracerProvider> {
    global::set_text_map_propagator(TextMapCompositePropagator::new(vec![
        Box::new(TraceContextPropagator::new()),
        Box::new(BaggagePropagator::new()),
    ]));
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()?;
    let resource = Resource::builder().with_service_name(SERVICE_NAME).build();
    let provider = SdkTracerProvider::builder()
        .with_resource(resource)
        .with_batch_exporter(exporter)
        .build();
    global::set_tracer_provider(provider.clone());

    Ok(provider)
}

fn required_value(
    read_environment: &impl Fn(&str) -> Option<String>,
    name: &str,
) -> eyre::Result<String> {
    optional_value(read_environment, name)
        .ok_or_else(|| eyre::eyre!("required environment variable {name} is not set or is empty"))
}

fn optional_value(
    read_environment: &impl Fn(&str) -> Option<String>,
    name: &str,
) -> Option<String> {
    read_environment(name).and_then(|value| {
        let value = value.trim();
        if value.is_empty() {
            None
        } else {
            Some(value.to_owned())
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{CompatibilityCommand, CompatibilityOptions};
    use std::collections::HashMap;
    use std::path::Path;

    #[test]
    fn options_load_an_optional_ready_file() {
        let mut environment = base_environment("subscribe");
        environment.insert(
            "TROGON_EVENTSTORE_READY_FILE",
            " /tmp/trogon-ready ".to_owned(),
        );

        let options = CompatibilityOptions::load_with(|name| environment.get(name).cloned())
            .expect("compatibility options");

        assert_eq!(options.command, CompatibilityCommand::Subscribe);
        assert_eq!(
            options.ready_file.as_ref().map(|file| file.path()),
            Some(Path::new("/tmp/trogon-ready"))
        );
        assert!(options.group.is_none());
    }

    #[test]
    fn persistent_commands_require_a_group() {
        let environment = base_environment("consume-persistent-subscription");

        let error = CompatibilityOptions::load_with(|name| environment.get(name).cloned())
            .err()
            .expect("missing group error");

        assert!(error.to_string().contains("TROGON_EVENTSTORE_GROUP"));
    }

    fn base_environment(command: &str) -> HashMap<&'static str, String> {
        HashMap::from([
            ("TROGON_EVENTSTORE_COMMAND", command.to_owned()),
            (
                "TROGON_EVENTSTORE_URI",
                "esdb://localhost:2113?tls=false".to_owned(),
            ),
            ("TROGON_EVENTSTORE_RUN_ID", "run-1".to_owned()),
            ("TROGON_EVENTSTORE_STREAM", "compatibility-1".to_owned()),
            (
                "OTEL_EXPORTER_OTLP_ENDPOINT",
                "http://localhost:4317".to_owned(),
            ),
        ])
    }
}
