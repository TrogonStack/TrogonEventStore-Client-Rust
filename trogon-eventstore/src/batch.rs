use crate::observability::{TROGON_EVENTSTORE_BATCH_CORRELATION_ID, client_operation, operation};
use crate::{EventData, Position, StreamState};
use opentelemetry::Context;
use opentelemetry::KeyValue;
use opentelemetry::trace::TraceContextExt;
use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender},
    oneshot,
};
use tracing::{debug, error, warn};

#[derive(Debug)]
pub(crate) struct In {
    req: Req,
    sender: oneshot::Sender<crate::Result<BatchWriteResult>>,
}

#[derive(Debug)]
pub(crate) struct Req {
    pub(crate) id: uuid::Uuid,
    pub(crate) stream_name: String,
    pub(crate) events: Vec<EventData>,
    pub(crate) expected_revision: StreamState,
}

impl Req {
    fn new(stream_name: String, events: Vec<EventData>, expected_revision: StreamState) -> Self {
        Self {
            id: uuid::Uuid::new_v4(),
            stream_name,
            events,
            expected_revision,
        }
    }
}

#[derive(Debug)]
pub(crate) struct Out {
    pub(crate) correlation_id: uuid::Uuid,
    pub(crate) result: crate::Result<BatchWriteResult>,
}

#[derive(Debug)]
pub(crate) enum BatchMsg {
    In(In),
    Out(Out),
    Error(crate::Error),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BatchWriteResult {
    stream_name: String,
    current_revision: Option<u64>,
    current_position: Option<Position>,
    stream_state: Option<StreamState>,
}

impl BatchWriteResult {
    pub fn new(
        stream_name: String,
        current_revision: Option<u64>,
        current_position: Option<Position>,
        stream_state: Option<StreamState>,
    ) -> Self {
        Self {
            stream_name,
            current_position,
            current_revision,
            stream_state,
        }
    }

    pub fn stream_name(&self) -> &str {
        self.stream_name.as_str()
    }

    pub fn current_revision(&self) -> Option<u64> {
        self.current_revision
    }

    pub fn current_position(&self) -> Option<Position> {
        self.current_position
    }

    pub fn stream_state(&self) -> Option<StreamState> {
        self.stream_state
    }
}

pub struct BatchAppendClient {
    sender: UnboundedSender<BatchMsg>,
}

impl BatchAppendClient {
    pub(crate) fn new(
        sender: UnboundedSender<BatchMsg>,
        mut receiver: UnboundedReceiver<BatchMsg>,
        forward: UnboundedSender<Req>,
    ) -> Self {
        tokio::spawn(async move {
            let mut reg = std::collections::HashMap::<
                uuid::Uuid,
                oneshot::Sender<crate::Result<BatchWriteResult>>,
            >::new();
            while let Some(msg) = receiver.recv().await {
                match msg {
                    BatchMsg::In(msg) => {
                        let correlation_id = msg.req.id;
                        if forward.send(msg.req).is_ok() {
                            reg.insert(correlation_id, msg.sender);
                            debug!("Send batch-append request {}", correlation_id);

                            continue;
                        }

                        error!("Batch-append session has been closed");
                        break;
                    }

                    BatchMsg::Out(resp) => {
                        if let Some(entry) = reg.remove(&resp.correlation_id) {
                            let failed = resp.result.is_err();
                            let _ = entry.send(resp.result);

                            if failed {
                                break;
                            }

                            continue;
                        }

                        warn!(
                            "Unknown batch-append response correlation id: {}",
                            resp.correlation_id
                        );
                    }

                    BatchMsg::Error(e) => {
                        for (_, resp_sender) in reg {
                            let _ = resp_sender.send(Err(e.clone()));
                        }

                        break;
                    }
                }
            }
        });

        Self { sender }
    }

    pub async fn append_to_stream<S: AsRef<str>>(
        &self,
        stream_name: S,
        stream_state: StreamState,
        events: Vec<EventData>,
    ) -> crate::Result<BatchWriteResult> {
        let stream_name = stream_name.as_ref().to_string();
        client_operation(
            operation::BATCH_APPEND_TO_STREAM.on_collection(stream_name.clone()),
            async {
                let (sender, receiver) = oneshot::channel();
                let req = Req::new(stream_name, events, stream_state);
                let context = Context::current();
                let span = context.span();
                if span.is_recording() {
                    span.set_attribute(KeyValue::new(
                        TROGON_EVENTSTORE_BATCH_CORRELATION_ID,
                        req.id.to_string(),
                    ));
                }

                let req = In { sender, req };

                if let Err(e) = self.sender.send(BatchMsg::In(req)) {
                    error!("[sending-end] Batch-append stream is closed: {}", e);

                    let status = tonic::Status::cancelled("Batch-append stream has been closed");
                    return Err(crate::Error::ServerError(status.to_string()));
                }

                receiver.await.unwrap_or_else(|e| {
                    error!("[receiving-end] Batch-append stream is closed: {}", e);

                    let status = tonic::Status::cancelled("Batch-append stream has been closed");

                    Err(crate::Error::ServerError(status.to_string()))
                })
            },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::{BatchAppendClient, BatchMsg, BatchWriteResult, In};
    use crate::StreamState;
    use crate::observability::{
        DB_COLLECTION_NAME, DB_OPERATION_NAME, TROGON_EVENTSTORE_BATCH_CORRELATION_ID,
    };
    use opentelemetry::global;
    use opentelemetry::trace::{Status, noop::NoopTracerProvider};
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};

    #[tokio::test]
    async fn batch_append_to_stream_records_its_protocol_correlation_id() {
        let _guard = crate::observability::TEST_GLOBALS.lock().await;
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        global::set_tracer_provider(provider.clone());
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let client = BatchAppendClient { sender };

        let operation = tokio::spawn(async move {
            client
                .append_to_stream("stream", StreamState::Any, Vec::new())
                .await
        });
        let BatchMsg::In(In { req, sender }) = receiver.recv().await.expect("batch request") else {
            panic!("expected an inbound batch request");
        };
        let correlation_id = req.id.to_string();
        sender
            .send(Ok(BatchWriteResult::new(
                "stream".to_string(),
                None,
                None,
                Some(StreamState::Any),
            )))
            .expect("batch response receiver");
        operation.await.expect("batch task").expect("batch result");
        provider.force_flush().unwrap();

        let spans = exporter.get_finished_spans().unwrap();
        let span = spans
            .iter()
            .find(|span| span.name == "batch_append_to_stream stream")
            .expect("batch append client span");
        assert!(span.attributes.iter().any(|attribute| {
            attribute.key.as_str() == TROGON_EVENTSTORE_BATCH_CORRELATION_ID
                && attribute.value.to_string() == correlation_id
        }));

        global::set_tracer_provider(NoopTracerProvider::new());
    }

    #[tokio::test]
    async fn concurrent_appends_route_reversed_responses_by_correlation_id() {
        let _guard = crate::observability::TEST_GLOBALS.lock().await;
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let response_sender = sender.clone();
        let (forward, mut forwarded) = tokio::sync::mpsc::unbounded_channel();
        let client = BatchAppendClient::new(sender, receiver, forward);

        let first = client.append_to_stream("first", StreamState::Any, Vec::new());
        let second = client.append_to_stream("second", StreamState::Any, Vec::new());
        let responses = async move {
            let first = forwarded.recv().await.expect("first request");
            let second = forwarded.recv().await.expect("second request");
            assert_ne!(first.id, second.id);

            response_sender
                .send(BatchMsg::Out(super::Out {
                    correlation_id: second.id,
                    result: Ok(BatchWriteResult::new(
                        second.stream_name,
                        None,
                        None,
                        Some(StreamState::Any),
                    )),
                }))
                .expect("second response");
            response_sender
                .send(BatchMsg::Out(super::Out {
                    correlation_id: first.id,
                    result: Ok(BatchWriteResult::new(
                        first.stream_name,
                        None,
                        None,
                        Some(StreamState::Any),
                    )),
                }))
                .expect("first response");
        };

        let (first, second, ()) = tokio::join!(first, second, responses);
        assert_eq!(first.expect("first result").stream_name(), "first");
        assert_eq!(second.expect("second result").stream_name(), "second");
    }

    #[tokio::test]
    async fn batch_append_to_stream_emits_a_logical_client_span() {
        let _guard = crate::observability::TEST_GLOBALS.lock().await;
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        global::set_tracer_provider(provider.clone());
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        drop(receiver);
        let client = BatchAppendClient { sender };

        let result = client
            .append_to_stream("stream", StreamState::Any, Vec::new())
            .await;
        assert!(result.is_err());
        provider.force_flush().unwrap();

        let spans = exporter.get_finished_spans().unwrap();
        let span = spans
            .iter()
            .find(|span| span.name == "batch_append_to_stream stream")
            .expect("batch append client span");
        assert!(matches!(span.status, Status::Error { .. }));
        assert!(span.attributes.iter().any(|attribute| {
            attribute.key.as_str() == DB_OPERATION_NAME
                && attribute.value.to_string() == "batch_append_to_stream"
        }));
        assert!(span.attributes.iter().any(|attribute| {
            attribute.key.as_str() == DB_COLLECTION_NAME && attribute.value.to_string() == "stream"
        }));

        global::set_tracer_provider(NoopTracerProvider::new());
    }
}
