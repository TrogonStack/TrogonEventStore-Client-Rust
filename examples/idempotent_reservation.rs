use serde::{Deserialize, Serialize};
use std::{collections::HashMap, error::Error, fmt};
use trogon_eventstore::{
    AppendToStreamOptions, Client, ClientSettings, Error as ClientError, EventData,
    ReadStreamOptions, StreamPosition, StreamState, WriteResult,
};
use uuid::Uuid;

const DEFAULT_CONNECTION_STRING: &str = "esdb://localhost:2113?tls=false";
const CONNECTION_STRING_ENV: &str = "TROGON_EVENTSTORE_CONNECTION_STRING";
const INVENTORY_CREATED_EVENT_TYPE: &str = "inventory-created";
const INVENTORY_RESERVED_EVENT_TYPE: &str = "inventory-reserved";
const INVENTORY_RELEASED_EVENT_TYPE: &str = "inventory-released";
const OPERATION_REQUESTED_EVENT_TYPE: &str = "inventory-operation-requested";
const OPERATION_PREPARED_EVENT_TYPE: &str = "inventory-operation-prepared";
const OPERATION_REJECTED_EVENT_TYPE: &str = "inventory-operation-rejected";
const OPERATION_COMPLETED_EVENT_TYPE: &str = "inventory-operation-completed";
const OPERATION_CONFLICTED_EVENT_TYPE: &str = "inventory-operation-conflicted";
const OPERATION_SCHEMA_VERSION: u8 = 1;
const MAX_DELIVERY_ATTEMPTS: usize = 3;
const OPERATION_ID_NAMESPACE: Uuid =
    Uuid::from_u128(0x7472_6f67_6f6e_4576_656e_7453_746f_7265);

type ExampleResult<T> = Result<T, Box<dyn Error>>;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
struct OperationId(Uuid);

impl OperationId {
    fn from_job_key(job_key: &str) -> Self {
        Self(Uuid::new_v5(&OPERATION_ID_NAMESPACE, job_key.as_bytes()))
    }

    fn stream_name(self) -> String {
        format!("inventory-operation-{}", self.0)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
struct ReservationId(Uuid);

impl ReservationId {
    fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct InventoryCreated {
    sku: String,
    available: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct InventoryReserved {
    operation_id: OperationId,
    reservation_id: ReservationId,
    client: String,
    sku: String,
    quantity: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct InventoryReleased {
    operation_id: OperationId,
    reservation_id: ReservationId,
    sku: String,
    quantity: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "event", rename_all = "snake_case")]
enum InventoryOperation {
    Reserve(InventoryReserved),
    Release(InventoryReleased),
}

impl InventoryOperation {
    fn reserve(
        operation_id: OperationId,
        reservation_id: ReservationId,
        client: &str,
        sku: &str,
        quantity: u32,
    ) -> Self {
        Self::Reserve(InventoryReserved {
            operation_id,
            reservation_id,
            client: client.to_owned(),
            sku: sku.to_owned(),
            quantity,
        })
    }

    fn release(
        operation_id: OperationId,
        reservation_id: ReservationId,
        sku: &str,
        quantity: u32,
    ) -> Self {
        Self::Release(InventoryReleased {
            operation_id,
            reservation_id,
            sku: sku.to_owned(),
            quantity,
        })
    }

    fn operation_id(&self) -> OperationId {
        match self {
            Self::Reserve(event) => event.operation_id,
            Self::Release(event) => event.operation_id,
        }
    }

    fn reservation_id(&self) -> ReservationId {
        match self {
            Self::Reserve(event) => event.reservation_id,
            Self::Release(event) => event.reservation_id,
        }
    }

    fn event_type(&self) -> &'static str {
        match self {
            Self::Reserve(_) => INVENTORY_RESERVED_EVENT_TYPE,
            Self::Release(_) => INVENTORY_RELEASED_EVENT_TYPE,
        }
    }

    fn payload(&self) -> serde_json::Result<Vec<u8>> {
        match self {
            Self::Reserve(event) => serde_json::to_vec(event),
            Self::Release(event) => serde_json::to_vec(event),
        }
    }

    fn event_data(&self, event_id: Uuid) -> ExampleResult<EventData> {
        let event = match self {
            Self::Reserve(event) => EventData::json(INVENTORY_RESERVED_EVENT_TYPE, event)?,
            Self::Release(event) => EventData::json(INVENTORY_RELEASED_EVENT_TYPE, event)?,
        };
        Ok(event.id(event_id))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct OperationRequested {
    schema_version: u8,
    inventory_stream: String,
    inventory_event_id: Uuid,
    inventory_event_type: String,
    inventory_event_payload: Vec<u8>,
    decision_event_id: Uuid,
    terminal_event_id: Uuid,
    operation: InventoryOperation,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct OperationPrepared {
    operation_id: OperationId,
    expected_inventory_revision: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
enum InventoryRejection {
    WrongSku,
    InsufficientInventory { available: u32, requested: u32 },
    ReservationNotActive,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct OperationRejected {
    operation_id: OperationId,
    reason: InventoryRejection,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct CommittedInventoryWrite {
    inventory_revision: u64,
    commit_position: u64,
    prepare_position: u64,
}

impl From<WriteResult> for CommittedInventoryWrite {
    fn from(write: WriteResult) -> Self {
        Self {
            inventory_revision: write.next_expected_version,
            commit_position: write.position.commit,
            prepare_position: write.position.prepare,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct OperationCompleted {
    operation_id: OperationId,
    inventory_write: CommittedInventoryWrite,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct InventoryConflict {
    expected_inventory_revision: u64,
    occupied_inventory_revision: u64,
    occupying_event_id: Uuid,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct OperationConflicted {
    operation_id: OperationId,
    conflict: InventoryConflict,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum OperationState {
    Requested(OperationRequested),
    Prepared {
        requested: OperationRequested,
        prepared: OperationPrepared,
    },
    Rejected {
        requested: OperationRequested,
        rejected: OperationRejected,
    },
    Completed {
        requested: OperationRequested,
        prepared: OperationPrepared,
        completed: OperationCompleted,
    },
    Conflicted {
        requested: OperationRequested,
        prepared: OperationPrepared,
        conflicted: OperationConflicted,
    },
}

impl OperationState {
    fn requested(&self) -> &OperationRequested {
        match self {
            Self::Requested(requested)
            | Self::Prepared { requested, .. }
            | Self::Rejected { requested, .. }
            | Self::Completed { requested, .. }
            | Self::Conflicted { requested, .. } => requested,
        }
    }

    fn revision(&self) -> u64 {
        match self {
            Self::Requested(_) => 0,
            Self::Prepared { .. } | Self::Rejected { .. } => 1,
            Self::Completed { .. } | Self::Conflicted { .. } => 2,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum OperationOutcome {
    Completed(CommittedInventoryWrite),
    Rejected(InventoryRejection),
    Conflicted(InventoryConflict),
}

#[derive(Debug)]
struct InventoryState {
    sku: String,
    available: u32,
    reservations: HashMap<ReservationId, u32>,
}

#[derive(Debug)]
struct InventoryView {
    state: InventoryState,
    revision: u64,
    event_count: usize,
}

#[derive(Debug)]
struct OperationIdConflict(OperationId);

impl fmt::Display for OperationIdConflict {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "operation ID {} was reused with different content",
            self.0.0
        )
    }
}

impl Error for OperationIdConflict {}

#[derive(Debug)]
struct InvalidOperationStream(String);

impl fmt::Display for InvalidOperationStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for InvalidOperationStream {}

enum StoredDecision {
    Prepared(Uuid, OperationPrepared),
    Rejected(Uuid, OperationRejected),
}

enum StoredTerminal {
    Completed(Uuid, OperationCompleted),
    Conflicted(Uuid, OperationConflicted),
}

enum InventoryAppendResult {
    Committed(CommittedInventoryWrite),
    Conflicted(InventoryConflict),
}

async fn read_inventory(client: &Client, stream: &str) -> ExampleResult<InventoryView> {
    let mut events = client
        .read_stream(stream, &ReadStreamOptions::default())
        .await?;
    let mut sku = None;
    let mut available = 0;
    let mut reservations = HashMap::new();
    let mut revision = None;
    let mut event_count = 0;

    while let Some(event) = events.next().await? {
        let event = event.get_original_event();
        revision = Some(event.revision);
        event_count += 1;

        match event.event_type.as_str() {
            INVENTORY_CREATED_EVENT_TYPE => {
                let created = event.as_json::<InventoryCreated>()?;
                sku = Some(created.sku);
                available = created.available;
            }
            INVENTORY_RESERVED_EVENT_TYPE => {
                let reserved = event.as_json::<InventoryReserved>()?;
                assert_eq!(event.id, reserved.operation_id.0);
                assert!(available >= reserved.quantity);
                available -= reserved.quantity;
                assert!(
                    reservations
                        .insert(reserved.reservation_id, reserved.quantity)
                        .is_none()
                );
            }
            INVENTORY_RELEASED_EVENT_TYPE => {
                let released = event.as_json::<InventoryReleased>()?;
                assert_eq!(event.id, released.operation_id.0);
                let quantity = reservations
                    .remove(&released.reservation_id)
                    .expect("the released reservation to be active");
                assert_eq!(quantity, released.quantity);
                available += released.quantity;
            }
            event_type => panic!("unexpected inventory event type: {event_type}"),
        }
    }

    Ok(InventoryView {
        state: InventoryState {
            sku: sku.expect("the inventory stream to contain its creation event"),
            available,
            reservations,
        },
        revision: revision.expect("the inventory stream to exist"),
        event_count,
    })
}

fn invalid_operation_stream(stream: &str, message: &str) -> Box<dyn Error> {
    Box::new(InvalidOperationStream(format!("{stream}: {message}")))
}

async fn read_operation(
    client: &Client,
    operation_id: OperationId,
) -> ExampleResult<Option<OperationState>> {
    let stream = operation_id.stream_name();
    let options = ReadStreamOptions::default().max_count(4);
    let mut events = match client.read_stream(stream.as_str(), &options).await {
        Ok(events) => events,
        Err(ClientError::ResourceNotFound) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut requested = None;
    let mut decision = None;
    let mut terminal = None;

    loop {
        let next = match events.next().await {
            Ok(next) => next,
            Err(ClientError::ResourceNotFound) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let Some(resolved) = next else {
            break;
        };
        let event = resolved.get_original_event();

        match event.event_type.as_str() {
            OPERATION_REQUESTED_EVENT_TYPE if event.revision == 0 && requested.is_none() => {
                requested = Some((event.id, event.as_json::<OperationRequested>()?));
            }
            OPERATION_PREPARED_EVENT_TYPE if event.revision == 1 && decision.is_none() => {
                decision = Some(StoredDecision::Prepared(
                    event.id,
                    event.as_json::<OperationPrepared>()?,
                ));
            }
            OPERATION_REJECTED_EVENT_TYPE if event.revision == 1 && decision.is_none() => {
                decision = Some(StoredDecision::Rejected(
                    event.id,
                    event.as_json::<OperationRejected>()?,
                ));
            }
            OPERATION_COMPLETED_EVENT_TYPE if event.revision == 2 && terminal.is_none() => {
                terminal = Some(StoredTerminal::Completed(
                    event.id,
                    event.as_json::<OperationCompleted>()?,
                ));
            }
            OPERATION_CONFLICTED_EVENT_TYPE if event.revision == 2 && terminal.is_none() => {
                terminal = Some(StoredTerminal::Conflicted(
                    event.id,
                    event.as_json::<OperationConflicted>()?,
                ));
            }
            _ => {
                return Err(invalid_operation_stream(
                    &stream,
                    &format!("invalid event at revision {}", event.revision),
                ));
            }
        }
    }

    let Some((request_event_id, requested)) = requested else {
        return match (decision, terminal) {
            (None, None) => Ok(None),
            _ => Err(invalid_operation_stream(
                &stream,
                "contains progress without a request",
            )),
        };
    };

    if request_event_id != operation_id.0
        || requested.operation.operation_id() != operation_id
        || requested.inventory_event_id != operation_id.0
        || requested.schema_version != OPERATION_SCHEMA_VERSION
        || requested.inventory_event_type != requested.operation.event_type()
        || requested.inventory_event_payload != requested.operation.payload()?
    {
        return Err(invalid_operation_stream(
            &stream,
            "contains an inconsistent request",
        ));
    }

    match (decision, terminal) {
        (None, None) => Ok(Some(OperationState::Requested(requested))),
        (Some(StoredDecision::Prepared(event_id, prepared)), None)
            if event_id == requested.decision_event_id
                && prepared.operation_id == operation_id =>
        {
            Ok(Some(OperationState::Prepared {
                requested,
                prepared,
            }))
        }
        (Some(StoredDecision::Rejected(event_id, rejected)), None)
            if event_id == requested.decision_event_id
                && rejected.operation_id == operation_id =>
        {
            Ok(Some(OperationState::Rejected {
                requested,
                rejected,
            }))
        }
        (
            Some(StoredDecision::Prepared(decision_event_id, prepared)),
            Some(StoredTerminal::Completed(terminal_event_id, completed)),
        ) if decision_event_id == requested.decision_event_id
            && terminal_event_id == requested.terminal_event_id
            && prepared.operation_id == operation_id
            && completed.operation_id == operation_id =>
        {
            Ok(Some(OperationState::Completed {
                requested,
                prepared,
                completed,
            }))
        }
        (
            Some(StoredDecision::Prepared(decision_event_id, prepared)),
            Some(StoredTerminal::Conflicted(terminal_event_id, conflicted)),
        ) if decision_event_id == requested.decision_event_id
            && terminal_event_id == requested.terminal_event_id
            && prepared.operation_id == operation_id
            && conflicted.operation_id == operation_id =>
        {
            Ok(Some(OperationState::Conflicted {
                requested,
                prepared,
                conflicted,
            }))
        }
        _ => Err(invalid_operation_stream(
            &stream,
            "contains an invalid state transition",
        )),
    }
}

fn validate_request(
    state: &OperationState,
    inventory_stream: &str,
    operation: &InventoryOperation,
) -> ExampleResult<()> {
    let requested = state.requested();
    if requested.inventory_stream != inventory_stream
        || requested.operation != *operation
        || requested.inventory_event_type != operation.event_type()
        || requested.inventory_event_payload != operation.payload()?
    {
        return Err(Box::new(OperationIdConflict(operation.operation_id())));
    }
    Ok(())
}

async fn append_operation_event(
    client: &Client,
    operation_id: OperationId,
    expected_revision: StreamState,
    minimum_stored_revision: u64,
    event: EventData,
) -> ExampleResult<OperationState> {
    let stream = operation_id.stream_name();
    let options = AppendToStreamOptions::default().stream_state(expected_revision);
    let append_error = client
        .append_to_stream(stream.as_str(), &options, event)
        .await
        .err();
    let stored = read_operation(client, operation_id).await?;

    match stored {
        Some(state) if state.revision() >= minimum_stored_revision => Ok(state),
        _ => match append_error {
            Some(error) => Err(error.into()),
            None => Err(invalid_operation_stream(
                &stream,
                "did not retain the appended transition",
            )),
        },
    }
}

async fn claim_operation(
    client: &Client,
    inventory_stream: &str,
    operation: &InventoryOperation,
) -> ExampleResult<OperationState> {
    let operation_id = operation.operation_id();
    if let Some(state) = read_operation(client, operation_id).await? {
        validate_request(&state, inventory_stream, operation)?;
        return Ok(state);
    }

    let requested = OperationRequested {
        schema_version: OPERATION_SCHEMA_VERSION,
        inventory_stream: inventory_stream.to_owned(),
        inventory_event_id: operation_id.0,
        inventory_event_type: operation.event_type().to_owned(),
        inventory_event_payload: operation.payload()?,
        decision_event_id: Uuid::new_v4(),
        terminal_event_id: Uuid::new_v4(),
        operation: operation.clone(),
    };
    let event = EventData::json(OPERATION_REQUESTED_EVENT_TYPE, &requested)?.id(operation_id.0);
    let stored = append_operation_event(
        client,
        operation_id,
        StreamState::NoStream,
        0,
        event,
    )
    .await?;

    // The stored request wins because idempotent appends do not compare payloads.
    validate_request(&stored, inventory_stream, operation)?;
    Ok(stored)
}

fn reject_inventory_command(
    inventory: &InventoryState,
    operation: &InventoryOperation,
) -> Option<InventoryRejection> {
    match operation {
        InventoryOperation::Reserve(event) if event.sku != inventory.sku => {
            Some(InventoryRejection::WrongSku)
        }
        InventoryOperation::Reserve(event) if inventory.available < event.quantity => {
            Some(InventoryRejection::InsufficientInventory {
                available: inventory.available,
                requested: event.quantity,
            })
        }
        InventoryOperation::Release(event) if event.sku != inventory.sku => {
            Some(InventoryRejection::WrongSku)
        }
        InventoryOperation::Release(event)
            if inventory.reservations.get(&event.reservation_id) != Some(&event.quantity) =>
        {
            Some(InventoryRejection::ReservationNotActive)
        }
        _ => None,
    }
}

async fn decide_requested(
    client: &Client,
    requested: &OperationRequested,
) -> ExampleResult<OperationState> {
    let inventory = read_inventory(client, &requested.inventory_stream).await?;
    let operation_id = requested.operation.operation_id();
    let event = match reject_inventory_command(&inventory.state, &requested.operation) {
        Some(reason) => {
            let rejected = OperationRejected {
                operation_id,
                reason,
            };
            EventData::json(OPERATION_REJECTED_EVENT_TYPE, &rejected)?
        }
        None => {
            let prepared = OperationPrepared {
                operation_id,
                expected_inventory_revision: inventory.revision,
            };
            EventData::json(OPERATION_PREPARED_EVENT_TYPE, &prepared)?
        }
    };

    append_operation_event(
        client,
        operation_id,
        StreamState::StreamRevision(0),
        1,
        event.id(requested.decision_event_id),
    )
    .await
}

async fn prepare_operation(
    client: &Client,
    inventory_stream: &str,
    operation: &InventoryOperation,
) -> ExampleResult<OperationState> {
    let state = claim_operation(client, inventory_stream, operation).await?;
    match state {
        OperationState::Requested(requested) => decide_requested(client, &requested).await,
        state => Ok(state),
    }
}

fn stored_inventory_event_matches(
    stored: &trogon_eventstore::RecordedEvent,
    requested: &OperationRequested,
) -> bool {
    stored.id == requested.inventory_event_id
        && stored.event_type == requested.inventory_event_type
        && stored.data.as_ref() == requested.inventory_event_payload.as_slice()
}

async fn append_inventory(
    client: &Client,
    requested: &OperationRequested,
    prepared: &OperationPrepared,
) -> ExampleResult<InventoryAppendResult> {
    let options = AppendToStreamOptions::default().stream_state(StreamState::StreamRevision(
        prepared.expected_inventory_revision,
    ));
    let event = requested
        .operation
        .event_data(requested.inventory_event_id)?;
    let append_error = match client
        .append_to_stream(requested.inventory_stream.as_str(), &options, event)
        .await
    {
        Ok(write) => {
            return Ok(InventoryAppendResult::Committed(write.into()));
        }
        Err(error) => error,
    };

    let intended_revision = prepared.expected_inventory_revision + 1;
    let options = ReadStreamOptions::default()
        .position(StreamPosition::Position(intended_revision))
        .max_count(1);
    let mut events = client
        .read_stream(requested.inventory_stream.as_str(), &options)
        .await?;
    let Some(resolved) = events.next().await? else {
        return Err(append_error.into());
    };
    let stored = resolved.get_original_event();

    if stored.revision == intended_revision && stored_inventory_event_matches(stored, requested) {
        return Ok(InventoryAppendResult::Committed(
            CommittedInventoryWrite {
                inventory_revision: stored.revision,
                commit_position: stored.position.commit,
                prepare_position: stored.position.prepare,
            },
        ));
    }

    Ok(InventoryAppendResult::Conflicted(InventoryConflict {
        expected_inventory_revision: prepared.expected_inventory_revision,
        occupied_inventory_revision: stored.revision,
        occupying_event_id: stored.id,
    }))
}

async fn finish_prepared(
    client: &Client,
    requested: &OperationRequested,
    prepared: &OperationPrepared,
) -> ExampleResult<OperationState> {
    let operation_id = requested.operation.operation_id();
    let event = match append_inventory(client, requested, prepared).await? {
        InventoryAppendResult::Committed(inventory_write) => {
            let completed = OperationCompleted {
                operation_id,
                inventory_write,
            };
            EventData::json(OPERATION_COMPLETED_EVENT_TYPE, &completed)?
        }
        InventoryAppendResult::Conflicted(conflict) => {
            let conflicted = OperationConflicted {
                operation_id,
                conflict,
            };
            EventData::json(OPERATION_CONFLICTED_EVENT_TYPE, &conflicted)?
        }
    };

    append_operation_event(
        client,
        operation_id,
        StreamState::StreamRevision(1),
        2,
        event.id(requested.terminal_event_id),
    )
    .await
}

fn terminal_outcome(state: OperationState) -> ExampleResult<OperationOutcome> {
    match state {
        OperationState::Rejected { rejected, .. } => {
            Ok(OperationOutcome::Rejected(rejected.reason))
        }
        OperationState::Completed { completed, .. } => {
            Ok(OperationOutcome::Completed(completed.inventory_write))
        }
        OperationState::Conflicted { conflicted, .. } => {
            Ok(OperationOutcome::Conflicted(conflicted.conflict))
        }
        state => Err(invalid_operation_stream(
            &state.requested().operation.operation_id().stream_name(),
            "has no terminal outcome",
        )),
    }
}

async fn execute_operation(
    client: &Client,
    inventory_stream: &str,
    operation: &InventoryOperation,
) -> ExampleResult<OperationOutcome> {
    let mut state = claim_operation(client, inventory_stream, operation).await?;

    loop {
        state = match state {
            OperationState::Requested(requested) => decide_requested(client, &requested).await?,
            OperationState::Prepared {
                requested,
                prepared,
            } => finish_prepared(client, &requested, &prepared).await?,
            terminal => return terminal_outcome(terminal),
        };
    }
}

async fn deliver_with_retries(
    client: &Client,
    inventory_stream: &str,
    operation: &InventoryOperation,
) -> ExampleResult<OperationOutcome> {
    for attempt in 1..=MAX_DELIVERY_ATTEMPTS {
        match execute_operation(client, inventory_stream, operation).await {
            Ok(outcome) => return Ok(outcome),
            Err(error)
                if attempt < MAX_DELIVERY_ATTEMPTS
                    && error.downcast_ref::<ClientError>().is_some() =>
            {
                tokio::task::yield_now().await;
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("the delivery loop returns on its final attempt")
}

async fn append_initial_inventory(
    client: &Client,
    stream: &str,
    inventory: &InventoryCreated,
    event_id: Uuid,
) -> ExampleResult<WriteResult> {
    let options = AppendToStreamOptions::default().stream_state(StreamState::NoStream);
    for attempt in 1..=MAX_DELIVERY_ATTEMPTS {
        let event = EventData::json(INVENTORY_CREATED_EVENT_TYPE, inventory)?.id(event_id);
        match client
            .append_to_stream(stream, &options, event)
            .await
        {
            Ok(write) => return Ok(write),
            Err(error) => {
                let read_options = ReadStreamOptions::default()
                    .position(StreamPosition::Position(0))
                    .max_count(1);
                if let Ok(mut events) = client.read_stream(stream, &read_options).await
                    && let Ok(Some(resolved)) = events.next().await
                {
                    let stored = resolved.get_original_event();
                    if stored.revision == 0
                        && stored.id == event_id
                        && stored.event_type == INVENTORY_CREATED_EVENT_TYPE
                        && stored.as_json::<InventoryCreated>()? == *inventory
                    {
                        return Ok(WriteResult {
                            next_expected_version: stored.revision,
                            position: stored.position,
                        });
                    }
                }

                if attempt == MAX_DELIVERY_ATTEMPTS {
                    return Err(error.into());
                }
                tokio::task::yield_now().await;
            }
        }
    }
    unreachable!("the append loop returns on its final attempt")
}

fn completed_write(outcome: &OperationOutcome) -> CommittedInventoryWrite {
    match outcome {
        OperationOutcome::Completed(write) => *write,
        outcome => panic!("expected a completed operation, got {outcome:?}"),
    }
}

async fn print_timeline(client: &Client, stream: &str) -> ExampleResult<()> {
    let mut events = client
        .read_stream(stream, &ReadStreamOptions::default())
        .await?;
    println!("{stream}");
    while let Some(event) = events.next().await? {
        let event = event.get_original_event();
        println!(
            "  revision {}: {} [event ID {}]",
            event.revision, event.event_type, event.id
        );
    }
    Ok(())
}

#[tokio::main]
async fn main() -> ExampleResult<()> {
    let connection_string = std::env::var(CONNECTION_STRING_ENV)
        .unwrap_or_else(|_| DEFAULT_CONNECTION_STRING.to_owned());
    let settings = connection_string.parse::<ClientSettings>()?;
    let first_client = Client::new(settings.clone())?;
    let second_client = Client::new(settings)?;
    let run_id = Uuid::new_v4();

    let sku = format!("sku-{run_id}");
    let inventory_stream = format!("inventory-{sku}");
    println!("1. Create {inventory_stream} with one available unit");
    let initial_inventory = InventoryCreated {
        sku: sku.clone(),
        available: 1,
    };
    let created = append_initial_inventory(
        &first_client,
        &inventory_stream,
        &initial_inventory,
        Uuid::new_v4(),
    )
    .await?;
    assert_eq!(created.next_expected_version, 0);

    let reserve_job_key = format!("{run_id}:reserve");
    let reserve_id = OperationId::from_job_key(&reserve_job_key);
    let reservation_id = ReservationId::new();
    let reserve = InventoryOperation::reserve(
        reserve_id,
        reservation_id,
        "checkout-worker",
        &sku,
        1,
    );
    let reconstructed_reserve = InventoryOperation::reserve(
        OperationId::from_job_key(&reserve_job_key),
        reservation_id,
        "checkout-worker",
        &sku,
        1,
    );
    assert_eq!(reserve, reconstructed_reserve);

    println!(
        "2. Claim job {reserve_job_key} as {} before reading inventory",
        reserve_id.stream_name()
    );
    assert!(matches!(
        claim_operation(&first_client, &inventory_stream, &reserve).await?,
        OperationState::Requested(_)
    ));
    assert_eq!(
        read_inventory(&second_client, &inventory_stream)
            .await?
            .event_count,
        1
    );

    println!("3. Recover the crashed delivery concurrently on two workers");
    let (first_reserve_result, second_reserve_result) = tokio::join!(
        deliver_with_retries(&first_client, &inventory_stream, &reserve),
        deliver_with_retries(&second_client, &inventory_stream, &reconstructed_reserve),
    );
    let first_reserve_result = first_reserve_result?;
    let second_reserve_result = second_reserve_result?;
    assert_eq!(first_reserve_result, second_reserve_result);
    assert_eq!(
        completed_write(&first_reserve_result).inventory_revision,
        1
    );

    let after_reserve = read_inventory(&first_client, &inventory_stream).await?;
    assert_eq!(after_reserve.event_count, 2);
    assert_eq!(after_reserve.state.available, 0);
    assert_eq!(after_reserve.state.reservations, [(reservation_id, 1)].into());

    let delayed_reserve_result =
        deliver_with_retries(&second_client, &inventory_stream, &reconstructed_reserve).await?;
    assert_eq!(delayed_reserve_result, first_reserve_result);
    assert_eq!(
        read_inventory(&first_client, &inventory_stream)
            .await?
            .event_count,
        2
    );

    println!("4. Reject reuse of the same operation ID with different content");
    let conflicting_reserve =
        InventoryOperation::reserve(reserve_id, reservation_id, "checkout-worker", &sku, 2);
    let conflict = deliver_with_retries(&second_client, &inventory_stream, &conflicting_reserve)
        .await
        .expect_err("an operation ID cannot be reused for another command");
    assert!(conflict.downcast_ref::<OperationIdConflict>().is_some());

    let release_job_key = format!("{run_id}:release");
    let release_id = OperationId::from_job_key(&release_job_key);
    let release = InventoryOperation::release(release_id, reservation_id, &sku, 1);
    println!("5. Prepare release operation {}", release_id.stream_name());
    let (release_requested, release_prepared) =
        match prepare_operation(&first_client, &inventory_stream, &release).await? {
            OperationState::Prepared {
                requested,
                prepared,
            } => (requested, prepared),
            state => panic!("expected a prepared release, got {state:?}"),
        };

    println!("6. Commit the release, then crash before recording completion");
    let release_write_before_crash =
        match append_inventory(&first_client, &release_requested, &release_prepared).await? {
            InventoryAppendResult::Committed(write) => write,
            InventoryAppendResult::Conflicted(conflict) => {
                panic!("the release unexpectedly conflicted: {conflict:?}")
            }
        };
    assert_eq!(release_write_before_crash.inventory_revision, 2);
    assert!(matches!(
        read_operation(&second_client, release_id).await?,
        Some(OperationState::Prepared { .. })
    ));

    println!("7. Recover the release concurrently without storing a duplicate event");
    let reconstructed_release = InventoryOperation::release(
        OperationId::from_job_key(&release_job_key),
        reservation_id,
        &sku,
        1,
    );
    let (first_release_result, second_release_result) = tokio::join!(
        deliver_with_retries(&first_client, &inventory_stream, &release),
        deliver_with_retries(&second_client, &inventory_stream, &reconstructed_release),
    );
    assert_eq!(
        completed_write(&first_release_result?),
        release_write_before_crash
    );
    assert_eq!(
        completed_write(&second_release_result?),
        release_write_before_crash
    );

    println!("8. Prepare two different reservations from the same inventory revision");
    let first_competing_id = OperationId::from_job_key(&format!("{run_id}:competing-a"));
    let second_competing_id = OperationId::from_job_key(&format!("{run_id}:competing-b"));
    let first_competing = InventoryOperation::reserve(
        first_competing_id,
        ReservationId::new(),
        "competing-worker-a",
        &sku,
        1,
    );
    let second_competing = InventoryOperation::reserve(
        second_competing_id,
        ReservationId::new(),
        "competing-worker-b",
        &sku,
        1,
    );
    for operation in [&first_competing, &second_competing] {
        match prepare_operation(&first_client, &inventory_stream, operation).await? {
            OperationState::Prepared { prepared, .. } => {
                assert_eq!(prepared.expected_inventory_revision, 2);
            }
            state => panic!("expected a prepared competitor, got {state:?}"),
        }
    }

    println!("9. Race both reservations; one commits and one becomes terminally conflicted");
    let (first_competing_result, second_competing_result) = tokio::join!(
        deliver_with_retries(&first_client, &inventory_stream, &first_competing),
        deliver_with_retries(&second_client, &inventory_stream, &second_competing),
    );
    let first_competing_result = first_competing_result?;
    let second_competing_result = second_competing_result?;
    let (winner, winner_result, loser_result) = match (
        &first_competing_result,
        &second_competing_result,
    ) {
        (OperationOutcome::Completed(_), OperationOutcome::Conflicted(_)) => (
            &first_competing,
            &first_competing_result,
            &second_competing_result,
        ),
        (OperationOutcome::Conflicted(_), OperationOutcome::Completed(_)) => (
            &second_competing,
            &second_competing_result,
            &first_competing_result,
        ),
        outcomes => panic!("expected one winner and one conflict, got {outcomes:?}"),
    };
    assert_eq!(completed_write(winner_result).inventory_revision, 3);
    let OperationOutcome::Conflicted(conflict) = loser_result else {
        unreachable!();
    };
    assert_eq!(conflict.expected_inventory_revision, 2);
    assert_eq!(conflict.occupied_inventory_revision, 3);
    assert_eq!(conflict.occupying_event_id, winner.operation_id().0);

    let winner_reservation_id = winner.reservation_id();
    let after_contention = read_inventory(&first_client, &inventory_stream).await?;
    assert_eq!(after_contention.event_count, 4);
    assert_eq!(after_contention.state.available, 0);
    assert_eq!(
        after_contention.state.reservations,
        [(winner_reservation_id, 1)].into()
    );

    let rejected_id = OperationId::from_job_key(&format!("{run_id}:sold-out"));
    let rejected = InventoryOperation::reserve(
        rejected_id,
        ReservationId::new(),
        "sold-out-worker",
        &sku,
        1,
    );
    println!("10. Persist a sold-out result for operation {}", rejected_id.0);
    let rejected_result = deliver_with_retries(&first_client, &inventory_stream, &rejected).await?;
    assert_eq!(
        rejected_result,
        OperationOutcome::Rejected(InventoryRejection::InsufficientInventory {
            available: 0,
            requested: 1,
        })
    );

    let winner_release_id = OperationId::from_job_key(&format!("{run_id}:winner-release"));
    let winner_release =
        InventoryOperation::release(winner_release_id, winner_reservation_id, &sku, 1);
    assert!(matches!(
        deliver_with_retries(&second_client, &inventory_stream, &winner_release).await?,
        OperationOutcome::Completed(_)
    ));
    let rejected_retry =
        deliver_with_retries(&second_client, &inventory_stream, &rejected).await?;
    assert_eq!(rejected_retry, rejected_result);

    let final_inventory = read_inventory(&first_client, &inventory_stream).await?;
    assert_eq!(final_inventory.revision, 4);
    assert_eq!(final_inventory.event_count, 5);
    assert_eq!(final_inventory.state.available, 1);
    assert!(final_inventory.state.reservations.is_empty());

    println!("\nIdempotent reservation timeline:\n");
    print_timeline(&first_client, &inventory_stream).await?;
    for operation_id in [
        reserve_id,
        release_id,
        first_competing_id,
        second_competing_id,
        rejected_id,
        winner_release_id,
    ] {
        print_timeline(&first_client, &operation_id.stream_name()).await?;
    }
    println!("\nA completed retry reads its three-event operation stream directly by ID.");
    println!(
        "A conflicted operation ID stays conflicted; retrying the business intent requires a new job key."
    );
    println!(
        "Inventory replay is still O(history) and O(active reservations); production aggregates need snapshots, projections, or narrower partitions."
    );
    println!("Storage remains one small stream per operation; lookup cost is bounded, storage is not.");

    Ok(())
}
