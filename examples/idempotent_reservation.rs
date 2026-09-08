use serde::{Deserialize, Serialize};
use std::{collections::HashMap, error::Error, fmt};
use trogon_eventstore::{
    AppendToStreamOptions, Client, ClientSettings, CurrentRevision, Error as ClientError,
    EventData, ReadStreamOptions, StreamState, WriteResult,
};
use uuid::Uuid;

const DEFAULT_CONNECTION_STRING: &str = "esdb://localhost:2113?tls=false";
const CONNECTION_STRING_ENV: &str = "TROGON_EVENTSTORE_CONNECTION_STRING";
const INVENTORY_CREATED_EVENT_TYPE: &str = "inventory-created";
const INVENTORY_RESERVED_EVENT_TYPE: &str = "inventory-reserved";
const INVENTORY_RELEASED_EVENT_TYPE: &str = "inventory-released";
const FIRST_CLIENT: &str = "checkout-a";
const SECOND_CLIENT: &str = "checkout-b";

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
struct OperationId(Uuid);

impl OperationId {
    fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
struct ReservationId(Uuid);

impl ReservationId {
    fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

#[derive(Debug, Deserialize, Serialize)]
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

#[derive(Clone, Debug, Eq, PartialEq)]
enum ProcessedOperation {
    Reserved {
        event_revision: u64,
        event: InventoryReserved,
    },
    Released {
        event_revision: u64,
        event: InventoryReleased,
    },
}

impl ProcessedOperation {
    fn event_revision(&self) -> u64 {
        match self {
            Self::Reserved { event_revision, .. } | Self::Released { event_revision, .. } => {
                *event_revision
            }
        }
    }
}

#[derive(Debug)]
struct InventoryState {
    available: u32,
    reservations: HashMap<ReservationId, u32>,
    // Folding this index from the inventory events keeps idempotency and inventory in one write.
    processed_operations: HashMap<OperationId, ProcessedOperation>,
}

#[derive(Clone)]
struct ReserveCommand(InventoryReserved);

impl ReserveCommand {
    fn new(client_name: &str, sku: &str) -> Self {
        let operation_id = OperationId::new();
        let reservation_id = ReservationId::new();
        Self(InventoryReserved {
            operation_id,
            reservation_id,
            client: client_name.to_owned(),
            sku: sku.to_owned(),
            quantity: 1,
        })
    }

    fn event(&self) -> Result<EventData, Box<dyn Error>> {
        Ok(EventData::json(INVENTORY_RESERVED_EVENT_TYPE, &self.0)?.id(self.0.operation_id.0))
    }
}

#[derive(Clone)]
struct ReleaseCommand(InventoryReleased);

impl ReleaseCommand {
    fn new(sku: &str, reservation_id: ReservationId) -> Self {
        let operation_id = OperationId::new();
        Self(InventoryReleased {
            operation_id,
            reservation_id,
            sku: sku.to_owned(),
            quantity: 1,
        })
    }

    fn event(&self) -> Result<EventData, Box<dyn Error>> {
        Ok(EventData::json(INVENTORY_RELEASED_EVENT_TYPE, &self.0)?.id(self.0.operation_id.0))
    }
}

#[derive(Debug)]
struct OperationIdConflict(OperationId);

impl fmt::Display for OperationIdConflict {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "operation ID {:?} was reused with different content", self.0)
    }
}

impl Error for OperationIdConflict {}

#[derive(Debug)]
struct InvalidInventoryCommand(&'static str);

impl fmt::Display for InvalidInventoryCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for InvalidInventoryCommand {}

#[derive(Debug)]
struct CommandResult {
    outcome: ProcessedOperation,
    appended: bool,
    write: Option<WriteResult>,
}

async fn read_inventory(
    client: &Client,
    stream: &str,
) -> Result<(InventoryState, u64), Box<dyn Error>> {
    let mut events = client
        .read_stream(stream, &ReadStreamOptions::default())
        .await?;
    let mut state = InventoryState {
        available: 0,
        reservations: HashMap::new(),
        processed_operations: HashMap::new(),
    };
    let mut revision = None;

    while let Some(event) = events.next().await? {
        let event = event.get_original_event();
        revision = Some(event.revision);

        match event.event_type.as_str() {
            INVENTORY_CREATED_EVENT_TYPE => {
                let created = event.as_json::<InventoryCreated>()?;
                state.available = created.available;
            }
            INVENTORY_RESERVED_EVENT_TYPE => {
                let reserved = event.as_json::<InventoryReserved>()?;
                assert_eq!(event.id, reserved.operation_id.0);
                assert!(state.available >= reserved.quantity);
                state.available -= reserved.quantity;
                assert!(
                    state
                        .reservations
                        .insert(reserved.reservation_id, reserved.quantity)
                        .is_none()
                );
                let operation_id = reserved.operation_id;
                assert!(
                    state
                        .processed_operations
                        .insert(
                            operation_id,
                            ProcessedOperation::Reserved {
                                event_revision: event.revision,
                                event: reserved,
                            },
                        )
                        .is_none()
                );
            }
            INVENTORY_RELEASED_EVENT_TYPE => {
                let released = event.as_json::<InventoryReleased>()?;
                assert_eq!(event.id, released.operation_id.0);
                let quantity = state
                    .reservations
                    .remove(&released.reservation_id)
                    .expect("the released reservation to be active");
                assert_eq!(quantity, released.quantity);
                state.available += released.quantity;
                let operation_id = released.operation_id;
                assert!(
                    state
                        .processed_operations
                        .insert(
                            operation_id,
                            ProcessedOperation::Released {
                                event_revision: event.revision,
                                event: released,
                            },
                        )
                        .is_none()
                );
            }
            event_type => panic!("unexpected inventory event type: {event_type}"),
        }
    }

    Ok((state, revision.expect("the inventory stream to exist")))
}

fn is_revision_conflict(error: &ClientError, expected: u64, current: u64) -> bool {
    matches!(
        error,
        ClientError::WrongExpectedVersion {
            expected: StreamState::StreamRevision(actual_expected),
            current: CurrentRevision::Current(actual_current),
        } if *actual_expected == expected && *actual_current == current
    )
}

async fn append_at(
    client: &Client,
    stream: &str,
    revision: u64,
    event: EventData,
) -> trogon_eventstore::Result<WriteResult> {
    let options =
        AppendToStreamOptions::default().stream_state(StreamState::StreamRevision(revision));
    client.append_to_stream(stream, &options, event).await
}

async fn reserve(
    client: &Client,
    stream: &str,
    command: &ReserveCommand,
) -> Result<CommandResult, Box<dyn Error>> {
    let (state, revision) = read_inventory(client, stream).await?;

    if let Some(previous) = state
        .processed_operations
        .get(&command.0.operation_id)
    {
        return match previous {
            ProcessedOperation::Reserved { event, .. } if event == &command.0 => {
                Ok(CommandResult {
                    outcome: previous.clone(),
                    appended: false,
                    write: None,
                })
            }
            _ => Err(Box::new(OperationIdConflict(command.0.operation_id))),
        };
    }

    if state.available < command.0.quantity {
        return Err(Box::new(InvalidInventoryCommand(
            "the requested inventory is not available",
        )));
    }

    let write = append_at(client, stream, revision, command.event()?).await?;
    let outcome = ProcessedOperation::Reserved {
        event_revision: write.next_expected_version,
        event: command.0.clone(),
    };

    Ok(CommandResult {
        outcome,
        appended: true,
        write: Some(write),
    })
}

async fn release(
    client: &Client,
    stream: &str,
    command: &ReleaseCommand,
) -> Result<CommandResult, Box<dyn Error>> {
    let (state, revision) = read_inventory(client, stream).await?;

    if let Some(previous) = state
        .processed_operations
        .get(&command.0.operation_id)
    {
        return match previous {
            ProcessedOperation::Released { event, .. } if event == &command.0 => {
                Ok(CommandResult {
                    outcome: previous.clone(),
                    appended: false,
                    write: None,
                })
            }
            _ => Err(Box::new(OperationIdConflict(command.0.operation_id))),
        };
    }

    if state.reservations.get(&command.0.reservation_id) != Some(&command.0.quantity) {
        return Err(Box::new(InvalidInventoryCommand(
            "the reservation is not active with the requested quantity",
        )));
    }

    let write = append_at(client, stream, revision, command.event()?).await?;
    let outcome = ProcessedOperation::Released {
        event_revision: write.next_expected_version,
        event: command.0.clone(),
    };

    Ok(CommandResult {
        outcome,
        appended: true,
        write: Some(write),
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let connection_string = std::env::var(CONNECTION_STRING_ENV)
        .unwrap_or_else(|_| DEFAULT_CONNECTION_STRING.to_owned());
    let settings = connection_string.parse::<ClientSettings>()?;
    let first_client = Client::new(settings.clone())?;
    let second_client = Client::new(settings)?;

    let sku = format!("sku-{}", Uuid::new_v4());
    let inventory_stream = format!("inventory-{sku}");
    let inventory = InventoryCreated {
        sku: sku.clone(),
        available: 1,
    };
    let created = first_client
        .append_to_stream(
            inventory_stream.as_str(),
            &AppendToStreamOptions::default().stream_state(StreamState::NoStream),
            EventData::json(INVENTORY_CREATED_EVENT_TYPE, &inventory)?.id(Uuid::new_v4()),
        )
        .await?;
    assert_eq!(created.next_expected_version, 0);

    let (first_state, first_revision) =
        read_inventory(&first_client, inventory_stream.as_str()).await?;
    let (second_state, second_revision) =
        read_inventory(&second_client, inventory_stream.as_str()).await?;
    assert_eq!(first_state.available, 1);
    assert_eq!(second_state.available, 1);
    assert_eq!(first_revision, second_revision);

    let first_attempt = ReserveCommand::new(FIRST_CLIENT, &sku);
    let second_attempt = ReserveCommand::new(SECOND_CLIENT, &sku);
    assert_ne!(first_attempt.0.operation_id.0, first_attempt.0.reservation_id.0);
    assert_ne!(second_attempt.0.operation_id.0, second_attempt.0.reservation_id.0);
    // The shared expected revision serializes decisions made from the same inventory state.
    let (first_result, second_result) = tokio::join!(
        append_at(
            &first_client,
            inventory_stream.as_str(),
            first_revision,
            first_attempt.event()?,
        ),
        append_at(
            &second_client,
            inventory_stream.as_str(),
            second_revision,
            second_attempt.event()?,
        ),
    );

    let (winner_client, winner, winner_write, loser_client, loser) =
        match (first_result, second_result) {
            (Ok(write), Err(error)) if is_revision_conflict(&error, 0, 1) => (
                &first_client,
                &first_attempt,
                write,
                &second_client,
                &second_attempt,
            ),
            (Err(error), Ok(write)) if is_revision_conflict(&error, 0, 1) => (
                &second_client,
                &second_attempt,
                write,
                &first_client,
                &first_attempt,
            ),
            results => panic!("expected one reservation winner and one conflict: {results:?}"),
        };
    assert_eq!(winner_write.next_expected_version, 1);

    // A conflict invalidates the loser's stale decision, so it must reload before deciding again.
    let (sold_out, sold_out_revision) =
        read_inventory(loser_client, inventory_stream.as_str()).await?;
    assert_eq!(sold_out.available, 0);
    assert_eq!(sold_out.reservations, [(winner.0.reservation_id, 1)].into());
    assert_eq!(sold_out_revision, 1);

    let winner_release = ReleaseCommand::new(&sku, winner.0.reservation_id);
    assert_ne!(winner_release.0.operation_id, winner.0.operation_id);
    let winner_release_result =
        release(winner_client, inventory_stream.as_str(), &winner_release).await?;
    assert!(winner_release_result.appended);
    assert_eq!(winner_release_result.outcome.event_revision(), 2);
    let winner_release_outcome = winner_release_result.outcome.clone();

    let (released, released_revision) =
        read_inventory(loser_client, inventory_stream.as_str()).await?;
    assert_eq!(released.available, 1);
    assert!(released.reservations.is_empty());

    let loser_result = reserve(loser_client, inventory_stream.as_str(), loser).await?;
    assert!(loser_result.appended);
    assert_eq!(loser_result.outcome.event_revision(), 3);
    let loser_outcome = loser_result.outcome.clone();

    let loser_release = ReleaseCommand::new(&sku, loser.0.reservation_id);
    assert_ne!(loser_release.0.operation_id, loser.0.operation_id);
    let loser_release_result =
        release(loser_client, inventory_stream.as_str(), &loser_release).await?;
    assert!(loser_release_result.appended);
    assert_eq!(loser_release_result.outcome.event_revision(), 4);
    let loser_release_outcome = loser_release_result.outcome.clone();

    let (available_again, available_again_revision) =
        read_inventory(winner_client, inventory_stream.as_str()).await?;
    assert_eq!(available_again.available, 1);
    assert!(available_again.reservations.is_empty());
    assert_eq!(available_again_revision, 4);

    let winner_again = ReserveCommand::new(&winner.0.client, &sku);
    let winner_again_result =
        reserve(winner_client, inventory_stream.as_str(), &winner_again).await?;
    assert!(winner_again_result.appended);
    assert_eq!(winner_again_result.outcome.event_revision(), 5);
    let winner_again_outcome = winner_again_result.outcome.clone();

    let winner_again_release = ReleaseCommand::new(&sku, winner_again.0.reservation_id);
    let winner_again_release_result = release(
        winner_client,
        inventory_stream.as_str(),
        &winner_again_release,
    )
    .await?;
    assert!(winner_again_release_result.appended);
    assert_eq!(winner_again_release_result.outcome.event_revision(), 6);
    let winner_again_release_outcome = winner_again_release_result.outcome.clone();

    // The original write tuple is enough only when a transport retry retained it unchanged.
    let winner_retry = append_at(
        winner_client,
        inventory_stream.as_str(),
        first_revision,
        winner.event()?,
    )
    .await?;
    assert_eq!(winner_retry.position, winner_write.position);

    let winner_outcome = ProcessedOperation::Reserved {
        event_revision: winner_write.next_expected_version,
        event: winner.0.clone(),
    };

    // Delayed redelivery has lost the old revision, so the folded operation outcome is authoritative.
    let winner_replay = reserve(winner_client, inventory_stream.as_str(), winner).await?;
    let winner_release_replay =
        release(winner_client, inventory_stream.as_str(), &winner_release).await?;
    let loser_replay = reserve(loser_client, inventory_stream.as_str(), loser).await?;
    let loser_release_replay =
        release(loser_client, inventory_stream.as_str(), &loser_release).await?;
    let winner_again_replay =
        reserve(winner_client, inventory_stream.as_str(), &winner_again).await?;
    let winner_again_release_replay = release(
        winner_client,
        inventory_stream.as_str(),
        &winner_again_release,
    )
    .await?;

    for (replay, original) in [
        (winner_replay, winner_outcome),
        (winner_release_replay, winner_release_outcome),
        (loser_replay, loser_outcome),
        (loser_release_replay, loser_release_outcome),
        (winner_again_replay, winner_again_outcome),
        (winner_again_release_replay, winner_again_release_outcome),
    ] {
        assert!(!replay.appended);
        assert!(replay.write.is_none());
        assert_eq!(replay.outcome, original);
    }

    let conflicting_command = ReserveCommand(InventoryReserved {
        operation_id: winner.0.operation_id,
        reservation_id: ReservationId::new(),
        client: winner.0.client.clone(),
        sku: sku.clone(),
        quantity: 1,
    });
    let conflict = reserve(
        winner_client,
        inventory_stream.as_str(),
        &conflicting_command,
    )
    .await
    .expect_err("reusing an operation ID with different content must fail");
    assert!(conflict.downcast_ref::<OperationIdConflict>().is_some());

    let (final_state, final_revision) =
        read_inventory(&first_client, inventory_stream.as_str()).await?;
    assert_eq!(final_revision, 6);
    assert_eq!(final_state.available, 1);
    assert!(final_state.reservations.is_empty());
    assert_eq!(final_state.processed_operations.len(), 6);

    println!(
        "{} won the first race and released reservation {}; {} then reserved and released after reloading revision {}",
        winner.0.client, winner.0.reservation_id.0, loser.0.client, released_revision
    );
    println!(
        "{} completed a second reserve/release cycle; six delayed command replays returned their original outcomes after revision {final_revision}",
        winner.0.client
    );
    println!(
        "The example retains every processed operation in the inventory stream; production systems must budget for that unbounded history and index cost"
    );

    Ok(())
}
