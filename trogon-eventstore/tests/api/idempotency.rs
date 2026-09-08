use crate::common::fresh_stream_id;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;
use trogon_eventstore::{
    AppendToStreamOptions, Client, CurrentRevision, Error, EventData, StreamState, WriteResult,
};
use uuid::Uuid;

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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct InventoryReserved {
    state: String,
    client: String,
    operation_id: OperationId,
    reservation_id: ReservationId,
    quantity: u32,
}

#[derive(Clone)]
struct ReserveCommand(InventoryReserved);

impl ReserveCommand {
    fn new(client: &str) -> Self {
        let operation_id = OperationId::new();
        let reservation_id = ReservationId::new();

        Self(InventoryReserved {
            state: "reserved".to_owned(),
            client: client.to_owned(),
            operation_id,
            reservation_id,
            quantity: 1,
        })
    }

    fn event(&self) -> EventData {
        EventData::json("inventory-reserved", &self.0)
            .unwrap()
            .id(self.0.operation_id.0)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct InventoryReleased {
    state: String,
    operation_id: OperationId,
    reservation_id: ReservationId,
    quantity: u32,
}

#[derive(Clone)]
struct ReleaseCommand(InventoryReleased);

impl ReleaseCommand {
    fn new(reservation_id: ReservationId) -> Self {
        let operation_id = OperationId::new();

        Self(InventoryReleased {
            state: "released".to_owned(),
            operation_id,
            reservation_id,
            quantity: 1,
        })
    }

    fn event(&self) -> EventData {
        EventData::json("inventory-released", &self.0)
            .unwrap()
            .id(self.0.operation_id.0)
    }
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
    processed_operations: HashMap<OperationId, ProcessedOperation>,
}

#[derive(Debug)]
struct CommandResult {
    outcome: ProcessedOperation,
    appended: bool,
    write: Option<WriteResult>,
}

fn event(id: Uuid, state: &str) -> EventData {
    EventData::json("inventory-reservation", &json!({ "state": state }))
        .unwrap()
        .id(id)
}

async fn append(
    client: &Client,
    stream: &str,
    state: StreamState,
    event: EventData,
) -> trogon_eventstore::Result<WriteResult> {
    let options = AppendToStreamOptions::default().stream_state(state);
    client.append_to_stream(stream, &options, event).await
}

async fn stored_events(client: &Client, stream: &str) -> eyre::Result<Vec<(Uuid, u64, Value)>> {
    let mut read = client.read_stream(stream, &Default::default()).await?;
    let mut events = Vec::new();

    while let Some(event) = read.next().await? {
        let event = event.get_original_event();
        events.push((event.id, event.revision, event.as_json()?));
    }

    Ok(events)
}

fn fold_inventory(events: &[(Uuid, u64, Value)]) -> InventoryState {
    let mut state = InventoryState {
        available: 0,
        reservations: HashMap::new(),
        processed_operations: HashMap::new(),
    };

    for (event_id, event_revision, event) in events {
        let quantity = event
            .get("quantity")
            .and_then(Value::as_u64)
            .unwrap_or_default() as u32;

        match event["state"].as_str().unwrap() {
            "created" => state.available = event["available"].as_u64().unwrap() as u32,
            "reserved" => {
                let reserved: InventoryReserved = serde_json::from_value(event.clone()).unwrap();
                assert_eq!(*event_id, reserved.operation_id.0);
                assert!(state.available >= quantity);
                state.available -= quantity;
                assert!(
                    state
                        .reservations
                        .insert(reserved.reservation_id, quantity)
                        .is_none()
                );
                assert!(
                    state
                        .processed_operations
                        .insert(
                            reserved.operation_id,
                            ProcessedOperation::Reserved {
                                event_revision: *event_revision,
                                event: reserved,
                            },
                        )
                        .is_none()
                );
            }
            "released" => {
                let released: InventoryReleased = serde_json::from_value(event.clone()).unwrap();
                assert_eq!(*event_id, released.operation_id.0);
                assert_eq!(
                    state.reservations.remove(&released.reservation_id),
                    Some(released.quantity)
                );
                state.available += released.quantity;
                assert!(
                    state
                        .processed_operations
                        .insert(
                            released.operation_id,
                            ProcessedOperation::Released {
                                event_revision: *event_revision,
                                event: released,
                            },
                        )
                        .is_none()
                );
            }
            state => panic!("unexpected inventory state: {state}"),
        }
    }

    state
}

async fn reserve(
    client: &Client,
    stream: &str,
    command: &ReserveCommand,
) -> eyre::Result<CommandResult> {
    let events = stored_events(client, stream).await?;
    let state = fold_inventory(&events);

    if let Some(previous) = state.processed_operations.get(&command.0.operation_id) {
        return match previous {
            ProcessedOperation::Reserved { event, .. } if event == &command.0 => {
                Ok(CommandResult {
                    outcome: previous.clone(),
                    appended: false,
                    write: None,
                })
            }
            _ => eyre::bail!("operation ID was reused with different content"),
        };
    }

    if state.available < command.0.quantity {
        eyre::bail!("the requested inventory is not available");
    }

    let revision = events.last().unwrap().1;
    let write = append(
        client,
        stream,
        StreamState::StreamRevision(revision),
        command.event(),
    )
    .await?;
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
) -> eyre::Result<CommandResult> {
    let events = stored_events(client, stream).await?;
    let state = fold_inventory(&events);

    if let Some(previous) = state.processed_operations.get(&command.0.operation_id) {
        return match previous {
            ProcessedOperation::Released { event, .. } if event == &command.0 => {
                Ok(CommandResult {
                    outcome: previous.clone(),
                    appended: false,
                    write: None,
                })
            }
            _ => eyre::bail!("operation ID was reused with different content"),
        };
    }

    if state.reservations.get(&command.0.reservation_id) != Some(&command.0.quantity) {
        eyre::bail!("the reservation is not active with the requested quantity");
    }

    let revision = events.last().unwrap().1;
    let write = append(
        client,
        stream,
        StreamState::StreamRevision(revision),
        command.event(),
    )
    .await?;
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

fn is_revision_conflict(error: &Error, expected: u64, current: u64) -> bool {
    matches!(
        error,
        Error::WrongExpectedVersion {
            expected: StreamState::StreamRevision(actual_expected),
            current: CurrentRevision::Current(actual_current),
        } if *actual_expected == expected && *actual_current == current
    )
}

async fn no_stream_retry_is_idempotent(client: &Client) -> eyre::Result<()> {
    let stream = fresh_stream_id("idempotency-no-stream");
    let event_id = Uuid::new_v4();

    let first = append(
        client,
        &stream,
        StreamState::NoStream,
        event(event_id, "reserved"),
    )
    .await?;
    let retry = append(
        client,
        &stream,
        StreamState::NoStream,
        event(event_id, "reserved"),
    )
    .await?;

    assert_eq!(first.next_expected_version, 0);
    assert_eq!(retry.next_expected_version, 0);
    assert_eq!(first.position, retry.position);
    assert_eq!(stored_events(client, &stream).await?.len(), 1);

    Ok(())
}

async fn explicit_revision_retry_is_idempotent(client: &Client) -> eyre::Result<()> {
    let stream = fresh_stream_id("idempotency-revision");
    let event_id = Uuid::new_v4();

    append(
        client,
        &stream,
        StreamState::NoStream,
        event(Uuid::new_v4(), "opened"),
    )
    .await?;
    let first = append(
        client,
        &stream,
        StreamState::StreamRevision(0),
        event(event_id, "reserved"),
    )
    .await?;
    let retry = append(
        client,
        &stream,
        StreamState::StreamRevision(0),
        event(event_id, "reserved"),
    )
    .await?;

    assert_eq!(first.next_expected_version, 1);
    assert_eq!(retry.next_expected_version, 1);
    assert_eq!(first.position, retry.position);
    assert_eq!(stored_events(client, &stream).await?.len(), 2);

    Ok(())
}

async fn retry_identity_does_not_include_payload(client: &Client) -> eyre::Result<()> {
    let stream = fresh_stream_id("idempotency-payload");
    let seed_id = Uuid::new_v4();
    let event_id = Uuid::new_v4();

    append(
        client,
        &stream,
        StreamState::NoStream,
        event(seed_id, "opened"),
    )
    .await?;
    append(
        client,
        &stream,
        StreamState::StreamRevision(0),
        event(event_id, "reserved"),
    )
    .await?;
    append(
        client,
        &stream,
        StreamState::StreamRevision(0),
        event(event_id, "released"),
    )
    .await?;

    assert_eq!(
        stored_events(client, &stream).await?,
        [
            (seed_id, 0, json!({ "state": "opened" })),
            (event_id, 1, json!({ "state": "reserved" })),
        ]
    );

    Ok(())
}

async fn same_id_at_a_different_revision_is_a_new_event(client: &Client) -> eyre::Result<()> {
    let stream = fresh_stream_id("idempotency-different-revision");
    let event_id = Uuid::new_v4();

    append(
        client,
        &stream,
        StreamState::NoStream,
        event(event_id, "reserved"),
    )
    .await?;
    let second = append(
        client,
        &stream,
        StreamState::StreamRevision(0),
        event(event_id, "released"),
    )
    .await?;

    assert_eq!(second.next_expected_version, 1);
    assert_eq!(
        stored_events(client, &stream).await?,
        [
            (event_id, 0, json!({ "state": "reserved" })),
            (event_id, 1, json!({ "state": "released" })),
        ]
    );

    Ok(())
}

async fn event_ids_are_not_unique_across_streams(client: &Client) -> eyre::Result<()> {
    let first_stream = fresh_stream_id("idempotency-first-stream");
    let second_stream = fresh_stream_id("idempotency-second-stream");
    let event_id = Uuid::new_v4();

    append(
        client,
        &first_stream,
        StreamState::NoStream,
        event(event_id, "reserved"),
    )
    .await?;
    append(
        client,
        &second_stream,
        StreamState::NoStream,
        event(event_id, "reserved"),
    )
    .await?;

    assert_eq!(stored_events(client, &first_stream).await?.len(), 1);
    assert_eq!(stored_events(client, &second_stream).await?.len(), 1);

    Ok(())
}

async fn competing_reservations_are_serialized_and_retryable(client: &Client) -> eyre::Result<()> {
    let stream = fresh_stream_id("idempotency-competing-reservations");
    let first_client = Client::new(client.settings().clone())?;
    let second_client = Client::new(client.settings().clone())?;
    let created_operation_id = Uuid::new_v4();

    append(
        &first_client,
        &stream,
        StreamState::NoStream,
        EventData::json(
            "inventory-created",
            &json!({ "state": "created", "available": 1 }),
        )?
        .id(created_operation_id),
    )
    .await?;

    let first_view = stored_events(&first_client, &stream).await?;
    let second_view = stored_events(&second_client, &stream).await?;
    assert_eq!(fold_inventory(&first_view).available, 1);
    assert_eq!(fold_inventory(&second_view).available, 1);
    let first_revision = first_view.last().unwrap().1;
    let second_revision = second_view.last().unwrap().1;
    assert_eq!(first_revision, second_revision);

    let first_attempt = ReserveCommand::new("checkout-a");
    let second_attempt = ReserveCommand::new("checkout-b");
    assert_ne!(
        first_attempt.0.operation_id.0,
        first_attempt.0.reservation_id.0
    );
    assert_ne!(
        second_attempt.0.operation_id.0,
        second_attempt.0.reservation_id.0
    );

    let (first_result, second_result) = tokio::join!(
        append(
            &first_client,
            &stream,
            StreamState::StreamRevision(first_revision),
            first_attempt.event(),
        ),
        append(
            &second_client,
            &stream,
            StreamState::StreamRevision(second_revision),
            second_attempt.event(),
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
            results => eyre::bail!(
                "expected one reservation winner and one revision conflict: {results:?}"
            ),
        };

    let sold_out_view = stored_events(loser_client, &stream).await?;
    let sold_out = fold_inventory(&sold_out_view);
    assert_eq!(sold_out.available, 0);
    assert_eq!(sold_out.reservations, [(winner.0.reservation_id, 1)].into());
    let sold_out_revision = sold_out_view.last().unwrap().1;
    assert_eq!(sold_out_revision, 1);

    let winner_release = ReleaseCommand::new(winner.0.reservation_id);
    assert_ne!(winner_release.0.operation_id, winner.0.operation_id);
    assert_ne!(winner_release.0.operation_id.0, winner.0.reservation_id.0);
    let winner_release_result = release(winner_client, &stream, &winner_release).await?;
    assert!(winner_release_result.appended);
    assert_eq!(winner_release_result.outcome.event_revision(), 2);
    let winner_release_outcome = winner_release_result.outcome.clone();

    let released_view = stored_events(loser_client, &stream).await?;
    let released = fold_inventory(&released_view);
    assert_eq!(released.available, 1);
    assert!(released.reservations.is_empty());
    let released_revision = released_view.last().unwrap().1;
    assert_eq!(released_revision, 2);

    let loser_result = reserve(loser_client, &stream, loser).await?;
    assert!(loser_result.appended);
    assert_eq!(loser_result.outcome.event_revision(), 3);
    let loser_outcome = loser_result.outcome.clone();

    let loser_release = ReleaseCommand::new(loser.0.reservation_id);
    assert_ne!(loser_release.0.operation_id, loser.0.operation_id);
    let loser_release_result = release(loser_client, &stream, &loser_release).await?;
    assert!(loser_release_result.appended);
    assert_eq!(loser_release_result.outcome.event_revision(), 4);
    let loser_release_outcome = loser_release_result.outcome.clone();

    let available_again_view = stored_events(winner_client, &stream).await?;
    let available_again = fold_inventory(&available_again_view);
    assert_eq!(available_again.available, 1);
    assert!(available_again.reservations.is_empty());
    let available_again_revision = available_again_view.last().unwrap().1;
    assert_eq!(available_again_revision, 4);

    let winner_again = ReserveCommand::new("checkout-winner-again");
    let winner_again_result = reserve(winner_client, &stream, &winner_again).await?;
    assert!(winner_again_result.appended);
    assert_eq!(winner_again_result.outcome.event_revision(), 5);
    let winner_again_outcome = winner_again_result.outcome.clone();

    let winner_again_release = ReleaseCommand::new(winner_again.0.reservation_id);
    let winner_again_release_result =
        release(winner_client, &stream, &winner_again_release).await?;
    assert!(winner_again_release_result.appended);
    assert_eq!(winner_again_release_result.outcome.event_revision(), 6);
    let winner_again_release_outcome = winner_again_release_result.outcome.clone();

    // The server can recognize a transport retry because the original write tuple is unchanged.
    let winner_retry = append(
        winner_client,
        &stream,
        StreamState::StreamRevision(first_revision),
        winner.event(),
    )
    .await?;
    assert_eq!(winner_retry.position, winner_write.position);

    let winner_outcome = ProcessedOperation::Reserved {
        event_revision: winner_write.next_expected_version,
        event: winner.0.clone(),
    };

    // Reconstructed commands need the outcome retained in the same authoritative stream.
    let winner_replay = reserve(winner_client, &stream, winner).await?;
    let winner_release_replay = release(winner_client, &stream, &winner_release).await?;
    let loser_replay = reserve(loser_client, &stream, loser).await?;
    let loser_release_replay = release(loser_client, &stream, &loser_release).await?;
    let winner_again_replay = reserve(winner_client, &stream, &winner_again).await?;
    let winner_again_release_replay =
        release(winner_client, &stream, &winner_again_release).await?;

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
        state: "reserved".to_owned(),
        client: winner.0.client.clone(),
        operation_id: winner.0.operation_id,
        reservation_id: ReservationId::new(),
        quantity: 1,
    });
    let conflict = reserve(winner_client, &stream, &conflicting_command)
        .await
        .expect_err("reusing an operation ID with different content must fail");
    assert_eq!(
        conflict.to_string(),
        "operation ID was reused with different content"
    );

    let final_view = stored_events(&first_client, &stream).await?;
    assert_eq!(final_view.len(), 7);
    assert_eq!(final_view.last().unwrap().1, 6);
    let final_state = fold_inventory(&final_view);
    assert_eq!(final_state.available, 1);
    assert!(final_state.reservations.is_empty());
    assert_eq!(final_state.processed_operations.len(), 6);
    assert_eq!(
        final_view.iter().map(|(id, _, _)| *id).collect::<Vec<_>>(),
        [
            created_operation_id,
            winner.0.operation_id.0,
            winner_release.0.operation_id.0,
            loser.0.operation_id.0,
            loser_release.0.operation_id.0,
            winner_again.0.operation_id.0,
            winner_again_release.0.operation_id.0,
        ]
    );

    Ok(())
}

pub async fn tests(client: Client) -> eyre::Result<()> {
    no_stream_retry_is_idempotent(&client).await?;
    explicit_revision_retry_is_idempotent(&client).await?;
    retry_identity_does_not_include_payload(&client).await?;
    same_id_at_a_different_revision_is_a_new_event(&client).await?;
    event_ids_are_not_unique_across_streams(&client).await?;
    competing_reservations_are_serialized_and_retryable(&client).await?;

    Ok(())
}
