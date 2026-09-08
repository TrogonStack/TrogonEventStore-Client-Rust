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

#[derive(Clone)]
struct ReservationAttempt {
    operation_id: OperationId,
    reservation_id: ReservationId,
    event: EventData,
}

impl ReservationAttempt {
    fn new(client: &str) -> Self {
        let operation_id = OperationId::new();
        let reservation_id = ReservationId::new();

        Self {
            operation_id,
            reservation_id,
            event: EventData::json(
                "inventory-reserved",
                &json!({
                    "state": "reserved",
                    "client": client,
                    "operation_id": operation_id,
                    "reservation_id": reservation_id,
                    "quantity": 1,
                }),
            )
            .unwrap()
            .id(operation_id.0),
        }
    }
}

#[derive(Clone)]
struct ReleaseAttempt {
    operation_id: OperationId,
    event: EventData,
}

impl ReleaseAttempt {
    fn new(reservation_id: ReservationId) -> Self {
        let operation_id = OperationId::new();

        Self {
            operation_id,
            event: EventData::json(
                "inventory-released",
                &json!({
                    "state": "released",
                    "operation_id": operation_id,
                    "reservation_id": reservation_id,
                    "quantity": 1,
                }),
            )
            .unwrap()
            .id(operation_id.0),
        }
    }
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

fn fold_inventory(events: &[(Uuid, u64, Value)]) -> (u32, HashMap<ReservationId, u32>) {
    let mut available = 0;
    let mut reservations = HashMap::new();

    for (event_id, _, event) in events {
        let quantity = event
            .get("quantity")
            .and_then(Value::as_u64)
            .unwrap_or_default() as u32;

        match event["state"].as_str().unwrap() {
            "created" => available = event["available"].as_u64().unwrap() as u32,
            "reserved" => {
                let operation_id: OperationId =
                    serde_json::from_value(event["operation_id"].clone()).unwrap();
                assert_eq!(*event_id, operation_id.0);
                assert!(available >= quantity);
                available -= quantity;
                let reservation_id =
                    serde_json::from_value(event["reservation_id"].clone()).unwrap();
                assert!(reservations.insert(reservation_id, quantity).is_none());
            }
            "released" => {
                let operation_id: OperationId =
                    serde_json::from_value(event["operation_id"].clone()).unwrap();
                assert_eq!(*event_id, operation_id.0);
                let reservation_id =
                    serde_json::from_value(event["reservation_id"].clone()).unwrap();
                assert_eq!(reservations.remove(&reservation_id), Some(quantity));
                available += quantity;
            }
            state => panic!("unexpected inventory state: {state}"),
        }
    }

    (available, reservations)
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
    assert_eq!(fold_inventory(&first_view), (1, HashMap::new()));
    assert_eq!(fold_inventory(&second_view), (1, HashMap::new()));
    let first_revision = first_view.last().unwrap().1;
    let second_revision = second_view.last().unwrap().1;
    assert_eq!(first_revision, second_revision);

    let first_attempt = ReservationAttempt::new("checkout-a");
    let second_attempt = ReservationAttempt::new("checkout-b");
    assert_ne!(first_attempt.operation_id.0, first_attempt.reservation_id.0);
    assert_ne!(
        second_attempt.operation_id.0,
        second_attempt.reservation_id.0
    );

    let (first_result, second_result) = tokio::join!(
        append(
            &first_client,
            &stream,
            StreamState::StreamRevision(first_revision),
            first_attempt.event.clone(),
        ),
        append(
            &second_client,
            &stream,
            StreamState::StreamRevision(second_revision),
            second_attempt.event.clone(),
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
    assert_eq!(
        fold_inventory(&sold_out_view),
        (0, [(winner.reservation_id, 1)].into())
    );
    let sold_out_revision = sold_out_view.last().unwrap().1;
    assert_eq!(sold_out_revision, 1);

    let winner_release = ReleaseAttempt::new(winner.reservation_id);
    assert_ne!(winner_release.operation_id, winner.operation_id);
    assert_ne!(winner_release.operation_id.0, winner.reservation_id.0);
    let winner_release_write = append(
        winner_client,
        &stream,
        StreamState::StreamRevision(sold_out_revision),
        winner_release.event.clone(),
    )
    .await?;

    let released_view = stored_events(loser_client, &stream).await?;
    assert_eq!(fold_inventory(&released_view), (1, HashMap::new()));
    let released_revision = released_view.last().unwrap().1;
    assert_eq!(released_revision, 2);

    let loser_write = append(
        loser_client,
        &stream,
        StreamState::StreamRevision(released_revision),
        loser.event.clone(),
    )
    .await?;
    assert_eq!(loser_write.next_expected_version, 3);

    let loser_release = ReleaseAttempt::new(loser.reservation_id);
    assert_ne!(loser_release.operation_id, loser.operation_id);
    let loser_release_write = append(
        loser_client,
        &stream,
        StreamState::StreamRevision(loser_write.next_expected_version),
        loser_release.event.clone(),
    )
    .await?;
    assert_eq!(loser_release_write.next_expected_version, 4);

    let available_again_view = stored_events(winner_client, &stream).await?;
    assert_eq!(fold_inventory(&available_again_view), (1, HashMap::new()));
    let available_again_revision = available_again_view.last().unwrap().1;
    assert_eq!(available_again_revision, 4);

    let winner_again = ReservationAttempt::new("checkout-winner-again");
    let winner_again_write = append(
        winner_client,
        &stream,
        StreamState::StreamRevision(available_again_revision),
        winner_again.event.clone(),
    )
    .await?;
    assert_eq!(winner_again_write.next_expected_version, 5);

    let winner_again_release = ReleaseAttempt::new(winner_again.reservation_id);
    let winner_again_release_write = append(
        winner_client,
        &stream,
        StreamState::StreamRevision(winner_again_write.next_expected_version),
        winner_again_release.event.clone(),
    )
    .await?;
    assert_eq!(winner_again_release_write.next_expected_version, 6);

    let winner_retry = append(
        winner_client,
        &stream,
        StreamState::StreamRevision(first_revision),
        winner.event.clone(),
    )
    .await?;
    let winner_release_retry = append(
        winner_client,
        &stream,
        StreamState::StreamRevision(sold_out_revision),
        winner_release.event,
    )
    .await?;
    let loser_retry = append(
        loser_client,
        &stream,
        StreamState::StreamRevision(released_revision),
        loser.event.clone(),
    )
    .await?;
    let loser_release_retry = append(
        loser_client,
        &stream,
        StreamState::StreamRevision(loser_write.next_expected_version),
        loser_release.event,
    )
    .await?;
    let winner_again_retry = append(
        winner_client,
        &stream,
        StreamState::StreamRevision(available_again_revision),
        winner_again.event,
    )
    .await?;
    let winner_again_release_retry = append(
        winner_client,
        &stream,
        StreamState::StreamRevision(winner_again_write.next_expected_version),
        winner_again_release.event,
    )
    .await?;
    assert_eq!(winner_retry.position, winner_write.position);
    assert_eq!(winner_release_retry.position, winner_release_write.position);
    assert_eq!(loser_retry.position, loser_write.position);
    assert_eq!(loser_release_retry.position, loser_release_write.position);
    assert_eq!(winner_again_retry.position, winner_again_write.position);
    assert_eq!(
        winner_again_release_retry.position,
        winner_again_release_write.position
    );

    let final_view = stored_events(&first_client, &stream).await?;
    assert_eq!(final_view.len(), 7);
    assert_eq!(final_view.last().unwrap().1, 6);
    assert_eq!(fold_inventory(&final_view), (1, HashMap::new()));
    assert_eq!(
        final_view.iter().map(|(id, _, _)| *id).collect::<Vec<_>>(),
        [
            created_operation_id,
            winner.operation_id.0,
            winner_release.operation_id.0,
            loser.operation_id.0,
            loser_release.operation_id.0,
            winner_again.operation_id.0,
            winner_again_release.operation_id.0,
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
