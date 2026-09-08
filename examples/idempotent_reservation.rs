use serde::{Deserialize, Serialize};
use std::{collections::HashMap, error::Error};
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

#[derive(Debug, Deserialize, Serialize)]
struct InventoryReserved {
    operation_id: OperationId,
    reservation_id: ReservationId,
    client: String,
    sku: String,
    quantity: u32,
}

#[derive(Debug, Deserialize, Serialize)]
struct InventoryReleased {
    operation_id: OperationId,
    reservation_id: ReservationId,
    sku: String,
    quantity: u32,
}

#[derive(Debug)]
struct InventoryState {
    available: u32,
    reservations: HashMap<ReservationId, u32>,
}

#[derive(Clone)]
struct ReservationAttempt {
    client_name: &'static str,
    operation_id: OperationId,
    reservation_id: ReservationId,
    event: EventData,
}

impl ReservationAttempt {
    fn new(client_name: &'static str, sku: &str) -> Result<Self, Box<dyn Error>> {
        let operation_id = OperationId::new();
        let reservation_id = ReservationId::new();
        let reservation = InventoryReserved {
            operation_id,
            reservation_id,
            client: client_name.to_owned(),
            sku: sku.to_owned(),
            quantity: 1,
        };

        Ok(Self {
            client_name,
            operation_id,
            reservation_id,
            event: EventData::json(INVENTORY_RESERVED_EVENT_TYPE, &reservation)?.id(operation_id.0),
        })
    }
}

#[derive(Clone)]
struct ReleaseAttempt {
    operation_id: OperationId,
    event: EventData,
}

impl ReleaseAttempt {
    fn new(sku: &str, reservation_id: ReservationId) -> Result<Self, Box<dyn Error>> {
        let operation_id = OperationId::new();
        let release = InventoryReleased {
            operation_id,
            reservation_id,
            sku: sku.to_owned(),
            quantity: 1,
        };

        Ok(Self {
            operation_id,
            event: EventData::json(INVENTORY_RELEASED_EVENT_TYPE, &release)?.id(operation_id.0),
        })
    }
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

    let first_attempt = ReservationAttempt::new(FIRST_CLIENT, &sku)?;
    let second_attempt = ReservationAttempt::new(SECOND_CLIENT, &sku)?;
    // The shared expected revision serializes decisions made from the same inventory state.
    let (first_result, second_result) = tokio::join!(
        append_at(
            &first_client,
            inventory_stream.as_str(),
            first_revision,
            first_attempt.event.clone(),
        ),
        append_at(
            &second_client,
            inventory_stream.as_str(),
            second_revision,
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
            results => panic!("expected one reservation winner and one conflict: {results:?}"),
        };
    assert_eq!(winner_write.next_expected_version, 1);

    // A conflict invalidates the loser's stale decision, so it must reload before deciding again.
    let (sold_out, sold_out_revision) =
        read_inventory(loser_client, inventory_stream.as_str()).await?;
    assert_eq!(sold_out.available, 0);
    assert_eq!(sold_out.reservations, [(winner.reservation_id, 1)].into());

    let winner_release = ReleaseAttempt::new(&sku, winner.reservation_id)?;
    assert_ne!(winner_release.operation_id, winner.operation_id);
    let winner_release_write = append_at(
        winner_client,
        inventory_stream.as_str(),
        sold_out_revision,
        winner_release.event.clone(),
    )
    .await?;
    assert_eq!(winner_release_write.next_expected_version, 2);

    let (released, released_revision) =
        read_inventory(loser_client, inventory_stream.as_str()).await?;
    assert_eq!(released.available, 1);
    assert!(released.reservations.is_empty());

    let loser_write = append_at(
        loser_client,
        inventory_stream.as_str(),
        released_revision,
        loser.event.clone(),
    )
    .await?;
    assert_eq!(loser_write.next_expected_version, 3);

    let loser_release = ReleaseAttempt::new(&sku, loser.reservation_id)?;
    assert_ne!(loser_release.operation_id, loser.operation_id);
    let loser_release_write = append_at(
        loser_client,
        inventory_stream.as_str(),
        loser_write.next_expected_version,
        loser_release.event.clone(),
    )
    .await?;
    assert_eq!(loser_release_write.next_expected_version, 4);

    let (available_again, available_again_revision) =
        read_inventory(winner_client, inventory_stream.as_str()).await?;
    assert_eq!(available_again.available, 1);
    assert!(available_again.reservations.is_empty());

    let winner_again = ReservationAttempt::new(winner.client_name, &sku)?;
    let winner_again_write = append_at(
        winner_client,
        inventory_stream.as_str(),
        available_again_revision,
        winner_again.event.clone(),
    )
    .await?;
    assert_eq!(winner_again_write.next_expected_version, 5);

    let winner_again_release = ReleaseAttempt::new(&sku, winner_again.reservation_id)?;
    let winner_again_release_write = append_at(
        winner_client,
        inventory_stream.as_str(),
        winner_again_write.next_expected_version,
        winner_again_release.event.clone(),
    )
    .await?;
    assert_eq!(winner_again_release_write.next_expected_version, 6);

    // Durable retries retain the original expected revision and event ID after later writes.
    let winner_retry = append_at(
        winner_client,
        inventory_stream.as_str(),
        first_revision,
        winner.event.clone(),
    )
    .await?;
    let winner_release_retry = append_at(
        winner_client,
        inventory_stream.as_str(),
        sold_out_revision,
        winner_release.event,
    )
    .await?;
    let loser_retry = append_at(
        loser_client,
        inventory_stream.as_str(),
        released_revision,
        loser.event.clone(),
    )
    .await?;
    let loser_release_retry = append_at(
        loser_client,
        inventory_stream.as_str(),
        loser_write.next_expected_version,
        loser_release.event,
    )
    .await?;
    let winner_again_retry = append_at(
        winner_client,
        inventory_stream.as_str(),
        available_again_revision,
        winner_again.event,
    )
    .await?;
    let winner_again_release_retry = append_at(
        winner_client,
        inventory_stream.as_str(),
        winner_again_write.next_expected_version,
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

    let (final_state, final_revision) =
        read_inventory(&first_client, inventory_stream.as_str()).await?;
    assert_eq!(final_revision, 6);
    assert_eq!(final_state.available, 1);
    assert!(final_state.reservations.is_empty());

    println!(
        "{} won the first race and released reservation {}; {} then reserved and released after reloading revision {}",
        winner.client_name, winner.reservation_id.0, loser.client_name, released_revision
    );
    println!(
        "{} completed a second reserve/release cycle; all six retries remained single events after revision {final_revision}",
        winner.client_name
    );

    Ok(())
}
