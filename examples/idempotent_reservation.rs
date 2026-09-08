use serde::{Deserialize, Serialize};
use std::error::Error;
use trogon_eventstore::{AppendToStreamOptions, Client, EventData, ReadStreamOptions, StreamState};
use uuid::Uuid;

const DEFAULT_CONNECTION_STRING: &str = "esdb://localhost:2113?tls=false";
const CONNECTION_STRING_ENV: &str = "TROGON_EVENTSTORE_CONNECTION_STRING";
const INVENTORY_CREATED_EVENT_TYPE: &str = "inventory-created";
const INVENTORY_RESERVED_EVENT_TYPE: &str = "inventory-reserved";

#[derive(Debug, Deserialize, Serialize)]
struct InventoryCreated {
    sku: String,
    available: u32,
}

#[derive(Debug, Deserialize, Serialize)]
struct InventoryReserved {
    operation_id: Uuid,
    order_id: String,
    sku: String,
    quantity: u32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let connection_string = std::env::var(CONNECTION_STRING_ENV)
        .unwrap_or_else(|_| DEFAULT_CONNECTION_STRING.to_owned());
    let client = Client::new(connection_string.parse()?)?;

    let operation_id = Uuid::new_v4();
    let sku = format!("sku-{}", Uuid::new_v4());
    let inventory_stream = format!("inventory-{sku}");
    let inventory = InventoryCreated {
        sku: sku.clone(),
        available: 100,
    };
    let created = client
        .append_to_stream(
            inventory_stream.as_str(),
            &AppendToStreamOptions::default().stream_state(StreamState::NoStream),
            EventData::json(INVENTORY_CREATED_EVENT_TYPE, &inventory)?.id(Uuid::new_v4()),
        )
        .await?;
    let expected_revision = created.next_expected_version;
    let reservation = InventoryReserved {
        operation_id,
        order_id: "order-123".to_owned(),
        sku,
        quantity: 2,
    };
    let event = EventData::json(INVENTORY_RESERVED_EVENT_TYPE, &reservation)?.id(operation_id);

    // Event IDs are not a stream-wide unique constraint, and `Any` only checks
    // recent IDs. A durable retry must retain both this revision and event ID.
    let options = AppendToStreamOptions::default()
        .stream_state(StreamState::StreamRevision(expected_revision));

    let first = client
        .append_to_stream(inventory_stream.as_str(), &options, event.clone())
        .await?;
    let retry = client
        .append_to_stream(inventory_stream.as_str(), &options, event)
        .await?;

    let mut events = client
        .read_stream(inventory_stream.as_str(), &ReadStreamOptions::default())
        .await?;
    let _created = events.next().await?.expect("the inventory to exist");
    let stored = events.next().await?.expect("the reservation to exist");
    assert!(events.next().await?.is_none());
    assert_eq!(stored.get_original_event().id, operation_id);
    assert_eq!(first.next_expected_version, retry.next_expected_version);

    println!(
        "operation {operation_id} was stored once at inventory revision {}",
        first.next_expected_version
    );

    Ok(())
}
