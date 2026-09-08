use crate::common::fresh_stream_id;
use serde_json::{Value, json};
use trogon_eventstore::{AppendToStreamOptions, Client, EventData, StreamState, WriteResult};
use uuid::Uuid;

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

pub async fn tests(client: Client) -> eyre::Result<()> {
    no_stream_retry_is_idempotent(&client).await?;
    explicit_revision_retry_is_idempotent(&client).await?;
    retry_identity_does_not_include_payload(&client).await?;
    same_id_at_a_different_revision_is_a_new_event(&client).await?;
    event_ids_are_not_unique_across_streams(&client).await?;

    Ok(())
}
