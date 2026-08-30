use crate::batch::BatchAppendClient;
use crate::grpc::{ClientSettings, GrpcClient};
use crate::observability::{client_operation, infallible_client_operation, operation};
use crate::options::batch_append::BatchAppendOptions;
use crate::options::persistent_subscription::PersistentSubscriptionOptions;
use crate::options::read_all::ReadAllOptions;
use crate::options::read_stream::ReadStreamOptions;
use crate::options::subscribe_to_stream::SubscribeToStreamOptions;
use crate::server_features::ServerInfo;
use crate::{
    DeletePersistentSubscriptionOptions, DeleteStreamOptions, GetPersistentSubscriptionInfoOptions,
    ListPersistentSubscriptionsOptions, MetadataStreamName, PersistentSubscription,
    PersistentSubscriptionInfo, PersistentSubscriptionToAllOptions, Position, ReadStream,
    ReplayParkedMessagesOptions, RestartPersistentSubscriptionSubsystem, RevisionOrPosition,
    StreamMetadata, StreamMetadataResult, StreamName, SubscribeToAllOptions,
    SubscribeToPersistentSubscriptionOptions, Subscription, TombstoneStreamOptions,
    VersionedMetadata, WriteResult, commands,
};
use crate::{
    EventData,
    options::append_to_stream::{AppendToStreamOptions, ToEvents},
};

/// Represents a client to a single node. `Client` maintains a full duplex
/// communication to TrogonEventStore.
///
/// Many threads can use a TrogonEventStore client at the same time
/// or a single thread can make many asynchronous requests.
#[derive(Clone)]
pub struct Client {
    pub(crate) http_client: reqwest::Client,
    pub(crate) client: GrpcClient,
}

impl Client {
    /// Creates a gRPC client to a TrogonEventStore database.
    pub fn new(settings: ClientSettings) -> eyre::Result<Self> {
        Client::with_runtime_handle(tokio::runtime::Handle::current(), settings)
    }

    /// Creates a gRPC client to a TrogonEventStore database using an existing tokio runtime.
    pub fn with_runtime_handle(
        handle: tokio::runtime::Handle,
        settings: ClientSettings,
    ) -> eyre::Result<Self> {
        let client = GrpcClient::create(handle, settings.clone())?;

        let http_client = reqwest::Client::builder()
            .danger_accept_invalid_certs(!settings.is_tls_certificate_verification_enabled())
            .https_only(settings.is_secure_mode_enabled())
            .build()
            .map_err(|e| crate::Error::InitializationError(e.to_string()))?;

        Ok(Client {
            http_client,
            client,
        })
    }

    pub fn settings(&self) -> &ClientSettings {
        self.client.connection_settings()
    }

    /// Returns the server information the client is connected to. If `None`, means you are dealing
    /// with a server older than 21.6 version.
    pub async fn server_info(&self) -> crate::Result<ServerInfo> {
        let handle = self.client.current_selected_node().await?;

        Ok(handle.server_info())
    }

    /// Sends events to a given stream.
    pub async fn append_to_stream<Events>(
        &self,
        stream_name: impl StreamName,
        options: &AppendToStreamOptions,
        events: Events,
    ) -> crate::Result<WriteResult>
    where
        Events: ToEvents,
    {
        let stream_name = stream_name.into_stream_name();
        client_operation(
            operation::APPEND_TO_STREAM.on_stream(&stream_name),
            commands::append_to_stream(&self.client, stream_name, options, events.into_events()),
        )
        .await
    }

    // Sets a stream metadata.
    pub async fn set_stream_metadata(
        &self,
        name: impl MetadataStreamName,
        options: &AppendToStreamOptions,
        metadata: &StreamMetadata,
    ) -> crate::Result<WriteResult> {
        let stream_name = name.into_metadata_stream_name();
        client_operation(
            operation::SET_STREAM_METADATA.on_stream(&stream_name),
            async {
                let event = EventData::json("$metadata", metadata)
                    .map_err(|e| crate::Error::InternalParsingError(e.to_string()))?;

                commands::append_to_stream(&self.client, stream_name, options, event.into_events())
                    .await
            },
        )
        .await
    }

    // Creates a batch-append client.
    pub async fn batch_append(
        &self,
        options: &BatchAppendOptions,
    ) -> crate::Result<BatchAppendClient> {
        client_operation(
            operation::BATCH_APPEND,
            commands::batch_append(&self.client, options),
        )
        .await
    }

    /// Reads events from a given stream. The reading can be done forward and
    /// backward.
    pub async fn read_stream(
        &self,
        stream_name: impl StreamName,
        options: &ReadStreamOptions,
    ) -> crate::Result<ReadStream> {
        let stream_name = stream_name.into_stream_name();
        client_operation(
            operation::READ_STREAM.on_stream(&stream_name),
            commands::read_stream(
                self.client.clone(),
                options,
                stream_name,
                options.max_count as u64,
            ),
        )
        .await
    }

    /// Reads events for the system stream `$all`. The reading can be done
    /// forward and backward.
    pub async fn read_all(&self, options: &ReadAllOptions) -> crate::Result<ReadStream> {
        client_operation(
            operation::READ_ALL.on_collection("$all"),
            commands::read_all(self.client.clone(), options, options.max_count as u64),
        )
        .await
    }

    /// Reads a stream metadata.
    pub async fn get_stream_metadata(
        &self,
        name: impl MetadataStreamName,
        options: &ReadStreamOptions,
    ) -> crate::Result<StreamMetadataResult> {
        let stream_name = name.into_metadata_stream_name();
        client_operation(
            operation::GET_STREAM_METADATA.on_stream(&stream_name),
            async {
                let mut stream = commands::read_stream(
                    self.client.clone(),
                    options,
                    stream_name,
                    options.max_count as u64,
                )
                .await?;

                match stream.next().await {
                    Ok(event) => {
                        let event = event.expect("to be defined");
                        let metadata = event
                            .get_original_event()
                            .as_json::<StreamMetadata>()
                            .map_err(|e| crate::Error::InternalParsingError(e.to_string()))?;

                        let metadata = VersionedMetadata {
                            stream: event.get_original_stream_id().to_string(),
                            version: event.get_original_event().revision,
                            metadata,
                        };

                        Ok(StreamMetadataResult::Success(Box::new(metadata)))
                    }
                    Err(e) => match e {
                        crate::Error::ResourceNotFound => Ok(StreamMetadataResult::NotFound),
                        crate::Error::ResourceDeleted => Ok(StreamMetadataResult::Deleted),
                        other => Err(other),
                    },
                }
            },
        )
        .await
    }

    /// Soft deletes a given stream.
    /// Makes use of Truncate before. When a stream is deleted, its Truncate
    /// before is set to the streams current last event number. When a soft
    /// deleted stream is read, the read will return a StreamNotFound. After
    /// deleting the stream, you are able to write to it again, continuing from
    /// where it left off.
    pub async fn delete_stream(
        &self,
        stream_name: impl StreamName,
        options: &DeleteStreamOptions,
    ) -> crate::Result<Option<Position>> {
        let stream_name = stream_name.into_stream_name();
        client_operation(
            operation::DELETE_STREAM.on_stream(&stream_name),
            commands::delete_stream(&self.client, stream_name, options),
        )
        .await
    }

    /// Hard deletes a given stream.
    /// A hard delete writes a tombstone event to the stream, permanently
    /// deleting it. The stream cannot be recreated or written to again.
    /// Tombstone events are written with the event type '$streamDeleted'. When
    /// a hard deleted stream is read, the read will return a StreamDeleted.
    pub async fn tombstone_stream(
        &self,
        stream_name: impl StreamName,
        options: &TombstoneStreamOptions,
    ) -> crate::Result<Option<Position>> {
        let stream_name = stream_name.into_stream_name();
        client_operation(
            operation::TOMBSTONE_STREAM.on_stream(&stream_name),
            commands::tombstone_stream(&self.client, stream_name, options),
        )
        .await
    }

    /// Subscribes to a given stream. This kind of subscription specifies a
    /// starting point (by default, the beginning of a stream). For a regular
    /// stream, that starting point will be an event number. For the system
    /// stream `$all`, it will be a position in the transaction file
    /// (see [`subscribe_to_all`]). This subscription will fetch every event
    /// until the end of the stream, then will dispatch subsequently written
    /// events.
    ///
    /// For example, if a starting point of 50 is specified when a stream has
    /// 100 events in it, the subscriber can expect to see events 51 through
    /// 100, and then any events subsequently written events until such time
    /// as the subscription is dropped or closed.
    ///
    /// [`subscribe_to_all`]: #method.subscribe_to_all_from
    pub async fn subscribe_to_stream(
        &self,
        stream_name: impl StreamName,
        options: &SubscribeToStreamOptions,
    ) -> Subscription {
        let stream_name = stream_name.into_stream_name();
        infallible_client_operation(
            operation::SUBSCRIBE_TO_STREAM.on_stream(&stream_name),
            async { commands::subscribe_to_stream(self.client.clone(), stream_name, options) },
        )
        .await
    }

    /// Like [`subscribe_to_stream`] but specific to system `$all` stream.
    ///
    /// [`subscribe_to_stream`]: #method.subscribe_to_stream
    pub async fn subscribe_to_all(&self, options: &SubscribeToAllOptions) -> Subscription {
        infallible_client_operation(operation::SUBSCRIBE_TO_ALL.on_collection("$all"), async {
            commands::subscribe_to_all(self.client.clone(), options)
        })
        .await
    }

    /// Creates a persistent subscription group on a stream.
    ///
    /// Persistent subscriptions are special kind of subscription where the
    /// server remembers the state of the subscription. This allows for many
    /// different modes of operations compared to a regular or catchup
    /// subscription where the client holds the subscription state.
    pub async fn create_persistent_subscription(
        &self,
        stream_name: impl StreamName,
        group_name: impl AsRef<str>,
        options: &PersistentSubscriptionOptions,
    ) -> crate::Result<()> {
        let stream_name = stream_name.into_stream_name();
        client_operation(
            operation::CREATE_PERSISTENT_SUBSCRIPTION.on_stream(&stream_name),
            commands::create_persistent_subscription(
                &self.client,
                stream_name,
                group_name.as_ref(),
                options,
            ),
        )
        .await
    }

    /// Creates a persistent subscription group on a the $all stream.
    pub async fn create_persistent_subscription_to_all(
        &self,
        group_name: impl AsRef<str>,
        options: &PersistentSubscriptionToAllOptions,
    ) -> crate::Result<()> {
        client_operation(
            operation::CREATE_PERSISTENT_SUBSCRIPTION_TO_ALL.on_collection("$all"),
            commands::create_persistent_subscription(
                &self.client,
                "",
                group_name.as_ref(),
                options,
            ),
        )
        .await
    }

    /// Updates a persistent subscription group on a stream.
    pub async fn update_persistent_subscription(
        &self,
        stream_name: impl StreamName,
        group_name: impl AsRef<str>,
        options: &PersistentSubscriptionOptions,
    ) -> crate::Result<()> {
        let stream_name = stream_name.into_stream_name();
        client_operation(
            operation::UPDATE_PERSISTENT_SUBSCRIPTION.on_stream(&stream_name),
            commands::update_persistent_subscription(
                &self.client,
                stream_name,
                group_name.as_ref(),
                options,
            ),
        )
        .await
    }

    /// Updates a persistent subscription group to $all.
    pub async fn update_persistent_subscription_to_all(
        &self,
        group_name: impl AsRef<str>,
        options: &PersistentSubscriptionToAllOptions,
    ) -> crate::Result<()> {
        client_operation(
            operation::UPDATE_PERSISTENT_SUBSCRIPTION_TO_ALL.on_collection("$all"),
            commands::update_persistent_subscription(
                &self.client,
                "",
                group_name.as_ref(),
                options,
            ),
        )
        .await
    }

    /// Deletes a persistent subscription group on a stream.
    pub async fn delete_persistent_subscription(
        &self,
        stream_name: impl StreamName,
        group_name: impl AsRef<str>,
        options: &DeletePersistentSubscriptionOptions,
    ) -> crate::Result<()> {
        let stream_name = stream_name.into_stream_name();
        client_operation(
            operation::DELETE_PERSISTENT_SUBSCRIPTION.on_stream(&stream_name),
            commands::delete_persistent_subscription(
                &self.client,
                stream_name,
                group_name.as_ref(),
                options,
                false,
            ),
        )
        .await
    }

    /// Deletes a persistent subscription group on the $all stream.
    pub async fn delete_persistent_subscription_to_all(
        &self,
        group_name: impl AsRef<str>,
        options: &DeletePersistentSubscriptionOptions,
    ) -> crate::Result<()> {
        client_operation(
            operation::DELETE_PERSISTENT_SUBSCRIPTION_TO_ALL.on_collection("$all"),
            commands::delete_persistent_subscription(
                &self.client,
                "",
                group_name.as_ref(),
                options,
                true,
            ),
        )
        .await
    }

    /// Connects to a persistent subscription group on a stream.
    pub async fn subscribe_to_persistent_subscription(
        &self,
        stream_name: impl StreamName,
        group_name: impl AsRef<str>,
        options: &SubscribeToPersistentSubscriptionOptions,
    ) -> crate::Result<PersistentSubscription> {
        let stream_name = stream_name.into_stream_name();
        client_operation(
            operation::SUBSCRIBE_TO_PERSISTENT_SUBSCRIPTION.on_stream(&stream_name),
            commands::subscribe_to_persistent_subscription(
                &self.client,
                stream_name,
                group_name.as_ref(),
                options,
                false,
            ),
        )
        .await
    }

    /// Connects to a persistent subscription group to $all stream.
    pub async fn subscribe_to_persistent_subscription_to_all(
        &self,
        group_name: impl AsRef<str>,
        options: &SubscribeToPersistentSubscriptionOptions,
    ) -> crate::Result<PersistentSubscription> {
        client_operation(
            operation::SUBSCRIBE_TO_PERSISTENT_SUBSCRIPTION_TO_ALL.on_collection("$all"),
            commands::subscribe_to_persistent_subscription(
                &self.client,
                "",
                group_name.as_ref(),
                options,
                true,
            ),
        )
        .await
    }

    /// Replays a persistent subscriptions parked events.
    pub async fn replay_parked_messages(
        &self,
        stream_name: impl AsRef<str>,
        group_name: impl AsRef<str>,
        options: &ReplayParkedMessagesOptions,
    ) -> crate::Result<()> {
        let stream_name = stream_name.as_ref().to_string();
        client_operation(
            operation::REPLAY_PARKED_MESSAGES.on_collection(stream_name.clone()),
            commands::replay_parked_messages(
                &self.client,
                &self.http_client,
                commands::RegularStream(stream_name),
                group_name,
                options,
            ),
        )
        .await
    }

    /// Replays a persistent subscriptions to $all parked events.
    pub async fn replay_parked_messages_to_all(
        &self,
        group_name: impl AsRef<str>,
        options: &ReplayParkedMessagesOptions,
    ) -> crate::Result<()> {
        client_operation(
            operation::REPLAY_PARKED_MESSAGES_TO_ALL.on_collection("$all"),
            commands::replay_parked_messages(
                &self.client,
                &self.http_client,
                commands::AllStream,
                group_name,
                options,
            ),
        )
        .await
    }

    /// Lists all persistent subscriptions to date.
    pub async fn list_all_persistent_subscriptions(
        &self,
        options: &ListPersistentSubscriptionsOptions,
    ) -> crate::Result<Vec<PersistentSubscriptionInfo<RevisionOrPosition>>> {
        client_operation(
            operation::LIST_ALL_PERSISTENT_SUBSCRIPTIONS,
            commands::list_all_persistent_subscriptions(&self.client, &self.http_client, options),
        )
        .await
    }

    /// List all persistent subscriptions of a specific stream.
    pub async fn list_persistent_subscriptions_for_stream(
        &self,
        stream_name: impl AsRef<str>,
        options: &ListPersistentSubscriptionsOptions,
    ) -> crate::Result<Vec<PersistentSubscriptionInfo<u64>>> {
        let stream_name = stream_name.as_ref().to_string();
        client_operation(
            operation::LIST_PERSISTENT_SUBSCRIPTIONS_FOR_STREAM.on_collection(stream_name.clone()),
            commands::list_persistent_subscriptions_for_stream(
                &self.client,
                &self.http_client,
                commands::RegularStream(stream_name),
                options,
            ),
        )
        .await
    }

    /// List all persistent subscriptions of the $all stream.
    pub async fn list_persistent_subscriptions_to_all(
        &self,
        options: &ListPersistentSubscriptionsOptions,
    ) -> crate::Result<Vec<PersistentSubscriptionInfo<Position>>> {
        client_operation(
            operation::LIST_PERSISTENT_SUBSCRIPTIONS_TO_ALL.on_collection("$all"),
            commands::list_persistent_subscriptions_for_stream(
                &self.client,
                &self.http_client,
                commands::AllStream,
                options,
            ),
        )
        .await
    }

    // Gets a specific persistent subscription info.
    pub async fn get_persistent_subscription_info(
        &self,
        stream_name: impl AsRef<str>,
        group_name: impl AsRef<str>,
        options: &GetPersistentSubscriptionInfoOptions,
    ) -> crate::Result<PersistentSubscriptionInfo<u64>> {
        let stream_name = stream_name.as_ref().to_string();
        client_operation(
            operation::GET_PERSISTENT_SUBSCRIPTION_INFO.on_collection(stream_name.clone()),
            commands::get_persistent_subscription_info(
                &self.client,
                &self.http_client,
                commands::RegularStream(stream_name),
                group_name,
                options,
            ),
        )
        .await
    }

    // Gets a specific persistent subscription info to $all.
    pub async fn get_persistent_subscription_info_to_all(
        &self,
        group_name: impl AsRef<str>,
        options: &GetPersistentSubscriptionInfoOptions,
    ) -> crate::Result<PersistentSubscriptionInfo<Position>> {
        client_operation(
            operation::GET_PERSISTENT_SUBSCRIPTION_INFO_TO_ALL.on_collection("$all"),
            commands::get_persistent_subscription_info(
                &self.client,
                &self.http_client,
                commands::AllStream,
                group_name,
                options,
            ),
        )
        .await
    }

    // Restarts the server persistent subscription subsystem.
    pub async fn restart_persistent_subscription_subsystem(
        &self,
        options: &RestartPersistentSubscriptionSubsystem,
    ) -> crate::Result<()> {
        client_operation(
            operation::RESTART_PERSISTENT_SUBSCRIPTION_SUBSYSTEM,
            commands::restart_persistent_subscription_subsystem(
                &self.client,
                &self.http_client,
                options,
            ),
        )
        .await
    }
}
