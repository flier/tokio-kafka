#![allow(unused_variables)]

use std::borrow::Cow;
use std::rc::Rc;
use std::time::Duration;
use std::usize;

use tokio_core::reactor::Handle;

use client::{Broker, BrokerRef, Client, ClientBuilder, ClientConfig, Cluster,
             ConsumerGroupAssignment, ConsumerGroupProtocol, FetchRecords, Generation,
             GetMetadata, GroupCoordinator, Heartbeat, InFlightMiddleware, JoinGroup,
             KafkaService, LeaveGroup, ListOffsets, LoadMetadata, Metadata, Metrics, OffsetCommit,
             OffsetFetch, PartitionData, ProduceRecords, SyncGroup, ToStaticBoxFuture};
use errors::ErrorKind;
use network::{OffsetAndMetadata, TopicPartition};
use protocol::{ApiKeys, ApiVersion, CorrelationId, DEFAULT_RESPONSE_MAX_BYTES, ErrorCode,
               FetchOffset, FetchPartition, FetchTopic, FetchTopicData, GenerationId,
               JoinGroupMember, JoinGroupProtocol, KafkaCode, Message, MessageSet, Offset,
               PartitionId, RequiredAcks, SyncGroupAssignment, Timestamp, UsableApiVersions};

#[derive(Clone, Debug)]
pub struct MockClient {
    pub metadata: Rc<Metadata>,
}

impl MockClient {
    pub fn new() -> Self {
        MockClient { metadata: Rc::new(Metadata::default()) }
    }

    pub fn with_metadata(metadata: Metadata) -> Self {
        MockClient { metadata: Rc::new(metadata) }
    }
}

impl<'a> Client<'a> for MockClient {
    fn handle(&self) -> &Handle {
        unimplemented!()
    }

    fn metadata(&self) -> GetMetadata {
        unimplemented!()
    }

    fn retry_strategy(&self) -> Vec<Duration> {
        unimplemented!()
    }

    fn produce_records(
        &self,
        acks: RequiredAcks,
        timeout: Duration,
        topic_partition: TopicPartition<'a>,
        records: Vec<Cow<'a, MessageSet>>,
    ) -> ProduceRecords {
        unimplemented!()
    }

    fn fetch_records(
        &self,
        fetch_max_wait: Duration,
        fetch_min_bytes: usize,
        fetch_max_bytes: usize,
        partitions: Vec<(TopicPartition<'a>, PartitionData)>,
    ) -> FetchRecords {
        unimplemented!()
    }

    fn list_offsets(&self, partitions: Vec<(TopicPartition<'a>, FetchOffset)>) -> ListOffsets {
        unimplemented!()
    }

    fn load_metadata(&mut self) -> LoadMetadata<'a> {
        unimplemented!()
    }

    fn offset_commit(
        &self,
        coordinator: BrokerRef,
        generation: Generation,
        retention_time: Option<Duration>,
        offsets: Vec<(TopicPartition<'a>, OffsetAndMetadata)>,
    ) -> OffsetCommit {
        unimplemented!()
    }

    fn offset_fetch(
        &self,
        coordinator: BrokerRef,
        generation: Generation,
        partitions: Vec<TopicPartition<'a>>,
    ) -> OffsetFetch {
        unimplemented!()
    }

    fn group_coordinator(&self, group_id: Cow<'a, str>) -> GroupCoordinator {
        self.metadata
            .brokers()
            .first()
            .cloned()
            .ok_or_else(|| {
                ErrorKind::KafkaError(KafkaCode::GroupCoordinatorNotAvailable.into()).into()
            })
            .static_boxed()
    }

    fn join_group(
        &self,
        coordinator: BrokerRef,
        group_id: Cow<'a, str>,
        session_timeout: i32,
        rebalance_timeout: i32,
        member_id: Cow<'a, str>,
        protocol_type: Cow<'a, str>,
        group_protocols: Vec<ConsumerGroupProtocol<'a>>,
    ) -> JoinGroup {
        unimplemented!()
    }

    fn heartbeat(&self, coordinator: BrokerRef, generation: Generation) -> Heartbeat {
        unimplemented!()
    }

    fn leave_group(&self, coordinator: BrokerRef, generation: Generation) -> LeaveGroup {
        unimplemented!()
    }

    fn sync_group(
        &self,
        coordinator: BrokerRef,
        generation: Generation,
        group_assignment: Option<Vec<ConsumerGroupAssignment<'a>>>,
    ) -> SyncGroup {
        unimplemented!()
    }
}
