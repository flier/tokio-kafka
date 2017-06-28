#![allow(unused_variables)]

use std::borrow::Cow;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;
use std::usize;

use tokio_core::reactor::Handle;

use client::{Broker, BrokerRef, Client, ConsumerGroupAssignment, ConsumerGroupProtocol,
             FetchRecords, Generation, GetMetadata, GroupCoordinator, Heartbeat, JoinGroup,
             LeaveGroup, ListOffsets, LoadMetadata, Metadata, OffsetCommit, OffsetFetch,
             PartitionData, ProduceRecords, SyncGroup, ToStaticBoxFuture};
use errors::ErrorKind;
use network::{OffsetAndMetadata, TopicPartition};
use protocol::{FetchOffset, KafkaCode, MessageSet, RequiredAcks};

#[derive(Clone, Debug)]
pub struct MockClient<'a> {
    pub metadata: Rc<Metadata>,
    pub group_coordinators: HashMap<Cow<'a, str>, Broker>,
}

impl<'a> MockClient<'a> {
    pub fn new() -> Self {
        MockClient {
            metadata: Rc::new(Metadata::default()),
            group_coordinators: HashMap::new(),
        }
    }

    pub fn with_metadata(metadata: Metadata) -> Self {
        MockClient {
            metadata: Rc::new(metadata),
            group_coordinators: HashMap::new(),
        }
    }
}

impl<'a> Client<'a> for MockClient<'a>
where
    Self: 'static,
{
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
        let metadata = self.metadata.clone();

        self.group_coordinators
            .get(&group_id)
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
