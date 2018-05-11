#![allow(unused_variables)]

use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;
use std::usize;

use bytes::Bytes;
use typemap::{Key, TypeMap};

use tokio_core::reactor::Handle;

use client::{Broker, BrokerRef, Client, Cluster, ConsumerGroup, ConsumerGroupAssignment, ConsumerGroupProtocol,
             FetchRecords, Generation, GetMetadata, GroupCoordinator, Heartbeat, JoinGroup, LeaveGroup, ListOffsets,
             LoadMetadata, Metadata, OffsetCommit, OffsetFetch, PartitionData, ProduceRecords, SyncGroup,
             ToStaticBoxFuture};
use consumer::Assignment;
use errors::{ErrorKind, Result};
use network::{OffsetAndMetadata, TopicPartition};
use protocol::{FetchOffset, IsolationLevel, KafkaCode, Message, RequiredAcks, Schema};

#[derive(Clone)]
pub struct MockClient<'a> {
    handle: Option<Handle>,
    metadata: Rc<Metadata>,
    group_coordinators: HashMap<Cow<'a, str>, Broker>,
    consumer_groups: HashMap<Cow<'a, str>, ConsumerGroup>,
    member_assignments: HashMap<Cow<'a, str>, Assignment<'a>>,
    future_responses: Rc<RefCell<TypeMap>>,
}

impl<'a> MockClient<'a> {
    pub fn new() -> Self {
        MockClient::with_metadata(Metadata::default())
    }

    pub fn with_metadata(metadata: Metadata) -> Self {
        MockClient {
            handle: None,
            metadata: Rc::new(metadata),
            group_coordinators: HashMap::new(),
            consumer_groups: HashMap::new(),
            member_assignments: HashMap::new(),
            future_responses: Rc::new(RefCell::new(TypeMap::custom())),
        }
    }

    pub fn with_handle(mut self, handle: Handle) -> Self {
        self.handle = Some(handle);
        self
    }

    pub fn with_group_coordinator(mut self, group_id: Cow<'a, str>, broker: Broker) -> Self {
        self.group_coordinators.insert(group_id, broker);
        self
    }

    pub fn with_consumer_group(mut self, consumer_group: ConsumerGroup) -> Self {
        let group_id = consumer_group.group_id.clone().into();

        self.consumer_groups.insert(group_id, consumer_group);
        self
    }

    pub fn with_group_member_as_leader(
        mut self,
        member_id: Cow<'a, str>,
        partitions: &[TopicPartition<'a>],
        user_data: Option<Cow<'a, [u8]>>,
    ) -> Self {
        self.member_assignments.insert(
            member_id,
            Assignment {
                partitions: partitions.to_vec(),
                user_data,
            },
        );
        self
    }

    pub fn with_group_member_as_follower(mut self, member_id: Cow<'a, str>) -> Self {
        self.member_assignments.insert(member_id, Assignment::default());
        self
    }

    pub fn with_future_response<T, F>(self, callback: F) -> Self
    where
        T: Key<Value = F>,
        F: 'static,
    {
        self.future_responses.borrow_mut().insert::<T>(callback);
        self
    }
}

impl Key for GroupCoordinator {
    type Value = Box<Fn(String) -> Result<Broker>>;
}

impl<'a> Client<'a> for MockClient<'a>
where
    Self: 'static,
{
    fn handle(&self) -> &Handle {
        &self.handle.as_ref().expect("should attach event loop with `with_core`")
    }

    fn metadata(&self) -> GetMetadata {
        GetMetadata::Loaded(self.metadata.clone())
    }

    fn retry_strategy(&self) -> Vec<Duration> {
        unimplemented!()
    }

    fn produce_records<I, M>(
        &self,
        acks: RequiredAcks,
        timeout: Duration,
        topic_partition: TopicPartition<'a>,
        records: I,
    ) -> ProduceRecords
    where
        I: IntoIterator<Item = M>,
        M: Into<Message>,
    {
        unimplemented!()
    }

    fn fetch_records(
        &self,
        fetch_max_wait: Duration,
        fetch_min_bytes: usize,
        fetch_max_bytes: usize,
        isolation_leve: IsolationLevel,
        partitions: Vec<(TopicPartition<'a>, PartitionData)>,
    ) -> FetchRecords {
        unimplemented!()
    }

    fn list_offsets<I>(&self, partitions: I) -> ListOffsets
    where
        I: IntoIterator<Item = (TopicPartition<'a>, FetchOffset)>,
    {
        unimplemented!()
    }

    fn load_metadata(&mut self) -> LoadMetadata<'a> {
        unimplemented!()
    }

    fn offset_commit<I>(
        &self,
        coordinator: Option<BrokerRef>,
        generation: Option<Generation>,
        retention_time: Option<Duration>,
        offsets: I,
    ) -> OffsetCommit
    where
        I: IntoIterator<Item = (TopicPartition<'a>, OffsetAndMetadata)>,
    {
        unimplemented!()
    }

    fn offset_fetch<I>(&self, coordinator: BrokerRef, generation: Generation, partitions: I) -> OffsetFetch
    where
        I: IntoIterator<Item = TopicPartition<'a>>,
    {
        unimplemented!()
    }

    fn group_coordinator(&self, group_id: Cow<'a, str>) -> GroupCoordinator {
        if let Some(callback) = self.future_responses.borrow_mut().remove::<GroupCoordinator>() {
            callback(String::from(group_id)).static_boxed()
        } else {
            let metadata = self.metadata.clone();

            self.group_coordinators
                .get(&group_id)
                .cloned()
                .ok_or_else(|| ErrorKind::KafkaError(KafkaCode::CoordinatorNotAvailable).into())
                .static_boxed()
        }
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
        if self.metadata.find_broker(coordinator).is_none() {
            Err(ErrorKind::KafkaError(KafkaCode::CoordinatorNotAvailable).into())
        } else if let Some(consumer_group) = self.consumer_groups.get(&group_id) {
            if group_protocols
                .iter()
                .all(|protocol| protocol.protocol_name != consumer_group.protocol)
            {
                Err(ErrorKind::KafkaError(KafkaCode::InconsistentGroupProtocol).into())
            } else {
                Ok(consumer_group.clone())
            }
        } else {
            Err(ErrorKind::KafkaError(KafkaCode::NotCoordinator).into())
        }.static_boxed()
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
        let group_id: Cow<'a, str> = generation.group_id.clone().into();
        let member_id: Cow<'a, str> = generation.member_id.clone().into();

        if self.metadata.find_broker(coordinator).is_none() {
            Err(ErrorKind::KafkaError(KafkaCode::CoordinatorNotAvailable).into())
        } else if let Some(consumer_group) = self.consumer_groups.get(&group_id) {
            if consumer_group.generation_id != generation.generation_id {
                Err(ErrorKind::KafkaError(KafkaCode::IllegalGeneration).into())
            } else if let Some(assignment) = self.member_assignments.get(&member_id) {
                Ok(Bytes::from(Schema::serialize(assignment).unwrap()))
            } else {
                Err(ErrorKind::KafkaError(KafkaCode::UnknownMemberId).into())
            }
        } else {
            Err(ErrorKind::KafkaError(KafkaCode::NotCoordinator).into())
        }.static_boxed()
    }
}
