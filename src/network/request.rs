use std::borrow::{Cow, ToOwned};
use std::collections::HashMap;
use std::time::Duration;

use bytes::BufMut;

use errors::Result;
use network::{OffsetAndMetadata, TopicPartition};
use protocol::{ApiKey, ApiKeys, ApiVersion, ApiVersionsRequest, CorrelationId, DescribeGroupsRequest, Encodable,
               FetchOffset, FetchRequest, FetchTopic, GenerationId, GroupCoordinatorRequest, HeartbeatRequest,
               IsolationLevel, JoinGroupProtocol, JoinGroupRequest, LeaveGroupRequest, ListGroupsRequest,
               ListOffsetRequest, ListPartitionOffset, ListTopicOffset, Message, MessageSet, MetadataRequest,
               OffsetCommitPartition, OffsetCommitRequest, OffsetCommitTopic, OffsetFetchPartition,
               OffsetFetchRequest, OffsetFetchTopic, PartitionId, ProducePartitionData, ProduceRequest,
               ProduceTopicData, Request, RequestHeader, RequiredAck, RequiredAcks, SyncGroupAssignment,
               SyncGroupRequest, ToMilliseconds, CONSUMER_REPLICA_ID, DEFAULT_TIMESTAMP};

#[derive(Debug)]
pub enum KafkaRequest<'a> {
    Produce(ProduceRequest<'a>),
    Fetch(FetchRequest<'a>),
    ListOffsets(ListOffsetRequest<'a>),
    Metadata(MetadataRequest<'a>),
    OffsetCommit(OffsetCommitRequest<'a>),
    OffsetFetch(OffsetFetchRequest<'a>),
    GroupCoordinator(GroupCoordinatorRequest<'a>),
    JoinGroup(JoinGroupRequest<'a>),
    Heartbeat(HeartbeatRequest<'a>),
    LeaveGroup(LeaveGroupRequest<'a>),
    SyncGroup(SyncGroupRequest<'a>),
    DescribeGroups(DescribeGroupsRequest<'a>),
    ListGroups(ListGroupsRequest<'a>),
    ApiVersions(ApiVersionsRequest<'a>),
}

impl<'a> KafkaRequest<'a> {
    pub fn header(&self) -> &RequestHeader {
        match *self {
            KafkaRequest::Produce(ref req) => &req.header,
            KafkaRequest::Fetch(ref req) => &req.header,
            KafkaRequest::ListOffsets(ref req) => &req.header,
            KafkaRequest::Metadata(ref req) => &req.header,
            KafkaRequest::OffsetCommit(ref req) => &req.header,
            KafkaRequest::OffsetFetch(ref req) => &req.header,
            KafkaRequest::GroupCoordinator(ref req) => &req.header,
            KafkaRequest::JoinGroup(ref req) => &req.header,
            KafkaRequest::Heartbeat(ref req) => &req.header,
            KafkaRequest::LeaveGroup(ref req) => &req.header,
            KafkaRequest::SyncGroup(ref req) => &req.header,
            KafkaRequest::DescribeGroups(ref req) => &req.header,
            KafkaRequest::ListGroups(ref req) => &req.header,
            KafkaRequest::ApiVersions(ref req) => &req.header,
        }
    }

    pub fn produce_records<I, M>(
        api_version: ApiVersion,
        correlation_id: CorrelationId,
        client_id: Option<Cow<'a, str>>,
        transactional_id: Option<Cow<'a, str>>,
        required_acks: RequiredAcks,
        ack_timeout: Duration,
        tp: &TopicPartition<'a>,
        records: I,
    ) -> KafkaRequest<'a>
    where
        I: IntoIterator<Item = M>,
        M: Into<Message>,
    {
        let request = ProduceRequest {
            header: RequestHeader {
                api_key: ApiKeys::Produce as ApiKey,
                api_version,
                correlation_id,
                client_id,
            },
            transactional_id,
            required_acks: required_acks as RequiredAck,
            ack_timeout: ack_timeout.as_millis() as i32,
            topics: vec![
                ProduceTopicData {
                    topic_name: tp.topic_name.to_owned(),
                    partitions: vec![
                        ProducePartitionData {
                            partition_id: tp.partition_id,
                            message_set: Cow::Owned(MessageSet {
                                messages: records
                                    .into_iter()
                                    .map(|record| record.into())
                                    .collect::<Vec<Message>>(),
                            }),
                        },
                    ],
                },
            ],
        };

        KafkaRequest::Produce(request)
    }

    pub fn fetch_records(
        api_version: ApiVersion,
        correlation_id: CorrelationId,
        client_id: Option<Cow<'a, str>>,
        max_wait_time: Duration,
        min_bytes: i32,
        max_bytes: i32,
        isolation_level: IsolationLevel,
        topics: Vec<FetchTopic<'a>>,
    ) -> KafkaRequest<'a> {
        let request = FetchRequest {
            header: RequestHeader {
                api_key: ApiKeys::Fetch as ApiKey,
                api_version,
                correlation_id,
                client_id,
            },
            replica_id: CONSUMER_REPLICA_ID,
            max_wait_time: max_wait_time.as_millis() as i32,
            min_bytes,
            max_bytes,
            isolation_level,
            topics,
            session: None,
        };

        KafkaRequest::Fetch(request)
    }

    pub fn list_offsets(
        api_version: ApiVersion,
        correlation_id: CorrelationId,
        client_id: Option<Cow<'a, str>>,
        topics: HashMap<Cow<'a, str>, Vec<(PartitionId, FetchOffset)>>,
    ) -> KafkaRequest<'a> {
        let topics = topics
            .into_iter()
            .map(|(topic_name, partitions)| ListTopicOffset {
                topic_name: topic_name.clone(),
                partitions: partitions
                    .into_iter()
                    .map(|(id, offset)| ListPartitionOffset {
                        partition_id: id,
                        timestamp: offset.into(),
                        max_number_of_offsets: 16,
                    })
                    .collect(),
            })
            .collect();

        let request = ListOffsetRequest {
            header: RequestHeader {
                api_key: ApiKeys::ListOffsets as ApiKey,
                api_version,
                correlation_id,
                client_id,
            },
            replica_id: -1,
            topics,
        };

        KafkaRequest::ListOffsets(request)
    }

    pub fn fetch_metadata<S: AsRef<str>>(
        api_version: ApiVersion,
        correlation_id: CorrelationId,
        client_id: Option<Cow<'a, str>>,
        topic_names: &[S],
    ) -> KafkaRequest<'a> {
        let request = MetadataRequest {
            header: RequestHeader {
                api_key: ApiKeys::Metadata as ApiKey,
                api_version,
                correlation_id,
                client_id,
            },
            topic_names: topic_names.iter().map(|s| Cow::from(s.as_ref().to_owned())).collect(),
        };

        KafkaRequest::Metadata(request)
    }

    pub fn offset_commit<I>(
        api_version: ApiVersion,
        correlation_id: CorrelationId,
        client_id: Option<Cow<'a, str>>,
        group_id: Option<Cow<'a, str>>,
        group_generation_id: Option<GenerationId>,
        member_id: Option<Cow<'a, str>>,
        retention_time: Option<Duration>,
        offsets: I,
    ) -> KafkaRequest<'a>
    where
        I: IntoIterator<Item = (TopicPartition<'a>, OffsetAndMetadata)>,
    {
        let topics = offsets
            .into_iter()
            .fold(HashMap::new(), |mut topics, (tp, offset)| {
                topics
                    .entry(tp.topic_name)
                    .or_insert_with(Vec::new)
                    .push(OffsetCommitPartition {
                        partition_id: tp.partition_id,
                        offset: offset.offset,
                        timestamp: DEFAULT_TIMESTAMP,
                        metadata: offset.metadata.map(|s| s.into()),
                    });
                topics
            })
            .into_iter()
            .map(|(topic_name, partitions)| OffsetCommitTopic { topic_name, partitions })
            .collect();

        let request = OffsetCommitRequest {
            header: RequestHeader {
                api_key: ApiKeys::OffsetCommit as ApiKey,
                api_version,
                correlation_id,
                client_id,
            },
            group_id,
            group_generation_id,
            member_id,
            retention_time: retention_time.map(|t| t.as_millis() as i64),
            topics,
        };

        KafkaRequest::OffsetCommit(request)
    }

    pub fn offset_fetch<I>(
        api_version: ApiVersion,
        correlation_id: CorrelationId,
        client_id: Option<Cow<'a, str>>,
        group_id: Cow<'a, str>,
        partitions: I,
    ) -> KafkaRequest<'a>
    where
        I: IntoIterator<Item = TopicPartition<'a>>,
    {
        let topics = partitions
            .into_iter()
            .fold(HashMap::new(), |mut topics, tp| {
                topics
                    .entry(tp.topic_name)
                    .or_insert_with(Vec::new)
                    .push(OffsetFetchPartition {
                        partition_id: tp.partition_id,
                    });
                topics
            })
            .into_iter()
            .map(|(topic_name, partitions)| OffsetFetchTopic { topic_name, partitions })
            .collect();
        let request = OffsetFetchRequest {
            header: RequestHeader {
                api_key: ApiKeys::OffsetFetch as ApiKey,
                api_version,
                correlation_id,
                client_id,
            },
            group_id,
            topics,
        };

        KafkaRequest::OffsetFetch(request)
    }

    pub fn group_coordinator(
        api_version: ApiVersion,
        correlation_id: CorrelationId,
        client_id: Option<Cow<'a, str>>,
        group_id: Cow<'a, str>,
    ) -> KafkaRequest<'a> {
        let request = GroupCoordinatorRequest {
            header: RequestHeader {
                api_key: ApiKeys::GroupCoordinator as ApiKey,
                api_version,
                correlation_id,
                client_id,
            },
            group_id,
        };

        KafkaRequest::GroupCoordinator(request)
    }

    pub fn heartbeat(
        correlation_id: CorrelationId,
        client_id: Option<Cow<'a, str>>,
        group_id: Cow<'a, str>,
        group_generation_id: GenerationId,
        member_id: Cow<'a, str>,
    ) -> KafkaRequest<'a> {
        let request = HeartbeatRequest {
            header: RequestHeader {
                api_key: ApiKeys::Heartbeat as ApiKey,
                api_version: 0,
                correlation_id,
                client_id,
            },
            group_id,
            group_generation_id,
            member_id,
        };

        KafkaRequest::Heartbeat(request)
    }

    pub fn join_group(
        api_version: ApiVersion,
        correlation_id: CorrelationId,
        client_id: Option<Cow<'a, str>>,
        group_id: Cow<'a, str>,
        session_timeout: i32,
        rebalance_timeout: i32,
        member_id: Cow<'a, str>,
        protocol_type: Cow<'a, str>,
        group_protocols: Vec<JoinGroupProtocol<'a>>,
    ) -> KafkaRequest<'a> {
        let request = JoinGroupRequest {
            header: RequestHeader {
                api_key: ApiKeys::JoinGroup as ApiKey,
                api_version,
                correlation_id,
                client_id,
            },
            group_id,
            session_timeout,
            rebalance_timeout,
            member_id,
            protocol_type,
            protocols: group_protocols,
        };

        KafkaRequest::JoinGroup(request)
    }

    pub fn leave_group(
        correlation_id: CorrelationId,
        client_id: Option<Cow<'a, str>>,
        group_id: Cow<'a, str>,
        member_id: Cow<'a, str>,
    ) -> KafkaRequest<'a> {
        let request = LeaveGroupRequest {
            header: RequestHeader {
                api_key: ApiKeys::LeaveGroup as ApiKey,
                api_version: 0,
                correlation_id,
                client_id,
            },
            group_id,
            member_id,
        };

        KafkaRequest::LeaveGroup(request)
    }

    pub fn sync_group(
        correlation_id: CorrelationId,
        client_id: Option<Cow<'a, str>>,
        group_id: Cow<'a, str>,
        group_generation_id: GenerationId,
        member_id: Cow<'a, str>,
        group_assignment: Vec<SyncGroupAssignment<'a>>,
    ) -> KafkaRequest<'a> {
        let request = SyncGroupRequest {
            header: RequestHeader {
                api_key: ApiKeys::SyncGroup as ApiKey,
                api_version: 0,
                correlation_id,
                client_id,
            },
            group_id,
            group_generation_id,
            member_id,
            group_assignment: group_assignment
                .into_iter()
                .map(|assignment| SyncGroupAssignment {
                    member_id: assignment.member_id,
                    member_assignment: assignment.member_assignment,
                })
                .collect(),
        };

        KafkaRequest::SyncGroup(request)
    }

    pub fn api_versions(correlation_id: CorrelationId, client_id: Option<Cow<'a, str>>) -> KafkaRequest<'a> {
        let request = ApiVersionsRequest {
            header: RequestHeader {
                api_key: ApiKeys::ApiVersions as ApiKey,
                api_version: 0,
                correlation_id,
                client_id,
            },
        };

        KafkaRequest::ApiVersions(request)
    }
}

impl<'a> Request for KafkaRequest<'a> {
    fn size(&self, api_version: ApiVersion) -> usize {
        match *self {
            KafkaRequest::Produce(ref req) => req.size(api_version),
            KafkaRequest::Fetch(ref req) => req.size(api_version),
            KafkaRequest::ListOffsets(ref req) => req.size(api_version),
            KafkaRequest::Metadata(ref req) => req.size(api_version),
            KafkaRequest::OffsetCommit(ref req) => req.size(api_version),
            KafkaRequest::OffsetFetch(ref req) => req.size(api_version),
            KafkaRequest::GroupCoordinator(ref req) => req.size(api_version),
            KafkaRequest::JoinGroup(ref req) => req.size(api_version),
            KafkaRequest::Heartbeat(ref req) => req.size(api_version),
            KafkaRequest::LeaveGroup(ref req) => req.size(api_version),
            KafkaRequest::SyncGroup(ref req) => req.size(api_version),
            KafkaRequest::DescribeGroups(ref req) => req.size(api_version),
            KafkaRequest::ListGroups(ref req) => req.size(api_version),
            KafkaRequest::ApiVersions(ref req) => req.size(api_version),
        }
    }
}

impl<'a> Encodable for KafkaRequest<'a> {
    fn encode<T: BufMut>(&self, dst: &mut T) -> Result<()> {
        match *self {
            KafkaRequest::Produce(ref req) => req.encode(dst),
            KafkaRequest::Fetch(ref req) => req.encode(dst),
            KafkaRequest::ListOffsets(ref req) => req.encode(dst),
            KafkaRequest::Metadata(ref req) => req.encode(dst),
            KafkaRequest::OffsetCommit(ref req) => req.encode(dst),
            KafkaRequest::OffsetFetch(ref req) => req.encode(dst),
            KafkaRequest::GroupCoordinator(ref req) => req.encode(dst),
            KafkaRequest::JoinGroup(ref req) => req.encode(dst),
            KafkaRequest::Heartbeat(ref req) => req.encode(dst),
            KafkaRequest::LeaveGroup(ref req) => req.encode(dst),
            KafkaRequest::SyncGroup(ref req) => req.encode(dst),
            KafkaRequest::DescribeGroups(ref req) => req.encode(dst),
            KafkaRequest::ListGroups(ref req) => req.encode(dst),
            KafkaRequest::ApiVersions(ref req) => req.encode(dst),
        }
    }
}
