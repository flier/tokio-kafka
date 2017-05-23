use std::borrow::Cow;
use std::time::Duration;
use std::collections::HashMap;

use bytes::{ByteOrder, BytesMut};

use errors::Result;
use protocol::{ApiKey, ApiKeys, ApiVersion, ApiVersionsRequest, CorrelationId,
               DescribeGroupsRequest, Encodable, FetchOffset, FetchRequest, GenerationId,
               GroupCoordinatorRequest, HeartbeatRequest, JoinGroupRequest, LeaveGroupRequest,
               ListGroupsRequest, ListOffsetRequest, ListPartitionOffset, ListTopicOffset,
               MessageSet, MetadataRequest, OffsetCommitRequest, OffsetFetchRequest, PartitionId,
               ProducePartitionData, ProduceRequest, ProduceTopicData, Record, RequestHeader,
               RequiredAck, RequiredAcks, SyncGroupRequest, ToMilliseconds};

/// A topic name and partition number
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct TopicPartition<'a> {
    pub topic_name: Cow<'a, str>,
    pub partition: PartitionId,
}

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

    pub fn produce_records(api_version: ApiVersion,
                           correlation_id: CorrelationId,
                           client_id: Option<Cow<'a, str>>,
                           required_acks: RequiredAcks,
                           ack_timeout: Duration,
                           tp: &TopicPartition<'a>,
                           records: Vec<Cow<'a, MessageSet>>)
                           -> KafkaRequest<'a> {
        let request = ProduceRequest {
            header: RequestHeader {
                api_key: ApiKeys::Produce as ApiKey,
                api_version: api_version,
                correlation_id: correlation_id,
                client_id: client_id,
            },
            required_acks: required_acks as RequiredAck,
            ack_timeout: ack_timeout.as_millis() as i32,
            topics: records
                .into_iter()
                .map(move |message_set| {
                    ProduceTopicData {
                        topic_name: tp.topic_name.to_owned().into(),
                        partitions: vec![ProducePartitionData {
                                             partition: tp.partition,
                                             message_set: message_set,
                                         }],
                    }
                })
                .collect(),
        };

        KafkaRequest::Produce(request)
    }

    pub fn list_offsets(api_version: ApiVersion,
                        correlation_id: CorrelationId,
                        client_id: Option<Cow<'a, str>>,
                        topics: HashMap<Cow<'a, str>, Vec<PartitionId>>,
                        offset: FetchOffset)
                        -> KafkaRequest<'a> {
        let topics = topics
            .iter()
            .map(|(topic_name, partitions)| {
                ListTopicOffset {
                    topic_name: topic_name.clone(),
                    partitions: partitions
                        .iter()
                        .map(|&id| {
                                 ListPartitionOffset {
                                     partition: id,
                                     timestamp: offset.into(),
                                     max_number_of_offsets: 16,
                                 }
                             })
                        .collect(),
                }
            })
            .collect();

        let request = ListOffsetRequest {
            header: RequestHeader {
                api_key: ApiKeys::ListOffsets as ApiKey,
                api_version: api_version,
                correlation_id: correlation_id,
                client_id: client_id,
            },
            replica_id: -1,
            topics: topics,
        };

        KafkaRequest::ListOffsets(request)
    }

    pub fn fetch_metadata<S: AsRef<str>>(api_version: ApiVersion,
                                         correlation_id: CorrelationId,
                                         client_id: Option<Cow<'a, str>>,
                                         topic_names: &[S])
                                         -> KafkaRequest<'a> {
        let request = MetadataRequest {
            header: RequestHeader {
                api_key: ApiKeys::Metadata as ApiKey,
                api_version: api_version,
                correlation_id: correlation_id,
                client_id: client_id,
            },
            topic_names: topic_names
                .iter()
                .map(|s| Cow::from(s.as_ref().to_owned()))
                .collect(),
        };

        KafkaRequest::Metadata(request)
    }

    pub fn group_coordinator(api_version: ApiVersion,
                             correlation_id: CorrelationId,
                             client_id: Option<Cow<'a, str>>,
                             group_id: Cow<'a, str>)
                             -> KafkaRequest<'a> {
        let request = GroupCoordinatorRequest {
            header: RequestHeader {
                api_key: ApiKeys::GroupCoordinator as ApiKey,
                api_version: api_version,
                correlation_id: correlation_id,
                client_id: client_id,
            },
            group_id: group_id,
        };

        KafkaRequest::GroupCoordinator(request)
    }

    pub fn heartbeat(correlation_id: CorrelationId,
                     client_id: Option<Cow<'a, str>>,
                     group_id: Cow<'a, str>,
                     group_generation_id: GenerationId,
                     member_id: Cow<'a, str>)
                     -> KafkaRequest<'a> {
        let request = HeartbeatRequest {
            header: RequestHeader {
                api_key: ApiKeys::Heartbeat as ApiKey,
                api_version: 0,
                correlation_id: correlation_id,
                client_id: client_id,
            },
            group_id: group_id,
            group_generation_id: group_generation_id,
            member_id: member_id,
        };

        KafkaRequest::Heartbeat(request)
    }

    pub fn leave_group(correlation_id: CorrelationId,
                       client_id: Option<Cow<'a, str>>,
                       group_id: Cow<'a, str>,
                       member_id: Cow<'a, str>)
                       -> KafkaRequest<'a> {
        let request = LeaveGroupRequest {
            header: RequestHeader {
                api_key: ApiKeys::LeaveGroup as ApiKey,
                api_version: 0,
                correlation_id: correlation_id,
                client_id: client_id,
            },
            group_id: group_id,
            member_id: member_id,
        };

        KafkaRequest::LeaveGroup(request)
    }

    pub fn api_versions(correlation_id: CorrelationId,
                        client_id: Option<Cow<'a, str>>)
                        -> KafkaRequest<'a> {
        let request = ApiVersionsRequest {
            header: RequestHeader {
                api_key: ApiKeys::ApiVersions as ApiKey,
                api_version: 0,
                correlation_id: correlation_id,
                client_id: client_id,
            },
        };

        KafkaRequest::ApiVersions(request)
    }
}

impl<'a> Record for KafkaRequest<'a> {
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
    fn encode<T: ByteOrder>(&self, dst: &mut BytesMut) -> Result<()> {
        match *self {
            KafkaRequest::Produce(ref req) => req.encode::<T>(dst),
            KafkaRequest::Fetch(ref req) => req.encode::<T>(dst),
            KafkaRequest::ListOffsets(ref req) => req.encode::<T>(dst),
            KafkaRequest::Metadata(ref req) => req.encode::<T>(dst),
            KafkaRequest::OffsetCommit(ref req) => req.encode::<T>(dst),
            KafkaRequest::OffsetFetch(ref req) => req.encode::<T>(dst),
            KafkaRequest::GroupCoordinator(ref req) => req.encode::<T>(dst),
            KafkaRequest::JoinGroup(ref req) => req.encode::<T>(dst),
            KafkaRequest::Heartbeat(ref req) => req.encode::<T>(dst),
            KafkaRequest::LeaveGroup(ref req) => req.encode::<T>(dst),
            KafkaRequest::SyncGroup(ref req) => req.encode::<T>(dst),
            KafkaRequest::DescribeGroups(ref req) => req.encode::<T>(dst),
            KafkaRequest::ListGroups(ref req) => req.encode::<T>(dst),
            KafkaRequest::ApiVersions(ref req) => req.encode::<T>(dst),
        }
    }
}
