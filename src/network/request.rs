use std::borrow::Cow;
use std::time::Duration;
use std::collections::HashMap;

use bytes::{ByteOrder, BytesMut};

use errors::Result;
use protocol::{ApiKey, ApiKeys, ApiVersion, ApiVersionsRequest, CorrelationId, Encodable,
               FetchOffset, FetchRequest, GroupCoordinatorRequest, ListOffsetRequest,
               ListPartitionOffset, ListTopicOffset, MessageSet, MetadataRequest,
               OffsetCommitRequest, OffsetFetchRequest, PartitionId, ProducePartitionData,
               ProduceRequest, ProduceTopicData, Record, RequestHeader, RequiredAck, RequiredAcks,
               ToMilliseconds};

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
                                         -> Self {
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

    pub fn fetch_api_versions(api_version: ApiVersion,
                              correlation_id: CorrelationId,
                              client_id: Option<Cow<'a, str>>)
                              -> Self {
        let request = ApiVersionsRequest {
            header: RequestHeader {
                api_key: ApiKeys::ApiVersions as ApiKey,
                api_version: api_version,
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
            KafkaRequest::ApiVersions(ref req) => req.encode::<T>(dst),
        }
    }
}
