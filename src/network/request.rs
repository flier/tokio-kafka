use std::borrow::Cow;
use std::time::Duration;
use std::collections::HashMap;

use bytes::{ByteOrder, BytesMut};

use errors::Result;
use protocol::{ApiKey, ApiKeys, ApiVersion, ApiVersionsRequest, CorrelationId, Encodable,
               FetchOffset, FetchRequest, ListOffsetRequest, ListPartitionOffset, ListTopicOffset,
               MessageSet, MetadataRequest, PartitionId, ProducePartition, ProduceRequest,
               ProduceTopic, RequestHeader, RequiredAck, RequiredAcks};

#[derive(Debug)]
pub enum KafkaRequest<'a> {
    Produce(ProduceRequest<'a>),
    Fetch(FetchRequest<'a>),
    ListOffsets(ListOffsetRequest<'a>),
    Metadata(MetadataRequest<'a>),
    ApiVersions(ApiVersionsRequest<'a>),
}

impl<'a> KafkaRequest<'a> {
    pub fn header(&self) -> &RequestHeader {
        match self {
            &KafkaRequest::Produce(ref req) => &req.header,
            &KafkaRequest::Fetch(ref req) => &req.header,
            &KafkaRequest::ListOffsets(ref req) => &req.header,
            &KafkaRequest::Metadata(ref req) => &req.header,
            &KafkaRequest::ApiVersions(ref req) => &req.header,
        }
    }

    pub fn produce_records(api_version: ApiVersion,
                           correlation_id: CorrelationId,
                           client_id: Option<Cow<'a, str>>,
                           required_acks: RequiredAcks,
                           timeout: Duration,
                           records: Vec<(&'a str, Vec<(PartitionId, MessageSet)>)>)
                           -> KafkaRequest<'a> {
        let request = ProduceRequest {
            header: RequestHeader {
                api_key: ApiKeys::Produce as ApiKey,
                api_version: api_version,
                correlation_id: correlation_id,
                client_id: client_id,
            },
            required_acks: required_acks as RequiredAck,
            timeout: timeout.as_secs() as i32 * 1000 + timeout.subsec_nanos() as i32 / 1000_000,
            topics: records
                .into_iter()
                .map(|(topic_name, partitions)| {
                    ProduceTopic {
                        topic_name: topic_name,
                        partitions: partitions
                            .into_iter()
                            .map(|(partition, message_set)| {
                                     ProducePartition {
                                         partition: partition,
                                         message_set: message_set,
                                     }
                                 })
                            .collect(),
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
                .map(|s| s.as_ref().to_owned())
                .collect(),
        };

        KafkaRequest::Metadata(request)
    }
}

impl<'a> Encodable for KafkaRequest<'a> {
    fn encode<T: ByteOrder>(self, dst: &mut BytesMut) -> Result<()> {
        match self {
            KafkaRequest::Produce(req) => req.encode::<T>(dst),
            KafkaRequest::Fetch(req) => req.encode::<T>(dst),
            KafkaRequest::ListOffsets(req) => req.encode::<T>(dst),
            KafkaRequest::Metadata(req) => req.encode::<T>(dst),
            KafkaRequest::ApiVersions(req) => req.encode::<T>(dst),
        }
    }
}