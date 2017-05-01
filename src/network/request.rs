use bytes::{ByteOrder, BytesMut};

use errors::Result;
use protocol::{ApiKey, ApiKeys, ApiVersion, ApiVersionsRequest, CorrelationId, Encodable,
               FetchOffset, FetchRequest, ListOffsetRequest, ListPartitionOffset, ListTopicOffset,
               MetadataRequest, PartitionId, ProduceRequest, RequestHeader};

#[derive(Debug)]
pub enum KafkaRequest {
    Produce(ProduceRequest),
    Fetch(FetchRequest),
    ListOffsets(ListOffsetRequest),
    Metadata(MetadataRequest),
    ApiVersions(ApiVersionsRequest),
}

impl KafkaRequest {
    pub fn header(&self) -> &RequestHeader {
        match self {
            &KafkaRequest::Produce(ref req) => &req.header,
            &KafkaRequest::Fetch(ref req) => &req.header,
            &KafkaRequest::ListOffsets(ref req) => &req.header,
            &KafkaRequest::Metadata(ref req) => &req.header,
            &KafkaRequest::ApiVersions(ref req) => &req.header,
        }
    }

    pub fn list_offsets<'a, I, S, P>(api_version: ApiVersion,
                                     correlation_id: CorrelationId,
                                     client_id: Option<String>,
                                     topics: I,
                                     offset: FetchOffset)
                                     -> Self
        where I: IntoIterator<Item = (S, P)>,
              S: AsRef<str>,
              P: AsRef<[PartitionId]>
    {
        let topics = topics
            .into_iter()
            .map(|(topic_name, partitions)| {
                ListTopicOffset {
                    topic_name: topic_name.as_ref().to_owned(),
                    partitions: partitions
                        .as_ref()
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

        KafkaRequest::ListOffsets(ListOffsetRequest {
                                      header: RequestHeader {
                                          api_key: ApiKeys::ListOffsets as ApiKey,
                                          api_version: api_version,
                                          correlation_id: correlation_id,
                                          client_id: client_id,
                                      },
                                      replica_id: -1,
                                      topics: topics,
                                  })
    }

    pub fn fetch_metadata<S: AsRef<str>>(api_version: ApiVersion,
                                         correlation_id: CorrelationId,
                                         client_id: Option<String>,
                                         topic_names: &[S])
                                         -> Self {
        KafkaRequest::Metadata(MetadataRequest {
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
                               })
    }
}

impl Encodable for KafkaRequest {
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