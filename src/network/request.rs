use bytes::{BytesMut, ByteOrder};

use errors::Result;
use protocol::{ApiKeys, FetchOffset, Encodable, RequestHeader, ProduceRequest, FetchRequest,
               ListOffsetRequest, ListTopicOffset, ListPartitionOffset, MetadataRequest,
               ApiVersionsRequest};

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

    pub fn list_offsets<'a, I, S, P>(api_version: i16,
                                     correlation_id: i32,
                                     client_id: Option<String>,
                                     topics: I,
                                     offset: FetchOffset)
                                     -> Self
        where I: Iterator<Item = (S, P)>,
              S: AsRef<str>,
              P: AsRef<[i32]>
    {
        let topics = topics
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
                                          api_key: ApiKeys::ListOffsets as i16,
                                          api_version: api_version,
                                          correlation_id: correlation_id,
                                          client_id: client_id,
                                      },
                                      replica_id: -1,
                                      topics: topics,
                                  })
    }

    pub fn fetch_metadata<S: AsRef<str>>(api_version: i16,
                                         correlation_id: i32,
                                         client_id: Option<String>,
                                         topic_names: &[S])
                                         -> Self {
        KafkaRequest::Metadata(MetadataRequest {
                                   header: RequestHeader {
                                       api_key: ApiKeys::Metadata as i16,
                                       api_version: api_version as i16,
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