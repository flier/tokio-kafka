use bytes::{BufMut, ByteOrder, BytesMut};
use std::borrow::Cow;

use nom::{IResult, be_i16, be_i32, be_i64};

use errors::Result;
use protocol::{parse_opt_string, parse_response_header, parse_string, ApiVersion, Encodable, ErrorCode, Offset,
               ParseTag, PartitionId, Record, RequestHeader, ResponseHeader, WriteExt, ARRAY_LEN_SIZE,
               PARTITION_ID_SIZE, STR_LEN_SIZE};

#[derive(Clone, Debug, PartialEq)]
pub struct OffsetFetchRequest<'a> {
    pub header: RequestHeader<'a>,
    /// The group id.
    pub group_id: Cow<'a, str>,
    /// Topic to fetch.
    pub topics: Vec<OffsetFetchTopic<'a>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct OffsetFetchTopic<'a> {
    /// The name of the topic.
    pub topic_name: Cow<'a, str>,
    /// Partitions to fetch offset.
    pub partitions: Vec<OffsetFetchPartition>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct OffsetFetchPartition {
    /// The id of the partition the fetch is for.
    pub partition_id: PartitionId,
}

#[derive(Clone, Debug, PartialEq)]
pub struct OffsetFetchResponse {
    pub header: ResponseHeader,
    /// Topics to fetch offsets.
    pub topics: Vec<OffsetFetchTopicStatus>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct OffsetFetchTopicStatus {
    /// The name of the topic.
    pub topic_name: String,
    /// Partitions to fetch offset.
    pub partitions: Vec<OffsetFetchPartitionStatus>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct OffsetFetchPartitionStatus {
    /// The id of the partition the fetch is for.
    pub partition_id: PartitionId,
    /// Last committed message offset.
    pub offset: Offset,
    /// Any associated metadata the client wants to keep.
    pub metadata: Option<String>,
    /// Error code.
    pub error_code: ErrorCode,
}

impl<'a> Record for OffsetFetchRequest<'a> {
    fn size(&self, api_version: ApiVersion) -> usize {
        self.header.size(api_version) + STR_LEN_SIZE + self.group_id.len()
            + self.topics.iter().fold(ARRAY_LEN_SIZE, |size, topic| {
                size + STR_LEN_SIZE + topic.topic_name.len()
                    + topic
                        .partitions
                        .iter()
                        .fold(ARRAY_LEN_SIZE, |size, _| size + PARTITION_ID_SIZE)
            })
    }
}

impl<'a> Encodable for OffsetFetchRequest<'a> {
    fn encode<T: ByteOrder>(&self, dst: &mut BytesMut) -> Result<()> {
        self.header.encode::<T>(dst)?;

        dst.put_str::<T, _>(Some(self.group_id.as_ref()))?;
        dst.put_array::<T, _, _>(&self.topics, |buf, topic| {
            buf.put_str::<T, _>(Some(topic.topic_name.as_ref()))?;
            buf.put_array::<T, _, _>(&topic.partitions, |buf, partition| {
                buf.put_i32::<T>(partition.partition_id);
                Ok(())
            })
        })
    }
}

impl OffsetFetchResponse {
    pub fn parse(buf: &[u8]) -> IResult<&[u8], Self> {
        parse_offset_fetch_response(buf)
    }
}

named!(
    parse_offset_fetch_response<OffsetFetchResponse>,
    parse_tag!(
        ParseTag::OffsetFetchResponse,
        do_parse!(
            header: parse_response_header >> topics: length_count!(be_i32, parse_offset_fetch_topic_status)
                >> (OffsetFetchResponse { header, topics })
        )
    )
);

named!(
    parse_offset_fetch_topic_status<OffsetFetchTopicStatus>,
    parse_tag!(
        ParseTag::OffsetFetchTopicStatus,
        do_parse!(
            topic_name: parse_string >> partitions: length_count!(be_i32, parse_offset_fetch_partition_status)
                >> (OffsetFetchTopicStatus { topic_name, partitions })
        )
    )
);

named!(
    parse_offset_fetch_partition_status<OffsetFetchPartitionStatus>,
    parse_tag!(
        ParseTag::OffsetFetchPartitionStatus,
        do_parse!(
            partition_id: be_i32 >> offset: be_i64 >> metadata: parse_opt_string >> error_code: be_i16
                >> (OffsetFetchPartitionStatus {
                    partition_id,
                    offset,
                    metadata,
                    error_code,
                })
        )
    )
);

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BigEndian;

    use nom::IResult;
    use protocol::*;

    #[test]
    fn test_encode_offset_fetch_request() {
        let req = OffsetFetchRequest {
            header: RequestHeader {
                api_key: ApiKeys::OffsetFetch as ApiKey,
                api_version: 0,
                correlation_id: 123,
                client_id: Some("client".into()),
            },
            group_id: "consumer".into(),
            topics: vec![
                OffsetFetchTopic {
                    topic_name: "topic".into(),
                    partitions: vec![OffsetFetchPartition { partition_id: 1 }],
                },
            ],
        };

        let data = vec![
            /* OffsetFetchRequest
             * RequestHeader */ 0, 9 /* api_key */, 0,
            0 /* api_version */, 0, 0, 0, 123 /* correlation_id */, 0, 6, b'c', b'l', b'i', b'e', b'n',
            b't' /* client_id */, 0, 8, b'c', b'o', b'n', b's', b'u', b'm', b'e', b'r' /* group_id */,
            /* topics: [OffsetFetchTopic] */ 0, 0, 0, 1, /* OffsetFetchTopic */ 0, 5, b't', b'o', b'p', b'i',
            b'c' /* topic_name */, /* partitions: [OffsetFetchPartition] */ 0, 0, 0, 1,
            /* OffsetFetchPartition */ 0, 0, 0, 1 /* partition */,
        ];

        let mut buf = BytesMut::with_capacity(128);

        req.encode::<BigEndian>(&mut buf).unwrap();

        assert_eq!(req.size(req.header.api_version), buf.len());

        assert_eq!(&buf[..], &data[..]);
    }

    #[test]
    fn test_parse_offset_fetch_response() {
        let response = OffsetFetchResponse {
            header: ResponseHeader { correlation_id: 123 },
            topics: vec![
                OffsetFetchTopicStatus {
                    topic_name: "topic".to_owned(),
                    partitions: vec![
                        OffsetFetchPartitionStatus {
                            partition_id: 1,
                            offset: 2,
                            metadata: Some("metadata".to_owned()),
                            error_code: 3,
                        },
                    ],
                },
            ],
        };

        let data = vec![
            /* ResponseHeader */ 0, 0, 0, 123 /* correlation_id */,
            /* topics: [OffsetCommitTopicStatus] */ 0, 0, 0, 1, 0, 5, b't', b'o', b'p', b'i',
            b'c' /* topic_name */, /* partitions: [OffsetCommitPartitionStatus] */ 0, 0, 0, 1, 0, 0, 0,
            1 /* partition */, 0, 0, 0, 0, 0, 0, 0, 2 /* offset */, 0, 8, b'm', b'e', b't', b'a', b'd', b'a',
            b't', b'a' /* metadata */, 0, 3 /* error_code */,
        ];

        let res = parse_offset_fetch_response(&data[..]);

        display_parse_error::<_>(&data[..], res.clone());

        assert_eq!(res, IResult::Done(&[][..], response));
    }
}
