use bytes::{BytesMut, BufMut, ByteOrder};

use nom::{be_i16, be_i32, be_i64};

use errors::Result;
use protocol::{Encodable, RequestHeader, ResponseHeader, MessageSet, parse_message_set, ParseTag,
               parse_string, parse_response_header, WriteExt};

#[derive(Clone, Debug, PartialEq)]
pub struct FetchRequest {
    pub header: RequestHeader,
    /// The replica id indicates the node id of the replica initiating this request.
    pub replica_id: i32,
    /// The maximum amount of time in milliseconds to block waiting if insufficient data is available at the time the request is issued.
    pub max_wait_time: i32,
    /// This is the minimum number of bytes of messages that must be available to give a response.
    pub min_bytes: i32,
    pub topics: Vec<FetchTopic>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FetchTopic {
    /// The name of the topic.
    pub topic_name: String,
    pub partitions: Vec<FetchPartition>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FetchPartition {
    /// The id of the partition the fetch is for.
    pub partition: i32,
    /// The offset to begin this fetch from.
    pub fetch_offset: i64,
    /// The maximum bytes to include in the message set for this partition.
    pub max_bytes: i32,
}

impl Encodable for FetchRequest {
    fn encode<T: ByteOrder>(self, dst: &mut BytesMut) -> Result<()> {
        self.header.encode::<T>(dst)?;

        dst.put_i32::<T>(self.replica_id);
        dst.put_i32::<T>(self.max_wait_time);
        dst.put_i32::<T>(self.min_bytes);
        dst.put_array::<T, _, _>(self.topics, |buf, topic| {
            buf.put_str::<T, _>(Some(topic.topic_name))?;
            buf.put_array::<T, _, _>(topic.partitions, |buf, partition| {
                buf.put_i32::<T>(partition.partition);
                buf.put_i64::<T>(partition.fetch_offset);
                buf.put_i32::<T>(partition.max_bytes);
                Ok(())
            })
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct FetchResponse {
    pub header: ResponseHeader,
    /// Duration in milliseconds for which the request was throttled due to quota violation.
    pub throttle_time: Option<i32>,
    pub topics: Vec<TopicData>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TopicData {
    /// The name of the topic this response entry is for.
    pub topic_name: String,
    pub partitions: Vec<PartitionData>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PartitionData {
    /// The id of the partition the fetch is for.
    pub partition: i32,
    pub error_code: i16,
    ///The offset at the end of the log for this partition.
    pub highwater_mark_offset: i64,
    pub message_set: MessageSet,
}

named_args!(pub parse_fetch_response(api_version: i16)<FetchResponse>,
    parse_tag!(ParseTag::FetchResponse,
        do_parse!(
            header: parse_response_header
         >> throttle_time: cond!(api_version > 0, be_i32)
         >> topics: length_count!(be_i32, apply!(parse_fetch_topic_data, api_version))
         >> (FetchResponse {
                header: header,
                throttle_time: throttle_time,
                topics: topics,
            })
        )
    )
);

named_args!(parse_fetch_topic_data(api_version: i16)<TopicData>,
    parse_tag!(ParseTag::TopicData,
        do_parse!(
            topic_name: parse_string
         >> partitions: length_count!(be_i32, apply!(parse_fetch_partition_data, api_version))
         >> (TopicData {
                topic_name: topic_name,
                partitions: partitions,
            })
        )
    )
);

named_args!(parse_fetch_partition_data(api_version: i16)<PartitionData>,
    parse_tag!(ParseTag::PartitionData,
        do_parse!(
            partition: be_i32
         >> error_code: be_i16
         >> offset: be_i64
         >> message_set: length_value!(be_i32, apply!(parse_message_set, api_version))
         >> (PartitionData {
                partition: partition,
                error_code: error_code,
                highwater_mark_offset: offset,
                message_set: message_set,
            })
        )
    )
);

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BigEndian};

    use nom::IResult;

    use super::*;
    use protocol::*;
    use compression::Compression;

    lazy_static!{
        static ref TEST_FETCH_REQUEST_DATA: Vec<u8> = vec![
            // FetchRequest
                // RequestHeader
                0, 1,                               // api_key
                0, 1,                               // api_version
                0, 0, 0, 123,                       // correlation_id
                0, 6, 99, 108, 105, 101, 110, 116,  // client_id
            0, 0, 0, 2,                             // replica_id
            0, 0, 0, 3,                             // max_wait_time
            0, 0, 0, 4,                             // max_bytes
                // topics: [FetchTopicData]
                0, 0, 0, 1,
                    // FetchTopicData
                    0, 5, 116, 111, 112, 105, 99,   // topic_name
                    // partitions: [FetchPartitionData]
                    0, 0, 0, 1,
                        // FetchPartitionData
                        0, 0, 0, 5,                 // partition
                        0, 0, 0, 0, 0, 0, 0, 6,     // fetch_offset
                        0, 0, 0, 7,                 // max_bytes
        ];

        static ref TEST_FETCH_RESPONSE_DATA: Vec<u8> = vec![
            // ResponseHeader
            0, 0, 0, 123,   // correlation_id
            0, 0, 0, 1,     // throttle_time
            // topics: [TopicData]
            0, 0, 0, 1,
                0, 5, b't', b'o', b'p', b'i', b'c', // topic_name
                // partitions: [PartitionData]
                0, 0, 0, 1,
                    0, 0, 0, 1,             // partition
                    0, 2,                   // error_code
                    0, 0, 0, 0, 0, 0, 0, 3, // highwater_mark_offset
                    // MessageSet
                    0, 0, 0, 46,            // size
                        // messages: [Message]
                        0, 0, 0, 1,
                            0, 0, 0, 0, 0, 0, 0, 0,             // offset
                            0, 0, 0, 30,                        // size
                            206, 63, 210, 11,                   // crc
                            1,                                  // magic
                            8,                                  // attributes
                            0, 0, 0, 0, 0, 0, 1, 200,           // timestamp
                            0, 0, 0, 3, 107, 101, 121,          // key
                            0, 0, 0, 5, 118, 97, 108, 117, 101  // value
        ];

        static ref TEST_FETCH_RESPONSE: FetchResponse = FetchResponse {
            header: ResponseHeader { correlation_id: 123 },
            throttle_time: Some(1),
            topics: vec![TopicData {
                topic_name: "topic".to_owned(),
                partitions: vec![PartitionData {
                    partition: 1,
                    error_code: 2,
                    highwater_mark_offset: 3,
                    message_set: MessageSet {
                        messages: vec![Message {
                            offset: 0,
                            compression: Compression::None,
                            key: Some(Bytes::from(&b"key"[..])),
                            value: Some(Bytes::from(&b"value"[..])),
                            timestamp: Some(Timestamp::LogAppendTime(456)),
                        }],
                    },
                }],
            }],
        };
    }

    #[test]
    fn test_encode_fetch_request() {
        let req = FetchRequest {
            header: RequestHeader {
                api_key: ApiKeys::Fetch as i16,
                api_version: 1,
                correlation_id: 123,
                client_id: Some("client".to_owned()),
            },
            replica_id: 2,
            max_wait_time: 3,
            min_bytes: 4,
            topics: vec![FetchTopic {
                topic_name: "topic".to_owned(),
                partitions: vec![FetchPartition {
                    partition: 5,
                    fetch_offset: 6,
                    max_bytes: 7
                }],
            }],
        };

        let mut buf = BytesMut::with_capacity(128);

        req.encode::<BigEndian>(&mut buf).unwrap();

        assert_eq!(&buf[..], &TEST_FETCH_REQUEST_DATA[..]);
    }

    #[test]
    fn test_parse_fetch_response() {
        let buf = TEST_FETCH_RESPONSE_DATA.as_slice();
        let res = parse_fetch_response(buf, 1);

        display_parse_error::<_>(&buf[..], res.clone());

        assert_eq!(res, IResult::Done(&[][..], TEST_FETCH_RESPONSE.clone()));
    }
}