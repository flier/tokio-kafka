use nom::{be_i16, be_i32, be_i64};

use bytes::{BytesMut, BufMut, ByteOrder};

use errors::Result;
use protocol::{RequestHeader, ResponseHeader, Encodable, ParseTag, parse_string,
               parse_response_header, WriteExt};

pub const LATEST_TIMESTAMP: i64 = -1;
pub const EARLIEST_TIMESTAMP: i64 = -2;

pub const CONSUMER_REPLICA_ID: i32 = -1;
pub const DEBUGGING_REPLICA_ID: i32 = -2;

#[derive(Clone, Debug, PartialEq)]
pub struct ListOffsetRequest {
    pub header: RequestHeader,
    /// Broker id of the follower. For normal consumers, use -1.
    pub replica_id: i32,
    /// Topics to list offsets.
    pub topics: Vec<ListTopicOffset>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ListTopicOffset {
    /// The name of the topic.
    pub topic_name: String,
    /// Partitions to list offset.
    pub partitions: Vec<ListPartitionOffset>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ListPartitionOffset {
    /// The id of the partition the fetch is for.
    pub partition: i32,
    /// Used to ask for all messages before a certain time (ms).
    pub time: i64,
    /// Maximum offsets to return.
    pub max_number_of_offsets: i32,
}

impl Encodable for ListOffsetRequest {
    fn encode<T: ByteOrder>(self, dst: &mut BytesMut) -> Result<()> {
        let api_version = self.header.api_version;

        self.header.encode::<T>(dst)?;

        dst.put_i32::<T>(self.replica_id);
        dst.put_array::<T, _, _>(self.topics, |buf, topic| {
            buf.put_str::<T, _>(Some(topic.topic_name))?;
            buf.put_array::<T, _, _>(topic.partitions, |buf, partition| {
                buf.put_i32::<T>(partition.partition);
                buf.put_i64::<T>(partition.time);
                if api_version == 0 {
                    buf.put_i32::<T>(partition.max_number_of_offsets);
                }
                Ok(())
            })
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ListOffsetResponse {
    pub header: ResponseHeader,
    pub topics: Vec<TopicOffset>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TopicOffset {
    /// The name of the topic.
    pub topic_name: String,
    pub partitions: Vec<PartitionOffset>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PartitionOffset {
    /// The id of the partition the fetch is for.
    pub partition: i32,
    pub error_code: i16,
    pub timestamp: Option<i64>,
    pub offsets: Vec<i64>,
}

named_args!(pub parse_list_offset_response(api_version: i16)<ListOffsetResponse>,
    parse_tag!(ParseTag::ListOffsetResponse,
        do_parse!(
            header: parse_response_header
         >> topics: length_count!(be_i32, apply!(parse_list_topic_offset, api_version))
         >> (ListOffsetResponse {
                header: header,
                topics: topics,
            })
        )
    )
);

named_args!(parse_list_topic_offset(api_version: i16)<TopicOffset>,
    parse_tag!(ParseTag::TopicOffset,
        do_parse!(
            topic_name: parse_string
         >> partitions: length_count!(be_i32, apply!(parse_list_partition_offset, api_version))
         >> (TopicOffset {
                topic_name: topic_name,
                partitions: partitions,
            })
        )
    )
);

named_args!(parse_list_partition_offset(api_version: i16)<PartitionOffset>,
    parse_tag!(ParseTag::PartitionOffset,
        do_parse!(
            partition: be_i32
         >> error_code: be_i16
         >> offsets: cond!(api_version == 0, length_count!(be_i32, be_i64))
         >> timestamp: cond!(api_version > 0, be_i64)
         >> offset: cond!(api_version > 0, be_i64)
         >> (PartitionOffset {
                partition: partition,
                error_code: error_code,
                timestamp: timestamp,
                offsets: if api_version == 0 { offsets.unwrap_or_default() } else { vec![offset.unwrap_or_default()] },
            })
        )
    )
);