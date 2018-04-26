use std::cmp;
use std::i64;

use bytes::{BufMut, ByteOrder, Bytes, BytesMut};
use crc::crc32;

use errors::Result;
use protocol::{parse_varint, Encodable, Message, MessageSet, Offset, Record, RecordFormat, Timestamp, VarIntExt,
               WriteExt, NULL_VARINT_SIZE_BYTES};

const BASE_OFFSET_LENGTH: usize = 8;
const LENGTH_LENGTH: usize = 4;
const PARTITION_LEADER_EPOCH_LENGTH: usize = 4;
const MAGIC_LENGTH: usize = 1;
const CRC_LENGTH: usize = 4;
const ATTRIBUTE_LENGTH: usize = 2;
const LAST_OFFSET_DELTA_LENGTH: usize = 4;
const FIRST_TIMESTAMP_LENGTH: usize = 8;
const MAX_TIMESTAMP_LENGTH: usize = 8;
const PRODUCER_ID_LENGTH: usize = 8;
const PRODUCER_EPOCH_LENGTH: usize = 2;
const BASE_SEQUENCE_LENGTH: usize = 4;
const RECORDS_COUNT_LENGTH: usize = 4;
const LOG_OVERHEAD: usize = BASE_OFFSET_LENGTH + LENGTH_LENGTH;
const RECORD_BATCH_OVERHEAD: usize = BASE_OFFSET_LENGTH + LENGTH_LENGTH + PARTITION_LEADER_EPOCH_LENGTH + MAGIC_LENGTH
    + CRC_LENGTH + ATTRIBUTE_LENGTH + LAST_OFFSET_DELTA_LENGTH
    + FIRST_TIMESTAMP_LENGTH + MAX_TIMESTAMP_LENGTH + PRODUCER_ID_LENGTH
    + PRODUCER_EPOCH_LENGTH + BASE_SEQUENCE_LENGTH + RECORDS_COUNT_LENGTH;
const RECORD_ATTRIBUTE_LENGTH: usize = 1;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct RecordBatch {
    /// Denotes the first offset in the RecordBatch.
    ///
    /// The 'offset_delta' of each Record in the batch would be be computed relative to this FirstOffset. In
    /// particular, the offset of each Record in the Batch is its 'offset_delta' + 'first_offset'.
    pub first_offset: Offset,
    /// The offset of the last message in the RecordBatch.
    ///
    /// This is used by the broker to ensure correct behavior even when Records within a batch are compacted out.
    pub last_offset_delta: i32,
    /// Introduced with KIP-101, this is set by the broker upon receipt of a
    /// produce request and is used to ensure no loss of data when there are
    /// leader changes with log truncation. Client developers do not need to
    /// worry about setting this value.
    pub partition_leader_epoch: i32,
    /// This byte holds metadata attributes about the message.
    pub attributes: i16,
    /// The timestamp of the first Record in the batch.
    ///
    /// The timestamp of each Record in the RecordBatch is its 'timestamp_delta' + 'first_timestamp'.
    pub first_timestamp: i64,
    /// The timestamp of the last Record in the batch.
    ///
    /// This is used by the broker to ensure the correct behavior even when Records within the batch are compacted out.
    pub max_timestamp: i64,
    /// Introduced in 0.11.0.0 for KIP-98, this is the broker assigned producerId received by the 'InitProducerId'
    /// request.
    ///
    /// Clients which want to support idempotent message delivery and transactions must set this field.
    pub producer_id: i64,
    /// Introduced in 0.11.0.0 for KIP-98, this is the broker assigned producerEpoch received by the 'InitProducerId'
    /// request.
    ///
    /// Clients which want to support idempotent message delivery and transactions must set this field.
    pub producer_epoch: i16,
    /// Introduced in 0.11.0.0 for KIP-98, this is the producer assigned sequence
    /// number which is used by the broker to deduplicate messages.
    ///
    /// Clients which want to support idempotent message delivery and transactions must set this field. The sequence
    /// number for each Record in the RecordBatch is its OffsetDelta + FirstSequence.
    pub first_sequence: i32,
    pub records: Vec<RecordBody>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct RecordBody {
    /// Record level attributes are presently unused.
    pub attributes: u8,
    pub timestamp_delta: i64,
    pub offset_delta: i32,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    /// Introduced in 0.11.0.0 for KIP-82, Kafka now supports application level record level headers.
    ///
    /// The Producer and Consumer APIS have been accordingly updated to write and read these headers.
    pub headers: Vec<RecordHeader>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RecordHeader {
    pub key: String,
    pub value: Option<Bytes>,
}

impl From<MessageSet> for RecordBatch {
    fn from(message_set: MessageSet) -> Self {
        let (first_offset, last_offset, first_timestamp, max_timestamp) = message_set.messages.iter().fold(
            (i64::MAX, i64::MIN, i64::MAX, i64::MIN),
            |(first_offset, last_offset, first_timestamp, max_timestamp), message| {
                let timestamp = message.timestamp.map(Timestamp::from);

                (
                    cmp::min(first_offset, message.offset),
                    cmp::max(last_offset, message.offset),
                    timestamp.map_or(first_timestamp, |timestamp| cmp::min(first_timestamp, timestamp)),
                    timestamp.map_or(max_timestamp, |timestamp| cmp::max(max_timestamp, timestamp)),
                )
            },
        );

        let records = message_set
            .messages
            .into_iter()
            .map(
                |Message {
                     offset,
                     timestamp,
                     compression,
                     key,
                     value,
                     headers,
                 }| RecordBody {
                    attributes: 0,
                    timestamp_delta: timestamp
                        .map(|timestamp| Timestamp::from(timestamp) - first_timestamp)
                        .unwrap_or_default(),
                    offset_delta: (offset - first_offset) as i32,
                    key,
                    value,
                    headers,
                },
            )
            .collect();

        RecordBatch {
            first_offset,
            last_offset_delta: (last_offset - first_offset) as i32,
            partition_leader_epoch: 0,
            attributes: 0,
            first_timestamp,
            max_timestamp,
            producer_id: 0,
            producer_epoch: 0,
            first_sequence: 0,
            records,
        }
    }
}

impl Record for RecordBatch {
    fn size(&self, record_format: RecordFormat) -> usize {
        assert_eq!(record_format, RecordFormat::V2);

        self.records
            .iter()
            .fold(RECORD_BATCH_OVERHEAD, |size, record| size + record.size(record_format))
    }
}

impl Encodable for RecordBatch {
    fn encode<T: ByteOrder>(&self, dst: &mut BytesMut) -> Result<()> {
        dst.put_i64::<T>(self.first_offset);
        dst.put_i32::<T>((RECORD_BATCH_OVERHEAD - LOG_OVERHEAD) as i32);
        dst.put_i32::<T>(self.partition_leader_epoch);
        dst.put_u8(RecordFormat::V2 as u8);
        let crc_off = dst.len();
        dst.put_i32::<T>(0); // CRC
        let crc_start = dst.len();
        dst.put_i16::<T>(self.attributes);
        dst.put_i32::<T>(self.last_offset_delta);
        dst.put_i64::<T>(self.first_timestamp);
        dst.put_i64::<T>(self.max_timestamp);
        dst.put_i64::<T>(self.producer_id);
        dst.put_i16::<T>(self.producer_epoch);
        dst.put_i32::<T>(self.first_sequence);
        dst.put_i32::<T>(self.records.len() as i32);

        let crc = crc32::checksum_ieee(&dst[crc_start..]);

        T::write_i32(&mut dst[crc_off..], crc as i32);

        for record in &self.records {
            record.encode::<T>(dst)?;
        }

        Ok(())
    }
}

impl Record for RecordBody {
    fn size(&self, record_format: RecordFormat) -> usize {
        assert_eq!(record_format, RecordFormat::V2);

        let record_overhead_size = RECORD_ATTRIBUTE_LENGTH + i32::size_of_varint(self.offset_delta as i32)
            + i64::size_of_varint(self.timestamp_delta);
        let key_size = self.key.as_ref().map_or(NULL_VARINT_SIZE_BYTES, |key| {
            i32::size_of_varint(key.len() as i32) + key.len()
        });
        let value_size = self.value.as_ref().map_or(NULL_VARINT_SIZE_BYTES, |value| {
            i32::size_of_varint(value.len() as i32) + value.len()
        });
        let headers_size = self.headers
            .iter()
            .fold(i32::size_of_varint(self.headers.len() as i32), |size, header| {
                let key = header.key.as_bytes();

                size + i32::size_of_varint(key.len() as i32) + key.len()
                    + header.value.as_ref().map_or(NULL_VARINT_SIZE_BYTES, |value| {
                        i32::size_of_varint(value.len() as i32) + value.len()
                    })
            });

        let size = record_overhead_size + key_size + value_size + headers_size;

        i32::size_of_varint(size as i32) + size
    }
}

impl Encodable for RecordBody {
    fn encode<T: ByteOrder>(&self, dst: &mut BytesMut) -> Result<()> {
        dst.put_vari32(self.size(RecordFormat::V2) as i32);
        dst.put_u8(self.attributes);
        dst.put_vari64(self.timestamp_delta);
        dst.put_vari32(self.offset_delta);
        dst.put_varbytes(self.key.as_ref())?;
        dst.put_varbytes(self.value.as_ref())?;

        dst.put_vari32(self.headers.len() as i32);

        for header in &self.headers {
            header.encode::<T>(dst)?;
        }

        Ok(())
    }
}

impl Record for RecordHeader {
    fn size(&self, record_format: RecordFormat) -> usize {
        assert_eq!(record_format, RecordFormat::V2);

        let key = self.key.as_bytes();

        i32::size_of_varint(key.len() as i32) + key.len()
            + self.value.as_ref().map_or(NULL_VARINT_SIZE_BYTES, |value| {
                i32::size_of_varint(value.len() as i32) + value.len()
            })
    }
}

impl Encodable for RecordHeader {
    fn encode<T: ByteOrder>(&self, dst: &mut BytesMut) -> Result<()> {
        dst.put_varbytes(Some(self.key.as_bytes()))?;
        dst.put_varbytes(self.value.as_ref())?;
        Ok(())
    }
}

named!(
    parse_record_header<RecordHeader>,
    do_parse!(
        key:
            map_res!(
                map!(parse_varbytes, |bytes| bytes.map(|s| s.to_vec()).unwrap_or_default()),
                String::from_utf8
            ) >> value: map!(parse_varbytes, |bytes| bytes.map(|s| Bytes::from(s))) >> (RecordHeader {
            key,
            value,
        })
    )
);

named!(
    parse_varbytes<Option<&[u8]>>,
    do_parse!(len: parse_varint >> s: cond!(len >= 0, take!(len)) >> (s))
);

#[cfg(test)]
mod tests {
    use bytes::{BigEndian, Bytes};

    use super::*;

    lazy_static! {
        static ref TEST_RECORD_HEADER: RecordHeader = RecordHeader {
            key: "key".to_owned(),
            value: Some(Bytes::from_static(b"value")),
        };
        static ref TEST_RECORD_BODY: RecordBody = RecordBody {
            attributes: 1,
            timestamp_delta: 2,
            offset_delta: 3,
            key: Some(Bytes::from_static(b"key")),
            value: Some(Bytes::from_static(b"value")),
            headers: vec![TEST_RECORD_HEADER.clone()],
        };
        static ref TEST_RECORD_BATCH: RecordBatch = RecordBatch {
            first_offset: 1,
            last_offset_delta: 2,
            partition_leader_epoch: 3,
            attributes: 4,
            first_timestamp: 5,
            max_timestamp: 6,
            producer_id: 7,
            producer_epoch: 8,
            first_sequence: 9,
            records: vec![TEST_RECORD_BODY.clone()],
        };

        #[cfg_attr(rustfmt, rustfmt_skip)]
        static ref ENCODED_RECORD_HEADER: &'static [u8] = &[
            //  header
            //      key
            0x06, b'k', b'e', b'y',
            //      value
            0x0A, b'v', b'a', b'l', b'u', b'e',
        ];

        #[cfg_attr(rustfmt, rustfmt_skip)]
        static ref ENCODED_RECORD_BODY: &'static [u8] = &[
            // record
            //      length
            0x32,
            //      attributes
            0x01,
            //      timestamp_delta
            0x04,
            //      offset_delta
            0x06,
            //      key
            0x06, b'k', b'e', b'y',
            //      value
            0x0A, b'v', b'a', b'l', b'u', b'e',
            //      headers count
            0x02,
            //      header
            //          key
            0x06, b'k', b'e', b'y',
            //          value
            0x0A, b'v', b'a', b'l', b'u', b'e',
        ];

        #[cfg_attr(rustfmt, rustfmt_skip)]
        static ref ENCODED_RECORD_BATCH: &'static [u8] = &[
            // record batch
            //  first offset
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
            //  length
            0x00, 0x00, 0x00, 0x31,
            //  partition_leader_epoch
            0x00, 0x00, 0x00, 0x03,
            //  magic
            0x02,
            //  crc
            0x36, 0x11, 0xA2, 0xEE,
            //  attributes
            0x00, 0x04,
            //  last_offset_delta
            0x00, 0x00, 0x00, 0x02,
            //  first_timestamp
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05,
            //  max_timestamp
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06,
            //  producer_id
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07,
            //  producer_epoch
            0x00, 0x08,
            //  first_sequence
            0x00, 0x00, 0x00, 0x09,
            //  records count
            0x00, 0x00, 0x00, 0x01,
            //  record
            //      length
            0x32,
            //      attributes
            0x01,
            //      timestamp_delta
            0x04,
            //      offset_delta
            0x06,
            //      key
            0x06, b'k', b'e', b'y',
            //      value
            0x0A, b'v', b'a', b'l', b'u', b'e',
            //      headers count
            0x02,
            //      header
            //          key
            0x06, b'k', b'e', b'y',
            //          value
            0x0A, b'v', b'a', b'l', b'u', b'e',
        ][..];
    }

    #[test]
    fn encode_record_batch() {
        let mut buf = BytesMut::with_capacity(256);

        let record = &*TEST_RECORD_BATCH;

        record.encode::<BigEndian>(&mut buf).unwrap();

        assert_eq!(record.size(RecordFormat::V2), buf.len());
        assert_eq!(
            &buf,
            &*ENCODED_RECORD_BATCH,
            "encoded record batch {:?} to buffer\n{}",
            record,
            hexdump!(&buf)
        );
    }

    #[test]
    fn encode_record_body() {
        let mut buf = BytesMut::with_capacity(256);

        let body = &*TEST_RECORD_BODY;

        body.encode::<BigEndian>(&mut buf).unwrap();

        assert_eq!(body.size(RecordFormat::V2), buf.len());
        assert_eq!(
            &buf,
            &*ENCODED_RECORD_BODY,
            "encoded record body {:?} to buffer\n{}",
            body,
            hexdump!(&buf)
        );
    }

    #[test]
    fn encode_record_header() {
        let mut buf = BytesMut::with_capacity(256);
        let header = &*TEST_RECORD_HEADER;

        header.encode::<BigEndian>(&mut buf).unwrap();

        assert_eq!(header.size(RecordFormat::V2), buf.len());
        assert_eq!(&buf, &*ENCODED_RECORD_HEADER);
    }
}
