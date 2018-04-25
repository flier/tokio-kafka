use bytes::{BufMut, ByteOrder, Bytes, BytesMut};
use crc::crc32;

use errors::Result;
use protocol::{Encodable, Offset, RecordFormat, WriteExt};

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
pub const RECORD_ATTRIBUTE_LENGTH: usize = 1;

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
    pub length: i32,
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

        Ok(())
    }
}

impl Encodable for RecordBody {
    fn encode<T: ByteOrder>(&self, dst: &mut BytesMut) -> Result<()> {
        dst.put_vari32(self.length)?;
        dst.put_u8(self.attributes);
        dst.put_vari64(self.timestamp_delta)?;
        dst.put_vari32(self.offset_delta)?;
        dst.put_varbytes(self.key.as_ref())?;
        dst.put_varbytes(self.value.as_ref())?;

        dst.put_vari32(self.headers.len() as i32)?;

        for header in &self.headers {
            header.encode::<T>(dst)?;
        }

        Ok(())
    }
}

impl Encodable for RecordHeader {
    fn encode<T: ByteOrder>(&self, dst: &mut BytesMut) -> Result<()> {
        dst.put_varbytes(Some(self.key.as_str()))?;
        dst.put_varbytes(self.value.as_ref())?;
        Ok(())
    }
}
