use std::sync::atomic::{AtomicUsize, Ordering};
use std::hash::{BuildHasher, BuildHasherDefault, Hash, Hasher};

use twox_hash::XxHash;

use protocol::PartitionId;
use producer::{Producer, ProducerRecord};

/// A partitioner is given a chance to choose/redefine a partition
/// for a message to be sent to Kafka.
pub trait Partitioner {
    /// Compute the partition for the given record.
    fn partition<'a, P: Producer<'a>>(&self,
                                      producer: &'a P,
                                      record: &mut ProducerRecord<'a, P::Key, P::Value>)
                                      -> Option<PartitionId>;
}

pub type DefaultHasher = XxHash;

/// The default partitioning strategy:
///
/// - If a partition is specified in the record, use it
/// - If no partition is specified but a key is present choose a partition based on a hash of the key
/// - If no partition or key is present choose a partition in a round-robin fashion
#[derive(Default)]
pub struct DefaultPartitioner<H: BuildHasher = BuildHasherDefault<DefaultHasher>> {
    hash_builder: H,
    records: AtomicUsize,
}

impl DefaultPartitioner {
    pub fn new() -> DefaultPartitioner<BuildHasherDefault<DefaultHasher>> {
        Default::default()
    }

    pub fn with_hasher<B: BuildHasher>(hash_builder: B) -> DefaultPartitioner<B> {
        DefaultPartitioner {
            hash_builder: hash_builder.into(),
            records: AtomicUsize::new(0),
        }
    }

    pub fn records(&self) -> usize {
        self.records.load(Ordering::Relaxed)
    }
}

impl<H> Partitioner for DefaultPartitioner<H>
    where H: BuildHasher
{
    fn partition<'a, P: Producer<'a>>(&self,
                                      producer: &'a P,
                                      record: &mut ProducerRecord<'a, P::Key, P::Value>)
                                      -> Option<PartitionId> {
        if let Some(partition) = record.partition {
            if partition >= 0 {
                // If a partition is specified in the record, use it
                return Some(partition);
            }
        }

        // TODO: use available partitions for topic in cluster
        if let Some(partitions) = producer.partitions_for(&record.topic_name) {
            let index = if let Some(ref key) = record.key {
                // If no partition is specified but a key is present choose a partition based on a hash of the key
                let mut hasher = self.hash_builder.build_hasher();
                key.hash(&mut hasher);
                hasher.finish() as usize
            } else {
                // If no partition or key is present choose a partition in a round-robin fashion
                self.records.fetch_add(1, Ordering::Relaxed)
            } % partitions.len();

            let partition = partitions[index].partition;

            record.partition = Some(partition);

            Some(partition)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::producer::mock::MockProducer;

    #[test]
    fn test_skip_partitioning() {
        let producer = MockProducer::default();
        let partitioner = DefaultPartitioner::new();
        let mut record = ProducerRecord::from_key_value("topic", "key", "value");

        assert_eq!(record.partition, None);

        // partition without topics
        partitioner.partition(&producer, &mut record);

        assert_eq!(record.partition, None);
    }

    #[test]
    fn test_key_partitioning() {
        let mut producer = MockProducer::default();

        producer
            .topics
            .entry("topic".to_owned())
            .or_insert(vec![])
            .extend((0..3).map(|id| ("topic".to_owned(), id)));

        let partitioner = DefaultPartitioner::new();
        let mut record = ProducerRecord::from_key_value("topic", "key", "value");

        // partition with key
        partitioner.partition(&producer, &mut record);

        assert_eq!(record.partition, Some(2));

        // partition without key
        record.key = None;

        for id in 0..100 {
            record.partition = None;
            assert_eq!(partitioner.partition(&producer, &mut record), Some(id % 3));
        }

        assert_eq!(partitioner.records(), 100);
    }
}
