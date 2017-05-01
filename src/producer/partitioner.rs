use std::hash::{BuildHasher, BuildHasherDefault, Hash, Hasher};

use twox_hash::XxHash;

use producer::{Producer, ProducerRecord};

/// A partitioner is given a chance to choose/redefine a partition
/// for a message to be sent to Kafka.
pub trait Partitioner {
    fn partition<P: Producer>(&mut self,
                              producer: &P,
                              record: &mut ProducerRecord<P::Key, P::Value>);
}

pub type DefaultHasher = XxHash;

pub struct DefaultPartitioner<H: BuildHasher = BuildHasherDefault<DefaultHasher>> {
    hash_builder: H,
    count: usize,
}

impl<H> Partitioner for DefaultPartitioner<H>
    where H: BuildHasher
{
    fn partition<P: Producer>(&mut self,
                              producer: &P,
                              record: &mut ProducerRecord<P::Key, P::Value>) {
        if let Some(partition) = record.partition {
            if partition > 0 {
                // If a partition is specified in the record, use it
                return;
            }
        }

        let partitions = producer.partitions_for(record.topic);
        let index = if let Some(ref key) = record.key {
            let mut hasher = self.hash_builder.build_hasher();
            key.hash(&mut hasher);
            hasher.finish() as usize % partitions.len()
        } else {
            self.count = self.count.wrapping_add(1);
            (self.count - 1) % partitions.len()
        };

        record.partition = Some(partitions[index].partition());
    }
}