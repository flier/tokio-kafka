use std::marker::PhantomData;

use bytes::{BytesMut, ByteOrder};

use tokio_io::codec::Encoder;

use errors::{Error, Result};
use codec::WriteExt;
use protocol::{RequestHeader, ResponseHeader};

#[derive(Debug, PartialEq)]
pub struct TopicMetadataRequest<'a> {
    pub header: RequestHeader<'a>,
    pub topic_name: &'a str,
}

pub struct TopicMetadataRequestEncoder<'a, T: 'a>(PhantomData<&'a T>);

impl<'a, T: 'a> TopicMetadataRequestEncoder<'a, T> {
    pub fn new() -> Self {
        TopicMetadataRequestEncoder(PhantomData)
    }
}

impl<'a, T: 'a + ByteOrder> Encoder for TopicMetadataRequestEncoder<'a, T> {
    type Item = TopicMetadataRequest<'a>;
    type Error = Error;

    fn encode(&mut self, req: Self::Item, dst: &mut BytesMut) -> Result<()> {
        dst.put_item::<T, _>(&req.header)?;
        dst.put_str::<T, _>(req.topic_name)
    }
}

#[derive(Debug, PartialEq)]
pub struct TopicMetadataResponse<'a> {
    pub header: ResponseHeader,
    pub brokers: Vec<BrokerMetadata<'a>>,
    pub topics: Vec<TopicMetadata<'a>>,
}
/*
impl<'a> TopicMetadataResponse<'a> {
    pub fn parse<T: 'a + AsRef<[u8]>>(buf: T) -> Result<TopicMetadataResponse<'a>> {}
}
*/
#[derive(Debug, PartialEq)]
pub struct BrokerMetadata<'a> {
    node_id: i32,
    host: &'a str,
    port: i32,
}

#[derive(Debug, PartialEq)]
pub struct TopicMetadata<'a> {
    error_code: i16,
    topic_name: &'a str,
    partitions: Vec<PartitionMetadata>,
}

#[derive(Debug, PartialEq)]
pub struct PartitionMetadata {
    error_code: i16,
    partition_id: i32,
    leader: i32,
    replicas: Vec<i32>,
    isr: Vec<i32>,
}