use bytes::{BytesMut, ByteOrder};

use errors::Result;
use protocol::{Encodable, RequestHeader, ProduceRequest, FetchRequest, MetadataRequest,
               ApiVersionsRequest};

#[derive(Debug)]
pub enum KafkaRequest {
    Produce(ProduceRequest),
    Fetch(FetchRequest),
    Metadata(MetadataRequest),
    ApiVersions(ApiVersionsRequest),
}

impl KafkaRequest {
    pub fn header(&self) -> &RequestHeader {
        match self {
            &KafkaRequest::Produce(ref req) => &req.header,
            &KafkaRequest::Fetch(ref req) => &req.header,
            &KafkaRequest::Metadata(ref req) => &req.header,
            &KafkaRequest::ApiVersions(ref req) => &req.header,
        }
    }
}

impl Encodable for KafkaRequest {
    fn encode<T: ByteOrder>(self, dst: &mut BytesMut) -> Result<()> {
        match self {
            KafkaRequest::Produce(req) => req.encode::<T>(dst),
            KafkaRequest::Fetch(req) => req.encode::<T>(dst),
            KafkaRequest::Metadata(req) => req.encode::<T>(dst),
            KafkaRequest::ApiVersions(req) => req.encode::<T>(dst),
        }
    }
}