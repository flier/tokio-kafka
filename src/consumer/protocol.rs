use protocol::Nullable;

pub struct ConsumerProtocol {}

pub struct ConsumerProtocolHeader {
    version: i16,
}

pub struct SubscriptionSchema {
    topics: Vec<String>,
    user_data: Nullable<Vec<u8>>,
}

pub struct TopicAssignmentSchema {
    topics: String,
    partitions: Vec<i32>,
}

pub struct AssignmentSchema {
    topic_partitions: Vec<TopicAssignmentSchema>,
    user_data: Nullable<Vec<u8>>,
}
