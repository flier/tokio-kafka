use std::mem;

/// This is a numeric id for the API being invoked (i.e. is it a metadata request, a produce
/// request, a fetch request, etc).
///
/// See [`ApiKeys`](enum.ApiKeys.html)
pub type ApiKey = i16;

/// Identifiers for all the Kafka APIs
///
/// The following are the numeric codes that the `ApiKey` in the request can take for each of the
/// below request types.
///
/// See [`ApiKey`](type.ApiKey.html)
#[derive(Debug, Copy, Clone, PartialEq)]
#[repr(i16)]
pub enum ApiKeys {
    Produce = 0,
    Fetch = 1,
    ListOffsets = 2,
    Metadata = 3,
    LeaderAndIsr = 4,
    StopReplica = 5,
    UpdateMetadata = 6,
    ControlledShutdown = 7,
    OffsetCommit = 8,
    OffsetFetch = 9,
    // ConsumerMetadata = 10,
    GroupCoordinator = 10,
    JoinGroup = 11,
    Heartbeat = 12,
    LeaveGroup = 13,
    SyncGroup = 14,
    DescribeGroups = 15,
    ListGroups = 16,
    SaslHandshake = 17,
    ApiVersions = 18,
    CreateTopics = 19,
    DeleteTopics = 20,
}

impl ApiKeys {
    /// Gets the key value.
    pub fn key(&self) -> ApiKey {
        unsafe { mem::transmute(*self) }
    }

    /// Gets the name.
    pub fn name(&self) -> &'static str {
        match *self {
            ApiKeys::Produce => "Produce",
            ApiKeys::Fetch => "Fetch",
            ApiKeys::ListOffsets => "ListOffsets",
            ApiKeys::Metadata => "Metadata",
            ApiKeys::LeaderAndIsr => "LeaderAndIsr",
            ApiKeys::StopReplica => "StopReplica",
            ApiKeys::UpdateMetadata => "UpdateMetadata",
            ApiKeys::ControlledShutdown => "ControlledShutdown",
            ApiKeys::OffsetCommit => "OffsetCommit",
            ApiKeys::OffsetFetch => "OffsetFetch",
            ApiKeys::GroupCoordinator => "GroupCoordinator",
            ApiKeys::JoinGroup => "JoinGroup",
            ApiKeys::Heartbeat => "Heartbeat",
            ApiKeys::LeaveGroup => "LeaveGroup",
            ApiKeys::SyncGroup => "SyncGroup",
            ApiKeys::DescribeGroups => "DescribeGroups",
            ApiKeys::ListGroups => "ListGroups",
            ApiKeys::SaslHandshake => "SaslHandshake",
            ApiKeys::ApiVersions => "ApiVersions",
            ApiKeys::CreateTopics => "CreateTopics",
            ApiKeys::DeleteTopics => "DeleteTopics",
        }
    }
}

impl From<ApiKey> for ApiKeys {
    fn from(v: ApiKey) -> Self {
        unsafe { mem::transmute(v) }
    }
}
