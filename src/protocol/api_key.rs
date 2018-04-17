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
    Produce,
    Fetch,
    ListOffsets,
    Metadata,
    LeaderAndIsr,
    StopReplica,
    UpdateMetadata,
    ControlledShutdown,
    OffsetCommit,
    OffsetFetch,
    GroupCoordinator, // ConsumerMetadata,
    JoinGroup,
    Heartbeat,
    LeaveGroup,
    SyncGroup,
    DescribeGroups,
    ListGroups,
    SaslHandshake,
    ApiVersions,
    CreateTopics,
    DeleteTopics,
    DeleteRecords,
    InitProducerId,
    OffsetForLeaderEpoch,
    AddPartitionsToTxn,
    AddOffsetsToTxn,
    EndTxn,
    WriteTxnMarkers,
    TxnOffsetCommit,
    DescribeAcls,
    CreateAcls,
    DeleteAcls,
    DescribeConfigs,
    AlterConfigs,
    AlterReplicaLogDirs,
    DescribeLogDirs,
    SaslAuthenticate,
    CreatePartitions,
    CreateDelegationToken,
    RenewDelegationToken,
    ExpireDelegationToken,
    DescribeDelegationToken,
    DeleteGroups,
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
            ApiKeys::DeleteRecords => "DeleteRecords",
            ApiKeys::InitProducerId => "InitProducerId",
            ApiKeys::OffsetForLeaderEpoch => "OffsetForLeaderEpoch",
            ApiKeys::AddPartitionsToTxn => "AddPartitionsToTxn",
            ApiKeys::AddOffsetsToTxn => "AddOffsetsToTxn",
            ApiKeys::EndTxn => "EndTxn",
            ApiKeys::WriteTxnMarkers => "WriteTxnMarkers",
            ApiKeys::TxnOffsetCommit => "TxnOffsetCommit",
            ApiKeys::DescribeAcls => "DescribeAcls",
            ApiKeys::CreateAcls => "CreateAcls",
            ApiKeys::DeleteAcls => "DeleteAcls",
            ApiKeys::DescribeConfigs => "DescribeConfigs",
            ApiKeys::AlterConfigs => "AlterConfigs",
            ApiKeys::AlterReplicaLogDirs => "AlterReplicaLogDirs",
            ApiKeys::DescribeLogDirs => "DescribeLogDirs",
            ApiKeys::SaslAuthenticate => "SaslAuthenticate",
            ApiKeys::CreatePartitions => "CreatePartitions",
            ApiKeys::CreateDelegationToken => "CreateDelegationToken",
            ApiKeys::RenewDelegationToken => "RenewDelegationToken",
            ApiKeys::ExpireDelegationToken => "ExpireDelegationToken",
            ApiKeys::DescribeDelegationToken => "DescribeDelegationToken",
            ApiKeys::DeleteGroups => "DeleteGroups",
        }
    }
}

impl From<ApiKey> for ApiKeys {
    fn from(v: ApiKey) -> Self {
        unsafe { mem::transmute(v) }
    }
}
