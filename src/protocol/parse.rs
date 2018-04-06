use std::borrow::Cow;
use std::collections::HashMap;
use std::str;

use bytes::Bytes;

use nom::{self, prepare_errors, print_offsets, IResult, be_i16, be_i32, error_to_u32};

macro_rules! parse_tag (
    ($i:expr, $tag:expr, $submac:ident!( $($args:tt)* )) => (
        {
            match $submac!($i, $($args)*) {
                $crate::nom::IResult::Done(i, o)    => $crate::nom::IResult::Done(i, o),
                $crate::nom::IResult::Incomplete(i) => $crate::nom::IResult::Incomplete(i),
                $crate::nom::IResult::Error(e)      => {
                    let code = ::nom::ErrorKind::Custom($tag as u32);

                    $crate::nom::IResult::Error(error_node_position!(code, $i, e))
                }
            }
        }
    );
    ($i:expr, $tag:expr, $f:expr) => (
        parse_tag!($i, $tag, call!($f));
    );
);

#[repr(i32)]
pub enum ParseTag {
    ResponseHeader = 8000,
    ApiKey = 8001,

    String = 8002,
    Bytes = 8003,

    MessageSet = 9001,
    Message = 9002,
    MessageCrc = 9003,

    ProduceResponse = 10000,
    ProduceTopicStatus = 10001,
    ProducePartitionStatus = 10002,

    FetchResponse = 10100,
    FetchTopicData = 10101,
    FetchPartitionData = 10102,

    ListOffsetResponse = 10200,
    ListOffsetTopicStatus = 10201,
    ListOffsetPartitionStatus = 10202,

    MetadataResponse = 10300,
    BrokerMetadata = 10301,
    TopicMetadata = 10302,
    PartitionMetadata = 10303,

    OffsetCommitResponse = 10800,
    OffsetCommitTopicStatus = 10801,
    OffsetCommitPartitionStatus = 10802,

    OffsetFetchResponse = 10900,
    OffsetFetchTopicStatus = 10901,
    OffsetFetchPartitionStatus = 10902,

    GroupCoordinatorResponse = 11000,

    JoinGroupResponse = 11100,
    JoinGroupMember = 11101,

    HeartbeatResponse = 11200,

    LeaveGroupResponse = 11300,

    SyncGroupResponse = 11400,

    DescribeGroupsResponse = 11500,
    DescribeGroupsGroupStatus = 11501,
    DescribeGroupsMemberStatus = 11502,

    ListGroupsResponse = 11600,
    ListGroupsGroupStatus = 11601,

    ApiVersionsResponse = 11800,
    ApiVersion = 11801,
}

lazy_static! {
    pub static ref PARSE_TAGS: HashMap<u32, &'static str> = {
        let mut h = HashMap::new();
        h.insert(ParseTag::ResponseHeader as u32, "ResponseHeader");

        h.insert(ParseTag::String as u32, "String");
        h.insert(ParseTag::Bytes as u32, "Bytes");

        h.insert(ParseTag::MessageSet as u32, "MessageSet");
        h.insert(ParseTag::Message as u32, "Message");
        h.insert(ParseTag::MessageCrc as u32, "MessageCrc");

        h.insert(ParseTag::ProduceResponse as u32, "ProduceResponse");
        h.insert(ParseTag::ProduceTopicStatus as u32, "ProduceTopicStatus");
        h.insert(ParseTag::ProducePartitionStatus as u32, "ProducePartitionStatus");

        h.insert(ParseTag::FetchResponse as u32, "FetchResponse");
        h.insert(ParseTag::FetchTopicData as u32, "FetchTopicData");
        h.insert(ParseTag::FetchPartitionData as u32, "FetchPartitionData");

        h.insert(ParseTag::ListOffsetResponse as u32, "OffsetResponse");
        h.insert(ParseTag::ListOffsetTopicStatus as u32, "ListOffsetTopicStatus");
        h.insert(ParseTag::ListOffsetPartitionStatus as u32, "ListOffsetPartitionStatus");

        h.insert(ParseTag::MetadataResponse as u32, "MetadataResponse");
        h.insert(ParseTag::BrokerMetadata as u32, "BrokerMetadata");
        h.insert(ParseTag::TopicMetadata as u32, "TopicMetadata");
        h.insert(ParseTag::PartitionMetadata as u32, "PartitionMetadata");

        h.insert(ParseTag::OffsetCommitResponse as u32, "OffsetCommitResponse");
        h.insert(ParseTag::OffsetCommitTopicStatus as u32, "OffsetCommitTopicStatus");
        h.insert(
            ParseTag::OffsetCommitPartitionStatus as u32,
            "OffsetCommitPartitionStatus",
        );

        h.insert(ParseTag::OffsetFetchResponse as u32, "OffsetFetchResponse");
        h.insert(ParseTag::OffsetFetchTopicStatus as u32, "OffsetFetchTopicStatus");
        h.insert(
            ParseTag::OffsetFetchPartitionStatus as u32,
            "OffsetFetchPartitionStatus",
        );

        h.insert(ParseTag::GroupCoordinatorResponse as u32, "GroupCoordinatorResponse");

        h.insert(ParseTag::JoinGroupResponse as u32, "JoinGroupResponse");
        h.insert(ParseTag::JoinGroupMember as u32, "JoinGroupMember");

        h.insert(ParseTag::HeartbeatResponse as u32, "HeartbeatResponse");

        h.insert(ParseTag::LeaveGroupResponse as u32, "LeaveGroupResponse");

        h.insert(ParseTag::SyncGroupResponse as u32, "SyncGroupResponse");

        h.insert(ParseTag::DescribeGroupsResponse as u32, "DescribeGroupsResponse");
        h.insert(ParseTag::DescribeGroupsGroupStatus as u32, "DescribeGroupsGroupStatus");
        h.insert(
            ParseTag::DescribeGroupsMemberStatus as u32,
            "DescribeGroupsMemberStatus",
        );

        h.insert(ParseTag::ListGroupsResponse as u32, "ListGroupsResponse");
        h.insert(ParseTag::ListGroupsGroupStatus as u32, "ListGroupsGroupStatus");

        h.insert(ParseTag::ApiVersionsResponse as u32, "ApiVersionsResponse");
        h.insert(ParseTag::ApiVersion as u32, "ApiVersion");
        h
    };
}

fn reset_color(v: &mut Vec<u8>) {
    v.push(0x1B);
    v.push(b'[');
    v.push(0);
    v.push(b'm');
}

fn write_color(v: &mut Vec<u8>, color: u8) {
    v.push(0x1B);
    v.push(b'[');
    v.push(1);
    v.push(b';');
    let s = color.to_string();
    let bytes = s.as_bytes();
    v.extend(bytes.iter().cloned());
    v.push(b'm');
}

fn print_codes<E>(colors: &HashMap<u32, (nom::ErrorKind<E>, u8)>, names: &HashMap<u32, &str>) -> String {
    let mut v = Vec::new();
    for (code, &(ref err, color)) in colors {
        if let Some(&s) = names.get(code) {
            let bytes = s.as_bytes();
            write_color(&mut v, color);
            v.extend(bytes.iter().cloned());
        } else {
            let s = err.description();
            let bytes = s.as_bytes();
            write_color(&mut v, color);
            v.extend(bytes.iter().cloned());
        }
        reset_color(&mut v);
        v.push(b' ');
    }
    reset_color(&mut v);

    String::from_utf8_lossy(&v[..]).into_owned()
}

fn generate_colors(v: &[(nom::ErrorKind<u32>, usize, usize)]) -> HashMap<u32, (nom::ErrorKind<u32>, u8)> {
    let mut h: HashMap<u32, (nom::ErrorKind<u32>, u8)> = HashMap::new();
    let mut color = 0;

    for &(ref c, _, _) in v.iter() {
        if let nom::ErrorKind::Custom(code) = *c {
            h.insert(code, (c.clone(), color + 31));
        } else {
            h.insert(error_to_u32(c), (c.clone(), color + 31));
        }
        color = (color + 1) % 7;
    }

    h
}

pub fn display_parse_error<O>(input: &[u8], res: IResult<&[u8], O>) {
    if let Some(v) = prepare_errors(input, res) {
        let colors = generate_colors(&v);
        trace!(
            "parsers codes: {}\n{}",
            print_codes(&colors, &PARSE_TAGS),
            print_offsets(input, 0, &v)
        );
    }
}

named!(pub parse_str<Cow<str>>,
    parse_tag!(ParseTag::String,
        do_parse!(
            len: be_i16
         >> s: cond_reduce!(len >= 0, map!(map_res!(take!(len), str::from_utf8), Cow::from))
         >> (s)
        )
    )
);

named!(pub parse_opt_str<Option<Cow<str>>>,
    parse_tag!(ParseTag::String,
        do_parse!(
            len: be_i16
         >> s: cond!(len >= 0, map!(map_res!(take!(len), str::from_utf8), Cow::from))
         >> (s)
        )
    )
);

named!(pub parse_opt_string<Option<String>>,
    parse_tag!(ParseTag::String,
        do_parse!(
            len: be_i16
         >> s: cond!(len >= 0, map!(map_res!(take!(len), str::from_utf8), ToOwned::to_owned))
         >> (s)
        )
    )
);

named!(pub parse_string<String>,
    parse_tag!(ParseTag::String,
        do_parse!(
            len: be_i16
         >> s: cond_reduce!(len >= 0, map!(map_res!(take!(len), str::from_utf8), ToOwned::to_owned))
         >> (s)
        )
    )
);

named!(pub parse_bytes<Bytes>,
    parse_tag!(ParseTag::Bytes,
        do_parse!(
            len: be_i32
         >> s: cond_reduce!(len >= 0, map!(take!(len), Bytes::from))
         >> (s)
        )
    )
);

named!(pub parse_opt_bytes<Option<Bytes>>,
    parse_tag!(ParseTag::Bytes,
        do_parse!(
            len: be_i32
         >> s: cond!(len >= 0, map!(take!(len), Bytes::from))
         >> (s)
        )
    )
);

#[cfg(test)]
mod tests {
    use std::str;

    use bytes::Bytes;

    use nom::verbose_errors::Err;
    use nom::{ErrorKind, IResult, Needed};

    use super::*;

    #[test]
    fn test_parse_str() {
        assert_eq!(parse_str(b"\0"), IResult::Incomplete(Needed::Size(2)));
        assert_eq!(
            parse_str(b"\xff\xff"),
            IResult::Error(Err::NodePosition(
                ErrorKind::Custom(ParseTag::String as u32),
                &[255, 255][..],
                vec![Err::Position(ErrorKind::CondReduce, &[][..])]
            ))
        );
        assert_eq!(parse_str(b"\0\0"), IResult::Done(&b""[..], Cow::from("")));
        assert_eq!(parse_str(b"\0\x04test"), IResult::Done(&b""[..], Cow::from("test")));
    }

    #[test]
    fn test_parse_opt_str() {
        assert_eq!(parse_opt_str(b"\0"), IResult::Incomplete(Needed::Size(2)));
        assert_eq!(parse_opt_str(b"\xff\xff"), IResult::Done(&b""[..], None));
        assert_eq!(parse_opt_str(b"\0\0"), IResult::Done(&b""[..], Some(Cow::from(""))));
        assert_eq!(
            parse_opt_str(b"\0\x04test"),
            IResult::Done(&b""[..], Some(Cow::from("test")))
        );
    }

    #[test]
    fn test_parse_string() {
        assert_eq!(parse_string(b"\0"), IResult::Incomplete(Needed::Size(2)));
        assert_eq!(
            parse_string(b"\xff\xff"),
            IResult::Error(Err::NodePosition(
                ErrorKind::Custom(ParseTag::String as u32),
                &[255, 255][..],
                vec![Err::Position(ErrorKind::CondReduce, &[][..])]
            ))
        );
        assert_eq!(parse_string(b"\0\0"), IResult::Done(&b""[..], "".to_owned()));
        assert_eq!(parse_string(b"\0\x04test"), IResult::Done(&b""[..], "test".to_owned()));
    }

    #[test]
    fn test_parse_opt_string() {
        assert_eq!(parse_opt_string(b"\0"), IResult::Incomplete(Needed::Size(2)));
        assert_eq!(parse_opt_string(b"\xff\xff"), IResult::Done(&b""[..], None));
        assert_eq!(parse_opt_string(b"\0\0"), IResult::Done(&b""[..], Some("".to_owned())));
        assert_eq!(
            parse_opt_string(b"\0\x04test"),
            IResult::Done(&b""[..], Some("test".to_owned()))
        );
    }

    #[test]
    fn test_parse_bytes() {
        assert_eq!(parse_bytes(b"\0"), IResult::Incomplete(Needed::Size(4)));
        assert_eq!(
            parse_bytes(b"\xff\xff\xff\xff"),
            IResult::Error(Err::NodePosition(
                ErrorKind::Custom(ParseTag::Bytes as u32),
                &[255, 255, 255, 255][..],
                vec![Err::Position(ErrorKind::CondReduce, &[][..])]
            ))
        );
        assert_eq!(parse_bytes(b"\0\0\0\0"), IResult::Done(&b""[..], Bytes::new()));
        assert_eq!(
            parse_bytes(b"\0\0\0\x04test"),
            IResult::Done(&b""[..], Bytes::from(&b"test"[..]))
        );
    }

    #[test]
    fn test_parse_opt_bytes() {
        assert_eq!(parse_opt_bytes(b"\0"), IResult::Incomplete(Needed::Size(4)));
        assert_eq!(parse_opt_bytes(b"\xff\xff\xff\xff"), IResult::Done(&b""[..], None));
        assert_eq!(
            parse_opt_bytes(b"\0\0\0\0"),
            IResult::Done(&b""[..], Some(Bytes::new()))
        );
        assert_eq!(
            parse_opt_bytes(b"\0\0\0\x04test"),
            IResult::Done(&b""[..], Some(Bytes::from(&b"test"[..])))
        );
    }
}
