use std::str;
use std::borrow::Cow;
use std::collections::HashMap;

use bytes::{BufMut, ByteOrder, Bytes, BytesMut};

use nom::{self, IResult, be_i16, be_i32, error_to_u32, prepare_errors, print_offsets};

use errors::{ErrorKind, Result};

pub trait Encodable {
    fn encode<T: ByteOrder>(self, buf: &mut BytesMut) -> Result<()>;
}

pub trait WriteExt: BufMut + Sized {
    fn put_str<T: ByteOrder, S: AsRef<str>>(&mut self, s: Option<S>) -> Result<()> {
        match s.as_ref() {
            Some(v) if v.as_ref().len() > i16::max_value() as usize => {
                bail!(ErrorKind::EncodeError("string exceeds the maximum size."))
            }
            Some(v) if !v.as_ref().is_empty() => {
                self.put_i16::<T>(v.as_ref().len() as i16);
                self.put_slice(v.as_ref().as_bytes());
                Ok(())
            }
            _ => {
                self.put_i16::<T>(-1);
                Ok(())
            }
        }
    }

    fn put_bytes<T: ByteOrder, D: AsRef<[u8]>>(&mut self, d: Option<D>) -> Result<()> {
        match d.as_ref() {
            Some(v) if v.as_ref().len() > i32::max_value() as usize => {
                bail!(ErrorKind::EncodeError("bytes exceeds the maximum size."))
            }
            Some(v) if !v.as_ref().is_empty() => {
                self.put_i32::<T>(v.as_ref().len() as i32);
                self.put_slice(v.as_ref());
                Ok(())
            }
            _ => {
                self.put_i32::<T>(-1);
                Ok(())
            }
        }
    }

    fn put_array<T, E, F>(&mut self, items: Vec<E>, mut callback: F) -> Result<()>
        where T: ByteOrder,
              F: FnMut(&mut Self, E) -> Result<()>
    {
        if items.len() > i32::max_value() as usize {
            bail!(ErrorKind::EncodeError("array exceeds the maximum size."))
        } else {
            self.put_i32::<T>(items.len() as i32);

            for item in items {
                callback(self, item)?;
            }

            Ok(())
        }
    }
}

impl<T: BufMut> WriteExt for T {}

#[macro_export]
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
    TopicStatus = 10001,
    PartitionStatus = 10002,

    FetchResponse = 10100,
    TopicData = 10101,
    PartitionData = 10102,

    ListOffsetResponse = 10200,
    TopicOffset = 10201,
    PartitionOffset = 10202,

    MetadataResponse = 10300,
    BrokerMetadata = 10301,
    TopicMetadata = 10302,
    PartitionMetadata = 10303,

    ApiVersionsResponse = 11800,
    ApiVersion = 11801,
}

lazy_static!{
    pub static ref PARSE_TAGS: HashMap<u32, &'static str> = {
        let mut h = HashMap::new();
        h.insert(ParseTag::ResponseHeader as u32, "ResponseHeader");

        h.insert(ParseTag::String as u32, "String");
        h.insert(ParseTag::Bytes as u32, "Bytes");

        h.insert(ParseTag::MessageSet as u32, "MessageSet");
        h.insert(ParseTag::Message as u32, "Message");
        h.insert(ParseTag::MessageCrc as u32, "MessageCrc");

        h.insert(ParseTag::ProduceResponse as u32, "ProduceResponse");
        h.insert(ParseTag::TopicStatus as u32, "TopicStatus");
        h.insert(ParseTag::PartitionStatus as u32, "PartitionStatus");

        h.insert(ParseTag::FetchResponse as u32, "FetchResponse");
        h.insert(ParseTag::TopicData as u32, "TopicData");
        h.insert(ParseTag::PartitionData as u32, "PartitionData");

        h.insert(ParseTag::ListOffsetResponse as u32, "OffsetResponse");
        h.insert(ParseTag::TopicOffset as u32, "TopicOffset");
        h.insert(ParseTag::PartitionOffset as u32, "PartitionOffset");

        h.insert(ParseTag::MetadataResponse as u32, "MetadataResponse");
        h.insert(ParseTag::BrokerMetadata as u32, "BrokerMetadata");
        h.insert(ParseTag::TopicMetadata as u32, "TopicMetadata");
        h.insert(ParseTag::PartitionMetadata as u32, "PartitionMetadata");

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

fn print_codes<E>(colors: &HashMap<u32, (nom::ErrorKind<E>, u8)>,
                  names: &HashMap<u32, &str>)
                  -> String {
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

fn generate_colors(v: &[(nom::ErrorKind<u32>, usize, usize)])
                   -> HashMap<u32, (nom::ErrorKind<u32>, u8)> {
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
        trace!("parsers codes: {}\n{}",
               print_codes(&colors, &PARSE_TAGS),
               print_offsets(input, 0, &v));
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
    use std::slice;
    use std::iter::repeat;

    use bytes::{BigEndian, Bytes};

    use nom::{ErrorKind, IResult, Needed};
    use nom::verbose_errors::Err;

    use super::*;

    #[test]
    fn nullable_str() {
        let mut buf = vec![];

        // write empty nullable string
        buf.put_str::<BigEndian, _>(Some("")).unwrap();

        assert_eq!(buf.as_slice(), &[255, 255]);

        buf.clear();

        // write null of nullable string
        buf.put_str::<BigEndian, String>(None).unwrap();

        assert_eq!(buf.as_slice(), &[255, 255]);

        buf.clear();

        // write nullable string
        buf.put_str::<BigEndian, _>(Some("test")).unwrap();

        assert_eq!(buf.as_slice(), &[0, 4, 116, 101, 115, 116]);

        buf.clear();

        // write encoded nullable string
        buf.put_str::<BigEndian, _>(Some("测试")).unwrap();

        assert_eq!(buf.as_slice(), &[0, 6, 230, 181, 139, 232, 175, 149]);

        buf.clear();

        // write too long nullable string
        let s = repeat(20)
            .take(i16::max_value() as usize + 1)
            .collect::<Vec<u8>>();

        assert!(buf.put_str::<BigEndian, _>(Some(String::from_utf8(s).unwrap()))
                    .err()
                    .is_some());
    }

    #[test]
    fn nullable_bytes() {
        let mut buf = vec![];

        // write empty nullable bytes
        buf.put_bytes::<BigEndian, _>(Some(&b""[..])).unwrap();

        assert_eq!(buf.as_slice(), &[255, 255, 255, 255]);

        buf.clear();

        // write null of nullable bytes
        buf.put_bytes::<BigEndian, &[u8]>(None).unwrap();

        assert_eq!(buf.as_slice(), &[255, 255, 255, 255]);

        buf.clear();

        // write nullable bytes
        buf.put_bytes::<BigEndian, _>(Some(&b"test"[..]))
            .unwrap();

        assert_eq!(buf.as_slice(), &[0, 0, 0, 4, 116, 101, 115, 116]);

        buf.clear();

        // write too long nullable bytes
        let s = unsafe { slice::from_raw_parts(buf.as_ptr(), i32::max_value() as usize + 1) };

        assert!(buf.put_bytes::<BigEndian, _>(Some(s)).err().is_some());
    }

    #[test]
    fn test_parse_str() {
        assert_eq!(parse_str(b"\0"), IResult::Incomplete(Needed::Size(2)));
        assert_eq!(parse_str(b"\xff\xff"),
                   IResult::Error(Err::NodePosition(ErrorKind::Custom(ParseTag::String as u32),
                                                    &[255, 255][..],
                                                    vec![Err::Position(ErrorKind::CondReduce,
                                                                       &[][..])])));
        assert_eq!(parse_str(b"\0\0"), IResult::Done(&b""[..], Cow::from("")));
        assert_eq!(parse_str(b"\0\x04test"),
                   IResult::Done(&b""[..], Cow::from("test")));
    }

    #[test]
    fn test_parse_opt_str() {
        assert_eq!(parse_opt_str(b"\0"), IResult::Incomplete(Needed::Size(2)));
        assert_eq!(parse_opt_str(b"\xff\xff"), IResult::Done(&b""[..], None));
        assert_eq!(parse_opt_str(b"\0\0"),
                   IResult::Done(&b""[..], Some(Cow::from(""))));
        assert_eq!(parse_opt_str(b"\0\x04test"),
                   IResult::Done(&b""[..], Some(Cow::from("test"))));
    }

    #[test]
    fn test_parse_string() {
        assert_eq!(parse_string(b"\0"), IResult::Incomplete(Needed::Size(2)));
        assert_eq!(parse_string(b"\xff\xff"),
                   IResult::Error(Err::NodePosition(ErrorKind::Custom(ParseTag::String as u32),
                                                    &[255, 255][..],
                                                    vec![Err::Position(ErrorKind::CondReduce,
                                                                       &[][..])])));
        assert_eq!(parse_string(b"\0\0"),
                   IResult::Done(&b""[..], "".to_owned()));
        assert_eq!(parse_string(b"\0\x04test"),
                   IResult::Done(&b""[..], "test".to_owned()));
    }

    #[test]
    fn test_parse_opt_string() {
        assert_eq!(parse_opt_string(b"\0"),
                   IResult::Incomplete(Needed::Size(2)));
        assert_eq!(parse_opt_string(b"\xff\xff"), IResult::Done(&b""[..], None));
        assert_eq!(parse_opt_string(b"\0\0"),
                   IResult::Done(&b""[..], Some("".to_owned())));
        assert_eq!(parse_opt_string(b"\0\x04test"),
                   IResult::Done(&b""[..], Some("test".to_owned())));
    }

    #[test]
    fn test_parse_bytes() {
        assert_eq!(parse_bytes(b"\0"), IResult::Incomplete(Needed::Size(4)));
        assert_eq!(parse_bytes(b"\xff\xff\xff\xff"),
                   IResult::Error(Err::NodePosition(ErrorKind::Custom(ParseTag::Bytes as u32),
                                                    &[255, 255, 255, 255][..],
                                                    vec![Err::Position(ErrorKind::CondReduce,
                                                                       &[][..])])));
        assert_eq!(parse_bytes(b"\0\0\0\0"),
                   IResult::Done(&b""[..], Bytes::new()));
        assert_eq!(parse_bytes(b"\0\0\0\x04test"),
                   IResult::Done(&b""[..], Bytes::from(&b"test"[..])));
    }

    #[test]
    fn test_parse_opt_bytes() {
        assert_eq!(parse_opt_bytes(b"\0"), IResult::Incomplete(Needed::Size(4)));
        assert_eq!(parse_opt_bytes(b"\xff\xff\xff\xff"),
                   IResult::Done(&b""[..], None));
        assert_eq!(parse_opt_bytes(b"\0\0\0\0"),
                   IResult::Done(&b""[..], Some(Bytes::new())));
        assert_eq!(parse_opt_bytes(b"\0\0\0\x04test"),
                   IResult::Done(&b""[..], Some(Bytes::from(&b"test"[..]))));
    }
}
