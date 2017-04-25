use std::str;
use std::borrow::Cow;
use std::collections::HashMap;

use bytes::{BufMut, ByteOrder};

use nom::{self, IResult, be_i16, be_i32, prepare_errors, error_to_u32, print_offsets};

use errors::{ErrorKind, Result};

pub trait Encodable {
    fn encode<T: ByteOrder, B: BufMut>(self, buf: B) -> Result<()>;
}

pub trait WriteExt: BufMut {
    fn put_str<T: ByteOrder, S: AsRef<str>>(&mut self, s: Option<S>) -> Result<()> {
        match s.as_ref() {
            Some(v) if v.as_ref().len() > i16::max_value() as usize => {
                bail!(ErrorKind::CodecError("String exceeds the maximum size."))
            }
            Some(v) if v.as_ref().len() > 0 => {
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
                bail!(ErrorKind::CodecError("Bytes exceeds the maximum size."))
            }
            Some(v) if v.as_ref().len() > 0 => {
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
            bail!(ErrorKind::CodecError("Array exceeds the maximum size."))
        } else {
            self.put_i32::<T>(items.len() as i32);

            for item in items {
                callback(self, item)?;
            }

            Ok(())
        }
    }

    fn put_item<T: ByteOrder, E: Encodable>(&mut self, item: E) -> Result<()> {
        item.encode::<T, _>(self)
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

                    return $crate::nom::IResult::Error(error_node_position!(code, $i, e));
                }
            }
        }
    );
    ($i:expr, $tag:expr, $f:expr) => (
        parse_tag!($i, $tag, call!($f));
    );
);

#[repr(u32)]
pub enum ParseTag {
    ResponseHeader = 1001,
    CorrelationId = 1002,
    MetadataBrokers = 1010,
    MetadataTopics = 1011,
}

lazy_static!{
    pub static ref PARSE_TAGS: HashMap<u32, &'static str> = {
        let mut h = HashMap::new();
        h.insert(ParseTag::ResponseHeader as u32, "response_header");
        h.insert(ParseTag::CorrelationId as u32, "correlation_id");
        h.insert(ParseTag::MetadataBrokers as u32, "metadata_brokers");
        h.insert(ParseTag::MetadataTopics as u32, "metadata_topics");
        h
    };
}

fn reset_color(v: &mut Vec<u8>) {
    v.push(0x1B);
    v.push('[' as u8);
    v.push(0);
    v.push('m' as u8);
}

fn write_color(v: &mut Vec<u8>, color: u8) {
    v.push(0x1B);
    v.push('[' as u8);
    v.push(1);
    v.push(';' as u8);
    let s = color.to_string();
    let bytes = s.as_bytes();
    v.extend(bytes.iter().cloned());
    v.push('m' as u8);
}

fn print_codes<E>(colors: HashMap<u32, (nom::ErrorKind<E>, u8)>,
                  names: HashMap<u32, &str>)
                  -> String {
    let mut v = Vec::new();
    for (code, &(ref err, color)) in &colors {
        if let Some(&s) = names.get(&code) {
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
        v.push(' ' as u8);
    }
    reset_color(&mut v);

    String::from_utf8_lossy(&v[..]).into_owned()
}

fn generate_colors(v: &[(nom::ErrorKind<u32>, usize, usize)])
                   -> HashMap<u32, (nom::ErrorKind<u32>, u8)> {
    let mut h: HashMap<u32, (nom::ErrorKind<u32>, u8)> = HashMap::new();
    let mut color = 0;

    for &(ref c, _, _) in v.iter() {
        if let &nom::ErrorKind::Custom(code) = c {
            h.insert(code, (c.clone(), color + 31));
        } else {
            h.insert(error_to_u32(&c), (c.clone(), color + 31));
        }
        color = color + 1 % 7;
    }

    h
}

pub fn display_parse_error<O>(input: &[u8], res: IResult<&[u8], O>) {
    if let Some(v) = prepare_errors(input, res) {
        let colors = generate_colors(&v);
        trace!("parsers codes: {}\n{}",
               print_codes(colors, PARSE_TAGS.clone()),
               print_offsets(input, 0, &v));
    }
}

named!(pub parse_str<Option<Cow<str>>>,
    do_parse!(
        len: be_i16
     >> s: cond!(len > 0, map!(map_res!(take!(len), str::from_utf8), Cow::from))
     >> (s)
    )
);

named!(pub parse_string<String>,
    do_parse!(
        len: be_i16
     >> s: cond_reduce!(len > 0, map!(map_res!(take!(len), str::from_utf8), ToOwned::to_owned))
     >> (s)
    )
);

named!(pub parse_bytes<Option<Cow<[u8]>>>,
    do_parse!(
        len: be_i32
     >> s: cond!(len > 0, map!(take!(len), Cow::from))
     >> (s)
    )
);

#[cfg(test)]
mod tests {
    use std::str;
    use std::slice;
    use std::iter::repeat;

    use bytes::BigEndian;

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
}
