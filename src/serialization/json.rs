use std::str;
use std::marker::PhantomData;

use bytes::{Buf, BufMut};

use serde;
use serde_json::{self, Value};

use errors::{Error, Result};
use serialization::{Deserializer, Serializer};

/// Serialize `String` with UTF-8 encoding
#[derive(Clone, Debug, Default)]
pub struct JsonSerializer<T> {
    pretty: bool,
    phantom: PhantomData<T>,
}

impl<T> JsonSerializer<T> {
    pub fn pretty() -> Self {
        JsonSerializer {
            pretty: true,
            phantom: PhantomData,
        }
    }
}

impl<T> Serializer for JsonSerializer<T>
    where T: serde::Serialize
{
    type Item = T;
    type Error = Error;

    fn serialize_to<B: BufMut>(&self,
                               _topic_name: &str,
                               data: Self::Item,
                               buf: &mut B)
                               -> Result<()> {
        let to_vec = if self.pretty {
            serde_json::to_vec_pretty
        } else {
            serde_json::to_vec
        };

        buf.put_slice(&to_vec(&data)?);
        Ok(())
    }
}

/// Deserialize `String` as UTF-8 encoding
#[derive(Clone, Debug, Default)]
pub struct JsonDeserializer<T> {
    phantom: PhantomData<T>,
}

impl<'de, T> Deserializer for JsonDeserializer<T>
    where T: serde::Deserialize<'de>
{
    type Item = T;
    type Error = Error;

    fn deserialize_to<B: Buf>(&self,
                              _topic_name: &str,
                              buf: &mut B,
                              data: &mut Self::Item)
                              -> Result<()> {
        let len = buf.remaining();
        let v: Value = serde_json::from_slice(buf.bytes())?;
        *data = T::deserialize(v)?;
        buf.advance(len);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use std::io::Cursor;

    use bytes::Bytes;

    use super::*;

    #[test]
    fn test_seraizlie() {
        let serializer = JsonSerializer::default();
        let mut buf = Vec::new();
        let d = Duration::new(123, 456);
        let json = r#"{"secs":123,"nanos":456}"#;

        serializer.serialize_to("topic", d, &mut buf).unwrap();

        assert_eq!(str::from_utf8(&buf).unwrap(), json);

        assert_eq!(serializer.serialize("topic", d).unwrap(), Bytes::from(json));
    }

    #[test]
    fn test_deserialize() {
        let deserializer = JsonDeserializer::default();
        let data = r#"{"secs":123,"nanos":456}"#;
        let d = Duration::new(123, 456);
        let mut cur = Cursor::new(data.clone());
        let mut s = Duration::default();

        deserializer
            .deserialize_to("topic", &mut cur, &mut s)
            .unwrap();

        assert_eq!(cur.position() as usize, data.len());
        assert_eq!(s, d);

        cur.set_position(0);

        assert_eq!(deserializer.deserialize("topic", &mut cur).unwrap(), d);
    }
}
