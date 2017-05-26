use std::fmt;
use std::str;
use std::io::prelude::*;
use std::marker::PhantomData;
use std::result::Result as StdResult;

use serde::ser::{self, Serialize, SerializeSeq};
use serde::de::{self, Deserialize, Visitor};

use byteorder::{BigEndian, ByteOrder, ReadBytesExt, WriteBytesExt};

use errors::{Error, ErrorKind, Result};

pub struct Schema {}

impl Schema {
    pub fn serialize<T: Serialize>(v: &T) -> Result<Vec<u8>> {
        let mut serializer = SchemaSerializer::<BigEndian>::new();

        v.serialize(&mut serializer)?;

        Ok(serializer.bytes())
    }

    pub fn deserialize<'de, T, R>(input: R) -> Result<T>
        where T: Deserialize<'de>,
              R: Read + Clone
    {
        let mut deserializer = SchemaDeserializer::<BigEndian, R>::new(input);
        Ok(T::deserialize(&mut deserializer)?)
    }
}

#[derive(Clone, Debug)]
pub enum SchemaType {
    BOOLEAN,
    INT8,
    INT16,
    INT32,
    INT64,
    STRING,
    NULLABLE_STRING,
    BYTES,
    NULLABLE_BYTES,
    VARINT,
    VARLONG,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct VarInt(i32);

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct VarLong(i64);

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Nullable<T>(Option<T>);

impl From<i32> for VarInt {
    fn from(v: i32) -> Self {
        VarInt(v)
    }
}

impl From<i64> for VarLong {
    fn from(v: i64) -> Self {
        VarLong(v)
    }
}

impl<T> From<Option<T>> for Nullable<T> {
    fn from(v: Option<T>) -> Self {
        Nullable(v)
    }
}

impl Serialize for VarInt {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
        where S: ser::Serializer
    {
        let mut buf = Vec::with_capacity(8);
        let mut v = ((self.0 << 1) ^ (self.0 >> 31)) as u32;
        while (v & !0x7F) != 0 {
            buf.push(((v & 0x7f) | 0x80) as u8);
            v >>= 7;
        }
        buf.push(v as u8);
        serializer.serialize_bytes(&buf)
    }
}

impl<'de> Deserialize<'de> for VarInt {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
        where D: de::Deserializer<'de>
    {
        deserializer.deserialize_newtype_struct("VarInt", VarIntVisitor)
    }
}

struct VarIntVisitor;

impl<'de> Visitor<'de> for VarIntVisitor {
    type Value = VarInt;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("variable-length bytes using zig-zag decoding")
    }

    fn visit_seq<A>(self, mut seq: A) -> StdResult<Self::Value, A::Error>
        where A: de::SeqAccess<'de>
    {
        let mut value = 0;
        let mut i = 0;

        while let Ok(Some(b)) = seq.next_element::<u8>() {
            if (b & 0x80) != 0 {
                value |= ((b & 0x7f) as u32) << i;
                i += 7;
                if i > 28 {
                    return Err(de::Error::invalid_value(de::Unexpected::Unsigned(b as u64), &self));
                }
            } else {
                value |= (b as u32) << i;
                break;
            }
        }

        let v = (value >> 1) as i32 ^ -((value & 1) as i32);

        trace!("serialized varint: {}", v);

        Ok(VarInt(v))
    }
}

impl Serialize for VarLong {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
        where S: ser::Serializer
    {
        let mut buf = Vec::with_capacity(16);
        let mut v = ((self.0 << 1) ^ (self.0 >> 63)) as u64;
        while (v & !0x7F) != 0 {
            buf.push(((v & 0x7f) | 0x80) as u8);
            v >>= 7;
        }
        buf.push(v as u8);
        serializer.serialize_bytes(&buf)
    }
}

impl<'de> Deserialize<'de> for VarLong {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
        where D: de::Deserializer<'de>
    {
        deserializer.deserialize_newtype_struct("VarLong", VarLongVisitor)
    }
}

struct VarLongVisitor;

impl<'de> Visitor<'de> for VarLongVisitor {
    type Value = VarLong;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("variable-length bytes using zig-zag decoding")
    }

    fn visit_seq<A>(self, mut seq: A) -> StdResult<Self::Value, A::Error>
        where A: de::SeqAccess<'de>
    {
        let mut value = 0;
        let mut i = 0;

        while let Ok(Some(b)) = seq.next_element::<u8>() {
            if (b & 0x80) != 0 {
                value |= ((b & 0x7f) as u64) << i;
                i += 7;
                if i > 63 {
                    return Err(de::Error::invalid_value(de::Unexpected::Unsigned(b as u64), &self));
                }
            } else {
                value |= (b as u64) << i;
                break;
            }
        }

        let v = (value >> 1) as i64 ^ -((value & 1) as i64);

        trace!("serialized varlong: {}", v);

        Ok(VarLong(v))
    }
}

impl Serialize for Nullable<String> {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
        where S: ser::Serializer
    {
        if let Some(ref v) = self.0 {
            serializer.serialize_str(&v)
        } else {
            serializer.serialize_i16(-1)
        }
    }
}

impl Serialize for Nullable<Vec<u8>> {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
        where S: ser::Serializer
    {
        if let Some(ref v) = self.0 {
            let mut seq = serializer.serialize_seq(Some(v.len()))?;

            for b in v {
                seq.serialize_element(b)?;
            }

            seq.end()
        } else {
            serializer.serialize_i32(-1)
        }
    }
}

impl<'de> Deserialize<'de> for Nullable<String> {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
        where D: de::Deserializer<'de>
    {
        let visitor = NullableVisitor::<String>::new();

        deserializer.deserialize_str(visitor)
    }
}

impl<'de> Deserialize<'de> for Nullable<Vec<u8>> {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
        where D: de::Deserializer<'de>
    {
        let visitor = NullableVisitor::<Vec<u8>>::new();

        deserializer.deserialize_seq(visitor)
    }
}

struct NullableVisitor<T> {
    phantom: PhantomData<T>,
}

impl<T> NullableVisitor<T> {
    pub fn new() -> Self {
        NullableVisitor { phantom: PhantomData }
    }
}

impl<'de> de::Visitor<'de> for NullableVisitor<String> {
    type Value = Nullable<String>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("variable-length array with length prefix")
    }

    fn visit_str<E>(self, v: &str) -> StdResult<Self::Value, E>
        where E: de::Error
    {
        if v.is_empty() {
            Ok(Nullable(None))
        } else {
            Ok(Nullable(Some(v.to_owned())))
        }
    }
}

impl<'de> de::Visitor<'de> for NullableVisitor<Vec<u8>> {
    type Value = Nullable<Vec<u8>>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("variable-length bytes with 32bit length prefix")
    }

    fn visit_seq<A>(self, mut seq: A) -> StdResult<Self::Value, A::Error>
        where A: de::SeqAccess<'de>
    {
        let mut v = Vec::with_capacity(seq.size_hint().unwrap_or(16));

        while let Ok(Some(b)) = seq.next_element() {
            v.push(b);
        }

        if v.is_empty() {
            Ok(Nullable(None))
        } else {
            Ok(Nullable(Some(v)))
        }
    }
}

struct SchemaSerializer<O> {
    buf: Vec<u8>,
    phantom: PhantomData<O>,
}

impl<O> SchemaSerializer<O> {
    pub fn new() -> Self {
        SchemaSerializer {
            buf: Vec::with_capacity(64),
            phantom: PhantomData,
        }
    }

    pub fn bytes(self) -> Vec<u8> {
        self.buf
    }
}

impl<'a, O: ByteOrder> ser::Serializer for &'a mut SchemaSerializer<O> {
    type Ok = ();
    type Error = Error;
    type SerializeSeq = Self;
    type SerializeTuple = ser::Impossible<Self::Ok, Self::Error>;
    type SerializeTupleStruct = ser::Impossible<Self::Ok, Self::Error>;
    type SerializeTupleVariant = ser::Impossible<Self::Ok, Self::Error>;
    type SerializeMap = ser::Impossible<Self::Ok, Self::Error>;
    type SerializeStruct = Self;
    type SerializeStructVariant = ser::Impossible<Self::Ok, Self::Error>;

    fn serialize_bool(mut self, v: bool) -> Result<()> {
        self.buf.write_u8(if v { 1 } else { 0 })?;

        Ok(())
    }
    fn serialize_i8(mut self, v: i8) -> Result<()> {
        self.buf.write_i8(v)?;

        Ok(())
    }
    fn serialize_i16(mut self, v: i16) -> Result<()> {
        self.buf.write_i16::<O>(v)?;

        Ok(())
    }
    fn serialize_i32(mut self, v: i32) -> Result<()> {
        self.buf.write_i32::<O>(v)?;

        Ok(())
    }
    fn serialize_i64(mut self, v: i64) -> Result<()> {
        self.buf.write_i64::<O>(v)?;

        Ok(())
    }
    fn serialize_u8(mut self, v: u8) -> Result<()> {
        self.buf.write_u8(v)?;

        Ok(())
    }
    fn serialize_u16(mut self, v: u16) -> Result<()> {
        self.buf.write_u16::<O>(v)?;

        Ok(())
    }
    fn serialize_u32(mut self, v: u32) -> Result<()> {
        self.buf.write_u32::<O>(v)?;

        Ok(())
    }
    fn serialize_u64(mut self, v: u64) -> Result<()> {
        self.buf.write_u64::<O>(v)?;

        Ok(())
    }
    fn serialize_f32(self, v: f32) -> Result<()> {
        self.buf.write_f32::<O>(v)?;

        Ok(())
    }
    fn serialize_f64(self, v: f64) -> Result<()> {
        self.buf.write_f64::<O>(v)?;

        Ok(())
    }
    fn serialize_char(self, _v: char) -> Result<()> {
        bail!(ErrorKind::SchemaError("unsupported type: char".to_owned()))
    }
    fn serialize_str(mut self, v: &str) -> Result<()> {
        trace!("serialize str, len={}", v.len());

        if v.len() > i16::max_value() as usize {
            bail!(ErrorKind::SchemaError(format!("string length {} is larger than the maximum string length.",
                                                 v.len())))
        }

        self.buf.write_i16::<O>(v.len() as i16)?;
        self.buf.write_all(v.as_bytes())?;

        Ok(())
    }
    fn serialize_bytes(mut self, v: &[u8]) -> Result<()> {
        trace!("serialize bytes, len={}", v.len());

        self.buf.write_all(v)?;

        Ok(())
    }
    fn serialize_none(self) -> Result<()> {
        Ok(())
    }
    fn serialize_some<T: ?Sized>(self, value: &T) -> Result<()>
        where T: Serialize
    {
        value.serialize(self)
    }
    fn serialize_unit(self) -> Result<()> {
        bail!(ErrorKind::SchemaError("unsupported type: unit".to_owned()))
    }
    fn serialize_unit_struct(self, name: &'static str) -> Result<()> {
        bail!(ErrorKind::SchemaError(format!("unsupported type: unit struct `{}`", name)))
    }
    fn serialize_unit_variant(self,
                              name: &'static str,
                              _variant_index: u32,
                              _variant: &'static str)
                              -> Result<()> {
        bail!(ErrorKind::SchemaError(format!("unsupported type: unit variant `{}`", name)))
    }
    fn serialize_newtype_struct<T: ?Sized>(self, name: &'static str, _value: &T) -> Result<()>
        where T: Serialize
    {
        bail!(ErrorKind::SchemaError(format!("unsupported type: new type struct `{}`", name)))
    }
    fn serialize_newtype_variant<T: ?Sized>(self,
                                            name: &'static str,
                                            _variant_index: u32,
                                            _variant: &'static str,
                                            _value: &T)
                                            -> Result<()>
        where T: Serialize
    {
        bail!(ErrorKind::SchemaError(format!("unsupported type: new type variant `{}`", name)))
    }
    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq> {
        trace!("serialize seq with {} elements", len.unwrap_or_default());

        if let Some(len) = len {
            if len > i32::max_value() as usize {
                bail!(ErrorKind::SchemaError(format!("bytes length {} is larger than the maximum bytes length.",
                                                     len)));
            }

            self.buf.write_i32::<O>(len as i32)?;
        } else {
            self.buf.write_i32::<O>(-1)?;
        }

        Ok(self)
    }
    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple> {
        trace!("serialize tuple with {} elements", len);

        bail!(ErrorKind::SchemaError("unsupported tuple".to_owned()))
    }
    fn serialize_tuple_struct(self,
                              name: &'static str,
                              len: usize)
                              -> Result<Self::SerializeTupleStruct> {
        trace!("serialize tuple struct `{}` with {} elements", name, len);

        bail!(ErrorKind::SchemaError(format!("unsupported tuple struct `{}`", name)))
    }
    fn serialize_tuple_variant(self,
                               name: &'static str,
                               _variant_index: u32,
                               variant: &'static str,
                               len: usize)
                               -> Result<Self::SerializeTupleVariant> {
        trace!("serialize tuple variant `{}::{}` with {} elements",
               name,
               variant,
               len);

        bail!(ErrorKind::SchemaError(format!("unsupported tuple variant: {}", name)))
    }
    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap> {
        trace!("serialize map with {} items", len.unwrap_or_default());

        bail!(ErrorKind::SchemaError("unsupported map".to_owned()))
    }
    fn serialize_struct(self, name: &'static str, len: usize) -> Result<Self::SerializeStruct> {
        trace!("serialize struct `{}` with {} fields", name, len);

        Ok(self)
    }
    fn serialize_struct_variant(self,
                                name: &'static str,
                                _variant_index: u32,
                                variant: &'static str,
                                len: usize)
                                -> Result<Self::SerializeStructVariant> {
        trace!("serialize struct variant `{}::{}` with {} elements",
               name,
               variant,
               len);

        bail!(ErrorKind::SchemaError(format!("unsupported struct variant: {}", name)))
    }
}

impl<'a, O: ByteOrder> ser::SerializeSeq for &'a mut SchemaSerializer<O> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<()>
        where T: Serialize
    {
        value.serialize(&mut **self)
    }
    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a, O: ByteOrder> ser::SerializeStruct for &'a mut SchemaSerializer<O> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: ?Sized>(&mut self, key: &'static str, value: &T) -> Result<()>
        where T: Serialize
    {
        trace!("serialize field `{}`", key);

        value.serialize(&mut **self)
    }
    fn end(self) -> Result<()> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct SchemaDeserializer<O, R> {
    input: R,
    phantom: PhantomData<O>,
}

impl<O, R> SchemaDeserializer<O, R> {
    pub fn new(input: R) -> Self {
        SchemaDeserializer {
            input: input,
            phantom: PhantomData,
        }
    }
}

impl<'de, 'a, O, R> de::Deserializer<'de> for &'a mut SchemaDeserializer<O, R>
    where O: ByteOrder,
          R: Read + Clone
{
    type Error = Error;

    fn deserialize_any<V>(self, _visitor: V) -> StdResult<V::Value, Self::Error>
        where V: Visitor<'de>
    {
        unimplemented!()
    }
    fn deserialize_bool<V>(self, visitor: V) -> StdResult<V::Value, Self::Error>
        where V: Visitor<'de>
    {
        let v = self.input.read_u8()? != 0;

        trace!("deserialized bool: {}", v);

        visitor.visit_bool(v)
    }
    fn deserialize_i8<V>(self, visitor: V) -> StdResult<V::Value, Self::Error>
        where V: Visitor<'de>
    {
        let v = self.input.read_i8()?;

        trace!("deserialized i8: {}", v);

        visitor.visit_i8(v)
    }
    fn deserialize_i16<V>(self, visitor: V) -> StdResult<V::Value, Self::Error>
        where V: Visitor<'de>
    {
        let v = self.input.read_i16::<O>()?;

        trace!("deserialized i16: {}", v);

        visitor.visit_i16(v)
    }
    fn deserialize_i32<V>(self, visitor: V) -> StdResult<V::Value, Self::Error>
        where V: Visitor<'de>
    {
        let v = self.input.read_i32::<O>()?;

        trace!("deserialized i32: {}", v);

        visitor.visit_i32(v)
    }
    fn deserialize_i64<V>(self, visitor: V) -> StdResult<V::Value, Self::Error>
        where V: Visitor<'de>
    {
        let v = self.input.read_i64::<O>()?;

        trace!("deserialized i64: {}", v);

        visitor.visit_i64(v)
    }
    fn deserialize_u8<V>(self, visitor: V) -> StdResult<V::Value, Self::Error>
        where V: Visitor<'de>
    {
        let v = self.input.read_u8()?;

        trace!("deserialized u8: {}", v);

        visitor.visit_u8(v)
    }
    fn deserialize_u16<V>(self, visitor: V) -> StdResult<V::Value, Self::Error>
        where V: Visitor<'de>
    {
        let v = self.input.read_u16::<O>()?;

        trace!("deserialized u16: {}", v);

        visitor.visit_u16(v)
    }
    fn deserialize_u32<V>(self, visitor: V) -> StdResult<V::Value, Self::Error>
        where V: Visitor<'de>
    {
        let v = self.input.read_u32::<O>()?;

        trace!("deserialized u32: {}", v);

        visitor.visit_u32(v)
    }
    fn deserialize_u64<V>(self, visitor: V) -> StdResult<V::Value, Self::Error>
        where V: Visitor<'de>
    {
        let v = self.input.read_u64::<O>()?;

        trace!("deserialized u64: {}", v);

        visitor.visit_u64(v)
    }
    fn deserialize_f32<V>(self, visitor: V) -> StdResult<V::Value, Self::Error>
        where V: Visitor<'de>
    {
        let v = self.input.read_f32::<O>()?;

        trace!("deserialized f32: {}", v);

        visitor.visit_f32(v)
    }
    fn deserialize_f64<V>(self, visitor: V) -> StdResult<V::Value, Self::Error>
        where V: Visitor<'de>
    {
        let v = self.input.read_f64::<O>()?;

        trace!("deserialized f64: {}", v);

        visitor.visit_f64(v)
    }
    fn deserialize_char<V>(self, _visitor: V) -> StdResult<V::Value, Self::Error>
        where V: Visitor<'de>
    {
        trace!("deserialize char");

        unimplemented!()
    }
    fn deserialize_str<V>(self, visitor: V) -> StdResult<V::Value, Self::Error>
        where V: Visitor<'de>
    {
        let len = self.input.read_i16::<O>()?;

        trace!("deserialize str with {} bytes", len);

        if len < 0 {
            visitor.visit_str("")
        } else {
            let mut buf = vec![0u8; len as usize];

            self.input.read_exact(&mut buf)?;

            let s = str::from_utf8(&buf)?;

            trace!("deserialized str: {}", s);

            visitor.visit_str(s)
        }
    }
    fn deserialize_string<V>(self, visitor: V) -> StdResult<V::Value, Self::Error>
        where V: Visitor<'de>
    {
        trace!("deserialize string");

        self.deserialize_str(visitor)
    }
    fn deserialize_bytes<V>(self, visitor: V) -> StdResult<V::Value, Self::Error>
        where V: Visitor<'de>
    {
        let len = self.input.read_i32::<O>()?;

        trace!("deserialize bytes with {} elements", len);

        if len > 0 {
            let mut buf = vec![0u8; len as usize];

            self.input.read_exact(&mut buf)?;

            visitor.visit_byte_buf(buf)
        } else {
            visitor.visit_borrowed_bytes(&[])
        }
    }
    fn deserialize_byte_buf<V>(self, visitor: V) -> StdResult<V::Value, Self::Error>
        where V: Visitor<'de>
    {
        trace!("deserialize bytes buf");

        self.deserialize_bytes(visitor)
    }
    fn deserialize_option<V>(self, _visitor: V) -> StdResult<V::Value, Self::Error>
        where V: Visitor<'de>
    {
        trace!("deserialize option");

        unimplemented!()
    }
    fn deserialize_unit<V>(self, visitor: V) -> StdResult<V::Value, Self::Error>
        where V: Visitor<'de>
    {
        trace!("deserialize unit");

        visitor.visit_unit()
    }
    fn deserialize_unit_struct<V>(self,
                                  name: &'static str,
                                  _visitor: V)
                                  -> StdResult<V::Value, Self::Error>
        where V: Visitor<'de>
    {
        trace!("deserialize unit struct `{}`", name);

        unimplemented!()
    }
    fn deserialize_newtype_struct<V>(self,
                                     name: &'static str,
                                     visitor: V)
                                     -> StdResult<V::Value, Self::Error>
        where V: Visitor<'de>
    {
        trace!("deserialize new type struct `{}`", name);

        visitor.visit_seq(SeqVisitor::<O, R>::new(self, isize::max_value()))
    }
    fn deserialize_seq<V>(self, visitor: V) -> StdResult<V::Value, Self::Error>
        where V: Visitor<'de>
    {
        let len = self.input.read_i32::<O>()?;

        trace!("deserialize seq with {} elements", len);

        visitor.visit_seq(SeqVisitor::<O, R>::new(self, len as isize))
    }
    fn deserialize_tuple<V>(self, len: usize, _visitor: V) -> StdResult<V::Value, Self::Error>
        where V: Visitor<'de>
    {
        trace!("deserialize tuple with {} elements", len);

        unimplemented!()
    }
    fn deserialize_tuple_struct<V>(self,
                                   name: &'static str,
                                   len: usize,
                                   _visitor: V)
                                   -> StdResult<V::Value, Self::Error>
        where V: Visitor<'de>
    {
        trace!("deserialize tuple struct `{}` with {} elements", name, len);

        unimplemented!()
    }
    fn deserialize_map<V>(self, _visitor: V) -> StdResult<V::Value, Self::Error>
        where V: Visitor<'de>
    {
        trace!("deserialize map");

        unimplemented!()
    }
    fn deserialize_struct<V>(self,
                             name: &'static str,
                             fields: &'static [&'static str],
                             visitor: V)
                             -> StdResult<V::Value, Self::Error>
        where V: Visitor<'de>
    {
        trace!("deserialize struct `{}` with {} fields", name, fields.len());

        visitor.visit_seq(StructVisitor::<O, R>::new(self, name, fields))
    }
    fn deserialize_enum<V>(self,
                           name: &'static str,
                           variants: &'static [&'static str],
                           _visitor: V)
                           -> StdResult<V::Value, Self::Error>
        where V: Visitor<'de>
    {
        trace!("deserialize enum `{}` with {} variants",
               name,
               variants.len());

        unimplemented!()
    }
    fn deserialize_identifier<V>(self, _visitor: V) -> StdResult<V::Value, Self::Error>
        where V: Visitor<'de>
    {
        trace!("deserialize identitfier");

        unimplemented!()
    }
    fn deserialize_ignored_any<V>(self, _visitor: V) -> StdResult<V::Value, Self::Error>
        where V: Visitor<'de>
    {
        trace!("deserialize ignored any");

        unimplemented!()
    }
}

struct SeqVisitor<'a, O: 'a, R: 'a> {
    deserializer: &'a mut SchemaDeserializer<O, R>,
    len: isize,
    pos: isize,
}

impl<'a, O: 'a, R: 'a> SeqVisitor<'a, O, R> {
    pub fn new(deserializer: &'a mut SchemaDeserializer<O, R>, len: isize) -> Self {
        SeqVisitor {
            deserializer: deserializer,
            len: len,
            pos: 0,
        }
    }
}

impl<'a, 'de, O, R> de::SeqAccess<'de> for SeqVisitor<'a, O, R>
    where O: 'a + ByteOrder,
          R: 'a + Read + Clone
{
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> StdResult<Option<T::Value>, Self::Error>
        where T: de::DeserializeSeed<'de>
    {
        if self.pos >= self.len {
            Ok(None)
        } else {
            trace!("deserialize #{} element", self.pos);

            self.pos += 1;

            seed.deserialize(&mut *self.deserializer).map(Some)
        }
    }

    fn size_hint(&self) -> Option<usize> {
        if self.len > 0 {
            Some(self.len as usize)
        } else {
            None
        }
    }
}

struct StructVisitor<'a, O: 'a, R: 'a> {
    deserializer: &'a mut SchemaDeserializer<O, R>,
    name: &'static str,
    fields: &'static [&'static str],
    pos: usize,
}

impl<'a, O: 'a, R: 'a> StructVisitor<'a, O, R> {
    pub fn new(deserializer: &'a mut SchemaDeserializer<O, R>,
               name: &'static str,
               fields: &'static [&'static str])
               -> Self {
        StructVisitor {
            deserializer: deserializer,
            name: name,
            fields: fields,
            pos: 0,
        }
    }
}

impl<'a, 'de, O, R> de::SeqAccess<'de> for StructVisitor<'a, O, R>
    where O: 'a + ByteOrder,
          R: 'a + Read + Clone
{
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> StdResult<Option<T::Value>, Self::Error>
        where T: de::DeserializeSeed<'de>
    {
        if self.pos >= self.fields.len() {
            Ok(None)
        } else {
            trace!("deserialize `{}::{}` field",
                   self.name,
                   self.fields[self.pos]);

            self.pos += 1;
            seed.deserialize(&mut *self.deserializer).map(Some)
        }
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.fields.len())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct TestSchema {
        boolean: bool,
        int8: i8,
        int16: i16,
        int32: i32,
        uint32: u32,
        int64: i64,
        string: String,
        nullable_string: Nullable<String>,
        null_string: Nullable<String>,
        bytes: Vec<u8>,
        nullable_bytes: Nullable<Vec<u8>>,
        null_bytes: Nullable<Vec<u8>>,
        varint: VarInt,
        varlong: VarLong,
        sub_schema: SubSchema,
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct SubSchema {
        name: String,
    }

    lazy_static! {
        static ref TEST_SCHEMA: TestSchema = TestSchema {
            boolean: true,
            int8: -8,
            int16: -16,
            int32: -32,
            uint32: 32,
            int64: -64,
            string: "str".to_owned(),
            nullable_string: Some("str".to_owned()).into(),
            null_string: None.into(),
            bytes: b"bytes".to_vec(),
            nullable_bytes: Some(b"bytes".to_vec()).into(),
            null_bytes: None.into(),
            varint: 123456.into(),
            varlong: 1234567890.into(),
            sub_schema: SubSchema { name: "name".to_owned() },
        };

        static ref TEST_DATA: Vec<u8> = vec![
            1,                                          // boolean
            248,                                        // int8
            255, 240,                                   // int16
            255, 255, 255, 224,                         // int32
            0, 0, 0, 32,                                // uint32
            255, 255, 255, 255, 255, 255, 255, 192,     // int64
            0, 3, b's', b't', b'r',                     // string
            0, 3, b's', b't', b'r',                     // nullable_string
            255, 255,                                   // null_string
            0, 0, 0, 5, b'b', b'y', b't', b'e', b's',   // bytes
            0, 0, 0, 5, b'b', b'y', b't', b'e', b's',   // nullable_bytes
            255, 255, 255, 255,                         // null_bytes
            128, 137, 15,                               // varint
            164, 139, 176, 153, 9,                      // varlong
            0, 4, 110, 97, 109, 101,                    // sub_schema
        ];
    }

    #[test]
    fn test_schema_serializer() {
        assert_eq!(Schema::serialize(&*TEST_SCHEMA).unwrap(), *TEST_DATA);
    }

    #[test]
    fn test_schema_deserializer() {
        let schema: TestSchema = Schema::deserialize(Cursor::new(TEST_DATA.clone())).unwrap();

        assert_eq!(schema, *TEST_SCHEMA);
    }
}
