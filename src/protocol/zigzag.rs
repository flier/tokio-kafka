use std::ops::ShrAssign;

use num::{PrimInt, ToPrimitive};
use bytes::BufMut;

pub const NULL_VARINT_SIZE_BYTES: usize = 1;

/// the variable-length zig-zag encoding from Google Protocol Buffers
///
/// http://code.google.com/apis/protocolbuffers/docs/encoding.html
pub trait ZigZag<T>: Sized {
    fn encode_zigzag(self) -> T;

    fn decode_zigzag(n: T) -> Self;
}

impl ZigZag<u32> for i32 {
    fn encode_zigzag(self) -> u32 {
        ((self << 1) ^ (self >> 31)) as u32
    }

    fn decode_zigzag(n: u32) -> Self {
        ((n >> 1) as i32) ^ (-((n & 1) as i32))
    }
}

impl ZigZag<u64> for i64 {
    fn encode_zigzag(self) -> u64 {
        ((self << 1) ^ (self >> 63)) as u64
    }

    fn decode_zigzag(n: u64) -> Self {
        ((n >> 1) as i64) ^ (-((n & 1) as i64))
    }
}

pub trait VarIntExt<T>: ZigZag<T>
where
    T: ShrAssign<usize> + PrimInt + ToPrimitive,
{
    fn size_of_varint(self) -> usize {
        let mut v = Self::encode_zigzag(self);
        let mut size = 1;

        while (v & !T::from(0x7F).unwrap()) != T::zero() {
            size += 1;
            v.shr_assign(7usize);
        }

        size
    }

    fn put_varint<B>(self, buf: &mut B)
    where
        B: BufMut,
    {
        let mut v = Self::encode_zigzag(self);

        while (v & !T::from(0x7F).unwrap()) != T::zero() {
            buf.put_u8(((v & T::from(0x7F).unwrap()) | T::from(0x80).unwrap()).to_u8().unwrap());
            v.shr_assign(7usize);
        }

        buf.put_u8((v & T::from(0x7F).unwrap()).to_u8().unwrap());
    }
}

impl VarIntExt<u32> for i32 {}
impl VarIntExt<u64> for i64 {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zig_zag() {
        fn test_zig_zag_pair(i: i32, u: u32) {
            assert_eq!(i32::encode_zigzag(i), u);
            assert_eq!(i32::decode_zigzag(u), i);
        }

        fn test_zig_zag_pair64(i: i64, u: u64) {
            assert_eq!(i64::encode_zigzag(i), u);
            assert_eq!(i64::decode_zigzag(u), i);
        }

        test_zig_zag_pair(0, 0);
        test_zig_zag_pair(-1, 1);
        test_zig_zag_pair(1, 2);
        test_zig_zag_pair(-2, 3);
        test_zig_zag_pair(2147483647, 4294967294);
        test_zig_zag_pair(-2147483648, 4294967295);
        test_zig_zag_pair64(9223372036854775807, 18446744073709551614);
        test_zig_zag_pair64(-9223372036854775808, 18446744073709551615);
    }

    #[test]
    fn test_varint() {
        fn test_varint(n: i64, s: &[u8]) {
            let mut buf = vec![];

            n.put_varint(&mut buf);

            assert_eq!(n.size_of_varint(), s.len());
            assert_eq!(
                buf.as_slice(),
                s,
                "varint {} encoded to {:?}, expected {:?}",
                n,
                &buf,
                s
            );
        }
        test_varint(0, &[0]);
        test_varint(-1, &[1]);
        test_varint(1, &[2]);
        test_varint(-2, &[3]);
        test_varint(2147483647, &[254, 255, 255, 255, 15]);
        test_varint(-2147483648, &[255, 255, 255, 255, 15]);

        test_varint(63, &[126]);
        test_varint(-64, &[127]);
        test_varint(64, &[128, 1]);
        test_varint(-123, &[245, 1]);
        test_varint(123, &[246, 1]);
    }
}
