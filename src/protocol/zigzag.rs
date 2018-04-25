use std::cmp::PartialEq;
use std::ops::{BitAnd, Not, ShrAssign};

use num::PrimInt;

pub const NULL_VARINT_SIZE_BYTES: usize = 1;

/// the variable-length zig-zag encoding from Google Protocol Buffers
///
/// http://code.google.com/apis/protocolbuffers/docs/encoding.html
pub trait ZigZag<T>: Sized
where
    T: BitAnd<Output = T> + ShrAssign<usize> + Not + PartialEq + PrimInt,
{
    fn encode_zigzag(n: Self) -> T;

    fn decode_zigzag(n: T) -> Self;

    fn size_of_varint(n: Self) -> usize {
        let mut v = Self::encode_zigzag(n);
        let mut size = 1;

        while (v & !T::from(0x7F).unwrap()) != T::zero() {
            size += 1;
            v.shr_assign(7usize);
        }

        size
    }
}

impl ZigZag<u32> for i32 {
    fn encode_zigzag(n: Self) -> u32 {
        ((n << 1) ^ (n >> 31)) as u32
    }

    fn decode_zigzag(n: u32) -> Self {
        ((n >> 1) as i32) ^ (-((n & 1) as i32))
    }
}

impl ZigZag<u64> for i64 {
    fn encode_zigzag(n: Self) -> u64 {
        ((n << 1) ^ (n >> 63)) as u64
    }

    fn decode_zigzag(n: u64) -> Self {
        ((n >> 1) as i64) ^ (-((n & 1) as i64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zig_zag() {
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
}
