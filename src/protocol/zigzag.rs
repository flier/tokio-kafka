/// the variable-length zig-zag encoding from Google Protocol Buffers
///
/// http://code.google.com/apis/protocolbuffers/docs/encoding.html
pub trait ZigZag {
    type Encoded;

    fn encode_zigzag(v: Self) -> Self::Encoded;

    fn decode_zigzag(v: Self::Encoded) -> Self;
}

impl ZigZag for i32 {
    type Encoded = u32;

    fn encode_zigzag(n: Self) -> Self::Encoded {
        ((n << 1) ^ (n >> 31)) as u32
    }

    fn decode_zigzag(n: Self::Encoded) -> Self {
        ((n >> 1) as i32) ^ (-((n & 1) as i32))
    }
}

impl ZigZag for i64 {
    type Encoded = u64;

    fn encode_zigzag(n: Self) -> Self::Encoded {
        ((n << 1) ^ (n >> 63)) as u64
    }

    fn decode_zigzag(n: Self::Encoded) -> Self {
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
