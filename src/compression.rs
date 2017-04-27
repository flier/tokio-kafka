use std::mem;

#[derive(Copy, Clone, Debug, PartialEq)]
#[repr(i8)]
pub enum Compression {
    None = 0,
    GZIP = 1,
    Snappy = 2,
    LZ4 = 3,
}

impl From<i8> for Compression {
    fn from(v: i8) -> Self {
        unsafe { mem::transmute(v & 0x07) }
    }
}
