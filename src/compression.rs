#[derive(Copy, Clone, Debug, PartialEq)]
#[repr(i8)]
pub enum Compression {
    None = 0,
    GZIP = 1,
    Snappy = 2,
    LZ4 = 3,
}