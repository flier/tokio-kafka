use std::mem;

#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[repr(i8)]
pub enum Compression {
    #[serde(rename = "none")]
    None = 0,
    #[serde(rename = "gzip")]
    GZIP = 1,
    #[serde(rename = "snappy")]
    Snappy = 2,
    #[serde(rename = "lz4")]
    LZ4 = 3,
}

impl From<i8> for Compression {
    fn from(v: i8) -> Self {
        unsafe { mem::transmute(v & 0x07) }
    }
}