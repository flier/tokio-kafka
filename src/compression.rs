use std::mem;
use std::str::FromStr;

use errors::{Error, ErrorKind, Result};

#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[repr(i8)]
pub enum Compression {
    None = 0,
    GZIP = 1,
    Snappy = 2,
    LZ4 = 3,
}

impl Default for Compression {
    fn default() -> Self {
        Compression::None
    }
}

impl From<i8> for Compression {
    fn from(v: i8) -> Self {
        unsafe { mem::transmute(v & 0x07) }
    }
}

impl FromStr for Compression {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "none" => Ok(Compression::None),
            "gzip" => Ok(Compression::GZIP),
            "snappy" => Ok(Compression::Snappy),
            "lz4" => Ok(Compression::LZ4),
            _ => bail!(ErrorKind::ParseError(format!("unknown compression: {}", s))),
        }
    }
}