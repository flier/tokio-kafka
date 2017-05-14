use std::mem;
use std::str::FromStr;

use errors::{Error, ErrorKind, Result};

#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[repr(i8)]
pub enum Compression {
    None = 0,
    #[cfg(feature = "gzip")]
    GZIP = 1,
    #[cfg(feature = "snappy")]
    Snappy = 2,
    #[cfg(feature = "lz4")]
    LZ4 = 3,
}

impl Default for Compression {
    fn default() -> Self {
        Compression::None
    }
}

impl From<i8> for Compression {
    fn from(v: i8) -> Self {
        unsafe { mem::transmute(v) }
    }
}

impl FromStr for Compression {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "none" => Ok(Compression::None),
            #[cfg(feature = "gzip")]
            "gzip" => Ok(Compression::GZIP),
            #[cfg(feature = "snappy")]
            "snappy" => Ok(Compression::Snappy),
            #[cfg(feature = "lz4")]
            "lz4" => Ok(Compression::LZ4),
            _ => bail!(ErrorKind::ParseError(format!("unknown compression: {}", s))),
        }
    }
}
