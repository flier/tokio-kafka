use protocol::{ApiKeys, KafkaCode};

error_chain!{
    foreign_links {
        IoError(::std::io::Error);
        TlsError(::native_tls::Error);
    }

    errors {
        NoHostError
        LockError
        ParseError(reason: String)
        CodecError(reason: &'static str)
        UnsupportedApiKey(key: ApiKeys)
        InvalidResponse
        KafkaError(code: KafkaCode)
        OtherError
    }
}

impl<T> From<::std::sync::PoisonError<T>> for Error {
    fn from(_: ::std::sync::PoisonError<T>) -> Self {
        ErrorKind::LockError.into()
    }
}

impl<P> From<::nom::verbose_errors::Err<P>> for Error
    where P: ::std::fmt::Debug
{
    fn from(err: ::nom::verbose_errors::Err<P>) -> Self {
        ErrorKind::ParseError(err.to_string()).into()
    }
}

lazy_static! {
    pub static ref CODEPAGE_HEX: Vec<char> = (0_u32..256)
        .map(|c| if 0x20 <= c && c <= 0x7E {
                ::std::char::from_u32(c).unwrap()
            } else {
                '.'
            })
        .collect();
}

macro_rules! hexdump {
    ($buf:expr) => (hexdump!($buf, 0));
    ($buf:expr, $off:expr) => (::hexplay::HexViewBuilder::new($buf)
                                  .codepage(&$crate::errors::CODEPAGE_HEX)
                                  .address_offset($off)
                                  .row_width(16)
                                  .finish());
}
