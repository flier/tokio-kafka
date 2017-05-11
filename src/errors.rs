use std::borrow::{Borrow, Cow};

use protocol::{ApiKeys, KafkaCode};

error_chain!{
    foreign_links {
        IoError(::std::io::Error);
        ParseIntError(::std::num::ParseIntError);
        TlsError(::native_tls::Error);
    }

    errors {
        ConfigError(reason: &'static str)
        NoHostError
        LockError
        ParseError(reason: String)
        CodecError(reason: &'static str)
        UnsupportedApiKey(key: ApiKeys) {
            description("unsupported API key")
            display("unsupported API key, {:?}", key)
        }
        IllegalArgument(reason: String) {
            description("invalid argument")
            display("invalid argument, {}", reason)
        }
        InvalidResponse
        Canceled
        KafkaError(code: KafkaCode) {
            description("kafka error")
            display("kafka error, {:?}", code)
        }
        OtherError
    }
}

unsafe impl Sync for Error {}

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

impl<'a> From<Cow<'a, str>> for Error {
    fn from(s: Cow<'a, str>) -> Self {
        ErrorKind::Msg(String::from(s.borrow())).into()
    }
}

macro_rules! hexdump {
    ($buf:expr) => (hexdump!($buf, 0));
    ($buf:expr, $off:expr) => (::hexplay::HexViewBuilder::new($buf)
                                  .codepage(::hexplay::CODEPAGE_ASCII)
                                  .address_offset($off)
                                  .row_width(16)
                                  .finish());
}
