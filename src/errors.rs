use std::error::Error as StdError;
use std::borrow::{Borrow, Cow};

use protocol::{ApiKeys, KafkaCode};

error_chain!{
    foreign_links {
        IoError(::std::io::Error);
        ParseIntError(::std::num::ParseIntError);
        TlsError(::native_tls::Error);
    }

    errors {
        ConfigError(reason: &'static str) {
            description("invalid config")
            display("invalid config, {:?}", reason)
        }
        LockError(reason: String) {
            description("lock failed")
            display("lock failed, {}", reason)
        }
        ParseError(reason: String) {
            description("fail to parse")
            display("fail to parse, {:?}", reason)
        }
        EncodeError(reason: &'static str) {
            description("fail to encode")
            display("fail to encode, {:?}", reason)
        }
        IllegalArgument(reason: String) {
            description("invalid argument")
            display("invalid argument, {}", reason)
        }
        UnexpectedResponse(api_key: ApiKeys) {
            description("unexpected response")
            display("unexpected response, {:?}", api_key)
        }
        Canceled(task: &'static str) {
            description("task canceled")
            display("task canceled, {}", task)
        }
        KafkaError(code: KafkaCode) {
            description("kafka error")
            display("kafka error, {:?}", code)
        }
    }
}

unsafe impl Sync for Error {}

impl<T> From<::std::sync::PoisonError<T>> for Error {
    fn from(err: ::std::sync::PoisonError<T>) -> Self {
        ErrorKind::LockError(StdError::description(&err).to_owned()).into()
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
