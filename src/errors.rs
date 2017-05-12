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
        TimeoutError(reason: String) {
            description("operation timed out")
            display("operation timed out, {:?}", reason)
        }
        RetryError(reason: String) {
            description("retry failed")
            display("retry failed, {:?}", reason)
        }
    }
}

unsafe impl Sync for Error {}
unsafe impl Send for Error {}

impl<'a> From<Cow<'a, str>> for Error {
    fn from(s: Cow<'a, str>) -> Self {
        ErrorKind::Msg(String::from(s.borrow())).into()
    }
}

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

impl<T> From<::tokio_timer::TimeoutError<T>> for Error {
    fn from(err: ::tokio_timer::TimeoutError<T>) -> Self {
        ErrorKind::TimeoutError(StdError::description(&err).to_owned()).into()
    }
}

impl<E: StdError> From<::tokio_retry::Error<E>> for Error {
    fn from(err: ::tokio_retry::Error<E>) -> Self {
        ErrorKind::RetryError(StdError::description(&err).to_owned()).into()
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
