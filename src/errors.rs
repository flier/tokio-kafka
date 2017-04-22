use std::sync::PoisonError;

use protocol::ApiKeys;

error_chain!{
    foreign_links {
        IoError(::std::io::Error);
        ParseError(::nom::ErrorKind);
        TlsError(::native_tls::Error);
    }

    errors {
        NoHostError
        LockError
        CodecError(reason: &'static str)
        UnsupportedApiKey(key: ApiKeys)
        OtherError
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_: PoisonError<T>) -> Self {
        ErrorKind::LockError.into()
    }
}