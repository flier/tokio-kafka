#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate log;
extern crate getopts;
extern crate pretty_env_logger;

extern crate futures;
extern crate tokio_core;
#[cfg(not(target_os = "windows"))]
extern crate tokio_file_unix;
extern crate tokio_io;

extern crate tokio_kafka;

use std::env;
use std::fs;
use std::io;
use std::path::Path;
use std::process;
use std::str;
use std::time::Duration;

use getopts::Options;

use futures::{Future, Sink, Stream};
use tokio_core::reactor::Core;
#[cfg(not(target_os = "windows"))]
use tokio_file_unix::{DelimCodec, File, Newline, StdFile};
use tokio_io::AsyncRead;
use tokio_io::codec::FramedRead;

use tokio_kafka::{BytesSerializer, Compression, KafkaProducer, KafkaVersion, Producer, ProducerInterceptor,
                  ProducerRecord, RecordMetadata, RequiredAcks, TopicRecord, DEFAULT_ACK_TIMEOUT_MILLIS,
                  DEFAULT_BATCH_SIZE, DEFAULT_LINGER_MILLIS, DEFAULT_MAX_CONNECTION_IDLE_TIMEOUT_MILLIS};

const DEFAULT_BROKER: &str = "127.0.0.1:9092";
const DEFAULT_CLIENT_ID: &str = "producer-1";
const DEFAULT_TOPIC: &str = "my-topic";

error_chain!{
    links {
        KafkaError(tokio_kafka::Error, tokio_kafka::ErrorKind);
    }
    foreign_links {
        IoError(::std::io::Error);
        Utf8Error(::std::string::FromUtf8Error);
        ArgError(::getopts::Fail);
    }
}

unsafe impl Sync for Error {}

#[derive(Clone, Debug)]
struct Config {
    brokers: Vec<String>,
    client_id: String,
    api_version_request: bool,
    broker_version: Option<KafkaVersion>,
    topic_name: String,
    input_file: Option<String>,
    batch_size: usize,
    compression: Compression,
    required_acks: RequiredAcks,
    idle_timeout: Duration,
    ack_timeout: Duration,
    linger: Duration,
}

impl Config {
    fn parse_cmdline() -> Result<Self> {
        let args: Vec<String> = env::args().collect();
        let program = Path::new(&args[0]).file_name().unwrap().to_str().unwrap();
        let mut opts = Options::new();

        opts.optflag("h", "help", "print this help menu.");
        opts.optopt(
            "b",
            "bootstrap-server",
            "Bootstrap broker(s) (host[:port], comma separated).",
            "HOSTS",
        );
        opts.optopt("", "client-id", "Specify the client id.", "ID");
        opts.optopt(
            "",
            "broker-version",
            "Specify broker versions [0.8.0, 0.8.1, 0.8.2, 0.9.0, auto].",
            "VERSION",
        );
        opts.optopt("t", "topic", "Specify target topic.", "NAME");
        opts.optopt("i", "input", "Specify input file.", "FILE");
        opts.optopt("n", "batch-size", "Send N message in one batch.", "N");
        opts.optopt(
            "c",
            "compression",
            "Compress messages [none, gzip, snappy, lz4].",
            "TYPE",
        );
        opts.optopt(
            "a",
            "required-acks",
            "Specify amount of required broker acknowledgments [none, one, all].",
            "TYPE",
        );
        opts.optopt("", "ack-timeout", "Specify time to wait for acks.", "MS");
        opts.optopt("", "idle-timeout", "Specify timeout for idle connections.", "MS");
        opts.optopt(
            "",
            "linger",
            "The producer groups together any records in the linger timeout.",
            "MS",
        );

        let m = opts.parse(&args[1..])?;

        if m.opt_present("h") {
            let brief = format!("Usage: {} [options]", program);

            print!("{}", opts.usage(&brief));

            process::exit(0);
        }

        let brokers = m.opt_str("b").map_or_else(
            || vec![DEFAULT_BROKER.to_owned()],
            |s| s.split(',').map(|s| s.trim().to_owned()).collect(),
        );

        let (api_version_request, broker_version) = m.opt_str("broker-version").map_or((false, None), |s| {
            if s == "auto" {
                (true, None)
            } else {
                (false, Some(s.parse().unwrap()))
            }
        });

        Ok(Config {
            brokers,
            client_id: m.opt_str("client-id").unwrap_or(DEFAULT_CLIENT_ID.to_owned()),
            api_version_request,
            broker_version,
            topic_name: m.opt_str("topic").unwrap_or_else(|| DEFAULT_TOPIC.to_owned()),
            input_file: m.opt_str("input"),
            batch_size: m.opt_str("batch-size")
                .map_or(DEFAULT_BATCH_SIZE, |s| s.parse().unwrap()),
            compression: m.opt_str("compression").map(|s| s.parse().unwrap()).unwrap_or_default(),
            required_acks: m.opt_str("required-acks")
                .map(|s| s.parse().unwrap())
                .unwrap_or_default(),
            idle_timeout: Duration::from_millis(
                m.opt_str("idle-timeout")
                    .map_or(DEFAULT_MAX_CONNECTION_IDLE_TIMEOUT_MILLIS, |s| s.parse().unwrap()),
            ),
            ack_timeout: Duration::from_millis(
                m.opt_str("ack-timeout")
                    .map_or(DEFAULT_ACK_TIMEOUT_MILLIS, |s| s.parse().unwrap()),
            ),
            linger: Duration::from_millis(
                m.opt_str("linger")
                    .map_or(DEFAULT_LINGER_MILLIS, |s| s.parse().unwrap()),
            ),
        })
    }
}

fn main() {
    pretty_env_logger::init();

    let config = Config::parse_cmdline().unwrap();

    debug!("parsed config: {:?}", config);

    run(config).unwrap();
}

fn run(config: Config) -> Result<()> {
    let core = Core::new()?;

    debug!("produce messages to {:?}", config.brokers);

    let handle = core.handle();

    let input_file = config.input_file.clone();

    match input_file {
        Some(ref filename) => {
            debug!("reading lines from file: {}", filename);

            let file = fs::File::open(filename)?;
            let io = File::new_nb(file)?.into_io(&handle)?;

            produce(config, core, io)
        }
        _ => {
            debug!("reading lines from STDIN");

            let stdin = io::stdin();
            let io = File::new_nb(StdFile(stdin.lock()))?.into_io(&handle)?;

            produce(config, core, io)
        }
    }
}

pub struct LogInterceptor {}

impl ProducerInterceptor for LogInterceptor {
    type Key = ();
    type Value = String;

    fn send(
        &self,
        record: ProducerRecord<Self::Key, Self::Value>,
    ) -> tokio_kafka::Result<ProducerRecord<Self::Key, Self::Value>> {
        debug!("sending {:?}", record);

        Ok(record)
    }

    fn ack(&self, result: &tokio_kafka::Result<RecordMetadata>) {
        debug!("acked {:?}", result);

        match *result {
            Ok(ref md) => trace!(
                "sent to {} #{} @{}, ts={}, key_size={}, value_size={}",
                md.topic_name,
                md.partition_id,
                md.offset,
                md.timestamp,
                md.serialized_key_size,
                md.serialized_value_size
            ),
            Err(ref err) => warn!("fail to produce records, {}", err),
        }
    }
}

fn produce<'a, I>(config: Config, mut core: Core, io: I) -> Result<()>
where
    I: AsyncRead,
{
    let handle = core.handle();

    let mut builder = KafkaProducer::with_bootstrap_servers(config.brokers, handle.clone())
        .with_client_id(config.client_id)
        .with_max_connection_idle(config.idle_timeout)
        .with_required_acks(config.required_acks)
        .with_compression(config.compression)
        .with_batch_size(config.batch_size)
        .with_ack_timeout(config.ack_timeout)
        .with_linger(config.linger)
        .without_key_serializer()
        .with_value_serializer(BytesSerializer::default())
        .with_default_partitioner()
        .with_interceptor(LogInterceptor {});

    if config.api_version_request {
        builder = builder.with_api_version_request()
    }
    if let Some(version) = config.broker_version {
        builder = builder.with_broker_version_fallback(version)
    }

    let producer = builder.build()?;

    let lines = FramedRead::new(io, DelimCodec(Newline))
        .and_then(|line| {
            String::from_utf8(line)
                .map(|line| line.trim().to_owned())
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
        })
        .filter(|line| !line.is_empty())
        .map(TopicRecord::from_value);

    let work = producer
        .topic(&config.topic_name)
        .and_then(|topic| topic.send_all(lines).map(|_| ()))
        .map_err(Error::from);

    core.run(work)
}
