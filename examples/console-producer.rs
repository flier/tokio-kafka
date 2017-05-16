#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate log;
extern crate pretty_env_logger;
extern crate getopts;

extern crate futures;
extern crate tokio_core;
extern crate tokio_io;
extern crate tokio_file_unix;

extern crate tokio_kafka;

use std::io;
use std::fs;
use std::str;
use std::env;
use std::process;
use std::time::Duration;
use std::path::Path;
use std::net::ToSocketAddrs;

use getopts::Options;

use futures::{Future, Stream, future};
use tokio_core::reactor::Core;
use tokio_io::AsyncRead;
use tokio_io::codec::FramedRead;
use tokio_file_unix::{DelimCodec, File, Newline, StdFile};

use tokio_kafka::{BytesSerializer, Client, Compression, Producer, ProducerBuilder,
                  ProducerInterceptor, ProducerRecord, RecordMetadata, RequiredAcks};
use tokio_kafka::consts::{DEFAULT_ACK_TIMEOUT_MILLIS, DEFAULT_BATCH_SIZE, DEFAULT_LINGER_MILLIS,
                          DEFAULT_MAX_CONNECTION_IDLE_TIMEOUT_MILLIS};

const DEFAULT_BROKER: &str = "127.0.0.1:9092";
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
        let program = Path::new(&args[0])
            .file_name()
            .unwrap()
            .to_str()
            .unwrap();
        let mut opts = Options::new();

        opts.optflag("h", "help", "print this help menu");
        opts.optopt("b",
                    "brokers",
                    "Bootstrap broker(s) (host[:port], comma separated)",
                    "HOSTS");
        opts.optopt("t", "topic", "Specify target topic", "NAME");
        opts.optopt("i", "input", "Specify input file", "FILE");
        opts.optopt("n", "batch-size", "Send N message in one batch.", "N");
        opts.optopt("c",
                    "compression",
                    "Compress messages [none, gzip, snappy, lz4]",
                    "TYPE");
        opts.optopt("a",
                    "required-acks",
                    "Specify amount of required broker acknowledgments [none, one, all]",
                    "TYPE");
        opts.optopt("", "ack-timeout", "Specify time to wait for acks", "MS");
        opts.optopt("",
                    "idle-timeout",
                    "Specify timeout for idle connections",
                    "MS");
        opts.optopt("",
                    "linger",
                    "The producer groups together any records in the linger timeout",
                    "MS");

        let m = opts.parse(&args[1..])?;

        if m.opt_present("h") {
            let brief = format!("Usage: {} [options]", program);

            print!("{}", opts.usage(&brief));

            process::exit(0);
        }

        let brokers = m.opt_str("b")
            .map_or_else(|| vec![DEFAULT_BROKER.to_owned()],
                         |s| s.split(',').map(|s| s.trim().to_owned()).collect());


        Ok(Config {
               brokers: brokers,
               topic_name: m.opt_str("topic")
                   .unwrap_or_else(|| DEFAULT_TOPIC.to_owned()),
               input_file: m.opt_str("input"),
               batch_size: m.opt_str("batch-size").map_or(DEFAULT_BATCH_SIZE, |s| s.parse().unwrap()),
               compression: m.opt_str("compression")
                   .map(|s| s.parse().unwrap())
                   .unwrap_or_default(),
               required_acks: m.opt_str("required-acks")
                   .map(|s| s.parse().unwrap())
                   .unwrap_or_default(),
               idle_timeout: Duration::from_millis(m.opt_str("idle-timeout")
                   .map_or(DEFAULT_MAX_CONNECTION_IDLE_TIMEOUT_MILLIS,
                           |s| s.parse().unwrap())),
               ack_timeout: Duration::from_millis(m.opt_str("ack-timeout")
                   .map_or(DEFAULT_ACK_TIMEOUT_MILLIS, |s| s.parse().unwrap())),
               linger: Duration::from_millis(m.opt_str("linger")
                   .map_or(DEFAULT_LINGER_MILLIS, |s| s.parse().unwrap())),
           })
    }
}

fn main() {
    pretty_env_logger::init().unwrap();

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

    fn send(&self,
            record: ProducerRecord<Self::Key, Self::Value>)
            -> tokio_kafka::Result<ProducerRecord<Self::Key, Self::Value>> {
        debug!("sending {:?}", record);

        Ok(record)
    }

    fn ack(&self, result: &tokio_kafka::Result<RecordMetadata>) {
        debug!("acked {:?}", result);
    }
}

fn produce<'a, I>(config: Config, mut core: Core, io: I) -> Result<()>
    where I: AsyncRead
{
    let hosts = config
        .brokers
        .iter()
        .flat_map(|s| s.to_socket_addrs().unwrap());

    let mut producer = ProducerBuilder::from_hosts(hosts, core.handle())
        .with_max_connection_idle(config.idle_timeout)
        .with_required_acks(config.required_acks)
        .with_compression(config.compression)
        .with_batch_size(config.batch_size)
        .with_ack_timeout(config.ack_timeout)
        .with_linger(config.linger)
        .without_key_serializer()
        .with_value_serializer(BytesSerializer::default())
        .with_default_partitioner()
        .with_interceptor(LogInterceptor {})
        .build()?;

    let client = producer.client();
    let handle = core.handle();

    let work = client
        .borrow_mut()
        .load_metadata()
        .map_err(Error::from)
        .and_then(|_| {
            FramedRead::new(io, DelimCodec(Newline))
                .and_then(|line| {
                              String::from_utf8(line)
                                  .map(|line| line.trim().to_owned())
                                  .map_err(|err| {
                                               io::Error::new(io::ErrorKind::InvalidData, err)
                                           })
                          })
                .filter(|line| !line.is_empty())
                .for_each(|line| {
                    let produce = producer
                        .send(ProducerRecord::from_value(&config.topic_name, line))
                        .map(|md| {
                            trace!("sent to {} #{} @{}, ts={}, key_size={}, value_size={}",
                                   md.topic_name,
                                   md.partition,
                                   md.offset,
                                   md.timestamp,
                                   md.serialized_key_size,
                                   md.serialized_value_size);
                        })
                        .map_err(|err| {
                                     warn!("fail to produce records, {}", err);
                                 });

                    handle.spawn(produce);

                    future::ok(())
                })
                .map_err(Error::from)
        });

    core.run(work)
}
