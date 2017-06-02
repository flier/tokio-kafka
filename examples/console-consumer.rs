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

use std::env;
use std::process;
use std::path::Path;
use std::net::ToSocketAddrs;

use getopts::Options;

use futures::{Future, Stream, future};
use tokio_core::reactor::Core;

use tokio_kafka::{BytesDeserializer, Consumer, ConsumerBuilder};

const DEFAULT_BROKER: &str = "127.0.0.1:9092";
const DEFAULT_TOPIC: &str = "my-topic";

error_chain!{
    links {
        KafkaError(tokio_kafka::Error, tokio_kafka::ErrorKind);
    }
    foreign_links {
        IoError(::std::io::Error);
        ArgError(::getopts::Fail);
    }
}

#[derive(Clone, Debug)]
struct Config {
    brokers: Vec<String>,
    topics: Vec<String>,
    group: String,
    no_commit: bool,
}

impl Config {
    fn parse_cmdline() -> Result<Self> {
        let args: Vec<String> = env::args().collect();
        let program = Path::new(&args[0]).file_name().unwrap().to_str().unwrap();
        let mut opts = Options::new();

        opts.optflag("h", "help", "print this help menu");
        opts.optopt("b",
                    "brokers",
                    "Bootstrap broker(s) (host[:port], comma separated)",
                    "HOSTS");
        opts.optopt("t", "topics", "Specify topics (comma separated)", "NAMES");
        opts.optopt("g", "group", "Specify the consumer group", "NAME");
        opts.optflag("", "no-commit", "Do not commit group offsets");

        let m = opts.parse(&args[1..])?;

        if m.opt_present("h") {
            let brief = format!("Usage: {} [options]", program);

            print!("{}", opts.usage(&brief));

            process::exit(0);
        }

        let brokers = m.opt_str("b")
            .map_or_else(|| vec![DEFAULT_BROKER.to_owned()],
                         |s| s.split(',').map(|s| s.trim().to_owned()).collect());
        let topics = m.opt_str("t")
            .map_or_else(|| vec![DEFAULT_TOPIC.to_owned()],
                         |s| s.split(',').map(|s| s.trim().to_owned()).collect());

        Ok(Config {
               brokers: brokers,
               topics: topics,
               group: m.opt_str("g").unwrap_or_default(),
               no_commit: m.opt_present("no-commit"),
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
    let mut core = Core::new()?;

    let hosts = config
        .brokers
        .iter()
        .flat_map(|s| s.to_socket_addrs().unwrap());

    let handle = core.handle();

    let builder = ConsumerBuilder::from_hosts(hosts, handle)
        .with_key_deserializer(BytesDeserializer::<String>::default())
        .with_value_deserializer(BytesDeserializer::<String>::default());

    let mut consumer = builder.build()?;

    let work = consumer
        .subscribe(&config.topics)
        .and_then(|topics| {
            topics.for_each(|record| {
                                debug!("consume record: key={:?}, value={:?}, ts={:?}",
                                       record.key,
                                       record.value,
                                       record.timestamp);

                                future::ok(())
                            })
        })
        .map(|_| ())
        .map_err(Error::from);

    core.run(work)
}
