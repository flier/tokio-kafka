#[macro_use]
extern crate error_chain;
extern crate pretty_env_logger;
extern crate getopts;

extern crate tokio_core;
extern crate tokio_kafka;

use std::env;
use std::process;
use std::path::Path;
use std::time::Duration;

use getopts::Options;

use tokio_core::reactor::Core;

use tokio_kafka::{Compression, RequiredAcks};
use tokio_kafka::consts::{DEFAULT_ACK_TIMEOUT_MILLIS, DEFAULT_MAX_CONNECTION_IDLE_TIMEOUT_MILLIS};

const DEFAULT_BROKER: &str = "127.0.0.1:9092";
const DEFAULT_TOPIC: &str = "my-topic";

error_chain!{
    links {
        KafkaError(tokio_kafka::Error, tokio_kafka::ErrorKind);
    }
    foreign_links {
        ArgError(::getopts::Fail);
    }
}

struct Config {
    brokers: Vec<String>,
    topic_name: String,
    input_file: Option<String>,
    batch_size: usize,
    compression: Compression,
    required_acks: RequiredAcks,
    idle_timeout: Duration,
    ack_timeout: Duration,
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
               batch_size: m.opt_str("batch-size").map_or(1, |s| s.parse().unwrap()),
               compression: m.opt_str("compression")
                   .map(|s| s.parse().unwrap())
                   .unwrap_or_default(),
               required_acks: m.opt_str("required-acks")
                   .map(|s| s.parse().unwrap())
                   .unwrap_or_default(),
               idle_timeout: m.opt_str("idle-timeout")
                   .map_or(Duration::from_millis(DEFAULT_MAX_CONNECTION_IDLE_TIMEOUT_MILLIS),
                           |s| Duration::from_millis(s.parse().unwrap())),
               ack_timeout: m.opt_str("ack-timeout")
                   .map_or(Duration::from_millis(DEFAULT_ACK_TIMEOUT_MILLIS),
                           |s| Duration::from_millis(s.parse().unwrap())),
           })
    }
}

fn main() {
    pretty_env_logger::init().unwrap();

    let config = Config::parse_cmdline().unwrap();

    let mut core = Core::new().unwrap();
}