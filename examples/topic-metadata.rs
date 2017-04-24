#[macro_use]
extern crate log;
#[macro_use]
extern crate error_chain;
extern crate pretty_env_logger;
extern crate getopts;
extern crate futures;
extern crate tokio_core;
extern crate tokio_kafka;

use std::env;
use std::process;
use std::path::Path;

use getopts::Options;

use futures::future::Future;
use tokio_core::reactor::Core;
use tokio_kafka::KafkaClient;

const DEFAULT_BROKER: &'static str = "localhost:9092";

error_chain!{
    foreign_links {
        ArgError(::getopts::Fail);
    }
}

struct Config {
    brokers: Vec<String>,
    topics: Vec<String>,
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
        opts.optopt("t", "topics", "Specify topics (comma separated)", "NAMES");

        let matches = opts.parse(&args[1..])?;

        if matches.opt_present("h") {
            let brief = format!("Usage: {} FILE [options]", program);

            print!("{}", opts.usage(&brief));

            process::exit(0);
        }

        let brokers = matches
            .opt_str("b")
            .map_or_else(|| vec![DEFAULT_BROKER.to_owned()],
                         |s| s.split(',').map(|s| s.trim().to_owned()).collect());

        let topics = matches
            .opt_str("t")
            .map_or_else(|| Vec::new(),
                         |s| s.split(',').map(|s| s.trim().to_owned()).collect());

        Ok(Config {
               brokers: brokers,
               topics: topics,
           })
    }
}

fn main() {
    pretty_env_logger::init().unwrap();

    let core = Core::new().unwrap();

    let config = Config::parse_cmdline().unwrap();

    let mut client = KafkaClient::from_hosts(&config.brokers, &core.handle());

    client.load_metadata().wait();
}