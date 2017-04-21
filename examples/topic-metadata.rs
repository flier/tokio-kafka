#[macro_use]
extern crate error_chain;
extern crate env_logger;
extern crate getopts;
extern crate tokio_kafka;

use std::env;
use std::process;
use std::path::Path;

use getopts::Options;

use tokio_core::reactor::Core;
use tokio_kafka::{KafkaConfig, KafkaClient};

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

        Ok(Config {
               brokers: matches
                   .opt_str("brokers")
                   .map_or(vec![DEFAULT_BROKER.to_owned()],
                           |s| s.split(',').map(|s| s.trim().to_owned()).collect()),
               topics: matches
                   .opt_str("topics")
                   .map_or(Vec::new(),
                           |s| s.split(',').map(|s| s.trim().to_owned()).collect()),
           })
    }
}

fn main() {
    env_logger::init().unwrap();

    let mut core = Core::new().unwrap();

    let config = Config::parse_cmdline().unwrap();

    let client = KafkaClient::from_hosts(&config.brokers);

    client.load_metadata(core.handle());
    /*
    client
        .topics()
        .map(|topics| topics.filter(|topic| config.topics.contains(topic.topic_name())));
        */
}