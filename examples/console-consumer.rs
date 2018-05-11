#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate log;
extern crate getopts;
extern crate pretty_env_logger;

extern crate futures;
extern crate tokio_core;
extern crate tokio_io;

extern crate tokio_kafka;

use std::env;
use std::path::Path;
use std::process;

use getopts::Options;

use futures::{Future, Stream};
use tokio_core::reactor::Core;

use tokio_kafka::{Consumer, KafkaConsumer, KafkaVersion, SeekTo, StringDeserializer, Subscribed};

const DEFAULT_BROKER: &str = "localhost:9092";
const DEFAULT_CLIENT_ID: &str = "consumer-1";
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
    client_id: String,
    api_version_request: bool,
    broker_version: Option<KafkaVersion>,
    topics: Vec<String>,
    group_id: Option<String>,
    offset: SeekTo,
    no_commit: bool,
    skip_message_on_error: bool,
    broker_fallback_version: KafkaVersion,
}

impl Config {
    fn parse_cmdline() -> Result<Self> {
        let args: Vec<String> = env::args().collect();
        let program = Path::new(&args[0]).file_name().unwrap().to_str().unwrap();
        let mut opts = Options::new();

        opts.optflag("h", "help", "print this help menu");
        opts.optopt(
            "b",
            "bootstrap-server",
            "Bootstrap broker(s) (host[:port], comma separated)",
            "HOSTS",
        );
        opts.optopt("", "client-id", "Specify the client id.", "ID");
        opts.optopt(
            "",
            "broker-version",
            "Specify broker versions [0.8.0, 0.8.1, 0.8.2, 0.9.0, auto].",
            "VERSION",
        );
        opts.optopt("g", "group-id", "Specify the consumer group.", "NAME");
        opts.optopt("t", "topics", "The topic id to consume on (comma separated).", "NAMES");
        opts.optopt("o", "offset", "The offset id to consume from (a non-negative number), or 'earliest' which means from beginning, or 'latest' which means from end (default: latest).", "OFFSET");
        opts.optflag("", "from-beginning", "If the consumer does not already have an established offset to consume from, start with the earliest message present in the log rather than the latest message.");
        opts.optflag("", "no-commit", "Do not commit group offsets.");
        opts.optflag(
            "",
            "skip-message-on-error",
            "If there is an error when processing a message, skip it instead of halt.",
        );
        opts.optopt(
            "v",
            "broker-fallback-version",
            "what version the broker is running.",
            "VERSION",
        );

        let m = opts.parse(&args[1..])?;

        if m.opt_present("h") {
            let brief = format!("Usage: {} [options]", program);

            print!("{}", opts.usage(&brief));

            process::exit(0);
        }

        let (api_version_request, broker_version) = m.opt_str("broker-version").map_or((false, None), |s| {
            if s == "auto" {
                (true, None)
            } else {
                (false, Some(s.parse().unwrap()))
            }
        });

        Ok(Config {
            brokers: m.opt_str("b").map_or_else(
                || vec![DEFAULT_BROKER.to_owned()],
                |s| s.split(',').map(|s| s.trim().to_owned()).collect(),
            ),
            client_id: m.opt_str("client-id").unwrap_or(DEFAULT_CLIENT_ID.to_owned()),
            api_version_request,
            broker_version,
            topics: m.opt_str("t").map_or_else(
                || vec![DEFAULT_TOPIC.to_owned()],
                |s| s.split(',').map(|s| s.trim().to_owned()).collect(),
            ),
            group_id: m.opt_str("g"),
            offset: m.opt_str("o").map_or_else(
                || {
                    if m.opt_present("from-beginning") {
                        SeekTo::Beginning
                    } else {
                        SeekTo::End
                    }
                },
                |s| s.parse().unwrap(),
            ),
            no_commit: m.opt_present("no-commit"),
            skip_message_on_error: m.opt_present("skip-message-on-error"),
            broker_fallback_version: if let Some(s) = m.opt_str("broker-fallback-version") {
                s.parse()?
            } else {
                KafkaVersion::default()
            },
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
    let mut core = Core::new()?;

    let handle = core.handle();

    let mut builder = KafkaConsumer::with_bootstrap_servers(config.brokers, handle)
        .with_client_id(config.client_id)
        .with_api_version_request(config.api_version_request)
        .with_broker_version_fallback(config.broker_fallback_version)
        .with_key_deserializer(StringDeserializer::default())
        .with_value_deserializer(StringDeserializer::default());

    if let Some(version) = config.broker_version {
        builder = builder.with_broker_version_fallback(version)
    }

    if let Some(group_id) = config.group_id {
        builder = builder.with_group_id(group_id);
    }

    let consumer = builder.build()?;
    let offset = config.offset;

    let work = consumer
        .subscribe(config.topics)
        .and_then(move |topics| {
            for partition in topics.assigment() {
                topics.seek(&partition, offset)?;
            }

            Ok(topics)
        })
        .and_then(|topics| {
            topics
                .clone()
                .for_each(|record| {
                    println!(
                        "{} {} {}",
                        record.timestamp.map(|ts| ts.to_string()).unwrap_or_default(),
                        record.key.unwrap_or_default(),
                        record.value.unwrap_or_default()
                    );

                    Ok(())
                })
                .and_then(move |_| topics.commit())
        })
        .map(|_| ())
        .from_err();

    core.run(work)
}
