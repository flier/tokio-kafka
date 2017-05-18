#[macro_use]
extern crate error_chain;
extern crate pretty_env_logger;
extern crate getopts;
extern crate futures;
extern crate tokio_core;
extern crate tokio_kafka;

use std::rc::Rc;
use std::env;
use std::cmp;
use std::process;
use std::path::Path;
use std::net::{SocketAddr, ToSocketAddrs};
use std::collections::HashMap;

use getopts::Options;

use futures::Future;
use futures::future;
use tokio_core::reactor::Core;
use tokio_kafka::{Client, Cluster, FetchOffset, KafkaClient, KafkaVersion, Metadata,
                  PartitionOffset};

const DEFAULT_BROKER: &'static str = "127.0.0.1:9092";

error_chain!{
    links {
        KafkaError(tokio_kafka::Error, tokio_kafka::ErrorKind);
    }
    foreign_links {
        ArgError(::getopts::Fail);
    }
}

struct Config {
    brokers: Vec<SocketAddr>,
    api_version_request: bool,
    broker_version: Option<KafkaVersion>,
    topics: Option<Vec<String>>,
    show_header: bool,
    show_host: bool,
    show_size: bool,
    topic_separators: bool,
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
        opts.optopt("",
                    "broker-version",
                    "Specify broker versions [0.8.0, 0.8.1, 0.8.2, 0.9.0, auto]",
                    "VERSION");
        opts.optopt("t", "topics", "Specify topics (comma separated)", "NAMES");
        opts.optflag("", "no-header", "Don't print headers");
        opts.optflag("", "no-host", "Don't print host:port of leaders");
        opts.optflag("", "no-size", "Don't print partition sizes");
        opts.optflag("", "no-empty-lines", "Don't separate topics by empty lines");

        let matches = opts.parse(&args[1..])?;

        if matches.opt_present("h") {
            let brief = format!("Usage: {} [options]", program);

            print!("{}", opts.usage(&brief));

            process::exit(0);
        }

        let brokers = matches
            .opt_str("b")
            .map_or_else(|| vec![DEFAULT_BROKER.to_owned()],
                         |s| s.split(',').map(|s| s.trim().to_owned()).collect());

        let (api_version_request, broker_version) = matches
            .opt_str("broker-version")
            .map_or((false, None), |s| if s == "auto" {
                (true, None)
            } else {
                (false, Some(s.parse().unwrap()))
            });

        let topics = matches
            .opt_str("t")
            .map(|s| s.split(',').map(|s| s.trim().to_owned()).collect());

        Ok(Config {
               brokers: brokers
                   .iter()
                   .flat_map(|s| s.to_socket_addrs().unwrap())
                   .collect(),
               api_version_request: api_version_request,
               broker_version: broker_version,
               topics: topics,
               show_header: !matches.opt_present("no-header"),
               show_host: !matches.opt_present("no-host"),
               show_size: !matches.opt_present("no-size"),
               topic_separators: !matches.opt_present("no-empty-lines"),
           })
    }
}

fn main() {
    pretty_env_logger::init().unwrap();

    let config = Config::parse_cmdline().unwrap();

    let mut core = Core::new().unwrap();

    let mut builder = KafkaClient::from_hosts(config.brokers.clone().into_iter(), core.handle());

    if config.api_version_request {
        builder = builder.with_api_version_request()
    }
    if let Some(version) = config.broker_version {
        builder = builder.with_broker_version_fallback(version)
    }

    let client = builder.build().unwrap();

    let topics = config.topics.clone();

    let work = client
        .metadata()
        .and_then(move |metadata| {
            let topics = topics.unwrap_or_else(|| {
                                                   metadata
                                                       .topic_names()
                                                       .iter()
                                                       .map(|&s| s.to_owned())
                                                       .collect()
                                               });

            let requests = vec![client.fetch_offsets(&topics, FetchOffset::Earliest),
                                client.fetch_offsets(&topics, FetchOffset::Latest)];

            future::join_all(requests).map(|responses| {
                                               dump_metadata(config,
                                                             metadata,
                                                             topics,
                                                             &responses[0],
                                                             &responses[1])
                                           })
        });

    core.run(work).unwrap();
}

fn dump_metadata<'a>(config: Config,
                     metadata: Rc<Metadata>,
                     topics: Vec<String>,
                     earliest_offsets: &HashMap<String, Vec<PartitionOffset>>,
                     latest_offsets: &HashMap<String, Vec<PartitionOffset>>) {
    let host_width = 2 +
                     metadata
                         .brokers()
                         .iter()
                         .map(|broker| broker.addr())
                         .fold(0, |width, (host, port)| {
        cmp::max(width, format!("{}:{}", host, port).len())
    });
    let topic_width = 2 +
                      metadata
                          .topic_names()
                          .iter()
                          .fold(0, |width, topic_name| cmp::max(width, topic_name.len()));

    if config.show_header {
        print!("{1:0$} {2:4} {3:4}", topic_width, "topic", "p-id", "l-id");

        if config.show_host {
            print!(" {1:>0$}", host_width, "(l-host)");
        }

        print!(" {:>12} {:>12}", "earliest", "latest");

        if config.show_size {
            print!(" {:>12}", "(size)");
        }

        println!("");
    }

    for (idx, (topic_name, partitions)) in metadata.topics().iter().enumerate() {
        let topic_name = topic_name.to_owned().into_owned();

        if !topics.contains(&topic_name) {
            continue;
        }

        if config.topic_separators && idx > 0 {
            println!("")
        }

        if let (Some(earliest), Some(latest)) =
            (earliest_offsets.get(&topic_name), latest_offsets.get(&topic_name)) {

            for partition_info in partitions.iter() {
                if let Some(leader) = partition_info.leader() {
                    if let (Some(broker), Some(earliest_offset), Some(latest_offset)) =
                        (metadata.find_broker(leader),
                         earliest
                             .iter()
                             .find(|offset| offset.partition == partition_info.partition),
                         latest
                             .iter()
                             .find(|offset| offset.partition == partition_info.partition)) {

                        print!("{1:0$} {2:>4} {3:>4}",
                               topic_width,
                               topic_name,
                               partition_info.partition,
                               broker.id());

                        if config.show_host {
                            let (host, port) = broker.addr();
                            print!(" {1:0$}", host_width, format!("({}:{})", host, port));
                        }

                        print!(" {:>12} {:>12}",
                               earliest_offset.offset,
                               latest_offset.offset);

                        if config.show_size {
                            print!(" {:>12}",
                                   format!("({})", latest_offset.offset - earliest_offset.offset));
                        }

                        println!("")
                    } else {
                        println!("{1:0$} - leader or offsets not found!\n",
                                 topic_width,
                                 topic_name);
                    }
                } else {
                    println!("{1:0$} - partition #{2} haven't leader!\n",
                             topic_width,
                             topic_name,
                             partition_info.partition);
                }
            }
        } else {
            println!("{1:0$} - not available!\n", topic_width, topic_name);
        }
    }
}
