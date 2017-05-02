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
use std::collections::HashMap;

use getopts::Options;

use futures::future::{self, Future};
use tokio_core::reactor::Core;
use tokio_kafka::{Client, Cluster, FetchOffset, KafkaClient, Metadata, PartitionOffset};

const DEFAULT_BROKER: &'static str = "127.0.0.1:9092";

error_chain!{
    foreign_links {
        ArgError(::getopts::Fail);
    }
}

struct Config {
    brokers: Vec<String>,
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

        let topics = matches
            .opt_str("t")
            .map(|s| s.split(',').map(|s| s.trim().to_owned()).collect());

        Ok(Config {
               brokers: brokers,
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

    let mut client = KafkaClient::from_hosts(&config.brokers, core.handle());

    let topics = config.topics.clone();

    let work = client
        .load_metadata()
        .and_then(move |metadata| {
            let topics = topics.unwrap_or_else(|| {
                                                   metadata
                                                       .topic_names()
                                                       .iter()
                                                       .map(|&s| s.to_owned())
                                                       .collect()
                                               });

            let requests = vec![client.fetch_offsets(topics.as_slice(), FetchOffset::Earliest),
                                client.fetch_offsets(topics.as_slice(), FetchOffset::Latest)];

            future::join_all(requests).map(|responses| {
                                               dump_metadata(config,
                                                             metadata,
                                                             &responses[0],
                                                             &responses[1]);
                                           })
        });

    core.run(work).unwrap();
}

fn dump_metadata<'a>(cfg: Config,
                     metadata: Rc<Metadata<'a>>,
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

    if cfg.show_header {
        print!("{1:0$} {2:4} {3:4}", topic_width, "topic", "p-id", "l-id");

        if cfg.show_host {
            print!(" {1:>0$}", host_width, "(l-host)");
        }

        print!(" {:>12} {:>12}", "earliest", "latest");

        if cfg.show_size {
            print!(" {:>12}", "(size)");
        }

        println!("");
    }

    for (idx, (topic_name, partitions)) in metadata.topics().iter().enumerate() {
        if cfg.topic_separators && idx > 0 {
            println!("")
        }

        if let (Some(earliest), Some(latest)) =
            (earliest_offsets.get(topic_name.to_owned()), latest_offsets.get(topic_name.to_owned())) {

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

                        if cfg.show_host {
                            let (host, port) = broker.addr();
                            print!(" {1:0$}", host_width, format!("({}:{})", host, port));
                        }

                        print!(" {:>12} {:>12}",
                               earliest_offset.offset,
                               latest_offset.offset);

                        if cfg.show_size {
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