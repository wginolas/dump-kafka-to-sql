extern crate kafka;
extern crate clap;

use std::thread;
use std::sync::mpsc::channel;
use kafka::consumer::{Consumer, FetchOffset};
use clap::{Arg, App, AppSettings};

#[derive(Clone)]
struct Args {
    topic: String,
    brokers: Vec<String>
}

fn parse_args() -> Args {
    let matches = App::new("dump-kafka-to-sql")
        .version("0.0.1")
        .author("Wolfgang Ginolas <wolfgang.ginolas@gwif.eu>")
        .about("Dump a Kafka topic into a SQLite database")
        .setting(AppSettings::ColoredHelp)
        .arg(Arg::with_name("TOPIC")
             .short("t")
             .long("topic")
             .help("A Kafka topic to read.")
             .takes_value(true)
             .required(true))
        .arg(Arg::with_name("BROKER")
             .short("b")
             .long("broker")
             .help("A Kafka broker. Multiple brokers can be specified. When no broker is given 'localhost:9092' is used.")
             .takes_value(true)
             .multiple(true))
        .get_matches();

    Args {
        topic: matches.value_of("TOPIC").unwrap_or("topic").to_string(),
        brokers: match matches.values_of("BROKER") {
            Some(x) => x.map(|s| s.to_string()).collect(),
            None => vec!["localhost:9092".to_string()]
        }
    }
}

fn read_topic(args: Args) {
    let mut c = Consumer::from_hosts(args.brokers, "dump-kafka-to-sql".to_string(), args.topic)
        .with_fetch_max_wait_time(100)
        .with_fetch_min_bytes(1_000)
        .with_fetch_max_bytes_per_partition(100_000)
        .with_fallback_offset(FetchOffset::Earliest)
        .with_retry_max_bytes_limit(1_000_000)
        .create().unwrap();
    loop {
        let message_sets = c.poll().unwrap();
        if message_sets.is_empty() {
            break;
        }
        for ms in message_sets.iter() {
            for m in ms.messages() {
                let s = String::from_utf8_lossy(m.value);
                println!("{} {} {} {}", ms.topic(), ms.partition(), m.offset, s);
            }
        }
        c.commit_consumed().unwrap();
    }
}

fn main() {
    let args = parse_args();
    let (rx, tx) = channel();
    let read_thread = thread::spawn(move|| read_topic(args.clone(), tx));
}
