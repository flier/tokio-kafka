# tokio-kafka [![Build Status](https://travis-ci.org/flier/tokio-kafka.svg?branch=develop)](https://travis-ci.org/flier/tokio-kafka)

This library is an experimental implementation of **async kafka client** in Rust.

NOTES: **`tokio-kafka` sill in the developing stage, the public API not stable, and not ready for production.**

If you want to play and contribute to the project, please try at your own risk.

## Usage

First, clone the project and add this to your `Cargo.toml`:

```bash
$ git clone https://github.com/flier/tokio-kafka
```

```toml
[dependencies]
tokio-kafka = { path = "../tokio-kafka" }
```

Next, add this to your crate:

```rust
#[macro_use]
extern crate tokio_kafka;
```

For more information about how you can use `tokio-kafka` with async I/O you can take a
look at [examples](examples).

## Features

### Public API
- [x] Consumer API
- [x] Producer API
- [ ] Streams API
- [ ] Connect API
- [ ] AdminClient API

### Compression
- [x] snappy
- [x] gzip
- [x] LZ4

### Consumer Group
- [ ] client-side coordinator (Kafka v0.8, zookeeper based)
- [x] server-side coordinator (Kafka v0.9 or later)

### Security
- [ ] SSL
- [ ] SASL (GSSAPI/Kerberos/SSPI, PLAIN, SCRAM)

### Statistics Metrics:
- [x] prometheus

### Compatibility

#### Broker version
- [x] Kafka 0.8
- [x] Kafka 0.9
- [x] Kafka 0.10
- [ ] Kafka 0.11
- [ ] Kafka 1.0
- [ ] Kafka 1.1

### futures
- [x] futures 0.1
- [ ] futures 0.2/1.0

# Configuration

## Global configuration properties

| Property                  | Range | Default | Description                                                                                                                                                                                   |
| ------------------------- | ----- | ------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `bootstrap.servers`       |       |         | A list of host/port pairs to use for establishing the initial connection to the Kafka cluster.                                                                                                |
| `client.id`               |       | null    | An id string to pass to the server when making requests.                                                                                                                                      |
| `connection.max.idle.ms`  |       | 5 s     | Close idle connections after the number of milliseconds specified by this config.                                                                                                             |
| `request.timeout.ms`      |       | 30 s    | The maximum amount of time the client will wait for the response of a request.                                                                                                                |
| `api.version.request`     |       | false   | Request broker's supported API versions to adjust functionality to available protocol features.                                                                                               |
| `broker.version.fallback` |       |         | Older broker versions (<0.10.0) provides no way for a client to query for supported protocol features                                                                                         |
| `metadata.max.age.ms`     |       | 5 m     | The period of time in milliseconds after which we force a refresh of metadata even if we haven't seen any partition leadership changes to proactively discover any new brokers or partitions. |
| `retry.backoff.ms`        |       | 100 ms  | The amount of time to wait before attempting to retry a failed request to a given topic partition.                                                                                            |

## Consumer configuration properties

| Property                        | Range                           | Default          | Description                                                                                                                                                                           |
| ------------------------------- | ------------------------------- | ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `group.id`                      |                                 | null             | A unique string that identifies the consumer group this consumer belongs to.                                                                                                          |
| `enable.auto.commit`            |                                 | false            | If `true` the consumer's offset will be periodically committed in the background.                                                                                                     |
| `auto.commit.interval.ms`       |                                 | 5 s              | The frequency in milliseconds that the consumer offsets are  auto-committed to Kafka.                                                                                                 |
| `heartbeat.interval.ms`         |                                 | 3 s              | The expected time between heartbeats to the consumer coordinator when using Kafka's group management facilities.                                                                      |
| `max.poll.records`              |                                 | 500              | The maximum number of records returned in a single call to poll().                                                                                                                    |
| `partition.assignment.strategy` |                                 | range,roundrobin | Name of partition assignment strategy to use when elected group leader assigns partitions to group members.                                                                           |
| `session.timeout.ms`            |                                 | 10 s             | The timeout used to detect consumer failures when using Kafka's group management facility.                                                                                            |
| `max.poll.interval.ms`          |                                 | 5 m              | The maximum delay between invocations of poll() when using consumer group management.                                                                                                 |
| `auto.offset.reset`             |                                 | latest           | What to do when there is no initial offset in Kafka or if the current offset does not exist any more on the server (e.g. because that data has been deleted)                          |
| `fetch.min.bytes`               |                                 | 1                | The minimum amount of data the server should return for a fetch request.                                                                                                              |
| `fetch.max.bytes`               |                                 | 50 MB            | The maximum amount of data the server should return for a fetch request.                                                                                                              |
| `fetch.max.wait.ms`             |                                 | 500 ms           | The maximum amount of time the server will block before answering the fetch request if there isn't sufficient data to immediately satisfy the requirement given by `fetch.min.bytes`. |
| `max.partition.fetch.bytes`     |                                 | 1 MB             | The maximum amount of data per-partition the server will return.                                                                                                                      |
| `isolation.level`               | read_uncommitted,read_committed | read_uncommitted | This setting controls the visibility of transactional records.                                                                                                                        |

## Producer configuration properties

| Property           | Range                   | Default | Description                                                                                                                             |
| ------------------ | ----------------------- | ------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| `timeout.ms`       |                         | 30s     | The maximum amount of time the server will wait for acknowledgments from followers to meet the acknowledgment requirements              |
| `compression.type` | none, gzip, snappy, lz4 | none    | The compression type for all data generated by the producer.                                                                            |
| `batch.size`       |                         | 16 KB   | The producer will attempt to batch records together into fewer requests whenever multiple records are being sent to the same partition. |
| `max.request.size` |                         | 1 MB    | The maximum size of a request in bytes.                                                                                                 |
| `linger.ms`        |                         | 0 ms    | The producer groups together any records that arrive in between request transmissions into a single batched request.                    |

# License

This project is licensed under either of

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or
   http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or
   http://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in Futures by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
