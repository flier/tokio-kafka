# tokio-kafka [![Build Status](https://travis-ci.org/flier/tokio-kafka.svg?branch=develop)](https://travis-ci.org/flier/tokio-kafka)

This library is an experimental implementation of **async kafka client** in Rust.

**`tokio-kafka` sill in the developing stage, the public API not stable, and not ready for production.**

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

### Compatibility

#### Kafka protocol
- [x] Kafka 0.8
- [x] Kafka 0.9
- [ ] Kafka 0.10
- [ ] Kafka 0.11
- [ ] Kafka 1.0
- [ ] Kafka 1.1

### futures
- [x] futures 0.1
- [ ] futures 0.2/1.0

# License

This project is licensed under either of

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or
   http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or
   http://opensource.org/licenses/MIT)

at your option.
