# Minimal Rust client for Memcached

[![ci](https://github.com/ArtemIsmagilov/mcmc-rs/actions/workflows/ci.yaml/badge.svg)](https://github.com/ArtemIsmagilov/mcmc-rs/actions/workflows/ci.yaml)
[![crates.io](https://img.shields.io/crates/v/mcmc-rs.svg)](https://crates.io/crates/mcmc-rs)
[![docs.rs](https://img.shields.io/docsrs/mcmc-rs)](https://docs.rs/mcmc-rs)

This crate provides working with memcached server.
All methods implemented.
Available TCP/Unix/UDP/TLS connections.

- [Connection] is a Enum that represents a
  connection to memcached server.
- [Pipeline] is a structure that represents a
  pipeline of memcached commands.
- [WatchStream] is a structure that represents a
  stream of watch events.
- [Pool] is a structure that represents a
  pool of connections.
- [ClientCrc32] is a structure that represents a
  Cluster connections with ModN hashing.
- [ClientHashRing] is a structure that represents a
  Cluster connections with Ring hashing.
- [ClientRendezvous] is a structure that represents a
  Cluster connections with Rendezvous hashing.

### smol-runtime feature by default
```toml
mcmc-rs = "0.8.0"
```

### tokio-runtime feature by flag
```toml
mcmc-rs = { version = "0.8.0", default-features = false, features = ["tokio-runtime"] }
```

## Examples
```rust
use smol::{block_on, io};

use mcmc_rs::Connection;

fn main() -> io::Result<()> {
    block_on(async {
        let mut conn = Connection::default().await?;
        conn.set(b"key", 0, 0, false, b"value").await?;
        let item = conn.get(b"key").await?.unwrap();
        println!("{item:#?}");
        Ok(())
    })
}
```
- There's a lot more in the [examples] directory.

## Tests

```bash
docker compose up
bash chmod_unix.bash
cargo test
docker compose down
```

## Benchmarks

```bash
cargo bench
```

## Test coverage

```bash
cargo llvm-cov
```

## Mutation testing

```bash
cargo mutants
```

## Quality code

```bash
debtmap analyze .
```

## Formatting doc tests 

```bash
cargo fmt -- --config format_code_in_doc_comments=true
```

## Fuzzing test

```bash
cargo fuzz run fuzz_target_1
```

## Security scanning

```bash
semgrep scan --config auto
```

## Links

- [Minimal C client](https://github.com/dormando/mcmc)
- [Golang Client](https://github.com/bradfitz/gomemcache/tree/master)
- [Protocol](https://github.com/memcached/memcached/blob/master/doc/protocol.txt)
- Rust Clients
  - [https://github.com/vavrusa/memcache-async]
  - [https://github.com/aisk/rust-memcache]
  - [https://github.com/Shopify/async-memcached]
- [Memcached doc](https://docs.memcached.org)
- [Managed pool](https://docs.rs/deadpool/0.12.2/deadpool/)
- [Distributed hashing with ModN, HashRing, Rendezvous](https://www.francofernando.com/blog/distributed-systems/2021-12-24-distributed-hashing/)
- [Sharding algorithms](https://chaotic.land/ru/posts/2024/09/data-sharding-algorithms/)
