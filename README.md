# Minimal Rust client for Memcached

[![ci](https://github.com/ArtemIsmagilov/mcmc-rs/actions/workflows/ci.yaml/badge.svg)](https://github.com/ArtemIsmagilov/mcmc-rs/actions/workflows/ci.yaml)
[![crates.io](https://img.shields.io/crates/v/mcmc-rs.svg)](https://crates.io/crates/mcmc-rs)
[![docs.rs](https://img.shields.io/docsrs/mcmc-rs)](https://docs.rs/mcmc-rs)

## Asynchronous memcached client on Rust.

This crate provides working with memcached server.
All methods implemented.
Available TCP/Unix/UDP connections.

- [Connection] is a Enum that represents a
  connection to memcached server.
- [Pipeline] is a structure that represents a
  pipeline of memcached commands.
- [WatchStream] is a structure that represents a
  stream of watch events.
- [ClientCrc32] is a structure that represents a
  Cluster connections with ModN hashing.
- [ClientHashRing] is a structure that represents a
  Cluster connections with Ring hashing.

## Example

### Connection mode

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

### Cluster mode

```rust
use smol::{block_on, io};

use mcmc_rs::{ClientCrc32, Connection};

fn main() -> io::Result<()> {
    block_on(async {
        let mut client = ClientCrc32::new(vec![
            Connection::default().await?,
            Connection::tcp_connect("127.0.0.1:11212").await?,
        ]);
        client.set(b"key", 0, 0, false, b"value").await?;
        let item = client.get(b"key").await?.unwrap();
        println!("{item:#?}");
        Ok(())
    })
}
```

### Pipeline mode

```rust
use smol::{block_on, io};

use mcmc_rs::Connection;

fn main() -> io::Result<()> {
    block_on(async {
        let mut conn = Connection::default().await?;
        let r = conn
            .pipeline()
            .set("key", 0, 0, false, "A")
            .set("key2", 0, 0, false, "A")
            .get("key")
            .get("key2")
            .version()
            .execute()
            .await?;
        println!("{r:#?}");
        Ok(())
    })
}
```

### Pool mode

```rust
use smol::{block_on, io};

use mcmc_rs::{AddrArg, Manager, Pool};

fn main() -> io::Result<()> {
    block_on(async {
        let mgr = Manager::new(AddrArg::Tcp("127.0.0.1:11211"));
        let pool = Pool::builder(mgr).build().unwrap();
        let mut conn = pool.get().await.unwrap();
        let result = conn.version().await?;
        println!("{result:#?}");
        Ok(())
    })
}
```

### Watch mode

```rust
use smol::{block_on, io};

use mcmc_rs::{Connection, WatchArg};

fn main() -> io::Result<()> {
    block_on(async {
        let mut conn = Connection::default().await?;
        let mut w = conn.watch(&[WatchArg::Fetchers]).await?;
        let mut conn = Connection::default().await?;
        conn.get(b"key").await?;
        println!("{:#?}", w.message().await?);
        Ok(())
    })
}
```

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
- [Distributed hashing with ModN, HashRing, Rendezvouz](https://www.francofernando.com/blog/distributed-systems/2021-12-24-distributed-hashing/)
- [Sharding algorithms](https://chaotic.land/ru/posts/2024/09/data-sharding-algorithms/)
