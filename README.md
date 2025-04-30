# Minimal Rust client for Memcached

[![CI](https://github.com/ArtemIsmagilov/mcmc-rs/actions/workflows/ci.yaml/badge.svg)](https://github.com/ArtemIsmagilov/mcmc-rs/actions/workflows/ci.yaml)
[![crates.io](https://img.shields.io/crates/v/bitflags.svg)](https://crates.io/crates/mcmc-rs)

## Example

### Connection mode

```rust
use smol::{block_on, io};

use mcmc_rs::{Connection, Item};

fn main() -> io::Result<()> {
    block_on(async {
        let mut conn = Connection::default().await?;
        conn.set(b"key", 0, 0, false, b"value").await?;
        let item: Item = conn.get(b"key").await?.unwrap();
        conn.delete(b"key", true).await?;
        conn.get_multi(&[b"key1", b"key2"]).await?;
        let version = conn.version().await?;
        println!("{version:#?}");
        Ok(())
    })
}
```

### Cluster mode

```rust
use smol::{block_on, io};

use mcmc_rs::{ClientCrc32, Connection, Item};

fn main() -> io::Result<()> {
    block_on(async {
        let mut client = ClientCrc32::new(vec![
            Connection::default().await?,
            Connection::tcp_connect("127.0.0.1:11212").await?,
        ]);
        client.set(b"key", 0, 0, false, b"value").await?;
        let item: Item = client.get(b"key").await?.unwrap();
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

## Tests

```bash
docker compose up -d
docker exec mcmc-rs-md-unix0-1 sh -c "chmod a+rw /tmp/memcached.sock"
cargo test
docker compose down
```

## Links

- [Minimal C client](https://github.com/dormando/mcmc)
- [Golang Client](https://github.com/bradfitz/gomemcache/tree/master)
- [Protocol](https://github.com/memcached/memcached/blob/master/doc/protocol.txt)
- Rust Clients
  - [https://github.com/vavrusa/memcache-async]
  - [https://github.com/aisk/rust-memcache]
- [Memcached doc](https://docs.memcached.org)
