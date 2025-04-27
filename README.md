# Minimal Rust client for Memcached

## Example

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

## Links

- [Minimal C client](https://github.com/dormando/mcmc)
- [Golang Client](https://github.com/bradfitz/gomemcache/tree/master)
- [Protocol](https://github.com/memcached/memcached/blob/master/doc/protocol.txt)
- Rust Clients
  - [https://github.com/vavrusa/memcache-async]
  - [https://github.com/aisk/rust-memcache]
- [Memcached doc](https://docs.memcached.org)
