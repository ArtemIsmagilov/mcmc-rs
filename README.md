# Minimal Rust client for Memcached

[![ci](https://github.com/ArtemIsmagilov/mcmc-rs/actions/workflows/ci.yaml/badge.svg)](https://github.com/ArtemIsmagilov/mcmc-rs/actions/workflows/ci.yaml)
[![crates.io](https://img.shields.io/crates/v/mcmc-rs.svg)](https://crates.io/crates/mcmc-rs)
[![docs.rs](https://img.shields.io/docsrs/mcmc-rs)](https://docs.rs/mcmc-rs)
[![XERJ Corpus Hub contributor](https://hub.xerj.org/contributors/ArtemIsmagilov/badge.svg)](https://hub.xerj.org/contributors/ArtemIsmagilov)

* This crate provides a comprehensive interface for interacting with Memcached servers.
* Implements the complete Memcached protocol.
* Available `TCP/Unix/UDP/TLS` connections.
* Cross-platform support for `Linux/macOS/Windows` .
* Typed API.
* Client-side validation of keys and meta-flag tokens.
* `TCP_NODELAY` is enabled automatically for TCP and TLS connections.

---

* [Connection] is a Enum that represents a
  connection to memcached server.
* [Pipeline] is a structure that represents a
  pipeline of memcached commands.
* [WatchStream] is a structure that represents a
  stream of watch events.
* [ClientCrc32] is a structure that represents a
  Cluster connections with ModN hashing.
* [ClientHashRing] is a structure that represents a
  Cluster connections with Ring hashing.
* [ClientRendezvous] is a structure that represents a
  Cluster connections with Rendezvous hashing.

## Features

The crate has no default features. You must enable exactly one async runtime;
TLS support is optional and is layered on top of the selected runtime.

### Async runtime (choose exactly one)

| Feature    | Description                                                    |
| ---------- | -------------------------------------------------------------- |
| `smol-rt`  | [`smol`](https://docs.rs/smol)/[`async-net`](https://docs.rs/async-net) runtime |
| `tokio-rt` | [`tokio`](https://docs.rs/tokio) runtime                       |

```toml
# smol, no TLS
mcmc-rs = { version = "0.15.0", features = ["smol-rt"] }

# tokio, no TLS
mcmc-rs = { version = "0.15.0", features = ["tokio-rt"] }
```

### TLS (optional)

Each TLS feature also enables the runtime it belongs to, so listing the TLS
feature alone is enough. Use at most one TLS backend.

| Feature                 | Runtime | Backend                                  |
| ----------------------- | ------- | ---------------------------------------- |
| `smol-rustls-tls`       | smol    | rustls via `futures-rustls`              |
| `tokio-rustls-tls`      | tokio   | rustls via `tokio-rustls`                |
| `smol-native-tls-tls`   | smol    | native-tls via `async-native-tls`        |
| `tokio-native-tls-tls`  | tokio   | native-tls via `tokio-native-tls`        |

```toml
# smol + rustls
mcmc-rs = { version = "0.15.0", features = ["smol-rustls-tls"] }

# tokio + native-tls
mcmc-rs = { version = "0.15.0", features = ["tokio-native-tls-tls"] }
```

`Connection::tls_connect` is available only when a TLS feature is enabled.

### rustls certificate roots

The rustls backend starts with an empty root store. Enable one or both of these
features to verify public certificates:

| Feature                  | Description                                      |
| ------------------------ | ------------------------------------------------ |
| `webpki-roots-tls`       | Bundled Mozilla root certificates                |
| `rustls-native-certs-tls`| Certificates from the operating system trust store |

```toml
mcmc-rs = { version = "0.15.0", features = ["smol-rustls-tls", "webpki-roots-tls"] }
```

Certificates passed through the `ca_path` argument of `Connection::tls_connect`
are always added to the root store. The native-tls backend uses the platform
trust store automatically.

### rustls CryptoProvider

`mcmc-rs` does not select or install a rustls `CryptoProvider`. With a rustls
backend, choose a provider in your application. The TLS helper crates may
enable `aws-lc-rs` through their default features, but you should not rely on
that. The simplest explicit option is to add the `rustls` dependency with its
default features, which enables the `aws-lc-rs` provider:

```toml
rustls = "0.23"
```

Alternatively, install the provider explicitly before creating the first TLS
connection:

```rust
rustls::crypto::aws_lc_rs::default_provider()
    .install_default()
    .expect("failed to install rustls CryptoProvider");
```

To use *ring* instead, enable that provider on the `rustls` dependency and
install it:

```toml
rustls = { version = "0.23", default-features = false, features = ["ring"] }
```

```rust
rustls::crypto::ring::default_provider()
    .install_default()
    .expect("failed to install rustls CryptoProvider");
```

Selecting the provider explicitly also prepares the code for rustls 0.24, where
this becomes mandatory. The native-tls backends do not use a rustls
`CryptoProvider`.

### Insecure TLS

`insecure-tls` allows passing `danger_insecure = true` to
`Connection::tls_connect`, which disables certificate and hostname
verification. Use it only for local testing.

## Examples

With `smol-rt` enabled:

```rust
use smol::block_on;

use mcmc_rs::Connection;

fn main() -> mcmc_rs::Result<()> {
    block_on(async {
        let mut conn = Connection::default().await?;
        conn.set(b"key", 0, 0, false, b"value").await?;
        let item = conn.get(b"key").await?;
        println!("{item:#?}");
        Ok(())
    })
}
```

* There's a lot more in the [examples](examples/)

## Client-Side Sharding

`mcmc-rs` provides three server selectors implementing the `ServerSelector` trait:

| Client             | Strategy                         | Hashing                                 | Best for                                            |
| ------------------ | -------------------------------- | --------------------------------------- | --------------------------------------------------- |
| `ClientCrc32`      | Modulo (`crc32(key) % N`)        | CRC32                                   | Static node lists, compatibility with legacy clients |
| `ClientHashRing`   | Consistent hash ring (`O(log V)`) | `rapidhash` (default) or custom hasher  | Large dynamic clusters, minimal key remapping       |
| `ClientRendezvous` | Rendezvous / HRW (`O(N)`)        | `rapidhash` (default) or custom hasher  | Small-to-medium dynamic clusters, zero vnode overhead |

### Hashing options and dynamic membership

`ClientHashRing` and `ClientRendezvous` use `rapidhash` with a fixed default seed out of the box, support custom seeds or any `std::hash::BuildHasher`, and allow adding or removing nodes at runtime by node name:

```rust,no_run
use std::collections::hash_map::DefaultHasher;
use std::hash::BuildHasherDefault;

use mcmc_rs::{ClientHashRing, ClientRendezvous, Connection, ServerSelector};

async fn run() -> mcmc_rs::Result<()> {
    // 1. Default: fast non-cryptographic rapidhash with a fixed seed
    let mut client = ClientRendezvous::new([
        ("127.0.0.1:11211", Connection::tcp_connect("127.0.0.1:11211").await?),
        ("127.0.0.1:11212", Connection::tcp_connect("127.0.0.1:11212").await?),
    ]);

    // Add or remove nodes dynamically without shifting existing node hashes
    client
        .add_server("127.0.0.1:11213", Connection::tcp_connect("127.0.0.1:11213").await?);
    let _removed = client.remove_server("127.0.0.1:11212");

    // 2. Custom seed: share a secret seed across app replicas to mitigate HashDoS
    //    when cache keys are derived from untrusted user input
    let mut seeded_client = ClientRendezvous::with_seed(
        [
            ("node1", Connection::tcp_connect("127.0.0.1:11211").await?),
            ("node2", Connection::tcp_connect("127.0.0.1:11212").await?),
        ],
        0x9e37_79b9_7f4a_7c15,
    );

    // 3. Custom BuildHasher: plug in any std::hash::BuildHasher (SipHash, xxHash, tag-based, etc.)
    let hasher = BuildHasherDefault::<DefaultHasher>::default();
    let mut custom_client = ClientHashRing::with_hasher(
        [
            ("node1", Connection::tcp_connect("127.0.0.1:11211").await?),
            ("node2", Connection::tcp_connect("127.0.0.1:11212").await?),
        ],
        hasher,
    );

    client.set(b"key", 0, 0, false, b"value").await?;
    seeded_client.set(b"key", 0, 0, false, b"value").await?;
    custom_client.set(b"key", 0, 0, false, b"value").await?;
    Ok(())
}
```

### Multi-key retrieval

All three selectors implement `get_multi`, `gets_multi`, `gat_multi` and
`gats_multi`. Keys are grouped by their target server, one multi-key command is
sent to each server concurrently, and the responses are merged into a single
`Vec<Item>`:

```rust,no_run
use mcmc_rs::{ClientRendezvous, Connection, ServerSelector};

async fn run() -> mcmc_rs::Result<()> {
    let mut client = ClientRendezvous::new([
        ("node1", Connection::tcp_connect("127.0.0.1:11211").await?),
        ("node2", Connection::tcp_connect("127.0.0.1:11212").await?),
    ]);

    let items = client.get_multi(&[b"key1", b"key2", b"key3"]).await?;
    for item in items {
        println!("{} = {:?}", item.key, item.data_block);
    }
    Ok(())
}
```

The returned order is not guaranteed to match the input order, because each
server returns its own items. Sort the result if order matters. An empty key
slice returns an empty vector without sending anything to the servers.

`Connection` also provides the same multi-key methods for a single server.
They are not available over UDP and panic if called on a UDP connection.

You can also implement the `ServerSelector` trait for your own types to build custom routing strategies (such as weighted nodes or Ketama-compatible rings). Custom selectors must implement both `select_server` for a single key and `select_servers` for grouping a batch of keys by server.

## Key and flag validation

Keys and meta-flag tokens are validated before anything is written to the
socket:

* a key must be non-empty, at most 250 bytes long, and must not contain
  whitespace or control characters;
* a meta-flag token must not contain whitespace or control characters.

Invalid input returns `MemcachedError::InvalidInput`. `Connection` methods
return this error directly, while `Pipeline` methods return `Self`, so the
error is reported later by `Pipeline::execute`.

## Tests

```bash
docker compose up
cargo test --features smol-rustls-tls
docker compose down
```

## Benchmarks

```bash
cargo bench --bench bench_time --features smol-rustls-tls
cargo bench --bench bench_perf --features smol-rustls-tls
```

## Profiling

* with samply

```bash
samply record cargo bench --bench bench_prof --features smol-rt
```

* with flamegraph-rs

```bash
cargo flamegraph --bench bench_prof --features smol-rt
```

## Test coverage

```bash
cargo llvm-cov --features smol-rustls-tls
```

## Mutation testing

```bash
cargo mutants --features smol-rustls-tls
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
cargo fuzz run fuzz_target_1 --features smol-rt
```

## Security scanning

```bash
opengrep scan --config auto
```

## Compile some configs

```bash
cargo hack check --feature-powerset --no-dev-deps --mutually-exclusive-features smol-rt,tokio-rt --at-least-one-of smol-rt,tokio-rt
```

## Links

* [Minimal C client](https://github.com/dormando/mcmc)
* [Golang Client](https://github.com/bradfitz/gomemcache/tree/master)
* [Protocol](https://github.com/memcached/memcached/blob/master/doc/protocol.txt)
* Rust Clients
  * [https://github.com/vavrusa/memcache-async]
  * [https://github.com/aisk/rust-memcache]
  * [https://github.com/Shopify/async-memcached]
* [Memcached doc](https://docs.memcached.org)
* [Managed pool](https://docs.rs/deadpool/0.12.2/deadpool/)
* [Distributed hashing with ModN, HashRing, Rendezvous](https://www.francofernando.com/blog/distributed-systems/2021-12-24-distributed-hashing/)
* [Sharding algorithms](https://chaotic.land/ru/posts/2024/09/data-sharding-algorithms/)
