# Minimal Rust client for Memcached

[![ci](https://github.com/ArtemIsmagilov/mcmc-rs/actions/workflows/ci.yaml/badge.svg)](https://github.com/ArtemIsmagilov/mcmc-rs/actions/workflows/ci.yaml)
[![crates.io](https://img.shields.io/crates/v/mcmc-rs.svg)](https://crates.io/crates/mcmc-rs)
[![docs.rs](https://img.shields.io/docsrs/mcmc-rs)](https://docs.rs/mcmc-rs)

An async Memcached client with a typed API and complete text-protocol support.

* TCP, Unix domain sockets, UDP and TLS.
* `smol` and `tokio` runtimes.
* Client-side validation of keys and meta-flag tokens.
* `TCP_NODELAY` is enabled automatically for TCP and TLS connections.
* Built-in cluster clients: modulo, consistent hash ring and rendezvous hashing.

## Features

The crate has no default features. Enable exactly one async runtime; TLS is
optional and is layered on top of the selected runtime.

### Async runtime (choose exactly one)

| Feature    | Description                                                                    |
| ---------- | ------------------------------------------------------------------------------ |
| `smol-rt`  | [`smol`](https://docs.rs/smol)/[`async-net`](https://docs.rs/async-net) runtime |
| `tokio-rt` | [`tokio`](https://docs.rs/tokio) runtime                                        |

```toml
# smol, no TLS
mcmc-rs = { version = "0.17.0", features = ["smol-rt"] }

# tokio, no TLS
mcmc-rs = { version = "0.17.0", features = ["tokio-rt"] }
```

### TLS (optional)

Each TLS feature also enables its runtime, so listing the TLS feature alone is
enough. Use at most one TLS backend.

| Feature                | Runtime | Backend                           |
| ---------------------- | ------- | --------------------------------- |
| `smol-rustls-tls`      | smol    | rustls via `futures-rustls`       |
| `tokio-rustls-tls`     | tokio   | rustls via `tokio-rustls`         |
| `smol-native-tls-tls`  | smol    | native-tls via `async-native-tls` |
| `tokio-native-tls-tls` | tokio   | native-tls via `tokio-native-tls` |

```toml
# smol + rustls
mcmc-rs = { version = "0.17.0", features = ["smol-rustls-tls"] }

# tokio + native-tls
mcmc-rs = { version = "0.17.0", features = ["tokio-native-tls-tls"] }
```

`Connection::tls_connect` is available only when a TLS feature is enabled.

### rustls certificate roots

The rustls backend starts with an empty root store. Enable one or both of these
features to verify public certificates:

| Feature                   | Description                                        |
| ------------------------- | -------------------------------------------------- |
| `webpki-roots-tls`        | Bundled Mozilla root certificates                  |
| `rustls-native-certs-tls` | Certificates from the operating system trust store |

```toml
mcmc-rs = { version = "0.17.0", features = ["smol-rustls-tls", "webpki-roots-tls"] }
```

`TlsConfig` selects which roots are trusted:
`TlsConfig::with_custom_ca_only` uses only the given PEM file,
`TlsConfig::with_additional_ca` adds it to the system roots, and
`TlsConfig::new` uses the system roots alone. The native-tls backend uses the
platform trust store automatically.

```rust,no_run
use mcmc_rs::{Connection, TlsConfig};

async fn connect() -> mcmc_rs::Result<()> {
    let mut conn = Connection::tls_connect(
        "localhost",
        11216,
        TlsConfig::with_additional_ca("ca.crt")
            .with_client_identity("client.crt", "client.key"),
    )
    .await?;
    let _version = conn.version().await?;
    Ok(())
}
```

### rustls CryptoProvider

`mcmc-rs` does not install a rustls `CryptoProvider`; install one in your
application before creating the first TLS connection:

```rust
rustls::crypto::aws_lc_rs::default_provider()
    .install_default()
    .expect("failed to install rustls CryptoProvider");
```

The simplest way to enable this provider is to add `rustls = "0.23"` to your
dependencies. For `ring`, see the crate documentation. The native-tls backends
do not use a rustls `CryptoProvider`.

### Insecure TLS

`insecure-tls` enables `TlsConfig::danger_accept_all()`, which disables
certificate and hostname verification. Use it only for local testing.

## Quick start

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

The same API works with Tokio. See
[examples](https://github.com/ArtemIsmagilov/mcmc-rs/tree/main/examples) for
connections, pipelines, watchers, TLS and sharding.

## Client-side sharding

`mcmc-rs` provides three server selectors implementing the `ServerSelector`
trait:

| Client             | Strategy                          | Hashing                                | Best for                                                  |
| ------------------ | --------------------------------- | -------------------------------------- | --------------------------------------------------------- |
| `ClientModN`       | Modulo (`hash(key) % N`)          | `rapidhash` (default) or custom hasher | Static node lists with equal node capacity                |
| `ClientHashRing`   | Consistent hash ring (`O(log V)`) | `rapidhash` (default) or custom hasher | Large dynamic clusters, weighted nodes, minimal remapping |
| `ClientRendezvous` | Rendezvous / HRW (`O(N)`)         | `rapidhash` (default) or custom hasher | Small-to-medium dynamic clusters, no virtual nodes        |

All selectors use `rapidhash` with a fixed default seed and support custom seeds
or any `std::hash::BuildHasher`. `ClientModN` works with a fixed
`Vec<Connection>`. `ClientHashRing` and `ClientRendezvous` identify nodes by name
and support adding or removing them at runtime:

```rust,no_run
use mcmc_rs::{ClientRendezvous, Connection, ServerSelector};

async fn run() -> mcmc_rs::Result<()> {
    let mut client = ClientRendezvous::new([
        ("node1", Connection::tcp_connect("127.0.0.1:11211").await?),
        ("node2", Connection::tcp_connect("127.0.0.1:11213").await?),
    ]);

    let _removed = client.remove_server("node2");
    client.set(b"key", 0, 0, false, b"value").await?;
    Ok(())
}
```

See
[examples/sharding.rs](https://github.com/ArtemIsmagilov/mcmc-rs/blob/main/examples/sharding.rs)
for seeded hashing, custom hashers and weighted nodes.

### Multi-key retrieval

All three selectors implement `get_multi`, `gets_multi`, `gat_multi` and
`gats_multi`. Keys are grouped by target server, one command is sent to each
server concurrently, and the results are merged into a `Vec<Item>`. The returned
order is not guaranteed to match the input order. An empty key slice returns an
empty vector without sending anything.

`Connection` exposes the same methods for a single server. They are not
available over UDP and panic if called on a UDP connection.

You can implement `ServerSelector` for custom routing strategies. Both
`select_server` and `select_servers` return a `Result`, so an empty selector can
report `MemcachedError::ClientConfig` instead of panicking.

## Key and flag validation

Keys and meta-flag tokens are validated before anything is written to the
socket:

* a key must be non-empty, at most 250 bytes long, and must not contain
  whitespace or control characters;
* a meta-flag token must not contain whitespace or control characters.

Invalid input returns `MemcachedError::InvalidInput`. `Connection` methods
return this error directly, while `Pipeline` methods return `Self`, so the error
is reported later by `Pipeline::execute`.

## Development

Tests, benchmarks, profiling, fuzzing and other maintainer commands are in
[CONTRIBUTING.md](https://github.com/ArtemIsmagilov/mcmc-rs/blob/main/CONTRIBUTING.md).

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
