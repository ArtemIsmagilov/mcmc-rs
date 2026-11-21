# Minimal Rust client for Memcached

[![ci](https://github.com/ArtemIsmagilov/mcmc-rs/actions/workflows/ci.yaml/badge.svg)](https://github.com/ArtemIsmagilov/mcmc-rs/actions/workflows/ci.yaml)
[![crates.io](https://img.shields.io/crates/v/mcmc-rs.svg)](https://crates.io/crates/mcmc-rs)
[![docs.rs](https://img.shields.io/docsrs/mcmc-rs)](https://docs.rs/mcmc-rs)
[![XERJ Corpus Hub contributor](https://hub.xerj.org/contributors/ArtemIsmagilov/badge.svg)](https://hub.xerj.org/contributors/ArtemIsmagilov)

* This crate provides a comprehensive interface for interacting with Memcached servers.
* Implements the complete Memcached protocol.
* Available `TCP/Unix/UDP/TLS` connections.
* Cross-platform support for `Linux/macOS/Windows` .
* Typed API

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
mcmc-rs = { version = "0.12.0", features = ["smol-rt"] }

# tokio, no TLS
mcmc-rs = { version = "0.12.0", features = ["tokio-rt"] }
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
mcmc-rs = { version = "0.12.0", features = ["smol-rustls-tls"] }

# tokio + native-tls
mcmc-rs = { version = "0.12.0", features = ["tokio-native-tls-tls"] }
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
mcmc-rs = { version = "0.12.0", features = ["smol-rustls-tls", "webpki-roots-tls"] }
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
