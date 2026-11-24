# Contributing

Thanks for improving `mcmc-rs`. This file collects the commands used to test,
benchmark, profile and check the crate.

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
cargo fuzz run fuzz_target_1 \
          --all-features \
          --target x86_64-unknown-linux-gnu \
          -- -max_total_time=30 \
          -dict=fuzz/memcached.dict \
          -stale_corpus_timeout=15
```

## Security scanning

```bash
opengrep scan --config auto
```

## Compile some configs

```bash
cargo hack check \
          --feature-powerset \
          --no-dev-deps \
          --mutually-exclusive-features \
          smol-rt,tokio-rt \
          --at-least-one-of \
          smol-rt,tokio-rt
```
