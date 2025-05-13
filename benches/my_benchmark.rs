use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use smol::block_on;

use mcmc_rs::Connection;

fn criterion_benchmark(c: &mut Criterion) {
    let mut conn = block_on(async { Connection::default().await }).unwrap();

    c.bench_function("get key0", |b| {
        b.iter(|| block_on(async { conn.get(black_box(b"key0")).await.unwrap() }))
    });

    c.bench_function("set key1", |b| {
        b.iter(|| {
            block_on(async {
                conn.set(
                    black_box(b"key1"),
                    black_box(0),
                    black_box(-1),
                    black_box(false),
                    black_box(b"value"),
                )
                .await
                .unwrap()
            })
        })
    });

    c.bench_function("set key2 noreply", |b| {
        b.iter(|| {
            block_on(async {
                conn.set(
                    black_box(b"key2"),
                    black_box(0),
                    black_box(-1),
                    black_box(true),
                    black_box(b"value"),
                )
                .await
                .unwrap()
            })
        })
    });

    c.bench_function("add key3", |b| {
        b.iter(|| {
            block_on(async {
                conn.add(
                    black_box(b"key3"),
                    black_box(0),
                    black_box(-1),
                    black_box(false),
                    black_box(b"value"),
                )
                .await
                .unwrap()
            })
        })
    });

    c.bench_function("add key4 noreply", |b| {
        b.iter(|| {
            block_on(async {
                conn.add(
                    black_box(b"key4"),
                    black_box(0),
                    black_box(-1),
                    black_box(true),
                    black_box(b"value"),
                )
                .await
                .unwrap()
            })
        })
    });

    c.bench_function("append key5", |b| {
        b.iter(|| {
            block_on(async {
                conn.append(
                    black_box(b"key5"),
                    black_box(0),
                    black_box(-1),
                    black_box(false),
                    black_box(b"value"),
                )
                .await
                .unwrap()
            })
        })
    });

    c.bench_function("append key6 noreply", |b| {
        b.iter(|| {
            block_on(async {
                conn.append(
                    black_box(b"key6"),
                    black_box(0),
                    black_box(-1),
                    black_box(true),
                    black_box(b"value"),
                )
                .await
                .unwrap()
            })
        })
    });

    c.bench_function("replace key7", |b| {
        b.iter(|| {
            block_on(async {
                conn.replace(
                    black_box(b"key7"),
                    black_box(0),
                    black_box(-1),
                    black_box(false),
                    black_box(b"value"),
                )
                .await
                .unwrap()
            })
        })
    });

    c.bench_function("replace key8 noreply", |b| {
        b.iter(|| {
            block_on(async {
                conn.replace(
                    black_box(b"key8"),
                    black_box(0),
                    black_box(-1),
                    black_box(true),
                    black_box(b"value"),
                )
                .await
                .unwrap()
            })
        })
    });

    c.bench_function("prepend key9", |b| {
        b.iter(|| {
            block_on(async {
                conn.replace(
                    black_box(b"key9"),
                    black_box(0),
                    black_box(-1),
                    black_box(false),
                    black_box(b"value"),
                )
                .await
                .unwrap()
            })
        })
    });

    c.bench_function("prepend key10 noreply", |b| {
        b.iter(|| {
            block_on(async {
                conn.replace(
                    black_box(b"key10"),
                    black_box(0),
                    black_box(-1),
                    black_box(true),
                    black_box(b"value"),
                )
                .await
                .unwrap()
            })
        })
    });

    c.bench_function("cas key11", |b| {
        b.iter(|| {
            block_on(async {
                conn.cas(
                    black_box(b"key11"),
                    black_box(0),
                    black_box(-1),
                    black_box(0),
                    black_box(false),
                    black_box(b"value"),
                )
                .await
                .unwrap()
            })
        })
    });

    c.bench_function("cas key12 noreply", |b| {
        b.iter(|| {
            block_on(async {
                conn.cas(
                    black_box(b"key12"),
                    black_box(0),
                    black_box(-1),
                    black_box(0),
                    black_box(true),
                    black_box(b"value"),
                )
                .await
                .unwrap()
            })
        })
    });

    c.bench_function("flush_all", |b| {
        b.iter(|| {
            block_on(async {
                conn.flush_all(black_box(None), black_box(false))
                    .await
                    .unwrap()
            })
        })
    });

    c.bench_function("flush_all noreply", |b| {
        b.iter(|| {
            block_on(async {
                conn.flush_all(black_box(None), black_box(true))
                    .await
                    .unwrap()
            })
        })
    });

    c.bench_function("version", |b| {
        b.iter(|| block_on(async { conn.version().await.unwrap() }))
    });

    c.bench_function("cache_memlimit", |b| {
        b.iter(|| {
            block_on(async {
                conn.cache_memlimit(black_box(10), black_box(false))
                    .await
                    .unwrap()
            })
        })
    });

    c.bench_function("cache_memlimit noreply", |b| {
        b.iter(|| {
            block_on(async {
                conn.cache_memlimit(black_box(10), black_box(true))
                    .await
                    .unwrap()
            })
        })
    });

    c.bench_function("delete", |b| {
        b.iter(|| {
            block_on(async {
                conn.delete(black_box(b"key13"), black_box(false))
                    .await
                    .unwrap()
            })
        })
    });

    c.bench_function("delete noreply", |b| {
        b.iter(|| {
            block_on(async {
                conn.delete(black_box(b"key14"), black_box(true))
                    .await
                    .unwrap()
            })
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
