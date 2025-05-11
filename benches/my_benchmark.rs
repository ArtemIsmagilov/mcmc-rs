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
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
