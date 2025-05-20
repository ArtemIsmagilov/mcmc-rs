use std::hint::black_box;
use std::time::Duration;

use criterion::{Criterion, criterion_group, criterion_main};
use smol::block_on;

use mcmc_rs::Connection;

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("benchmark_group");
    group.sample_size(10);
    group.measurement_time(Duration::from_millis(100));
    group.warm_up_time(Duration::from_millis(100));

    let mut conn = block_on(async { Connection::default().await }).unwrap();

    group.bench_function("get", |b| {
        b.iter(|| block_on(async { conn.get(black_box(b"key0")).await.unwrap() }))
    });

    group.bench_function("gets", |b| {
        b.iter(|| block_on(async { conn.gets(black_box(b"key1")).await.unwrap() }))
    });

    group.bench_function("gat", |b| {
        b.iter(|| block_on(async { conn.gat(black_box(-1), black_box(b"key2")).await.unwrap() }))
    });

    group.bench_function("gats", |b| {
        b.iter(|| block_on(async { conn.gats(black_box(-1), black_box(b"key3")).await.unwrap() }))
    });

    group.bench_function("get multi", |b| {
        b.iter(|| block_on(async { conn.get_multi(black_box(&[b"key4"])).await.unwrap() }))
    });

    group.bench_function("gets multi", |b| {
        b.iter(|| block_on(async { conn.gets_multi(black_box(&[b"key5"])).await.unwrap() }))
    });

    group.bench_function("gat multi", |b| {
        b.iter(|| {
            block_on(async {
                conn.gat_multi(black_box(-1), black_box(&["key6"]))
                    .await
                    .unwrap()
            })
        })
    });

    group.bench_function("gats multi", |b| {
        b.iter(|| {
            block_on(async {
                conn.gats_multi(black_box(-1), black_box(&["key7"]))
                    .await
                    .unwrap()
            })
        })
    });

    group.bench_function("set", |b| {
        b.iter(|| {
            block_on(async {
                conn.set(
                    black_box("key8"),
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

    group.bench_function("set noreply", |b| {
        b.iter(|| {
            block_on(async {
                conn.set(
                    black_box(b"key9"),
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

    group.bench_function("add", |b| {
        b.iter(|| {
            block_on(async {
                conn.add(
                    black_box(b"key10"),
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

    group.bench_function("add noreply", |b| {
        b.iter(|| {
            block_on(async {
                conn.add(
                    black_box(b"key11"),
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

    group.bench_function("append", |b| {
        b.iter(|| {
            block_on(async {
                conn.append(
                    black_box(b"key12"),
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

    group.bench_function("append noreply", |b| {
        b.iter(|| {
            block_on(async {
                conn.append(
                    black_box(b"key13"),
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

    group.bench_function("replace", |b| {
        b.iter(|| {
            block_on(async {
                conn.replace(
                    black_box(b"key14"),
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

    group.bench_function("replace noreply", |b| {
        b.iter(|| {
            block_on(async {
                conn.replace(
                    black_box(b"key15"),
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

    group.bench_function("prepend", |b| {
        b.iter(|| {
            block_on(async {
                conn.replace(
                    black_box(b"key16"),
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

    group.bench_function("prepend noreply", |b| {
        b.iter(|| {
            block_on(async {
                conn.replace(
                    black_box(b"key17"),
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

    group.bench_function("cas", |b| {
        b.iter(|| {
            block_on(async {
                conn.cas(
                    black_box(b"key18"),
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

    group.bench_function("cas noreply", |b| {
        b.iter(|| {
            block_on(async {
                conn.cas(
                    black_box(b"key19"),
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

    group.bench_function("flush_all", |b| {
        b.iter(|| {
            block_on(async {
                conn.flush_all(black_box(None), black_box(false))
                    .await
                    .unwrap()
            })
        })
    });

    group.bench_function("flush_all noreply", |b| {
        b.iter(|| {
            block_on(async {
                conn.flush_all(black_box(None), black_box(true))
                    .await
                    .unwrap()
            })
        })
    });

    group.bench_function("version", |b| {
        b.iter(|| block_on(async { conn.version().await.unwrap() }))
    });

    group.bench_function("cache_memlimit", |b| {
        b.iter(|| {
            block_on(async {
                conn.cache_memlimit(black_box(10), black_box(false))
                    .await
                    .unwrap()
            })
        })
    });

    group.bench_function("cache_memlimit noreply", |b| {
        b.iter(|| {
            block_on(async {
                conn.cache_memlimit(black_box(10), black_box(true))
                    .await
                    .unwrap()
            })
        })
    });

    group.bench_function("delete", |b| {
        b.iter(|| {
            block_on(async {
                conn.delete(black_box(b"key20"), black_box(false))
                    .await
                    .unwrap()
            })
        })
    });

    group.bench_function("delete noreply", |b| {
        b.iter(|| {
            block_on(async {
                conn.delete(black_box(b"key21"), black_box(true))
                    .await
                    .unwrap()
            })
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
