use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use smol::block_on;

use mcmc_rs::Connection;

fn criterion_benchmark(c: &mut Criterion) {
    for (name, mut conn) in [
        (
            "tcp",
            block_on(async { Connection::default().await }).unwrap(),
        ),
        (
            "unix",
            block_on(async { Connection::unix_connect("/tmp/memcached0.sock").await }).unwrap(),
        ),
        (
            "udp",
            block_on(async { Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await })
                .unwrap(),
        ),
        (
            "tls",
            block_on(async { Connection::tls_connect("localhost", 11216, "cert.pem").await })
                .unwrap(),
        ),
    ] {
        c.bench_function(&format!("{name}->get"), |b| {
            b.iter(|| block_on(async { conn.get(black_box(b"key0")).await.unwrap() }))
        });

        c.bench_function(&format!("{name}->version"), |b| {
            b.iter(|| block_on(async { conn.version().await.unwrap() }))
        });
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
