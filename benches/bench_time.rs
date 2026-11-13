use criterion::{Criterion, async_executor::SmolExecutor, criterion_group, criterion_main};
use mcmc_rs::Connection;
use std::hint::black_box;
use std::time::Instant;

fn criterion_benchmark(c: &mut Criterion) {
    let targets = ["unix", "tls", "tcp", "udp"];
    for name in targets {
        c.bench_function(&format!("small value: {name}->get"), |b| {
            b.to_async(SmolExecutor).iter_custom(|iters| async move {
                let mut x = match name {
                    "tcp" => Connection::default().await.unwrap(),
                    "unix" => Connection::unix_connect("/tmp/memcached0.sock")
                        .await
                        .unwrap(),
                    "udp" => Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214")
                        .await
                        .unwrap(),
                    "tls" => Connection::tls_connect("localhost", 11216, "ca.crt")
                        .await
                        .unwrap(),
                    _ => unreachable!(),
                };
                x.set("key0", 0, 0, false, b"a").await.unwrap();
                let start = Instant::now();
                for _i in 0..iters {
                    x.get(black_box(b"key0")).await.unwrap();
                }
                start.elapsed()
            });
        });
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
