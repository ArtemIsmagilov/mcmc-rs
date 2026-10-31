use criterion::{Criterion, criterion_group, criterion_main};
use mcmc_rs::Connection;
use std::hint::black_box;

fn criterion_benchmark(c: &mut Criterion) {
    let targets = ["unix", "tls", "tcp", "udp"];

    for name in targets {
        c.bench_function(&format!("large value: {name}->get"), |b| {
            b.to_async(criterion::async_executor::SmolExecutor)
                .iter_with_setup(
                    || {
                        smol::block_on(async {
                            let mut c = match name {
                                "tcp" => Connection::default().await.unwrap(),
                                "unix" => Connection::unix_connect("/tmp/memcached0.sock")
                                    .await
                                    .unwrap(),
                                "udp" => Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214")
                                    .await
                                    .unwrap(),
                                "tls" => Connection::tls_connect("localhost", 11216, "cert.pem")
                                    .await
                                    .unwrap(),
                                _ => unreachable!(),
                            };
                            c.set("key0", 0, 0, false, vec![b'a'; 4096]).await.unwrap();
                            c
                        })
                    },
                    |mut conn| async move { conn.get(black_box(b"key0")).await.unwrap() },
                );
        });
    }

    for name in targets {
        c.bench_function(&format!("small value: {name}->get"), |b| {
            b.to_async(criterion::async_executor::SmolExecutor)
                .iter_with_setup(
                    || {
                        smol::block_on(async {
                            let mut c = match name {
                                "tcp" => Connection::default().await.unwrap(),
                                "unix" => Connection::unix_connect("/tmp/memcached0.sock")
                                    .await
                                    .unwrap(),
                                "udp" => Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214")
                                    .await
                                    .unwrap(),
                                "tls" => Connection::tls_connect("localhost", 11216, "cert.pem")
                                    .await
                                    .unwrap(),
                                _ => unreachable!(),
                            };
                            c.set("key0", 0, 0, false, b"a").await.unwrap();
                            c
                        })
                    },
                    |mut conn| async move { conn.get(black_box(b"key0")).await.unwrap() },
                );
        });
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
