use gungraun::prelude::*;
use std::hint::black_box;

use mcmc_rs::{Connection, Item};

fn expensive_setup(n: &str) -> Connection {
    smol::block_on(async {
        let mut c = match n {
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
        c.set("key0", 0, 0, false, b"a").await.unwrap();
        c
    })
}

#[library_benchmark]
#[benches::with_setup(args = ["unix", "tls", "tcp", "udp"], setup = expensive_setup)]
fn bench_processing(mut c: Connection) -> Item {
    black_box(smol::block_on(async {
        c.get(black_box(b"key0")).await.unwrap().unwrap()
    }))
}

library_benchmark_group!(name = bench_processing_group, benchmarks = bench_processing);
main!(library_benchmark_groups = bench_processing_group);
