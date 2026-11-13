use mcmc_rs::Connection;
use std::hint::black_box;

fn main() -> mcmc_rs::Result<()> {
    smol::block_on(async {
        let mut conn = Connection::unix_connect("/tmp/memcached0.sock").await?;
        conn.set("key0", 0, 0, false, b"a").await?;
        for _ in 0..100_000 {
            black_box(conn.get(black_box(b"key0")).await?);
        }
        Ok(())
    })
}
