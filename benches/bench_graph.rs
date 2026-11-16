use mcmc_rs::{Connection, StorageResult};

fn main() -> mcmc_rs::Result<()> {
    smol::block_on(async {
        let mut conn = Connection::unix_connect("/tmp/memcached0.sock").await?;
        for _ in 0..100_000 {
            let result = conn.set("key0", 0, 0, false, b"a").await?;
            assert_eq!(result, StorageResult::Stored);
            let result = conn.get(b"key0").await?;
            assert!(result.is_some());
        }
        Ok(())
    })
}
