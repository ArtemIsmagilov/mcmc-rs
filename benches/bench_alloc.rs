use smol::block_on;

use mcmc_rs::Connection;

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

fn main() -> mcmc_rs::Result<()> {
    block_on(async {
        let mut conn = Connection::default().await?;
        conn.set("key0", 0, 0, false, b"a").await?;

        let _profiler = dhat::Profiler::new_heap();
        conn.get(b"key0").await?;

        Ok(())
    })
}
