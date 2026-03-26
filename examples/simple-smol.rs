use smol::{block_on, io};

use mcmc_rs::{AddrArg, ClientCrc32, Connection, Manager, Pool, WatchArg};

fn main() -> io::Result<()> {
    block_on(async {
        // Connection mode
        let mut conn = Connection::default().await?;
        conn.set(b"key", 0, 0, false, b"value").await?;
        let item = conn.get(b"key").await?;
        println!("{item:#?}");
        // Cluster mode
        let mut client = ClientCrc32::new(vec![
            Connection::default().await?,
            Connection::tcp_connect("127.0.0.1:11213").await?,
        ]);
        client.set(b"key", 0, 0, false, b"value").await?;
        let item = client.get(b"key").await?;
        println!("{item:#?}");
        // Pipeline mode
        let r = conn
            .pipeline()
            .set("key", 0, 0, false, "A")
            .set("key2", 0, 0, false, "A")
            .get("key")
            .get("key2")
            .version()
            .execute()
            .await?;
        println!("{r:#?}");
        // Pool mode
        let mgr = Manager::new(AddrArg::Tcp("127.0.0.1:11211"));
        let pool = Pool::builder(mgr).build().unwrap();
        let mut conn = pool.get().await.unwrap();
        let r = conn.version().await?;
        println!("{r:#?}");
        // Watch mode
        let conn = Connection::default().await?;
        let mut w = conn.watch(&[WatchArg::Fetchers]).await?;
        let mut conn = Connection::default().await?;
        conn.get(b"key").await?;
        println!("{:#?}", w.message().await?);
        // TCP connection
        let mut conn = Connection::default().await?;
        let version = conn.version().await?;
        println!("tcp: {version:?}");
        // Unix connection
        let mut conn = Connection::unix_connect("/tmp/memcached0.sock").await?;
        let version = conn.version().await?;
        println!("unix: {version:?}");
        // UDP connection
        let mut conn = Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?;
        let version = conn.version().await?;
        println!("udp: {version:?}");
        // TLS connection
        let mut conn = Connection::tls_connect("localhost", 11216, "cert.pem").await?;
        let version = conn.version().await?;
        println!("tls: {version:?}");
        Ok(())
    })
}
