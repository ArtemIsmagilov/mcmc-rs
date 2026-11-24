use std::collections::hash_map::DefaultHasher;
use std::hash::BuildHasherDefault;

use smol::block_on;

use mcmc_rs::{ClientHashRing, ClientModN, ClientRendezvous, Connection, ServerSelector};

fn main() -> mcmc_rs::Result<()> {
    block_on(async {
        // Modulo hashing over a fixed connection list.
        let mut modn = ClientModN::new(vec![
            Connection::tcp_connect("127.0.0.1:11211").await?,
            Connection::tcp_connect("127.0.0.1:11213").await?,
        ]);
        modn.set(b"key", 0, 0, false, b"value").await?;
        let item = modn.get(b"key").await?;
        println!("modn: {item:#?}");

        // Consistent hash ring. Nodes can be added and removed by name.
        let mut ring = ClientHashRing::new([
            ("node1", Connection::tcp_connect("127.0.0.1:11211").await?),
            ("node2", Connection::tcp_connect("127.0.0.1:11213").await?),
        ]);
        if let Some(conn) = ring.remove_server("node2") {
            ring.add_server("node2", conn);
        }
        ring.set(b"key", 0, 0, false, b"value").await?;
        println!("ring node for key: {:?}", ring.server_for_key(b"key"));

        // Rendezvous hashing with a custom seed. Sharing the seed across app
        // replicas keeps key routing identical between them.
        let mut rendezvous = ClientRendezvous::with_seed(
            [
                ("node1", Connection::tcp_connect("127.0.0.1:11211").await?),
                ("node2", Connection::tcp_connect("127.0.0.1:11213").await?),
            ],
            42,
        );
        rendezvous.set(b"key", 0, 0, false, b"value").await?;
        let items = rendezvous
            .get_multi(&[b"key".as_slice(), b"missing".as_slice()])
            .await?;
        println!("rendezvous: {items:#?}");

        // Weighted ring: a larger weight receives a larger share of the keys.
        let weighted = ClientHashRing::with_weights([
            (
                "node1",
                Connection::tcp_connect("127.0.0.1:11211").await?,
                100,
            ),
            (
                "node2",
                Connection::tcp_connect("127.0.0.1:11213").await?,
                300,
            ),
        ]);
        assert!(weighted.server_for_key(b"key").is_some());

        // Any `std::hash::BuildHasher` can be used instead of `rapidhash`.
        let hasher = BuildHasherDefault::<DefaultHasher>::default();
        let mut custom = ClientHashRing::with_hasher(
            [
                ("node1", Connection::tcp_connect("127.0.0.1:11211").await?),
                ("node2", Connection::tcp_connect("127.0.0.1:11213").await?),
            ],
            hasher,
        );
        custom.set(b"key", 0, 0, false, b"value").await?;

        Ok(())
    })
}
