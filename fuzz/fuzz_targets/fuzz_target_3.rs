#![no_main]

use libfuzzer_sys::fuzz_target;
use smol::block_on;
use smol::io::Cursor;

use mcmc_rs::version_cmd;

fuzz_target!(|data: String| {
    // fuzzed code goes here
    block_on(async {
        if data.is_empty() || data.contains("\n") || data.contains("\r") {
            return;
        }
        // version\r\nVERSION 1.2.3\r\n
        let mut w = Vec::from(b"version\r\nVERSION ");
        w.extend(data.as_bytes());
        w.extend(b"\r\n");

        version_cmd(&mut Cursor::new(w)).await.unwrap();
    })
});
