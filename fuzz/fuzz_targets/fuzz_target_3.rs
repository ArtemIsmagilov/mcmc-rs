#![no_main]

use libfuzzer_sys::fuzz_target;
use smol::block_on;
use smol::io::Cursor;

use mcmc_rs::version_cmd;

fuzz_target!(|data: &[u8]| {
    // fuzzed code goes here
    block_on(async {
        if data.is_empty()
            || data.contains(&b'\n')
            || data.contains(&b'\r')
            || String::from_utf8(data.to_vec()).is_err()
        {
            return;
        }
        // version\r\nVERSION 1.2.3\r\n
        let mut w = Vec::from(b"version\r\nVERSION ");
        w.extend(data);
        w.extend(b"\r\n");

        version_cmd(&mut Cursor::new(w)).await.unwrap();
    })
});
