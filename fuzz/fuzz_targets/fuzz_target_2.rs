#![no_main]

use libfuzzer_sys::fuzz_target;
use smol::block_on;
use smol::io::Cursor;

use mcmc_rs::storage_cmd;

fuzz_target!(|data: &[u8]| {
    // fuzzed code goes here
    block_on(async {
        if data.is_empty() {
            return;
        }
        // append key 0 0 5\r\nvalue\r\nSTORED\r\n
        let mut w = Vec::from(b"append key 0 0 ");
        w.extend(data.len().to_string().as_bytes());
        w.extend(b"\r\n");
        w.extend(data);
        w.extend(b"\r\nSTORED\r\n");

        storage_cmd(
            &mut Cursor::new(w),
            b"append",
            b"key",
            0,
            0,
            None,
            false,
            data,
        )
        .await
        .unwrap();
    })
});
