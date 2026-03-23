#![no_main]

use libfuzzer_sys::fuzz_target;
use smol::block_on;
use smol::io::Cursor;

use mcmc_rs::retrieval_cmd;

fuzz_target!(|data: &[u8]| {
    // fuzzed code goes here
    block_on(async {
        if data.is_empty() {
            return;
        }
        //get key\r\nVALUE key 0 1\r\na\r\nEND\r\n
        let mut w = Vec::from(b"get key\r\nVALUE key 0 ");
        w.extend(data.len().to_string().as_bytes());
        w.extend(b"\r\n");
        w.extend(data);
        w.extend(b"\r\nEND\r\n");

        retrieval_cmd(&mut Cursor::new(w), b"get", None, &[b"key"])
            .await
            .unwrap();
    })
});
