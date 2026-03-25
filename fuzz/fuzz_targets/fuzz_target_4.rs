#![no_main]

use std::io::Write;

use libfuzzer_sys::fuzz_target;
use smol::block_on;
use smol::io::Cursor;

use mcmc_rs::incr_decr_cmd;

fuzz_target!(|data: u64| {
    // fuzzed code goes here
    block_on(async {
        // incr key 1\r\n2\r\n
        let mut w = Vec::new();
        write!(&mut w, "incr key {data}\r\n{data}\r\n").unwrap();

        incr_decr_cmd(&mut Cursor::new(w), b"incr", b"key", data, false)
            .await
            .unwrap();
    })
});
