#![no_main]

use libfuzzer_sys::fuzz_target;
use smol::{block_on, io::Cursor};

use mcmc_rs::{
    LruCrawlerCrawlArg, LruCrawlerMetadumpArg, LruCrawlerMgdumpArg, PipelineExpect, auth_cmd,
    delete_cmd, execute_cmd, flush_all_cmd, incr_decr_cmd, lru_crawler_crawl_cmd,
    lru_crawler_metadump_cmd, lru_crawler_mgdump_cmd, ma_cmd, md_cmd, me_cmd, mg_cmd, mn_cmd,
    ms_cmd, retrieval_cmd, slabs_reassign_cmd, stats_cmd, storage_cmd, touch_cmd, version_cmd,
};

fuzz_target!(|data: &[u8]| {
    let Some((&tag, payload)) = data.split_first() else {
        return;
    };

    block_on(async {
        let mut c = Cursor::new(payload.to_owned());
        match tag % 18 {
            0 => {
                let _ = retrieval_cmd(&mut c, b"get", None, &[b"key"]).await;
            }
            1 => {
                let _ = storage_cmd(&mut c, b"set", b"key", 0, 0, None, false, b"v").await;
            }
            2 => {
                let _ = delete_cmd(&mut c, b"key", false).await;
            }
            3 => {
                let _ = incr_decr_cmd(&mut c, b"incr", b"key", 1, false).await;
            }
            4 => {
                let _ = touch_cmd(&mut c, b"key", 0, false).await;
            }
            5 => {
                let _ = version_cmd(&mut c).await;
            }
            6 => {
                let _ = flush_all_cmd(&mut c, None, false).await;
            }
            7 => {
                let _ = auth_cmd(&mut c, b"user", b"pass").await;
            }
            8 => {
                let _ = stats_cmd(&mut c, None).await;
            }
            9 => {
                let _ = mg_cmd(&mut c, b"key", &[]).await;
            }
            10 => {
                let _ = ms_cmd(&mut c, b"key", &[], b"v").await;
            }
            11 => {
                let _ = md_cmd(&mut c, b"key", &[]).await;
            }
            12 => {
                let _ = ma_cmd(&mut c, b"key", &[]).await;
            }
            13 => {
                let _ = me_cmd(&mut c, b"key").await;
            }
            14 => {
                let _ = mn_cmd(&mut c).await;
            }
            15 => {
                let _ = lru_crawler_crawl_cmd(&mut c, LruCrawlerCrawlArg::All).await;
            }
            16 => {
                let _ = slabs_reassign_cmd(&mut c, 1, 2).await;
            }
            _ => {
                let _ = lru_crawler_metadump_cmd(&mut c, LruCrawlerMetadumpArg::All).await;
                let _ = lru_crawler_mgdump_cmd(
                    &mut Cursor::new(payload.to_owned()),
                    LruCrawlerMgdumpArg::All,
                )
                .await;
                let _ = execute_cmd(
                    &mut Cursor::new(payload.to_owned()),
                    &[
                        (b"version\r\n".to_vec(), PipelineExpect::Version),
                        (b"get key\r\n".to_vec(), PipelineExpect::RetrievalOne),
                        (b"mg key v\r\n".to_vec(), PipelineExpect::MetaGet),
                    ],
                )
                .await;
                let _ = retrieval_cmd(
                    &mut Cursor::new(b"END\r\n".to_vec()),
                    b"get",
                    None,
                    &[payload],
                )
                .await;
            }
        }
    });
});
