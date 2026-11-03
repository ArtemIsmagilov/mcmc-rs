//! Minimal Rust client for Memcached
//!
//! This crate provides working with memcached server.
//! All methods implemented.
//! Available TCP/Unix/UDP/TLS connections.
//!
//! - [Connection] is a Enum that represents a
//!   connection to memcached server.
//! - [Pipeline] is a structure that represents a
//!   pipeline of memcached commands.
//! - [WatchStream] is a structure that represents a
//!   stream of watch events.
//! - [ClientCrc32] is a structure that represents a
//!   Cluster connections with ModN hashing.
//! - [ClientHashRing] is a structure that represents a
//!   Cluster connections with Ring hashing.
//! - [ClientRendezvous] is a structure that represents a
//!   Cluster connections with Rendezvous hashing.
//!
//! # Examples
//!
//! ```
//! use smol::block_on;
//!
//! use mcmc_rs::{Connection, StorageResult};
//!
//! fn main() -> mcmc_rs::Result<()> {
//!     block_on(async {
//!         let mut conn = Connection::default().await?;
//!         assert_eq!(
//!             conn.set(b"doc_main", 0, 0, false, b"value").await?,
//!             StorageResult::Stored
//!         );
//!         let item = conn.get(b"doc_main").await?.unwrap();
//!         assert_eq!(item.data_block, b"value");
//!         Ok(())
//!     })
//! }
//! ```

use std::collections::HashMap;
use std::io::Write;

use async_native_tls::{Certificate, TlsConnector, TlsStream};
use crc32fast::hash as crc32;
use hashring::HashRing;
use hrw_hash::HrwNodes;

#[cfg(all(feature = "smol-runtime", feature = "tokio-runtime"))]
compile_error!(
    "feature \"smol-runtime\" and feature \"tokio-runtime\" cannot be enabled at the same time"
);
#[cfg(feature = "smol-runtime")]
mod rt {
    pub use smol::fs;
    pub use smol::io::{self, BufReader, Cursor};
    #[cfg(unix)]
    pub use smol::net::unix::UnixStream;
    pub use smol::net::{TcpStream, UdpSocket};
    pub use smol::prelude::*;
}
#[cfg(feature = "tokio-runtime")]
mod rt {
    pub use std::io::Cursor;
    pub use tokio::fs;
    pub use tokio::io::{
        self, AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader,
    };
    #[cfg(unix)]
    pub use tokio::net::UnixStream;
    pub use tokio::net::{TcpStream, UdpSocket};
}
use rt::*;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("I/O error: {0}")]
    IoError(#[from] io::Error),

    #[error("Client sent a nonexistent command name. Error.")]
    Error,

    #[error("The input doesn't conform to the protocol. Client error: {0}")]
    ClientError(String),

    #[error("Internal server error: {0}")]
    ServerError(String),

    #[error("Unexpected error: {0}")]
    UnexpectedError(String),
}

pub type Result<T> = std::result::Result<T, Error>;

fn check_error(line: String) -> Error {
    if line == "ERROR\r\n" {
        return Error::Error;
    };
    if line.starts_with("CLIENT_ERROR") {
        return Error::ClientError(
            line.trim()
                .strip_prefix("CLIENT_ERROR ")
                .unwrap()
                .to_string(),
        );
    }
    if line.starts_with("SERVER_ERROR") {
        return Error::ServerError(
            line.trim()
                .strip_prefix("SERVER_ERROR ")
                .unwrap()
                .to_string(),
        );
    };
    Error::UnexpectedError(line)
}

#[derive(Debug, PartialEq)]
pub enum StorageResult {
    Stored,
    NotStored,
    Exists,
    NotFound,
}

#[derive(Debug, PartialEq)]
pub enum DeleteResult {
    Deleted,
    NotFound,
}

#[derive(Debug, PartialEq)]
pub enum TouchResult {
    Touched,
    NotFound,
}

#[derive(Debug, PartialEq)]
pub enum LruCrawlerResult {
    Ok,
    Busy(String),
    BadClass(String),
}

#[derive(Debug, PartialEq)]
pub enum LruCrawlerDumpResult {
    Ok(Vec<String>),
    Busy(String),
    BadClass(String),
}

#[derive(Debug, PartialEq)]
pub enum SlabsReassignResult {
    Ok,
    Busy(String),
    BadClass(String),
    NoSpare(String),
    NotFull(String),
    UnSafe(String),
    Same(String),
}

#[derive(Debug, PartialEq)]
pub enum MgStatus {
    WithBody,
    WithoutBody,
    NotFound,
}

#[derive(Debug, PartialEq)]
pub enum MdStatus {
    Deleted,
    NotFound,
    Exists,
}

#[derive(Debug, PartialEq)]
pub enum MaStatus {
    WithBody,
    WithoutBody,
    NotFound,
    NotStored,
    Exists,
}

pub enum StatsArg {
    Settings,
    Items,
    Sizes,
    Slabs,
    Conns,
}

pub enum SlabsAutomoveArg {
    Zero,
    One,
    Two,
}

pub enum LruCrawlerArg {
    Enable,
    Disable,
}

pub enum LruCrawlerCrawlArg<'a> {
    Classids(&'a [usize]),
    All,
}

pub enum LruCrawlerMetadumpArg<'a> {
    Classids(&'a [usize]),
    All,
    Hash,
}

pub enum LruCrawlerMgdumpArg<'a> {
    Classids(&'a [usize]),
    All,
    Hash,
}

pub enum WatchArg {
    Fetchers,
    Mutations,
    Evictions,
    Connevents,
    Proxyreqs,
    Proxyevents,
    Proxyuser,
    Deletions,
}

pub enum LruMode {
    Flat,
    Segmented,
}

pub enum LruArg {
    Tune {
        percent_hot: u8,
        percent_warm: u8,
        max_hot_factor: f32,
        max_warm_factor: f32,
    },
    Mode(LruMode),
    TempTtl(i64),
}

pub enum VerbosityArg {
    Off,
    Error,
    Info,
    Debug,
}

#[derive(Debug, PartialEq)]
pub struct Item {
    pub key: String,
    pub flags: u32,
    pub cas_unique: Option<u64>,
    pub data_block: Vec<u8>,
}

#[derive(Debug, PartialEq)]
pub enum PipelineResponse {
    OptionItem(Option<Item>),
    VecItem(Vec<Item>),
    String(String),
    OptionString(Option<String>),
    VecString(Vec<String>),
    Unit(()),
    Value(Option<u64>),
    HashMap(HashMap<String, String>),
    MetaGet(MgItem),
    MetaSet(MsItem),
    MetaDelete(MdItem),
    MetaArithmetic(MaItem),
    StorageResult(StorageResult),
    DeleteResult(DeleteResult),
    TouchResult(TouchResult),
    LruCrawlerResult(LruCrawlerResult),
    LruCrawlerDumpResult(LruCrawlerDumpResult),
    SlabsReassignResult(SlabsReassignResult),
}

pub enum MsMode {
    Add,
    Append,
    Prepend,
    Replace,
    Set,
}

pub enum MaMode {
    Incr,
    Decr,
}

pub enum MsFlag {
    Base64Key,
    ReturnCas,
    CompareCas(u64),
    NewCas(u64),
    SetFlags(u32),
    Invalidate,
    ReturnKey,
    Opaque(String),
    ReturnSize,
    Ttl(i64),
    Mode(MsMode),
    Autovivify(i64),
    ProxyHint(String),
    LocationHint(String),
}

pub enum MgFlag {
    Base64Key,
    ReturnCas,
    CheckCas(u64),
    ReturnFlags,
    ReturnHit,
    ReturnKey,
    ReturnLastAccess,
    Opaque(String),
    ReturnSize,
    ReturnTtl,
    UnBump,
    ReturnValue,
    NewCas(u64),
    Autovivify(i64),
    RecacheTtl(i64),
    UpdateTtl(i64),
    ProxyHint(String),
    LocationHint(String),
}

pub enum MdFlag {
    Base64Key,
    CompareCas(u64),
    NewCas(u64),
    Invalidate,
    ReturnKey,
    Opaque(String),
    UpdateTtl(i64),
    LeaveKey,
    ProxyHint(String),
    LocationHint(String),
}

pub enum MaFlag {
    Base64Key,
    CompareCas(u64),
    NewCas(u64),
    AutoCreate(i64),
    InitValue(u64),
    DeltaApply(u64),
    UpdateTtl(i64),
    Mode(MaMode),
    Opaque(String),
    ReturnTtl,
    ReturnCas,
    ReturnValue,
    ReturnKey,
    ProxyHint(String),
    LocationHint(String),
}

#[derive(Debug, PartialEq)]
pub struct MgItem {
    pub status: MgStatus,
    pub base64_key: bool,
    pub cas: Option<u64>,
    pub flags: Option<u32>,
    pub hit: Option<u8>,
    pub key: Option<String>,
    pub last_access_ttl: Option<i64>,
    pub opaque: Option<String>,
    pub size: Option<usize>,
    pub ttl: Option<i64>,
    pub data_block: Option<Vec<u8>>,
    pub won_recache: bool,
    pub stale: bool,
    pub already_win: bool,
}

#[derive(Debug, PartialEq)]
pub struct MsItem {
    pub status: StorageResult,
    pub cas: Option<u64>,
    pub key: Option<String>,
    pub opaque: Option<String>,
    pub size: Option<usize>,
    pub base64_key: bool,
}

#[derive(Debug, PartialEq)]
pub struct MdItem {
    pub status: MdStatus,
    pub key: Option<String>,
    pub opaque: Option<String>,
    pub base64_key: bool,
}

#[derive(Debug, PartialEq)]
pub struct MaItem {
    pub status: MaStatus,
    pub opaque: Option<String>,
    pub ttl: Option<i64>,
    pub cas: Option<u64>,
    pub number: Option<u64>,
    pub key: Option<String>,
    pub base64_key: bool,
}

async fn parse_storage_rp<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    noreply: bool,
) -> Result<StorageResult> {
    if noreply {
        return Ok(StorageResult::Stored);
    }
    let mut line = String::new();
    s.read_line(&mut line).await?;
    match line.as_str() {
        "STORED\r\n" => Ok(StorageResult::Stored),
        "NOT_STORED\r\n" => Ok(StorageResult::NotStored),
        "EXISTS\r\n" => Ok(StorageResult::Exists),
        "NOT_FOUND\r\n" => Ok(StorageResult::NotFound),
        _ => Err(check_error(line)),
    }
}

async fn parse_retrieval_rp<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> Result<Vec<Item>> {
    let mut line = String::new();
    let mut items = Vec::new();
    while s.read_line(&mut line).await? > 0 && line.starts_with("VALUE") {
        let mut split = line.split(' ');
        split.next();
        let (key, flags, bytes, cas_unique) = (
            split.next().unwrap().to_string(),
            split.next().unwrap().parse().unwrap(),
            split.next().unwrap().trim_end().parse().unwrap(),
            split.next().map(|x| x.trim_end().parse().unwrap()),
        );
        let mut data_block = vec![0; bytes + 2];
        s.read_exact(&mut data_block).await?;
        data_block.truncate(bytes);
        items.push(Item {
            key,
            flags,
            cas_unique,
            data_block,
        });
        line.clear();
    }
    match line.as_str() {
        "END\r\n" => Ok(items),
        _ => Err(check_error(line)),
    }
}

async fn parse_version_rp<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> Result<String> {
    let mut line = String::new();
    let n = s.read_line(&mut line).await?;
    if line.starts_with("VERSION") {
        Ok(line[8..n - 2].to_string())
    } else {
        Err(check_error(line))
    }
}

async fn parse_ok_rp<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S, noreply: bool) -> Result<()> {
    if noreply {
        return Ok(());
    }
    let mut line = String::new();
    s.read_line(&mut line).await?;
    match line.as_str() {
        "OK\r\n" => Ok(()),
        _ => Err(check_error(line)),
    }
}

async fn parse_delete_rp<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    noreply: bool,
) -> Result<DeleteResult> {
    if noreply {
        return Ok(DeleteResult::Deleted);
    }
    let mut line = String::new();
    s.read_line(&mut line).await?;
    match line.as_str() {
        "DELETED\r\n" => Ok(DeleteResult::Deleted),
        "NOT_FOUND\r\n" => Ok(DeleteResult::NotFound),
        _ => Err(check_error(line)),
    }
}

async fn parse_auth_rp<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> Result<()> {
    let mut line = String::new();
    s.read_line(&mut line).await?;
    match line.as_str() {
        "STORED\r\n" => Ok(()),
        _ => Err(check_error(line)),
    }
}

async fn parse_incr_decr_rp<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    noreply: bool,
) -> Result<Option<u64>> {
    if noreply {
        return Ok(None);
    }
    let mut line = String::new();
    s.read_line(&mut line).await?;
    if line == "NOT_FOUND\r\n" {
        return Ok(None);
    }
    match line.trim_end().parse() {
        Ok(v) => Ok(Some(v)),
        Err(_) => Err(check_error(line)),
    }
}

async fn parse_touch_rp<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    noreply: bool,
) -> Result<TouchResult> {
    if noreply {
        return Ok(TouchResult::Touched);
    }
    let mut line = String::new();
    s.read_line(&mut line).await?;
    match line.as_str() {
        "TOUCHED\r\n" => Ok(TouchResult::Touched),
        "NOT_FOUND\r\n" => Ok(TouchResult::NotFound),
        _ => Err(check_error(line)),
    }
}

async fn parse_stats_rp<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
) -> Result<HashMap<String, String>> {
    let mut items = HashMap::new();
    let mut data = String::new();
    while s.read_line(&mut data).await? > 0 && data.starts_with("STAT") {
        let mut split = data.split(' ');
        split.next();
        let (k, v) = (
            split.next().unwrap().to_string(),
            split.next().unwrap().trim_end().to_string(),
        );
        items.insert(k, v);
        data.clear();
    }
    match data.as_str() {
        "END\r\n" => Ok(items),
        _ => Err(check_error(data)),
    }
}

async fn parse_lru_crawler_rp<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
) -> Result<LruCrawlerResult> {
    let mut line = String::new();
    s.read_line(&mut line).await?;
    if line == "OK\r\n" {
        return Ok(LruCrawlerResult::Ok);
    }
    if line.starts_with("BUSY ") {
        return Ok(LruCrawlerResult::Busy(
            line.trim().strip_prefix("BUSY ").unwrap().to_string(),
        ));
    }
    if line.starts_with("BADCLASS ") {
        return Ok(LruCrawlerResult::BadClass(
            line.trim().strip_prefix("BADCLASS ").unwrap().to_string(),
        ));
    }
    Err(check_error(line))
}

async fn parse_lru_crawler_metadump_rp<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
) -> Result<LruCrawlerDumpResult> {
    let mut line = String::new();
    let mut items = Vec::new();
    s.read_line(&mut line).await?;
    if line.starts_with("BUSY ") {
        return Ok(LruCrawlerDumpResult::Busy(
            line.trim().strip_prefix("BUSY ").unwrap().to_string(),
        ));
    }
    if line.starts_with("BADCLASS ") {
        return Ok(LruCrawlerDumpResult::BadClass(
            line.trim().strip_prefix("BADCLASS ").unwrap().to_string(),
        ));
    }
    if line == "OK\r\n" {
        while {
            line.clear();
            s.read_line(&mut line).await? > 0
        } && line.starts_with("key=")
        {
            items.push(line.trim_end().to_string())
        }
    }
    match line.as_str() {
        "END\r\n" => Ok(LruCrawlerDumpResult::Ok(items)),
        _ => Err(check_error(line)),
    }
}

async fn parse_lru_crawler_mgdump_rp<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
) -> Result<LruCrawlerDumpResult> {
    let mut line = String::new();
    let mut items = Vec::new();
    s.read_line(&mut line).await?;
    if line.starts_with("BUSY ") {
        return Ok(LruCrawlerDumpResult::Busy(
            line.trim().strip_prefix("BUSY ").unwrap().to_string(),
        ));
    }
    if line.starts_with("BADCLASS ") {
        return Ok(LruCrawlerDumpResult::BadClass(
            line.trim().strip_prefix("BADCLASS ").unwrap().to_string(),
        ));
    }
    if line == "OK\r\n" {
        while {
            line.clear();
            s.read_line(&mut line).await? > 0
        } && line.starts_with("mg ")
        {
            items.push(line.to_string())
        }
    }
    match line.as_str() {
        "EN\r\n" => Ok(LruCrawlerDumpResult::Ok(items)),
        _ => Err(check_error(line)),
    }
}

async fn parse_mn_rp<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> Result<()> {
    let mut line = String::new();
    s.read_line(&mut line).await?;
    match line.as_str() {
        "MN\r\n" => Ok(()),
        _ => Err(check_error(line)),
    }
}

async fn parse_me_rp<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> Result<Option<String>> {
    let mut line = String::new();
    let n = s.read_line(&mut line).await?;
    match line.as_str() {
        "EN\r\n" => Ok(None),
        _ if line.starts_with("ME") => Ok(Some(line[3..n - 2].to_string())),
        _ => Err(check_error(line)),
    }
}

async fn parse_mg_rp<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> Result<MgItem> {
    let mut line = String::new();
    s.read_line(&mut line).await?;
    let status;
    let (
        mut base64_key,
        mut cas,
        mut flags,
        mut hit,
        mut key,
        mut last_access_ttl,
        mut opaque,
        mut size,
        mut ttl,
        mut data_block,
        mut won_recache,
        mut stale,
        mut already_win,
    ) = (
        false, None, None, None, None, None, None, None, None, None, false, false, false,
    );
    let mut split = line.trim_end().split(' ');
    let data_len = if line.starts_with("VA") {
        status = MgStatus::WithBody;
        split.next();
        Some(split.next().unwrap().parse().unwrap())
    } else if line.starts_with("HD") {
        status = MgStatus::WithoutBody;
        split.next();
        None
    } else if line.starts_with("EN") {
        status = MgStatus::NotFound;
        split.next();
        None
    } else {
        return Err(check_error(line));
    };
    for flag in split {
        let f = &flag[1..];
        match &flag[..1] {
            "b" => base64_key = true,
            "c" => cas = Some(f.parse().unwrap()),
            "f" => flags = Some(f.parse().unwrap()),
            "h" => hit = Some(f.parse().unwrap()),
            "k" => key = Some(f.to_string()),
            "l" => last_access_ttl = Some(f.parse().unwrap()),
            "O" => opaque = Some(f.to_string()),
            "s" => size = Some(f.parse().unwrap()),
            "t" => ttl = Some(f.parse().unwrap()),
            "W" => won_recache = true,
            "X" => stale = true,
            "Z" => already_win = true,
            other => unreachable!("unexpected mg flag: {other}"),
        }
    }
    if let Some(a) = data_len {
        let mut buf = vec![0; a + 2];
        s.read_exact(&mut buf).await?;
        buf.truncate(a);
        data_block = Some(buf);
    }
    Ok(MgItem {
        status,
        base64_key,
        cas,
        flags,
        hit,
        key,
        last_access_ttl,
        opaque,
        size,
        ttl,
        data_block,
        won_recache,
        stale,
        already_win,
    })
}

async fn parse_ms_rp<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> Result<MsItem> {
    let mut line = String::new();
    s.read_line(&mut line).await?;
    let (mut cas, mut key, mut opaque, mut size, mut base64_key) = (None, None, None, None, false);
    let status = if line.starts_with("HD") {
        StorageResult::Stored
    } else if line.starts_with("NS") {
        StorageResult::NotStored
    } else if line.starts_with("EX") {
        StorageResult::Exists
    } else if line.starts_with("NF") {
        StorageResult::NotFound
    } else {
        return Err(check_error(line));
    };
    let mut split = line.trim_end().split(' ');
    split.next();
    for flag in split {
        let f = &flag[1..];
        match &flag[..1] {
            "c" => cas = Some(f.parse().unwrap()),
            "k" => key = Some(f.to_string()),
            "O" => opaque = Some(f.to_string()),
            "s" => size = Some(f.parse().unwrap()),
            "b" => base64_key = true,
            other => unreachable!("unexpected ms flag: {other}"),
        }
    }
    Ok(MsItem {
        status,
        cas,
        opaque,
        key,
        size,
        base64_key,
    })
}

async fn parse_md_rp<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> Result<MdItem> {
    let mut line = String::new();
    s.read_line(&mut line).await?;
    let (mut key, mut opaque, mut base64_key) = (None, None, false);
    let status = if line.starts_with("HD") {
        MdStatus::Deleted
    } else if line.starts_with("NF") {
        MdStatus::NotFound
    } else if line.starts_with("EX") {
        MdStatus::Exists
    } else {
        return Err(check_error(line));
    };
    let mut split = line.trim_end().split(' ');
    split.next();
    for flag in split {
        let f = &flag[1..];
        match &flag[..1] {
            "k" => key = Some(f.to_string()),
            "O" => opaque = Some(f.to_string()),
            "b" => base64_key = true,
            other => unreachable!("unexpected md flag: {other}"),
        }
    }
    Ok(MdItem {
        status,
        key,
        opaque,
        base64_key,
    })
}

async fn parse_ma_rp<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> Result<MaItem> {
    let mut line = String::new();
    s.read_line(&mut line).await?;
    let status;
    let (mut opaque, mut ttl, mut cas, mut number, mut key, mut base64_key) =
        (None, None, None, None, None, false);
    let mut split = line.trim_end().split(' ');
    let data_len = if line.starts_with("VA") {
        split.next();
        status = MaStatus::WithBody;
        Some(split.next().unwrap().parse().unwrap())
    } else if line.starts_with("HD") {
        split.next();
        status = MaStatus::WithoutBody;
        None
    } else if line.starts_with("NS") {
        split.next();
        status = MaStatus::NotStored;
        None
    } else if line.starts_with("EX") {
        split.next();
        status = MaStatus::Exists;
        None
    } else if line.starts_with("NF") {
        split.next();
        status = MaStatus::NotFound;
        None
    } else {
        return Err(check_error(line));
    };
    for flag in split {
        let f = &flag[1..];
        match &flag[..1] {
            "O" => opaque = Some(f.to_string()),
            "t" => ttl = Some(f.parse().unwrap()),
            "c" => cas = Some(f.parse().unwrap()),
            "k" => key = Some(f.to_string()),
            "b" => base64_key = true,
            other => unreachable!("unexpected ma flag: {other}"),
        }
    }
    if let Some(a) = data_len {
        let mut buf = String::with_capacity(a + 2);
        s.read_line(&mut buf).await?;
        buf.truncate(a);
        number = Some(buf.parse().unwrap());
    }
    Ok(MaItem {
        status,
        opaque,
        ttl,
        cas,
        number,
        key,
        base64_key,
    })
}

async fn parse_slabs_reassign_rp<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
) -> Result<SlabsReassignResult> {
    let mut line = String::new();
    s.read_line(&mut line).await?;
    if line == "OK\r\n" {
        return Ok(SlabsReassignResult::Ok);
    }
    if line.starts_with("BUSY ") {
        return Ok(SlabsReassignResult::Busy(
            line.trim().strip_prefix("BUSY ").unwrap().to_string(),
        ));
    }
    if line.starts_with("BADCLASS ") {
        return Ok(SlabsReassignResult::BadClass(
            line.trim().strip_prefix("BADCLASS ").unwrap().to_string(),
        ));
    }
    if line.starts_with("NOSPARE ") {
        return Ok(SlabsReassignResult::NoSpare(
            line.trim().strip_prefix("NOSPARE ").unwrap().to_string(),
        ));
    }
    if line.starts_with("NOTFULL ") {
        return Ok(SlabsReassignResult::NotFull(
            line.trim().strip_prefix("NOTFULL ").unwrap().to_string(),
        ));
    }
    if line.starts_with("UNSAFE ") {
        return Ok(SlabsReassignResult::UnSafe(
            line.trim().strip_prefix("UNSAFE ").unwrap().to_string(),
        ));
    }
    if line.starts_with("SAME ") {
        return Ok(SlabsReassignResult::Same(
            line.trim().strip_prefix("SAME ").unwrap().to_string(),
        ));
    }
    Err(check_error(line))
}

fn build_storage_cmd(
    command_name: &[u8],
    key: &[u8],
    flags: u32,
    exptime: i64,
    cas_unique: Option<u64>,
    noreply: bool,
    data_block: &[u8],
) -> Vec<u8> {
    let mut w = Vec::from(command_name);
    w.push(b' ');
    w.extend(key);
    w.push(b' ');
    write!(&mut w, "{flags} {exptime} {}", data_block.len()).unwrap();
    if let Some(x) = cas_unique {
        write!(&mut w, " {x}").unwrap()
    }
    if noreply {
        w.extend(b" noreply")
    }
    w.extend(b"\r\n");
    w.extend(data_block);
    w.extend(b"\r\n");
    w
}

fn build_retrieval_cmd(command_name: &[u8], exptime: Option<i64>, keys: &[&[u8]]) -> Vec<u8> {
    let mut w = Vec::from(command_name);
    if let Some(x) = exptime {
        write!(&mut w, " {x}").unwrap()
    }
    keys.iter().for_each(|&x| {
        w.push(b' ');
        w.extend(x)
    });
    w.extend(b"\r\n");
    w
}

fn build_version_cmd() -> &'static [u8] {
    b"version\r\n"
}

fn build_quit_cmd() -> &'static [u8] {
    b"quit\r\n"
}

fn build_shutdown_cmd(graceful: bool) -> &'static [u8] {
    if graceful {
        b"shutdown graceful\r\n"
    } else {
        b"shutdown\r\n"
    }
}

fn build_cache_memlimit_cmd(limit: usize, noreply: bool) -> Vec<u8> {
    format!(
        "cache_memlimit {limit}{}\r\n",
        if noreply { " noreply" } else { "" }
    )
    .into_bytes()
}

fn build_flush_all_cmd(exptime: Option<i64>, noreply: bool) -> Vec<u8> {
    let mut w = Vec::from(b"flush_all");
    if let Some(x) = exptime {
        write!(&mut w, " {x}").unwrap()
    }
    if noreply {
        w.extend(b" noreply")
    }
    w.extend(b"\r\n");
    w
}

fn build_delete_cmd(key: &[u8], noreply: bool) -> Vec<u8> {
    let mut w = Vec::from(b"delete ");
    w.extend(key);
    if noreply {
        w.extend(b" noreply")
    }
    w.extend(b"\r\n");
    w
}

fn build_auth_cmd(username: &[u8], password: &[u8]) -> Vec<u8> {
    let mut w = Vec::new();
    write!(
        &mut w,
        "set _ _ _ {}\r\n",
        username.len() + password.len() + 1
    )
    .unwrap();
    w.extend(username);
    w.push(b' ');
    w.extend(password);
    w.extend(b"\r\n");
    w
}

fn build_incr_decr_cmd(command_name: &[u8], key: &[u8], value: u64, noreply: bool) -> Vec<u8> {
    let mut w = Vec::from(command_name);
    w.push(b' ');
    w.extend(key);
    write!(
        &mut w,
        " {value}{}\r\n",
        if noreply { " noreply" } else { "" }
    )
    .unwrap();
    w
}

fn build_touch_cmd(key: &[u8], exptime: i64, noreply: bool) -> Vec<u8> {
    let mut w = Vec::from(b"touch ");
    w.extend(key);
    write!(
        &mut w,
        " {exptime}{}\r\n",
        if noreply { " noreply" } else { "" }
    )
    .unwrap();
    w
}

fn build_stats_cmd(arg: Option<StatsArg>) -> &'static [u8] {
    match arg {
        Some(a) => match a {
            StatsArg::Settings => b"stats settings\r\n",
            StatsArg::Items => b"stats items\r\n",
            StatsArg::Sizes => b"stats sizes\r\n",
            StatsArg::Slabs => b"stats slabs\r\n",
            StatsArg::Conns => b"stats conns\r\n",
        },
        None => b"stats\r\n",
    }
}

fn build_slabs_automove_cmd(arg: SlabsAutomoveArg) -> &'static [u8] {
    match arg {
        SlabsAutomoveArg::Zero => b"slabs automove 0\r\n",
        SlabsAutomoveArg::One => b"slabs automove 1\r\n",
        SlabsAutomoveArg::Two => b"slabs automove 2\r\n",
    }
}

fn build_lru_crawler_cmd(arg: LruCrawlerArg) -> &'static [u8] {
    match arg {
        LruCrawlerArg::Enable => b"lru_crawler enable\r\n",
        LruCrawlerArg::Disable => b"lru_crawler disable\r\n",
    }
}

fn build_lru_clawler_sleep_cmd(microseconds: usize) -> Vec<u8> {
    format!("lru_crawler sleep {microseconds}\r\n").into_bytes()
}

fn build_lru_crawler_tocrawl_cmd(arg: u32) -> Vec<u8> {
    format!("lru_crawler tocrawl {arg}\r\n").into_bytes()
}

fn build_lru_clawler_crawl_cmd(arg: LruCrawlerCrawlArg) -> Vec<u8> {
    let mut w = Vec::from(b"lru_crawler crawl ");
    match arg {
        LruCrawlerCrawlArg::Classids(ids) => ids.iter().enumerate().for_each(|(index, id)| {
            if index == 0 {
                write!(&mut w, "{}", id).unwrap()
            } else {
                write!(&mut w, ",{}", id).unwrap()
            }
        }),
        LruCrawlerCrawlArg::All => w.extend(b"all"),
    }
    w.extend(b"\r\n");
    w
}

fn build_slabs_reassign_cmd(source_class: isize, dest_class: isize) -> Vec<u8> {
    format!("slabs reassign {source_class} {dest_class}\r\n").into_bytes()
}

fn build_lru_clawler_metadump_cmd(arg: LruCrawlerMetadumpArg) -> Vec<u8> {
    let mut w = Vec::from(b"lru_crawler metadump ");
    match arg {
        LruCrawlerMetadumpArg::Classids(ids) => ids.iter().enumerate().for_each(|(index, id)| {
            if index == 0 {
                write!(&mut w, "{}", id).unwrap()
            } else {
                write!(&mut w, ",{}", id).unwrap()
            }
        }),
        LruCrawlerMetadumpArg::All => w.extend(b"all"),
        LruCrawlerMetadumpArg::Hash => w.extend(b"hash"),
    }
    w.extend(b"\r\n");
    w
}

fn build_lru_clawler_mgdump_cmd(arg: LruCrawlerMgdumpArg) -> Vec<u8> {
    let mut w = Vec::from(b"lru_crawler mgdump ");
    match arg {
        LruCrawlerMgdumpArg::Classids(ids) => ids.iter().enumerate().for_each(|(index, id)| {
            if index == 0 {
                write!(&mut w, "{}", id).unwrap()
            } else {
                write!(&mut w, ",{}", id).unwrap()
            }
        }),
        LruCrawlerMgdumpArg::All => w.extend(b"all"),
        LruCrawlerMgdumpArg::Hash => w.extend(b"hash"),
    }
    w.extend(b"\r\n");
    w
}

fn build_mn_cmd() -> &'static [u8] {
    b"mn\r\n"
}

fn build_me_cmd(key: &[u8]) -> Vec<u8> {
    let mut w = Vec::from(b"me ");
    w.extend(key);
    w.extend(b"\r\n");
    w
}

fn build_watch_cmd(arg: &[WatchArg]) -> Vec<u8> {
    let mut w = Vec::from(b"watch");
    arg.iter().for_each(|a| {
        w.extend(match a {
            WatchArg::Fetchers => b" fetchers".as_slice(),
            WatchArg::Mutations => b" mutations",
            WatchArg::Evictions => b" evictions",
            WatchArg::Connevents => b" connevents",
            WatchArg::Proxyreqs => b" proxyreqs",
            WatchArg::Proxyevents => b" proxyevents",
            WatchArg::Proxyuser => b" proxyuser",
            WatchArg::Deletions => b" deletions",
        })
    });
    w.extend(b"\r\n");
    w
}

fn build_mc_cmd(
    command_name: &[u8],
    key: &[u8],
    flags: &[u8],
    data_block: Option<&[u8]>,
) -> Vec<u8> {
    let mut w = Vec::from(command_name);
    w.push(b' ');
    w.extend(key);
    if let Some(x) = data_block {
        write!(&mut w, " {}", x.len()).unwrap();
        w.extend(flags);
        w.extend(b"\r\n");
        w.extend(x);
        w.extend(b"\r\n");
    } else {
        w.extend(flags);
        w.extend(b"\r\n");
    }
    w
}

fn build_ms_flags(flags: &[MsFlag]) -> Vec<u8> {
    let mut w = Vec::new();
    flags.iter().for_each(|x| match x {
        MsFlag::Base64Key => w.extend(b" b"),
        MsFlag::ReturnCas => w.extend(b" c"),
        MsFlag::CompareCas(token) => write!(&mut w, " C{token}").unwrap(),
        MsFlag::NewCas(token) => write!(&mut w, " E{token}").unwrap(),
        MsFlag::SetFlags(token) => write!(&mut w, " F{token}").unwrap(),
        MsFlag::Invalidate => w.extend(b" I"),
        MsFlag::ReturnKey => w.extend(b" k"),
        MsFlag::Opaque(token) => write!(&mut w, " O{token}").unwrap(),
        MsFlag::ReturnSize => w.extend(b" s"),
        MsFlag::Ttl(token) => write!(&mut w, " T{token}").unwrap(),
        MsFlag::Mode(token) => match token {
            MsMode::Add => w.extend(b" ME"),
            MsMode::Append => w.extend(b" MA"),
            MsMode::Prepend => w.extend(b" MP"),
            MsMode::Replace => w.extend(b" MR"),
            MsMode::Set => w.extend(b" MS"),
        },
        MsFlag::Autovivify(token) => write!(&mut w, " N{token}").unwrap(),
        MsFlag::ProxyHint(token) => write!(&mut w, " P{token}").unwrap(),
        MsFlag::LocationHint(token) => write!(&mut w, " L{token}").unwrap(),
    });
    w
}

fn build_mg_flags(flags: &[MgFlag]) -> Vec<u8> {
    let mut w = Vec::new();
    flags.iter().for_each(|x| match x {
        MgFlag::Base64Key => w.extend(b" b"),
        MgFlag::ReturnCas => w.extend(b" c"),
        MgFlag::CheckCas(token) => write!(&mut w, " C{token}").unwrap(),
        MgFlag::ReturnFlags => w.extend(b" f"),
        MgFlag::ReturnHit => w.extend(b" h"),
        MgFlag::ReturnKey => w.extend(b" k"),
        MgFlag::ReturnLastAccess => w.extend(b" l"),
        MgFlag::Opaque(token) => write!(&mut w, " O{token}").unwrap(),
        MgFlag::ReturnSize => w.extend(b" s"),
        MgFlag::ReturnTtl => w.extend(b" t"),
        MgFlag::UnBump => w.extend(b" u"),
        MgFlag::ReturnValue => w.extend(b" v"),
        MgFlag::NewCas(token) => write!(&mut w, " E{token}").unwrap(),
        MgFlag::Autovivify(token) => write!(&mut w, " N{token}").unwrap(),
        MgFlag::RecacheTtl(token) => write!(&mut w, " R{token}").unwrap(),
        MgFlag::UpdateTtl(token) => write!(&mut w, " T{token}").unwrap(),
        MgFlag::ProxyHint(token) => write!(&mut w, " P{token}").unwrap(),
        MgFlag::LocationHint(token) => write!(&mut w, " L{token}").unwrap(),
    });
    w
}

fn build_md_flags(flags: &[MdFlag]) -> Vec<u8> {
    let mut w = Vec::new();
    flags.iter().for_each(|x| match x {
        MdFlag::Base64Key => w.extend(b" b"),
        MdFlag::CompareCas(token) => write!(&mut w, " C{token}").unwrap(),
        MdFlag::NewCas(token) => write!(&mut w, " E{token}").unwrap(),
        MdFlag::Invalidate => w.extend(b" I"),
        MdFlag::ReturnKey => w.extend(b" k"),
        MdFlag::Opaque(token) => write!(&mut w, " O{token}").unwrap(),
        MdFlag::UpdateTtl(token) => write!(&mut w, " T{token}").unwrap(),
        MdFlag::LeaveKey => w.extend(b" x"),
        MdFlag::ProxyHint(token) => write!(&mut w, " P{token}").unwrap(),
        MdFlag::LocationHint(token) => write!(&mut w, " L{token}").unwrap(),
    });
    w
}

fn build_ma_flags(flags: &[MaFlag]) -> Vec<u8> {
    let mut w = Vec::new();
    flags.iter().for_each(|x| match x {
        MaFlag::Base64Key => w.extend(b" b"),
        MaFlag::CompareCas(token) => write!(&mut w, " C{token}").unwrap(),
        MaFlag::NewCas(token) => write!(&mut w, " E{token}").unwrap(),
        MaFlag::AutoCreate(token) => write!(&mut w, " N{token}").unwrap(),
        MaFlag::InitValue(token) => write!(&mut w, " J{token}").unwrap(),
        MaFlag::DeltaApply(token) => write!(&mut w, " D{token}").unwrap(),
        MaFlag::UpdateTtl(token) => write!(&mut w, " T{token}").unwrap(),
        MaFlag::Mode(token) => match token {
            MaMode::Incr => w.extend(b" M+"),
            MaMode::Decr => w.extend(b" M-"),
        },
        MaFlag::Opaque(token) => write!(&mut w, " O{token}").unwrap(),
        MaFlag::ReturnTtl => w.extend(b" t"),
        MaFlag::ReturnCas => w.extend(b" c"),
        MaFlag::ReturnValue => w.extend(b" v"),
        MaFlag::ReturnKey => w.extend(b" k"),
        MaFlag::ProxyHint(token) => write!(&mut w, " P{token}").unwrap(),
        MaFlag::LocationHint(token) => write!(&mut w, " L{token}").unwrap(),
    });
    w
}

fn build_lru_cmd(arg: LruArg) -> Vec<u8> {
    let mut w = Vec::new();
    match arg {
        LruArg::Tune {
            percent_hot,
            percent_warm,
            max_hot_factor,
            max_warm_factor,
        } => write!(
            &mut w,
            "lru tune {percent_hot} {percent_warm} {max_hot_factor} {max_warm_factor}\r\n"
        )
        .unwrap(),
        LruArg::Mode(mode) => match mode {
            LruMode::Flat => w.extend(b"lru mode flat\r\n"),
            LruMode::Segmented => w.extend(b"lru mode segmented\r\n"),
        },
        LruArg::TempTtl(ttl) => write!(&mut w, "lru temp_ttl {ttl}\r\n").unwrap(),
    }
    w
}

fn build_verbosity_cmd(arg: VerbosityArg, noreply: bool) -> Vec<u8> {
    let a = match arg {
        VerbosityArg::Off => 0,
        VerbosityArg::Error => 1,
        VerbosityArg::Info => 2,
        VerbosityArg::Debug => 3,
    };
    format!("verbosity {a}{}\r\n", if noreply { " noreply" } else { "" }).into_bytes()
}

async fn udp_send_cmd(s: &mut UdpSocket, r: &mut u16, cmd: &[u8]) -> Result<()> {
    *r = r.wrapping_add(1);
    let mut msg = Vec::with_capacity(8 + cmd.len());
    msg.extend(r.to_be_bytes());
    msg.extend([0, 0, 0, 1, 0, 0]);
    msg.extend(cmd);
    s.send(&msg).await?;
    Ok(())
}

async fn udp_recv_rp(s: &mut UdpSocket, r: &u16) -> Result<Vec<u8>> {
    let mut result = HashMap::new();
    let mut buf = [0; 1400];
    loop {
        let n = s.recv(&mut buf).await?;
        if n < 8 {
            return Err(Error::UnexpectedError("Invalid UDP header".to_string()));
        }
        let request_id = u16::from_be_bytes([buf[0], buf[1]]);
        let sequence_number = u16::from_be_bytes([buf[2], buf[3]]);
        let total_number_datagrams = u16::from_be_bytes([buf[4], buf[5]]);
        if *r != request_id {
            continue;
        }
        result.insert(sequence_number, buf[8..n].to_vec());
        if total_number_datagrams == result.len() as u16 {
            break;
        }
    }
    Ok((0..result.len() as u16)
        .flat_map(|x| result.remove(&x).unwrap())
        .collect())
}

async fn version_cmd_udp(s: &mut UdpSocket, r: &mut u16) -> Result<String> {
    udp_send_cmd(s, r, build_version_cmd()).await?;
    parse_version_rp(&mut Cursor::new(udp_recv_rp(s, r).await?)).await
}

pub async fn version_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> Result<String> {
    s.write_all(build_version_cmd()).await?;
    parse_version_rp(s).await
}

async fn quit_cmd_udp(s: &mut UdpSocket, r: &mut u16) -> Result<()> {
    udp_send_cmd(s, r, build_quit_cmd()).await
}

async fn quit_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> Result<()> {
    Ok(s.write_all(build_quit_cmd()).await?)
}

async fn shutdown_cmd_udp(s: &mut UdpSocket, r: &mut u16, graceful: bool) -> Result<()> {
    udp_send_cmd(s, r, build_shutdown_cmd(graceful)).await
}

async fn shutdown_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    graceful: bool,
) -> Result<()> {
    Ok(s.write_all(build_shutdown_cmd(graceful)).await?)
}

async fn cache_memlimit_cmd_udp(
    s: &mut UdpSocket,
    r: &mut u16,
    limit: usize,
    noreply: bool,
) -> Result<()> {
    udp_send_cmd(s, r, &build_cache_memlimit_cmd(limit, noreply)).await?;
    if noreply {
        Ok(())
    } else {
        parse_ok_rp(&mut Cursor::new(udp_recv_rp(s, r).await?), noreply).await
    }
}

async fn cache_memlimit_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    limit: usize,
    noreply: bool,
) -> Result<()> {
    s.write_all(&build_cache_memlimit_cmd(limit, noreply))
        .await?;
    parse_ok_rp(s, noreply).await
}

async fn flush_all_cmd_udp(
    s: &mut UdpSocket,
    r: &mut u16,
    exptime: Option<i64>,
    noreply: bool,
) -> Result<()> {
    udp_send_cmd(s, r, &build_flush_all_cmd(exptime, noreply)).await?;
    if noreply {
        Ok(())
    } else {
        parse_ok_rp(&mut Cursor::new(udp_recv_rp(s, r).await?), noreply).await
    }
}

async fn flush_all_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    exptime: Option<i64>,
    noreply: bool,
) -> Result<()> {
    s.write_all(&build_flush_all_cmd(exptime, noreply)).await?;
    parse_ok_rp(s, noreply).await
}

async fn storage_cmd_udp(
    s: &mut UdpSocket,
    r: &mut u16,
    command_name: &[u8],
    key: &[u8],
    flags: u32,
    exptime: i64,
    cas_unique: Option<u64>,
    noreply: bool,
    data_block: &[u8],
) -> Result<StorageResult> {
    udp_send_cmd(
        s,
        r,
        &build_storage_cmd(
            command_name,
            key,
            flags,
            exptime,
            cas_unique,
            noreply,
            data_block,
        ),
    )
    .await?;
    if noreply {
        Ok(StorageResult::Stored)
    } else {
        parse_storage_rp(&mut Cursor::new(udp_recv_rp(s, r).await?), noreply).await
    }
}

pub async fn storage_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    command_name: &[u8],
    key: &[u8],
    flags: u32,
    exptime: i64,
    cas_unique: Option<u64>,
    noreply: bool,
    data_block: &[u8],
) -> Result<StorageResult> {
    s.write_all(&build_storage_cmd(
        command_name,
        key,
        flags,
        exptime,
        cas_unique,
        noreply,
        data_block,
    ))
    .await?;
    parse_storage_rp(s, noreply).await
}

async fn delete_cmd_udp(
    s: &mut UdpSocket,
    r: &mut u16,
    key: &[u8],
    noreply: bool,
) -> Result<DeleteResult> {
    udp_send_cmd(s, r, &build_delete_cmd(key, noreply)).await?;
    if noreply {
        Ok(DeleteResult::Deleted)
    } else {
        parse_delete_rp(&mut Cursor::new(udp_recv_rp(s, r).await?), noreply).await
    }
}

async fn delete_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    key: &[u8],
    noreply: bool,
) -> Result<DeleteResult> {
    s.write_all(&build_delete_cmd(key, noreply)).await?;
    parse_delete_rp(s, noreply).await
}

async fn auth_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    username: &[u8],
    password: &[u8],
) -> Result<()> {
    s.write_all(&build_auth_cmd(username, password)).await?;
    parse_auth_rp(s).await
}

async fn incr_decr_cmd_udp(
    s: &mut UdpSocket,
    r: &mut u16,
    command_name: &[u8],
    key: &[u8],
    value: u64,
    noreply: bool,
) -> Result<Option<u64>> {
    udp_send_cmd(
        s,
        r,
        &build_incr_decr_cmd(command_name, key, value, noreply),
    )
    .await?;
    if noreply {
        Ok(None)
    } else {
        parse_incr_decr_rp(&mut Cursor::new(udp_recv_rp(s, r).await?), noreply).await
    }
}

pub async fn incr_decr_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    command_name: &[u8],
    key: &[u8],
    value: u64,
    noreply: bool,
) -> Result<Option<u64>> {
    s.write_all(&build_incr_decr_cmd(command_name, key, value, noreply))
        .await?;
    parse_incr_decr_rp(s, noreply).await
}

async fn touch_cmd_udp(
    s: &mut UdpSocket,
    r: &mut u16,
    key: &[u8],
    exptime: i64,
    noreply: bool,
) -> Result<TouchResult> {
    udp_send_cmd(s, r, &build_touch_cmd(key, exptime, noreply)).await?;
    if noreply {
        Ok(TouchResult::Touched)
    } else {
        parse_touch_rp(&mut Cursor::new(udp_recv_rp(s, r).await?), noreply).await
    }
}

async fn touch_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    key: &[u8],
    exptime: i64,
    noreply: bool,
) -> Result<TouchResult> {
    s.write_all(&build_touch_cmd(key, exptime, noreply)).await?;
    parse_touch_rp(s, noreply).await
}

async fn retrieval_cmd_udp(
    s: &mut UdpSocket,
    r: &mut u16,
    command_name: &[u8],
    exptime: Option<i64>,
    keys: &[&[u8]],
) -> Result<Vec<Item>> {
    udp_send_cmd(s, r, &build_retrieval_cmd(command_name, exptime, keys)).await?;
    parse_retrieval_rp(&mut Cursor::new(udp_recv_rp(s, r).await?)).await
}

pub async fn retrieval_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    command_name: &[u8],
    exptime: Option<i64>,
    keys: &[&[u8]],
) -> Result<Vec<Item>> {
    s.write_all(&build_retrieval_cmd(command_name, exptime, keys))
        .await?;
    parse_retrieval_rp(s).await
}

async fn stats_cmd_udp(
    s: &mut UdpSocket,
    r: &mut u16,
    arg: Option<StatsArg>,
) -> Result<HashMap<String, String>> {
    udp_send_cmd(s, r, build_stats_cmd(arg)).await?;
    parse_stats_rp(&mut Cursor::new(udp_recv_rp(s, r).await?)).await
}

async fn stats_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    arg: Option<StatsArg>,
) -> Result<HashMap<String, String>> {
    s.write_all(build_stats_cmd(arg)).await?;
    parse_stats_rp(s).await
}

async fn slabs_automove_cmd_udp(
    s: &mut UdpSocket,
    r: &mut u16,
    arg: SlabsAutomoveArg,
) -> Result<()> {
    udp_send_cmd(s, r, build_slabs_automove_cmd(arg)).await?;
    parse_ok_rp(&mut Cursor::new(udp_recv_rp(s, r).await?), false).await
}

async fn slabs_automove_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    arg: SlabsAutomoveArg,
) -> Result<()> {
    s.write_all(build_slabs_automove_cmd(arg)).await?;
    parse_ok_rp(s, false).await
}

async fn lru_crawler_cmd_udp(s: &mut UdpSocket, r: &mut u16, arg: LruCrawlerArg) -> Result<()> {
    udp_send_cmd(s, r, build_lru_crawler_cmd(arg)).await?;
    parse_ok_rp(&mut Cursor::new(udp_recv_rp(s, r).await?), false).await
}

async fn lru_crawler_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    arg: LruCrawlerArg,
) -> Result<()> {
    s.write_all(build_lru_crawler_cmd(arg)).await?;
    parse_ok_rp(s, false).await
}

async fn lru_crawler_sleep_cmd_udp(
    s: &mut UdpSocket,
    r: &mut u16,
    microseconds: usize,
) -> Result<()> {
    udp_send_cmd(s, r, &build_lru_clawler_sleep_cmd(microseconds)).await?;
    parse_ok_rp(&mut Cursor::new(udp_recv_rp(s, r).await?), false).await
}

async fn lru_crawler_sleep_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    microseconds: usize,
) -> Result<()> {
    s.write_all(&build_lru_clawler_sleep_cmd(microseconds))
        .await?;
    parse_ok_rp(s, false).await
}

async fn lru_crawler_tocrawl_cmd_udp(s: &mut UdpSocket, r: &mut u16, arg: u32) -> Result<()> {
    udp_send_cmd(s, r, &build_lru_crawler_tocrawl_cmd(arg)).await?;
    parse_ok_rp(&mut Cursor::new(udp_recv_rp(s, r).await?), false).await
}

async fn lru_crawler_tocrawl_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    arg: u32,
) -> Result<()> {
    s.write_all(&build_lru_crawler_tocrawl_cmd(arg)).await?;
    parse_ok_rp(s, false).await
}

async fn lru_crawler_crawl_cmd_udp(
    s: &mut UdpSocket,
    r: &mut u16,
    arg: LruCrawlerCrawlArg<'_>,
) -> Result<LruCrawlerResult> {
    udp_send_cmd(s, r, &build_lru_clawler_crawl_cmd(arg)).await?;
    parse_lru_crawler_rp(&mut Cursor::new(udp_recv_rp(s, r).await?)).await
}

async fn lru_crawler_crawl_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    arg: LruCrawlerCrawlArg<'_>,
) -> Result<LruCrawlerResult> {
    s.write_all(&build_lru_clawler_crawl_cmd(arg)).await?;
    parse_lru_crawler_rp(s).await
}

async fn slabs_reassign_cmd_udp(
    s: &mut UdpSocket,
    r: &mut u16,
    source_class: isize,
    dest_class: isize,
) -> Result<SlabsReassignResult> {
    udp_send_cmd(s, r, &build_slabs_reassign_cmd(source_class, dest_class)).await?;
    parse_slabs_reassign_rp(&mut Cursor::new(udp_recv_rp(s, r).await?)).await
}

async fn slabs_reassign_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    source_class: isize,
    dest_class: isize,
) -> Result<SlabsReassignResult> {
    s.write_all(&build_slabs_reassign_cmd(source_class, dest_class))
        .await?;
    parse_slabs_reassign_rp(s).await
}

async fn lru_crawler_metadump_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    arg: LruCrawlerMetadumpArg<'_>,
) -> Result<LruCrawlerDumpResult> {
    s.write_all(&build_lru_clawler_metadump_cmd(arg)).await?;
    parse_lru_crawler_metadump_rp(s).await
}

async fn lru_crawler_mgdump_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    arg: LruCrawlerMgdumpArg<'_>,
) -> Result<LruCrawlerDumpResult> {
    s.write_all(&build_lru_clawler_mgdump_cmd(arg)).await?;
    parse_lru_crawler_mgdump_rp(s).await
}

async fn mn_cmd_udp(s: &mut UdpSocket, r: &mut u16) -> Result<()> {
    udp_send_cmd(s, r, build_mn_cmd()).await?;
    parse_mn_rp(&mut Cursor::new(udp_recv_rp(s, r).await?)).await
}

async fn mn_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> Result<()> {
    s.write_all(build_mn_cmd()).await?;
    parse_mn_rp(s).await
}

async fn me_cmd_udp(s: &mut UdpSocket, r: &mut u16, key: &[u8]) -> Result<Option<String>> {
    udp_send_cmd(s, r, &build_me_cmd(key)).await?;
    parse_me_rp(&mut Cursor::new(udp_recv_rp(s, r).await?)).await
}

async fn me_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    key: &[u8],
) -> Result<Option<String>> {
    s.write_all(&build_me_cmd(key)).await?;
    parse_me_rp(s).await
}

async fn execute_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    cmds: &[Vec<u8>],
) -> Result<Vec<PipelineResponse>> {
    s.write_all(&cmds.concat()).await?;
    let mut result = Vec::new();
    for cmd in cmds {
        if cmd.starts_with(b"gets ")
            || cmd.starts_with(b"get ")
            || cmd.starts_with(b"gats ")
            || cmd.starts_with(b"gat ")
        {
            if (cmd.starts_with(b"gat") && cmd.iter().filter(|x| x == &&b' ').count() == 2)
                || (cmd.starts_with(b"get") && cmd.iter().filter(|x| x == &&b' ').count() == 1)
            {
                result.push(PipelineResponse::OptionItem(
                    parse_retrieval_rp(s).await?.pop(),
                ))
            } else {
                result.push(PipelineResponse::VecItem(parse_retrieval_rp(s).await?))
            }
        } else if cmd.starts_with(b"set _ _ _ ") {
            result.push(PipelineResponse::Unit(parse_auth_rp(s).await?))
        } else if cmd.starts_with(b"set ")
            || cmd.starts_with(b"add ")
            || cmd.starts_with(b"replace ")
            || cmd.starts_with(b"append ")
            || cmd.starts_with(b"prepend ")
            || cmd.starts_with(b"cas ")
        {
            let mut split = cmd.split(|x| x == &b'\r');
            let n = split.next().unwrap();
            result.push(PipelineResponse::StorageResult(
                parse_storage_rp(s, n.ends_with(b"noreply")).await?,
            ))
        } else if cmd == build_version_cmd() {
            result.push(PipelineResponse::String(parse_version_rp(s).await?))
        } else if cmd.starts_with(b"delete ") {
            result.push(PipelineResponse::DeleteResult(
                parse_delete_rp(s, cmd.ends_with(b"noreply\r\n")).await?,
            ))
        } else if cmd.starts_with(b"incr ") || cmd.starts_with(b"decr ") {
            result.push(PipelineResponse::Value(
                parse_incr_decr_rp(s, cmd.ends_with(b"noreply\r\n")).await?,
            ))
        } else if cmd.starts_with(b"touch ") {
            result.push(PipelineResponse::TouchResult(
                parse_touch_rp(s, cmd.ends_with(b"noreply\r\n")).await?,
            ))
        } else if cmd == build_quit_cmd() || cmd.starts_with(b"shutdown") {
            result.push(PipelineResponse::Unit(()))
        } else if cmd.starts_with(b"flush_all") || cmd.starts_with(b"cache_memlimit ") {
            result.push(PipelineResponse::Unit(
                parse_ok_rp(s, cmd.ends_with(b"noreply\r\n")).await?,
            ))
        } else if cmd.starts_with(b"slabs automove ")
            || cmd.starts_with(b"lru_crawler sleep ")
            || cmd.starts_with(b"lru_crawler tocrawl ")
            || cmd == build_lru_crawler_cmd(LruCrawlerArg::Enable)
            || cmd == build_lru_crawler_cmd(LruCrawlerArg::Disable)
        {
            result.push(PipelineResponse::Unit(parse_ok_rp(s, false).await?))
        } else if cmd.starts_with(b"lru_crawler crawl ") {
            result.push(PipelineResponse::LruCrawlerResult(
                parse_lru_crawler_rp(s).await?,
            ))
        } else if cmd.starts_with(b"slabs reassign ") {
            result.push(PipelineResponse::SlabsReassignResult(
                parse_slabs_reassign_rp(s).await?,
            ));
        } else if cmd == build_mn_cmd() {
            result.push(PipelineResponse::Unit(parse_mn_rp(s).await?))
        } else if cmd.starts_with(b"stats") {
            result.push(PipelineResponse::HashMap(parse_stats_rp(s).await?))
        } else if cmd.starts_with(b"lru_crawler metadump ") {
            result.push(PipelineResponse::LruCrawlerDumpResult(
                parse_lru_crawler_metadump_rp(s).await?,
            ))
        } else if cmd.starts_with(b"lru_crawler mgdump ") {
            result.push(PipelineResponse::LruCrawlerDumpResult(
                parse_lru_crawler_mgdump_rp(s).await?,
            ))
        } else if cmd.starts_with(b"mg ") {
            result.push(PipelineResponse::MetaGet(parse_mg_rp(s).await?))
        } else if cmd.starts_with(b"ms ") {
            result.push(PipelineResponse::MetaSet(parse_ms_rp(s).await?))
        } else if cmd.starts_with(b"md ") {
            result.push(PipelineResponse::MetaDelete(parse_md_rp(s).await?))
        } else if cmd.starts_with(b"ma ") {
            result.push(PipelineResponse::MetaArithmetic(parse_ma_rp(s).await?))
        } else if cmd.starts_with(b"lru ") {
            result.push(PipelineResponse::Unit(parse_ok_rp(s, false).await?))
        } else if cmd.starts_with(b"me ") {
            result.push(PipelineResponse::OptionString(parse_me_rp(s).await?))
        } else {
            assert!(cmd.starts_with(b"verbosity "));
            result.push(PipelineResponse::Unit(
                parse_ok_rp(s, cmd.ends_with(b" noreply\r\n")).await?,
            ))
        }
    }
    Ok(result)
}

async fn watch_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    arg: &[WatchArg],
) -> Result<()> {
    s.write_all(&build_watch_cmd(arg)).await?;
    parse_ok_rp(s, false).await
}

async fn ms_cmd_udp(
    s: &mut UdpSocket,
    r: &mut u16,
    key: &[u8],
    flags: &[MsFlag],
    data_block: &[u8],
) -> Result<MsItem> {
    udp_send_cmd(
        s,
        r,
        &build_mc_cmd(b"ms", key, &build_ms_flags(flags), Some(data_block)),
    )
    .await?;
    parse_ms_rp(&mut Cursor::new(udp_recv_rp(s, r).await?)).await
}

async fn ms_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    key: &[u8],
    flags: &[MsFlag],
    data_block: &[u8],
) -> Result<MsItem> {
    s.write_all(&build_mc_cmd(
        b"ms",
        key,
        &build_ms_flags(flags),
        Some(data_block),
    ))
    .await?;
    parse_ms_rp(s).await
}

async fn mg_cmd_udp(
    s: &mut UdpSocket,
    r: &mut u16,
    key: &[u8],
    flags: &[MgFlag],
) -> Result<MgItem> {
    udp_send_cmd(
        s,
        r,
        &build_mc_cmd(b"mg", key, &build_mg_flags(flags), None),
    )
    .await?;
    parse_mg_rp(&mut Cursor::new(udp_recv_rp(s, r).await?)).await
}

async fn mg_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    key: &[u8],
    flags: &[MgFlag],
) -> Result<MgItem> {
    s.write_all(&build_mc_cmd(b"mg", key, &build_mg_flags(flags), None))
        .await?;
    parse_mg_rp(s).await
}

async fn md_cmd_udp(
    s: &mut UdpSocket,
    r: &mut u16,
    key: &[u8],
    flags: &[MdFlag],
) -> Result<MdItem> {
    udp_send_cmd(
        s,
        r,
        &build_mc_cmd(b"md", key, &build_md_flags(flags), None),
    )
    .await?;
    parse_md_rp(&mut Cursor::new(udp_recv_rp(s, r).await?)).await
}

async fn md_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    key: &[u8],
    flags: &[MdFlag],
) -> Result<MdItem> {
    s.write_all(&build_mc_cmd(b"md", key, &build_md_flags(flags), None))
        .await?;
    parse_md_rp(s).await
}

async fn ma_cmd_udp(
    s: &mut UdpSocket,
    r: &mut u16,
    key: &[u8],
    flags: &[MaFlag],
) -> Result<MaItem> {
    udp_send_cmd(
        s,
        r,
        &build_mc_cmd(b"ma", key, &build_ma_flags(flags), None),
    )
    .await?;
    parse_ma_rp(&mut Cursor::new(udp_recv_rp(s, r).await?)).await
}

async fn ma_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    key: &[u8],
    flags: &[MaFlag],
) -> Result<MaItem> {
    s.write_all(&build_mc_cmd(b"ma", key, &build_ma_flags(flags), None))
        .await?;
    parse_ma_rp(s).await
}

async fn lru_cmd_udp(s: &mut UdpSocket, r: &mut u16, arg: LruArg) -> Result<()> {
    udp_send_cmd(s, r, &build_lru_cmd(arg)).await?;
    parse_ok_rp(&mut Cursor::new(udp_recv_rp(s, r).await?), false).await
}

async fn lru_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S, arg: LruArg) -> Result<()> {
    s.write_all(&build_lru_cmd(arg)).await?;
    parse_ok_rp(s, false).await
}

async fn verbosity_cmd_udp(
    s: &mut UdpSocket,
    r: &mut u16,
    arg: VerbosityArg,
    noreply: bool,
) -> Result<()> {
    udp_send_cmd(s, r, &build_verbosity_cmd(arg, noreply)).await?;
    if noreply {
        return Ok(());
    }
    parse_ok_rp(&mut Cursor::new(udp_recv_rp(s, r).await?), noreply).await
}

async fn verbosity_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    arg: VerbosityArg,
    noreply: bool,
) -> Result<()> {
    s.write_all(&build_verbosity_cmd(arg, noreply)).await?;
    parse_ok_rp(s, noreply).await
}

pub enum Connection {
    Tcp(BufReader<TcpStream>),
    #[cfg(unix)]
    Unix(BufReader<UnixStream>),
    Udp(UdpSocket, u16),
    Tls(BufReader<TlsStream<TcpStream>>),
}
impl Connection {
    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// assert_eq!(
    ///     conn.set(b"doc_conn_default", 0, 0, false, b"value").await?,
    ///     StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     conn.get(b"doc_conn_default").await?.unwrap().data_block,
    ///     b"value"
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn default() -> Result<Self> {
        Ok(Connection::Tcp(BufReader::new(
            TcpStream::connect("127.0.0.1:11211").await?,
        )))
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::tcp_connect("127.0.0.1:11211").await?;
    /// assert_eq!(
    ///     conn.set(b"doc_conn_tcp_connect", 0, 0, false, b"value")
    ///         .await?,
    ///     StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     conn.get(b"doc_conn_tcp_connect").await?.unwrap().data_block,
    ///     b"value"
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn tcp_connect(addr: &str) -> Result<Self> {
        Ok(Connection::Tcp(BufReader::new(
            TcpStream::connect(addr).await?,
        )))
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::unix_connect("/tmp/memcached0.sock").await?;
    /// assert_eq!(
    ///     conn.set(b"doc_conn_unix_connect", 0, 0, false, b"value")
    ///         .await?,
    ///     StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     conn.get(b"doc_conn_unix_connect")
    ///         .await?
    ///         .unwrap()
    ///         .data_block,
    ///     b"value"
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    #[cfg(unix)]
    pub async fn unix_connect(path: &str) -> Result<Self> {
        Ok(Connection::Unix(BufReader::new(
            UnixStream::connect(path).await?,
        )))
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?;
    /// assert_eq!(
    ///     conn.set(b"doc_conn_udp_connect", 0, 0, false, b"value")
    ///         .await?,
    ///     StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     conn.get(b"doc_conn_udp_connect").await?.unwrap().data_block,
    ///     b"value"
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn udp_connect(bind_addr: &str, connect_addr: &str) -> Result<Self> {
        let s = UdpSocket::bind(bind_addr).await?;
        s.connect(connect_addr).await?;
        Ok(Connection::Udp(s, 0))
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::tls_connect("localhost", 11216, "cert.pem").await?;
    /// assert_eq!(
    ///     conn.set(b"doc_conn_tls_connect", 0, 0, false, b"value")
    ///         .await?,
    ///     StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     conn.get(b"doc_conn_tls_connect").await?.unwrap().data_block,
    ///     b"value"
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn tls_connect(hostname: &str, port: u16, ca_path: &str) -> Result<Self> {
        let cert = fs::read(ca_path).await?;
        let tcp_stream = TcpStream::connect(format!("{hostname}:{port}")).await?;
        let connector =
            TlsConnector::new().add_root_certificate(Certificate::from_pem(&cert).unwrap());
        Ok(Connection::Tls(BufReader::new(
            connector.connect(hostname, tcp_stream).await.unwrap(),
        )))
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    /// ] {
    ///     let version = c.version().await?;
    ///     assert!(!version.is_empty());
    ///     assert!(version.split('.').all(|part| part.parse::<u64>().is_ok()));
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn version(&mut self) -> Result<String> {
        match self {
            Connection::Tcp(s) => version_cmd(s).await,
            #[cfg(unix)]
            Connection::Unix(s) => version_cmd(s).await,
            Connection::Udp(s, r) => version_cmd_udp(s, r).await,
            Connection::Tls(s) => version_cmd(s).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    /// ] {
    ///     assert!(c.quit().await.is_ok());
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn quit(mut self) -> Result<()> {
        match &mut self {
            Connection::Tcp(s) => quit_cmd(s).await,
            #[cfg(unix)]
            Connection::Unix(s) => quit_cmd(s).await,
            Connection::Udp(s, r) => quit_cmd_udp(s, r).await,
            Connection::Tls(s) => quit_cmd(s).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::tcp_connect("127.0.0.1:11213").await?,
    ///     Connection::unix_connect("/tmp/memcached1.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11215").await?,
    ///     Connection::tls_connect("localhost", 11217, "cert.pem").await?,
    /// ] {
    ///     assert_eq!(c.shutdown(true).await?, ());
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn shutdown(mut self, graceful: bool) -> Result<()> {
        match &mut self {
            Connection::Tcp(s) => shutdown_cmd(s, graceful).await,
            #[cfg(unix)]
            Connection::Unix(s) => shutdown_cmd(s, graceful).await,
            Connection::Udp(s, r) => shutdown_cmd_udp(s, r, graceful).await,
            Connection::Tls(s) => shutdown_cmd(s, graceful).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    /// ] {
    ///     assert_eq!(c.cache_memlimit(64, false).await?, ());
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn cache_memlimit(&mut self, limit: usize, noreply: bool) -> Result<()> {
        match self {
            Connection::Tcp(s) => cache_memlimit_cmd(s, limit, noreply).await,
            #[cfg(unix)]
            Connection::Unix(s) => cache_memlimit_cmd(s, limit, noreply).await,
            Connection::Udp(s, r) => cache_memlimit_cmd_udp(s, r, limit, noreply).await,
            Connection::Tls(s) => cache_memlimit_cmd(s, limit, noreply).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    /// ] {
    ///     assert_eq!(
    ///         c.set(b"doc_conn_flush_all", 0, 0, false, b"value").await?,
    ///         StorageResult::Stored
    ///     );
    ///     assert_eq!(
    ///         c.get(b"doc_conn_flush_all").await?.unwrap().data_block,
    ///         b"value"
    ///     );
    ///     assert_eq!(c.flush_all(Some(999), false).await?, ());
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn flush_all(&mut self, exptime: Option<i64>, noreply: bool) -> Result<()> {
        match self {
            Connection::Tcp(s) => flush_all_cmd(s, exptime, noreply).await,
            #[cfg(unix)]
            Connection::Unix(s) => flush_all_cmd(s, exptime, noreply).await,
            Connection::Udp(s, r) => flush_all_cmd_udp(s, r, exptime, noreply).await,
            Connection::Tls(s) => flush_all_cmd(s, exptime, noreply).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, Item, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    /// ] {
    ///     assert_eq!(
    ///         c.set(b"doc_conn_set", 7, 0, false, b"value").await?,
    ///         StorageResult::Stored
    ///     );
    ///     assert_eq!(
    ///         c.get(b"doc_conn_set").await?,
    ///         Some(Item {
    ///             key: "doc_conn_set".to_string(),
    ///             flags: 7,
    ///             cas_unique: None,
    ///             data_block: b"value".to_vec()
    ///         })
    ///     );
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn set(
        &mut self,
        key: impl AsRef<[u8]>,
        flags: u32,
        exptime: i64,
        noreply: bool,
        data_block: impl AsRef<[u8]>,
    ) -> Result<StorageResult> {
        match self {
            Connection::Tcp(s) => {
                storage_cmd(
                    s,
                    b"set",
                    key.as_ref(),
                    flags,
                    exptime,
                    None,
                    noreply,
                    data_block.as_ref(),
                )
                .await
            }
            #[cfg(unix)]
            Connection::Unix(s) => {
                storage_cmd(
                    s,
                    b"set",
                    key.as_ref(),
                    flags,
                    exptime,
                    None,
                    noreply,
                    data_block.as_ref(),
                )
                .await
            }
            Connection::Udp(s, r) => {
                storage_cmd_udp(
                    s,
                    r,
                    b"set",
                    key.as_ref(),
                    flags,
                    exptime,
                    None,
                    noreply,
                    data_block.as_ref(),
                )
                .await
            }
            Connection::Tls(s) => {
                storage_cmd(
                    s,
                    b"set",
                    key.as_ref(),
                    flags,
                    exptime,
                    None,
                    noreply,
                    data_block.as_ref(),
                )
                .await
            }
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    /// ] {
    ///     let _ = c.delete(b"doc_conn_add", false).await?;
    ///     assert_eq!(
    ///         c.add(b"doc_conn_add", 0, 0, false, b"first").await?,
    ///         StorageResult::Stored
    ///     );
    ///     assert_eq!(
    ///         c.add(b"doc_conn_add", 0, 0, false, b"second").await?,
    ///         StorageResult::NotStored
    ///     );
    ///     assert_eq!(c.get(b"doc_conn_add").await?.unwrap().data_block, b"first");
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn add(
        &mut self,
        key: impl AsRef<[u8]>,
        flags: u32,
        exptime: i64,
        noreply: bool,
        data_block: impl AsRef<[u8]>,
    ) -> Result<StorageResult> {
        match self {
            Connection::Tcp(s) => {
                storage_cmd(
                    s,
                    b"add",
                    key.as_ref(),
                    flags,
                    exptime,
                    None,
                    noreply,
                    data_block.as_ref(),
                )
                .await
            }
            #[cfg(unix)]
            Connection::Unix(s) => {
                storage_cmd(
                    s,
                    b"add",
                    key.as_ref(),
                    flags,
                    exptime,
                    None,
                    noreply,
                    data_block.as_ref(),
                )
                .await
            }
            Connection::Udp(s, r) => {
                storage_cmd_udp(
                    s,
                    r,
                    b"add",
                    key.as_ref(),
                    flags,
                    exptime,
                    None,
                    noreply,
                    data_block.as_ref(),
                )
                .await
            }
            Connection::Tls(s) => {
                storage_cmd(
                    s,
                    b"add",
                    key.as_ref(),
                    flags,
                    exptime,
                    None,
                    noreply,
                    data_block.as_ref(),
                )
                .await
            }
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    /// ] {
    ///     let _ = c.delete(b"doc_conn_replace", false).await?;
    ///     assert_eq!(
    ///         c.replace(b"doc_conn_replace", 0, 0, false, b"value")
    ///             .await?,
    ///         StorageResult::NotStored
    ///     );
    ///     assert_eq!(
    ///         c.set(b"doc_conn_replace", 0, 0, false, b"first").await?,
    ///         StorageResult::Stored
    ///     );
    ///     assert_eq!(
    ///         c.replace(b"doc_conn_replace", 0, 0, false, b"second")
    ///             .await?,
    ///         StorageResult::Stored
    ///     );
    ///     assert_eq!(
    ///         c.get(b"doc_conn_replace").await?.unwrap().data_block,
    ///         b"second"
    ///     );
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn replace(
        &mut self,
        key: impl AsRef<[u8]>,
        flags: u32,
        exptime: i64,
        noreply: bool,
        data_block: impl AsRef<[u8]>,
    ) -> Result<StorageResult> {
        match self {
            Connection::Tcp(s) => {
                storage_cmd(
                    s,
                    b"replace",
                    key.as_ref(),
                    flags,
                    exptime,
                    None,
                    noreply,
                    data_block.as_ref(),
                )
                .await
            }
            #[cfg(unix)]
            Connection::Unix(s) => {
                storage_cmd(
                    s,
                    b"replace",
                    key.as_ref(),
                    flags,
                    exptime,
                    None,
                    noreply,
                    data_block.as_ref(),
                )
                .await
            }
            Connection::Udp(s, r) => {
                storage_cmd_udp(
                    s,
                    r,
                    b"replace",
                    key.as_ref(),
                    flags,
                    exptime,
                    None,
                    noreply,
                    data_block.as_ref(),
                )
                .await
            }
            Connection::Tls(s) => {
                storage_cmd(
                    s,
                    b"replace",
                    key.as_ref(),
                    flags,
                    exptime,
                    None,
                    noreply,
                    data_block.as_ref(),
                )
                .await
            }
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    /// ] {
    ///     let _ = c.delete(b"doc_conn_append", false).await?;
    ///     assert_eq!(
    ///         c.append(b"doc_conn_append", 0, 0, false, b"-tail").await?,
    ///         StorageResult::NotStored
    ///     );
    ///     assert_eq!(
    ///         c.set(b"doc_conn_append", 0, 0, false, b"base").await?,
    ///         StorageResult::Stored
    ///     );
    ///     assert_eq!(
    ///         c.append(b"doc_conn_append", 0, 0, false, b"-tail").await?,
    ///         StorageResult::Stored
    ///     );
    ///     assert_eq!(
    ///         c.get(b"doc_conn_append").await?.unwrap().data_block,
    ///         b"base-tail"
    ///     );
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn append(
        &mut self,
        key: impl AsRef<[u8]>,
        flags: u32,
        exptime: i64,
        noreply: bool,
        data_block: impl AsRef<[u8]>,
    ) -> Result<StorageResult> {
        match self {
            Connection::Tcp(s) => {
                storage_cmd(
                    s,
                    b"append",
                    key.as_ref(),
                    flags,
                    exptime,
                    None,
                    noreply,
                    data_block.as_ref(),
                )
                .await
            }
            #[cfg(unix)]
            Connection::Unix(s) => {
                storage_cmd(
                    s,
                    b"append",
                    key.as_ref(),
                    flags,
                    exptime,
                    None,
                    noreply,
                    data_block.as_ref(),
                )
                .await
            }
            Connection::Udp(s, r) => {
                storage_cmd_udp(
                    s,
                    r,
                    b"append",
                    key.as_ref(),
                    flags,
                    exptime,
                    None,
                    noreply,
                    data_block.as_ref(),
                )
                .await
            }
            Connection::Tls(s) => {
                storage_cmd(
                    s,
                    b"append",
                    key.as_ref(),
                    flags,
                    exptime,
                    None,
                    noreply,
                    data_block.as_ref(),
                )
                .await
            }
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    /// ] {
    ///     let _ = c.delete(b"doc_conn_prepend", false).await?;
    ///     assert_eq!(
    ///         c.prepend(b"doc_conn_prepend", 0, 0, false, b"head-")
    ///             .await?,
    ///         StorageResult::NotStored
    ///     );
    ///     assert_eq!(
    ///         c.set(b"doc_conn_prepend", 0, 0, false, b"base").await?,
    ///         StorageResult::Stored
    ///     );
    ///     assert_eq!(
    ///         c.prepend(b"doc_conn_prepend", 0, 0, false, b"head-")
    ///             .await?,
    ///         StorageResult::Stored
    ///     );
    ///     assert_eq!(
    ///         c.get(b"doc_conn_prepend").await?.unwrap().data_block,
    ///         b"head-base"
    ///     );
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn prepend(
        &mut self,
        key: impl AsRef<[u8]>,
        flags: u32,
        exptime: i64,
        noreply: bool,
        data_block: impl AsRef<[u8]>,
    ) -> Result<StorageResult> {
        match self {
            Connection::Tcp(s) => {
                storage_cmd(
                    s,
                    b"prepend",
                    key.as_ref(),
                    flags,
                    exptime,
                    None,
                    noreply,
                    data_block.as_ref(),
                )
                .await
            }
            #[cfg(unix)]
            Connection::Unix(s) => {
                storage_cmd(
                    s,
                    b"prepend",
                    key.as_ref(),
                    flags,
                    exptime,
                    None,
                    noreply,
                    data_block.as_ref(),
                )
                .await
            }
            Connection::Udp(s, r) => {
                storage_cmd_udp(
                    s,
                    r,
                    b"prepend",
                    key.as_ref(),
                    flags,
                    exptime,
                    None,
                    noreply,
                    data_block.as_ref(),
                )
                .await
            }
            Connection::Tls(s) => {
                storage_cmd(
                    s,
                    b"prepend",
                    key.as_ref(),
                    flags,
                    exptime,
                    None,
                    noreply,
                    data_block.as_ref(),
                )
                .await
            }
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    /// ] {
    ///     let _ = c.delete(b"doc_conn_cas", false).await?;
    ///     assert_eq!(
    ///         c.cas(b"doc_conn_cas", 0, 0, 0, false, b"value").await?,
    ///         StorageResult::NotFound
    ///     );
    ///     assert_eq!(
    ///         c.set(b"doc_conn_cas", 0, 0, false, b"base").await?,
    ///         StorageResult::Stored
    ///     );
    ///     let cas = c.gets(b"doc_conn_cas").await?.unwrap().cas_unique.unwrap();
    ///     assert_eq!(
    ///         c.cas(b"doc_conn_cas", 0, 0, cas, false, b"new").await?,
    ///         StorageResult::Stored
    ///     );
    ///     assert_eq!(
    ///         c.cas(b"doc_conn_cas", 0, 0, cas, false, b"stale").await?,
    ///         StorageResult::Exists
    ///     );
    ///     assert_eq!(c.get(b"doc_conn_cas").await?.unwrap().data_block, b"new");
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn cas(
        &mut self,
        key: impl AsRef<[u8]>,
        flags: u32,
        exptime: i64,
        cas_unique: u64,
        noreply: bool,
        data_block: impl AsRef<[u8]>,
    ) -> Result<StorageResult> {
        match self {
            Connection::Tcp(s) => {
                storage_cmd(
                    s,
                    b"cas",
                    key.as_ref(),
                    flags,
                    exptime,
                    Some(cas_unique),
                    noreply,
                    data_block.as_ref(),
                )
                .await
            }
            #[cfg(unix)]
            Connection::Unix(s) => {
                storage_cmd(
                    s,
                    b"cas",
                    key.as_ref(),
                    flags,
                    exptime,
                    Some(cas_unique),
                    noreply,
                    data_block.as_ref(),
                )
                .await
            }
            Connection::Udp(s, r) => {
                storage_cmd_udp(
                    s,
                    r,
                    b"cas",
                    key.as_ref(),
                    flags,
                    exptime,
                    Some(cas_unique),
                    noreply,
                    data_block.as_ref(),
                )
                .await
            }
            Connection::Tls(s) => {
                storage_cmd(
                    s,
                    b"cas",
                    key.as_ref(),
                    flags,
                    exptime,
                    Some(cas_unique),
                    noreply,
                    data_block.as_ref(),
                )
                .await
            }
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::tcp_connect("127.0.0.1:11212").await?,
    ///     Connection::unix_connect("/tmp/memcached2.sock").await?,
    ///     Connection::tls_connect("localhost", 11218, "cert.pem").await?,
    /// ] {
    ///     c.auth(b"a", b"a").await?;
    ///     assert!(!c.version().await?.is_empty());
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn auth(
        &mut self,
        username: impl AsRef<[u8]>,
        password: impl AsRef<[u8]>,
    ) -> Result<()> {
        match self {
            Connection::Tcp(s) => auth_cmd(s, username.as_ref(), password.as_ref()).await,
            #[cfg(unix)]
            Connection::Unix(s) => auth_cmd(s, username.as_ref(), password.as_ref()).await,
            Connection::Udp(_s, _r) => {
                unreachable!("Cannot enable UDP while using binary SASL authentication.")
            }
            Connection::Tls(s) => auth_cmd(s, username.as_ref(), password.as_ref()).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, DeleteResult, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    /// ] {
    ///     assert_eq!(
    ///         c.set(b"doc_conn_delete", 0, 0, false, b"value").await?,
    ///         StorageResult::Stored
    ///     );
    ///     assert_eq!(
    ///         c.delete(b"doc_conn_delete", false).await?,
    ///         DeleteResult::Deleted
    ///     );
    ///     assert_eq!(
    ///         c.delete(b"doc_conn_delete", false).await?,
    ///         DeleteResult::NotFound
    ///     );
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn delete(&mut self, key: impl AsRef<[u8]>, noreply: bool) -> Result<DeleteResult> {
        match self {
            Connection::Tcp(s) => delete_cmd(s, key.as_ref(), noreply).await,
            #[cfg(unix)]
            Connection::Unix(s) => delete_cmd(s, key.as_ref(), noreply).await,
            Connection::Udp(s, r) => delete_cmd_udp(s, r, key.as_ref(), noreply).await,
            Connection::Tls(s) => delete_cmd(s, key.as_ref(), noreply).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    /// ] {
    ///     let _ = c.delete(b"doc_conn_incr", false).await?;
    ///     assert_eq!(c.incr(b"doc_conn_incr", 1, false).await?, None);
    ///     assert_eq!(
    ///         c.set(b"doc_conn_incr", 0, 0, false, b"10").await?,
    ///         StorageResult::Stored
    ///     );
    ///     assert_eq!(c.incr(b"doc_conn_incr", 5, false).await?, Some(15));
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn incr(
        &mut self,
        key: impl AsRef<[u8]>,
        value: u64,
        noreply: bool,
    ) -> Result<Option<u64>> {
        match self {
            Connection::Tcp(s) => incr_decr_cmd(s, b"incr", key.as_ref(), value, noreply).await,
            #[cfg(unix)]
            Connection::Unix(s) => incr_decr_cmd(s, b"incr", key.as_ref(), value, noreply).await,
            Connection::Udp(s, r) => {
                incr_decr_cmd_udp(s, r, b"incr", key.as_ref(), value, noreply).await
            }
            Connection::Tls(s) => incr_decr_cmd(s, b"incr", key.as_ref(), value, noreply).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    /// ] {
    ///     let _ = c.delete(b"doc_conn_decr", false).await?;
    ///     assert_eq!(c.decr(b"doc_conn_decr", 1, false).await?, None);
    ///     assert_eq!(
    ///         c.set(b"doc_conn_decr", 0, 0, false, b"10").await?,
    ///         StorageResult::Stored
    ///     );
    ///     assert_eq!(c.decr(b"doc_conn_decr", 5, false).await?, Some(5));
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn decr(
        &mut self,
        key: impl AsRef<[u8]>,
        value: u64,
        noreply: bool,
    ) -> Result<Option<u64>> {
        match self {
            Connection::Tcp(s) => incr_decr_cmd(s, b"decr", key.as_ref(), value, noreply).await,
            #[cfg(unix)]
            Connection::Unix(s) => incr_decr_cmd(s, b"decr", key.as_ref(), value, noreply).await,
            Connection::Udp(s, r) => {
                incr_decr_cmd_udp(s, r, b"decr", key.as_ref(), value, noreply).await
            }
            Connection::Tls(s) => incr_decr_cmd(s, b"decr", key.as_ref(), value, noreply).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, StorageResult, TouchResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    /// ] {
    ///     assert_eq!(
    ///         c.touch(b"doc_conn_touch_missing", 60, false).await?,
    ///         TouchResult::NotFound
    ///     );
    ///     assert_eq!(
    ///         c.set(b"doc_conn_touch", 0, 0, false, b"value").await?,
    ///         StorageResult::Stored
    ///     );
    ///     assert_eq!(
    ///         c.touch(b"doc_conn_touch", 60, false).await?,
    ///         TouchResult::Touched
    ///     );
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn touch(
        &mut self,
        key: impl AsRef<[u8]>,
        exptime: i64,
        noreply: bool,
    ) -> Result<TouchResult> {
        match self {
            Connection::Tcp(s) => touch_cmd(s, key.as_ref(), exptime, noreply).await,
            #[cfg(unix)]
            Connection::Unix(s) => touch_cmd(s, key.as_ref(), exptime, noreply).await,
            Connection::Udp(s, r) => touch_cmd_udp(s, r, key.as_ref(), exptime, noreply).await,
            Connection::Tls(s) => touch_cmd(s, key.as_ref(), exptime, noreply).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, Item, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    /// ] {
    ///     assert_eq!(
    ///         c.set(b"doc_conn_get", 0, 0, false, b"value").await?,
    ///         StorageResult::Stored
    ///     );
    ///     assert_eq!(
    ///         c.get(b"doc_conn_get").await?,
    ///         Some(Item {
    ///             key: "doc_conn_get".to_string(),
    ///             flags: 0,
    ///             cas_unique: None,
    ///             data_block: b"value".to_vec()
    ///         })
    ///     );
    ///     assert_eq!(c.get(b"doc_conn_get_missing").await?, None);
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn get(&mut self, key: impl AsRef<[u8]>) -> Result<Option<Item>> {
        match self {
            Connection::Tcp(s) => Ok(retrieval_cmd(s, b"get", None, &[key.as_ref()]).await?.pop()),
            #[cfg(unix)]
            Connection::Unix(s) => Ok(retrieval_cmd(s, b"get", None, &[key.as_ref()]).await?.pop()),
            Connection::Udp(s, r) => Ok(retrieval_cmd_udp(s, r, b"get", None, &[key.as_ref()])
                .await?
                .pop()),
            Connection::Tls(s) => Ok(retrieval_cmd(s, b"get", None, &[key.as_ref()]).await?.pop()),
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    /// ] {
    ///     assert_eq!(
    ///         c.set(b"doc_conn_gets", 0, 0, false, b"value").await?,
    ///         StorageResult::Stored
    ///     );
    ///     let item = c.gets(b"doc_conn_gets").await?.unwrap();
    ///     assert_eq!(item.key, "doc_conn_gets");
    ///     assert_eq!(item.data_block, b"value");
    ///     assert!(item.cas_unique.is_some());
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn gets(&mut self, key: impl AsRef<[u8]>) -> Result<Option<Item>> {
        match self {
            Connection::Tcp(s) => Ok(retrieval_cmd(s, b"gets", None, &[key.as_ref()])
                .await?
                .pop()),
            #[cfg(unix)]
            Connection::Unix(s) => Ok(retrieval_cmd(s, b"gets", None, &[key.as_ref()])
                .await?
                .pop()),
            Connection::Udp(s, r) => Ok(retrieval_cmd_udp(s, r, b"gets", None, &[key.as_ref()])
                .await?
                .pop()),
            Connection::Tls(s) => Ok(retrieval_cmd(s, b"gets", None, &[key.as_ref()])
                .await?
                .pop()),
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    /// ] {
    ///     assert_eq!(
    ///         c.set(b"doc_conn_gat", 7, 0, false, b"value").await?,
    ///         StorageResult::Stored
    ///     );
    ///     let item = c.gat(60, b"doc_conn_gat").await?.unwrap();
    ///     assert_eq!(item.key, "doc_conn_gat");
    ///     assert_eq!(item.flags, 7);
    ///     assert_eq!(item.data_block, b"value");
    ///     assert_eq!(item.cas_unique, None);
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn gat(&mut self, exptime: i64, key: impl AsRef<[u8]>) -> Result<Option<Item>> {
        match self {
            Connection::Tcp(s) => Ok(retrieval_cmd(s, b"gat", Some(exptime), &[key.as_ref()])
                .await?
                .pop()),
            #[cfg(unix)]
            Connection::Unix(s) => Ok(retrieval_cmd(s, b"gat", Some(exptime), &[key.as_ref()])
                .await?
                .pop()),
            Connection::Udp(s, r) => {
                Ok(
                    retrieval_cmd_udp(s, r, b"gat", Some(exptime), &[key.as_ref()])
                        .await?
                        .pop(),
                )
            }
            Connection::Tls(s) => Ok(retrieval_cmd(s, b"gat", Some(exptime), &[key.as_ref()])
                .await?
                .pop()),
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    /// ] {
    ///     assert_eq!(
    ///         c.set(b"doc_conn_gats", 7, 0, false, b"value").await?,
    ///         StorageResult::Stored
    ///     );
    ///     let item = c.gats(60, b"doc_conn_gats").await?.unwrap();
    ///     assert_eq!(item.key, "doc_conn_gats");
    ///     assert_eq!(item.flags, 7);
    ///     assert_eq!(item.data_block, b"value");
    ///     assert!(item.cas_unique.is_some());
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn gats(&mut self, exptime: i64, key: impl AsRef<[u8]>) -> Result<Option<Item>> {
        match self {
            Connection::Tcp(s) => Ok(retrieval_cmd(s, b"gats", Some(exptime), &[key.as_ref()])
                .await?
                .pop()),
            #[cfg(unix)]
            Connection::Unix(s) => Ok(retrieval_cmd(s, b"gats", Some(exptime), &[key.as_ref()])
                .await?
                .pop()),
            Connection::Udp(s, r) => {
                Ok(
                    retrieval_cmd_udp(s, r, b"gats", Some(exptime), &[key.as_ref()])
                        .await?
                        .pop(),
                )
            }
            Connection::Tls(s) => Ok(retrieval_cmd(s, b"gats", Some(exptime), &[key.as_ref()])
                .await?
                .pop()),
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, Item, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    /// ] {
    ///     assert_eq!(
    ///         c.set(b"doc_get_multi_a", 0, 0, false, b"a").await?,
    ///         StorageResult::Stored
    ///     );
    ///     assert_eq!(
    ///         c.set(b"doc_get_multi_b", 0, 0, false, b"b").await?,
    ///         StorageResult::Stored
    ///     );
    ///     assert_eq!(
    ///         c.get_multi(&[b"doc_get_multi_a", b"doc_get_multi_b"])
    ///             .await?,
    ///         vec![
    ///             Item {
    ///                 key: "doc_get_multi_a".to_string(),
    ///                 flags: 0,
    ///                 cas_unique: None,
    ///                 data_block: b"a".to_vec()
    ///             },
    ///             Item {
    ///                 key: "doc_get_multi_b".to_string(),
    ///                 flags: 0,
    ///                 cas_unique: None,
    ///                 data_block: b"b".to_vec()
    ///             }
    ///         ]
    ///     );
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn get_multi(&mut self, keys: &[impl AsRef<[u8]>]) -> Result<Vec<Item>> {
        match self {
            Connection::Tcp(s) => {
                retrieval_cmd(
                    s,
                    b"get",
                    None,
                    &keys.iter().map(|x| x.as_ref()).collect::<Vec<&[u8]>>(),
                )
                .await
            }
            #[cfg(unix)]
            Connection::Unix(s) => {
                retrieval_cmd(
                    s,
                    b"get",
                    None,
                    &keys.iter().map(|x| x.as_ref()).collect::<Vec<&[u8]>>(),
                )
                .await
            }
            Connection::Udp(_s, _r) => unreachable!("Udp not work with multicommands"),
            Connection::Tls(s) => {
                retrieval_cmd(
                    s,
                    b"get",
                    None,
                    &keys.iter().map(|x| x.as_ref()).collect::<Vec<&[u8]>>(),
                )
                .await
            }
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    /// ] {
    ///     assert_eq!(
    ///         c.set(b"doc_gets_multi_a", 0, 0, false, b"a").await?,
    ///         StorageResult::Stored
    ///     );
    ///     assert_eq!(
    ///         c.set(b"doc_gets_multi_b", 0, 0, false, b"b").await?,
    ///         StorageResult::Stored
    ///     );
    ///     let items = c
    ///         .gets_multi(&[b"doc_gets_multi_a", b"doc_gets_multi_b"])
    ///         .await?;
    ///     assert_eq!(items.len(), 2);
    ///     assert_eq!(items[0].key, "doc_gets_multi_a");
    ///     assert_eq!(items[1].key, "doc_gets_multi_b");
    ///     assert_eq!(items[0].data_block, b"a");
    ///     assert_eq!(items[1].data_block, b"b");
    ///     assert!(items.iter().all(|item| item.cas_unique.is_some()));
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn gets_multi(&mut self, keys: &[impl AsRef<[u8]>]) -> Result<Vec<Item>> {
        match self {
            Connection::Tcp(s) => {
                retrieval_cmd(
                    s,
                    b"gets",
                    None,
                    &keys.iter().map(|x| x.as_ref()).collect::<Vec<&[u8]>>(),
                )
                .await
            }
            #[cfg(unix)]
            Connection::Unix(s) => {
                retrieval_cmd(
                    s,
                    b"gets",
                    None,
                    &keys.iter().map(|x| x.as_ref()).collect::<Vec<&[u8]>>(),
                )
                .await
            }
            Connection::Udp(_s, _r) => unreachable!("Udp not work with multicommands"),
            Connection::Tls(s) => {
                retrieval_cmd(
                    s,
                    b"gets",
                    None,
                    &keys.iter().map(|x| x.as_ref()).collect::<Vec<&[u8]>>(),
                )
                .await
            }
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, Item, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    /// ] {
    ///     assert_eq!(
    ///         c.set(b"doc_gat_multi_a", 0, 0, false, b"a").await?,
    ///         StorageResult::Stored
    ///     );
    ///     assert_eq!(
    ///         c.set(b"doc_gat_multi_b", 0, 0, false, b"b").await?,
    ///         StorageResult::Stored
    ///     );
    ///     assert_eq!(
    ///         c.gat_multi(60, &[b"doc_gat_multi_a", b"doc_gat_multi_b"])
    ///             .await?,
    ///         vec![
    ///             Item {
    ///                 key: "doc_gat_multi_a".to_string(),
    ///                 flags: 0,
    ///                 cas_unique: None,
    ///                 data_block: b"a".to_vec()
    ///             },
    ///             Item {
    ///                 key: "doc_gat_multi_b".to_string(),
    ///                 flags: 0,
    ///                 cas_unique: None,
    ///                 data_block: b"b".to_vec()
    ///             }
    ///         ]
    ///     );
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn gat_multi(
        &mut self,
        exptime: i64,
        keys: &[impl AsRef<[u8]>],
    ) -> Result<Vec<Item>> {
        match self {
            Connection::Tcp(s) => {
                retrieval_cmd(
                    s,
                    b"gat",
                    Some(exptime),
                    &keys.iter().map(|x| x.as_ref()).collect::<Vec<&[u8]>>(),
                )
                .await
            }
            #[cfg(unix)]
            Connection::Unix(s) => {
                retrieval_cmd(
                    s,
                    b"gat",
                    Some(exptime),
                    &keys.iter().map(|x| x.as_ref()).collect::<Vec<&[u8]>>(),
                )
                .await
            }
            Connection::Udp(_s, _r) => unreachable!("Udp not work with multicommands"),
            Connection::Tls(s) => {
                retrieval_cmd(
                    s,
                    b"gat",
                    Some(exptime),
                    &keys.iter().map(|x| x.as_ref()).collect::<Vec<&[u8]>>(),
                )
                .await
            }
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    /// ] {
    ///     assert_eq!(
    ///         c.set(b"doc_gats_multi_a", 0, 0, false, b"a").await?,
    ///         StorageResult::Stored
    ///     );
    ///     assert_eq!(
    ///         c.set(b"doc_gats_multi_b", 0, 0, false, b"b").await?,
    ///         StorageResult::Stored
    ///     );
    ///     let items = c
    ///         .gats_multi(60, &[b"doc_gats_multi_a", b"doc_gats_multi_b"])
    ///         .await?;
    ///     assert_eq!(items.len(), 2);
    ///     assert_eq!(items[0].key, "doc_gats_multi_a");
    ///     assert_eq!(items[1].key, "doc_gats_multi_b");
    ///     assert_eq!(items[0].data_block, b"a");
    ///     assert_eq!(items[1].data_block, b"b");
    ///     assert!(items.iter().all(|item| item.cas_unique.is_some()));
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn gats_multi(
        &mut self,
        exptime: i64,
        keys: &[impl AsRef<[u8]>],
    ) -> Result<Vec<Item>> {
        match self {
            Connection::Tcp(s) => {
                retrieval_cmd(
                    s,
                    b"gats",
                    Some(exptime),
                    &keys.iter().map(|x| x.as_ref()).collect::<Vec<&[u8]>>(),
                )
                .await
            }
            #[cfg(unix)]
            Connection::Unix(s) => {
                retrieval_cmd(
                    s,
                    b"gats",
                    Some(exptime),
                    &keys.iter().map(|x| x.as_ref()).collect::<Vec<&[u8]>>(),
                )
                .await
            }
            Connection::Udp(_s, _r) => unreachable!("Udp not work with multicommands"),
            Connection::Tls(s) => {
                retrieval_cmd(
                    s,
                    b"gats",
                    Some(exptime),
                    &keys.iter().map(|x| x.as_ref()).collect::<Vec<&[u8]>>(),
                )
                .await
            }
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, StatsArg};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    /// ] {
    ///     let result = c.stats(None).await?;
    ///     assert!(result.contains_key("pid"));
    ///     assert!(result.contains_key("uptime"));
    ///     assert!(result["pid"].parse::<u32>().is_ok());
    ///     assert!(result["uptime"].parse::<u64>().is_ok());
    ///     let version = c.version().await?;
    ///     assert_eq!(result.get("version"), Some(&version));
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn stats(&mut self, arg: Option<StatsArg>) -> Result<HashMap<String, String>> {
        match self {
            Connection::Tcp(s) => stats_cmd(s, arg).await,
            #[cfg(unix)]
            Connection::Unix(s) => stats_cmd(s, arg).await,
            Connection::Udp(s, r) => stats_cmd_udp(s, r, arg).await,
            Connection::Tls(s) => stats_cmd(s, arg).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, SlabsAutomoveArg, StatsArg};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    /// ] {
    ///     assert!(c.slabs_automove(SlabsAutomoveArg::Zero).await.is_ok());
    ///     let settings = c.stats(Some(StatsArg::Settings)).await?;
    ///     assert_eq!(settings.get("slab_automove").map(String::as_str), Some("0"));
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn slabs_automove(&mut self, arg: SlabsAutomoveArg) -> Result<()> {
        match self {
            Connection::Tcp(s) => slabs_automove_cmd(s, arg).await,
            #[cfg(unix)]
            Connection::Unix(s) => slabs_automove_cmd(s, arg).await,
            Connection::Udp(s, r) => slabs_automove_cmd_udp(s, r, arg).await,
            Connection::Tls(s) => slabs_automove_cmd(s, arg).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, LruCrawlerArg, StatsArg};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    /// ] {
    ///     let _ = c.lru_crawler(LruCrawlerArg::Enable).await;
    ///     let settings = c.stats(Some(StatsArg::Settings)).await?;
    ///     assert_eq!(settings.get("lru_crawler").map(String::as_str), Some("yes"));
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn lru_crawler(&mut self, arg: LruCrawlerArg) -> Result<()> {
        match self {
            Connection::Tcp(s) => lru_crawler_cmd(s, arg).await,
            #[cfg(unix)]
            Connection::Unix(s) => lru_crawler_cmd(s, arg).await,
            Connection::Udp(s, r) => lru_crawler_cmd_udp(s, r, arg).await,
            Connection::Tls(s) => lru_crawler_cmd(s, arg).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, LruCrawlerArg, StatsArg};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    /// ] {
    ///     let _ = c.lru_crawler(LruCrawlerArg::Enable).await;
    ///     assert!(c.lru_crawler_sleep(1_000_000).await.is_ok());
    ///     let settings = c.stats(Some(StatsArg::Settings)).await?;
    ///     assert_eq!(
    ///         settings.get("lru_crawler_sleep").map(String::as_str),
    ///         Some("1000000")
    ///     );
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn lru_crawler_sleep(&mut self, microseconds: usize) -> Result<()> {
        match self {
            Connection::Tcp(s) => lru_crawler_sleep_cmd(s, microseconds).await,
            #[cfg(unix)]
            Connection::Unix(s) => lru_crawler_sleep_cmd(s, microseconds).await,
            Connection::Udp(s, r) => lru_crawler_sleep_cmd_udp(s, r, microseconds).await,
            Connection::Tls(s) => lru_crawler_sleep_cmd(s, microseconds).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, LruCrawlerArg, StatsArg};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    /// ] {
    ///     let _ = c.lru_crawler(LruCrawlerArg::Enable).await;
    ///     assert!(c.lru_crawler_tocrawl(1).await.is_ok());
    ///     let settings = c.stats(Some(StatsArg::Settings)).await?;
    ///     assert_eq!(
    ///         settings.get("lru_crawler_tocrawl").map(String::as_str),
    ///         Some("1")
    ///     );
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn lru_crawler_tocrawl(&mut self, arg: u32) -> Result<()> {
        match self {
            Connection::Tcp(s) => lru_crawler_tocrawl_cmd(s, arg).await,
            #[cfg(unix)]
            Connection::Unix(s) => lru_crawler_tocrawl_cmd(s, arg).await,
            Connection::Udp(s, r) => lru_crawler_tocrawl_cmd_udp(s, r, arg).await,
            Connection::Tls(s) => lru_crawler_tocrawl_cmd(s, arg).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, LruCrawlerArg, LruCrawlerCrawlArg};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    /// ] {
    ///     c.lru_crawler_crawl(LruCrawlerCrawlArg::All).await?;
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn lru_crawler_crawl(
        &mut self,
        arg: LruCrawlerCrawlArg<'_>,
    ) -> Result<LruCrawlerResult> {
        match self {
            Connection::Tcp(s) => lru_crawler_crawl_cmd(s, arg).await,
            #[cfg(unix)]
            Connection::Unix(s) => lru_crawler_crawl_cmd(s, arg).await,
            Connection::Udp(s, r) => lru_crawler_crawl_cmd_udp(s, r, arg).await,
            Connection::Tls(s) => lru_crawler_crawl_cmd(s, arg).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    /// ] {
    ///     c.slabs_reassign(1, 2).await?;
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn slabs_reassign(
        &mut self,
        source_class: isize,
        dest_class: isize,
    ) -> Result<SlabsReassignResult> {
        match self {
            Connection::Tcp(s) => slabs_reassign_cmd(s, source_class, dest_class).await,
            #[cfg(unix)]
            Connection::Unix(s) => slabs_reassign_cmd(s, source_class, dest_class).await,
            Connection::Udp(s, r) => slabs_reassign_cmd_udp(s, r, source_class, dest_class).await,
            Connection::Tls(s) => slabs_reassign_cmd(s, source_class, dest_class).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, LruCrawlerArg, LruCrawlerMetadumpArg, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    /// ] {
    ///     assert_eq!(
    ///         c.set(b"doc_conn_metadump", 0, 0, false, b"value").await?,
    ///         StorageResult::Stored
    ///     );
    ///     c.lru_crawler_metadump(LruCrawlerMetadumpArg::All).await?;
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn lru_crawler_metadump(
        &mut self,
        arg: LruCrawlerMetadumpArg<'_>,
    ) -> Result<LruCrawlerDumpResult> {
        match self {
            Connection::Tcp(s) => lru_crawler_metadump_cmd(s, arg).await,
            #[cfg(unix)]
            Connection::Unix(s) => lru_crawler_metadump_cmd(s, arg).await,
            Connection::Udp(_s, _r) => unreachable!("this command not work with udp connection!"),
            Connection::Tls(s) => lru_crawler_metadump_cmd(s, arg).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, LruCrawlerArg, LruCrawlerMgdumpArg, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    /// ] {
    ///     assert_eq!(
    ///         c.set(b"doc_conn_mgdump", 0, 0, false, b"value").await?,
    ///         StorageResult::Stored
    ///     );
    ///     c.lru_crawler_mgdump(LruCrawlerMgdumpArg::All).await?;
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn lru_crawler_mgdump(
        &mut self,
        arg: LruCrawlerMgdumpArg<'_>,
    ) -> Result<LruCrawlerDumpResult> {
        match self {
            Connection::Tcp(s) => lru_crawler_mgdump_cmd(s, arg).await,
            #[cfg(unix)]
            Connection::Unix(s) => lru_crawler_mgdump_cmd(s, arg).await,
            Connection::Udp(_s, _r) => unreachable!("this command not work with udp connection!"),
            Connection::Tls(s) => lru_crawler_mgdump_cmd(s, arg).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    /// ] {
    ///     assert!(c.mn().await.is_ok());
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn mn(&mut self) -> Result<()> {
        match self {
            Connection::Tcp(s) => mn_cmd(s).await,
            #[cfg(unix)]
            Connection::Unix(s) => mn_cmd(s).await,
            Connection::Udp(s, r) => mn_cmd_udp(s, r).await,
            Connection::Tls(s) => mn_cmd(s).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    /// ] {
    ///     assert_eq!(
    ///         c.set(b"doc_conn_me", 0, 0, false, b"value").await?,
    ///         StorageResult::Stored
    ///     );
    ///     let info = c.me(b"doc_conn_me").await?.unwrap();
    ///     assert!(info.starts_with("doc_conn_me "));
    ///     assert!(info.contains("size="));
    ///     assert_eq!(c.me(b"doc_conn_me_missing").await?, None);
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn me(&mut self, key: impl AsRef<[u8]>) -> Result<Option<String>> {
        match self {
            Connection::Tcp(s) => me_cmd(s, key.as_ref()).await,
            #[cfg(unix)]
            Connection::Unix(s) => me_cmd(s, key.as_ref()).await,
            Connection::Udp(s, r) => me_cmd_udp(s, r, key.as_ref()).await,
            Connection::Tls(s) => me_cmd(s, key.as_ref()).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, StorageResult, WatchArg};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for (mut c1, mut c2) in [
    ///     (Connection::default().await?, Connection::default().await?),
    ///     (
    ///         Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///         Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     ),
    ///     (
    ///         Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    ///         Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    ///     ),
    /// ] {
    ///     let mut watcher = c1.watch(&[WatchArg::Mutations]).await?;
    ///     assert_eq!(
    ///         c2.set(b"doc_conn_watch", 0, 0, false, b"value").await?,
    ///         StorageResult::Stored
    ///     );
    ///     let mut found = false;
    ///     for _ in 0..100 {
    ///         match watcher.message().await? {
    ///             Some(message) if message.contains("key=doc_conn_watch") => {
    ///                 found = true;
    ///                 break;
    ///             }
    ///             Some(_) => continue,
    ///             None => break,
    ///         }
    ///     }
    ///     assert!(found);
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn watch(mut self, arg: &[WatchArg]) -> Result<WatchStream> {
        match &mut self {
            Connection::Tcp(s) => watch_cmd(s, arg).await?,
            #[cfg(unix)]
            Connection::Unix(s) => watch_cmd(s, arg).await?,
            Connection::Udp(_s, _r) => unreachable!("this command not work with udp!"),
            Connection::Tls(s) => watch_cmd(s, arg).await?,
        };
        Ok(WatchStream(self))
    }

    pub fn pipeline(&mut self) -> Pipeline<'_> {
        Pipeline::new(self)
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, MgFlag, MgStatus, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    /// ] {
    ///     assert_eq!(
    ///         c.set(b"doc_conn_mg", 7, 0, false, b"hello").await?,
    ///         StorageResult::Stored
    ///     );
    ///     let item = c
    ///         .mg(
    ///             b"doc_conn_mg",
    ///             &[
    ///                 MgFlag::ReturnKey,
    ///                 MgFlag::ReturnFlags,
    ///                 MgFlag::ReturnSize,
    ///                 MgFlag::ReturnTtl,
    ///                 MgFlag::ReturnValue,
    ///                 MgFlag::ReturnCas,
    ///             ],
    ///         )
    ///         .await?;
    ///     assert_eq!(item.status, MgStatus::WithBody);
    ///     assert_eq!(item.key.as_deref(), Some("doc_conn_mg"));
    ///     assert_eq!(item.flags, Some(7));
    ///     assert_eq!(item.size, Some(5));
    ///     assert_eq!(item.ttl, Some(-1));
    ///     assert_eq!(item.data_block.as_deref(), Some(&b"hello"[..]));
    ///     assert!(item.cas.is_some());
    ///     let item = c.mg(b"doc_conn_mg_missing", &[MgFlag::ReturnKey]).await?;
    ///     assert_eq!(item.status, MgStatus::NotFound);
    ///     assert_eq!(item.key.as_deref(), Some("doc_conn_mg_missing"));
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn mg(&mut self, key: impl AsRef<[u8]>, flags: &[MgFlag]) -> Result<MgItem> {
        match self {
            Connection::Tcp(s) => mg_cmd(s, key.as_ref(), flags).await,
            #[cfg(unix)]
            Connection::Unix(s) => mg_cmd(s, key.as_ref(), flags).await,
            Connection::Udp(s, r) => mg_cmd_udp(s, r, key.as_ref(), flags).await,
            Connection::Tls(s) => mg_cmd(s, key.as_ref(), flags).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, MsFlag, MsMode, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    /// ] {
    ///     let _ = c.delete(b"doc_conn_ms", false).await?;
    ///     let item = c
    ///         .ms(
    ///             b"doc_conn_ms",
    ///             &[MsFlag::ReturnKey, MsFlag::ReturnSize, MsFlag::ReturnCas],
    ///             b"hello",
    ///         )
    ///         .await?;
    ///     assert_eq!(item.status, StorageResult::Stored);
    ///     assert_eq!(item.key.as_deref(), Some("doc_conn_ms"));
    ///     assert_eq!(item.size, Some(5));
    ///     assert!(item.cas.is_some());
    ///     let item = c
    ///         .ms(
    ///             b"doc_conn_ms",
    ///             &[MsFlag::Mode(MsMode::Add), MsFlag::ReturnKey],
    ///             b"again",
    ///         )
    ///         .await?;
    ///     assert_eq!(item.status, StorageResult::NotStored);
    ///     assert_eq!(c.get(b"doc_conn_ms").await?.unwrap().data_block, b"hello");
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn ms(
        &mut self,
        key: impl AsRef<[u8]>,
        flags: &[MsFlag],
        data_block: impl AsRef<[u8]>,
    ) -> Result<MsItem> {
        match self {
            Connection::Tcp(s) => ms_cmd(s, key.as_ref(), flags, data_block.as_ref()).await,
            #[cfg(unix)]
            Connection::Unix(s) => ms_cmd(s, key.as_ref(), flags, data_block.as_ref()).await,
            Connection::Udp(s, r) => {
                ms_cmd_udp(s, r, key.as_ref(), flags, data_block.as_ref()).await
            }
            Connection::Tls(s) => ms_cmd(s, key.as_ref(), flags, data_block.as_ref()).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, MdFlag, MdStatus, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    /// ] {
    ///     assert_eq!(
    ///         c.set(b"doc_conn_md", 0, 0, false, b"value").await?,
    ///         StorageResult::Stored
    ///     );
    ///     let item = c.md(b"doc_conn_md", &[MdFlag::ReturnKey]).await?;
    ///     assert_eq!(item.status, MdStatus::Deleted);
    ///     assert_eq!(item.key.as_deref(), Some("doc_conn_md"));
    ///     assert_eq!(c.get(b"doc_conn_md").await?, None);
    ///     let item = c.md(b"doc_conn_md", &[MdFlag::ReturnKey]).await?;
    ///     assert_eq!(item.status, MdStatus::NotFound);
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn md(&mut self, key: impl AsRef<[u8]>, flags: &[MdFlag]) -> Result<MdItem> {
        match self {
            Connection::Tcp(s) => md_cmd(s, key.as_ref(), flags).await,
            #[cfg(unix)]
            Connection::Unix(s) => md_cmd(s, key.as_ref(), flags).await,
            Connection::Udp(s, r) => md_cmd_udp(s, r, key.as_ref(), flags).await,
            Connection::Tls(s) => md_cmd(s, key.as_ref(), flags).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, MaFlag, MaMode, MaStatus, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    /// ] {
    ///     let _ = c.delete(b"doc_conn_ma", false).await?;
    ///     let item = c
    ///         .ma(
    ///             b"doc_conn_ma",
    ///             &[
    ///                 MaFlag::AutoCreate(0),
    ///                 MaFlag::InitValue(5),
    ///                 MaFlag::Mode(MaMode::Incr),
    ///                 MaFlag::ReturnValue,
    ///                 MaFlag::ReturnKey,
    ///             ],
    ///         )
    ///         .await?;
    ///     assert_eq!(item.status, MaStatus::WithBody);
    ///     assert_eq!(item.number, Some(5));
    ///     assert_eq!(item.key.as_deref(), Some("doc_conn_ma"));
    ///     let item = c
    ///         .ma(
    ///             b"doc_conn_ma",
    ///             &[
    ///                 MaFlag::DeltaApply(7),
    ///                 MaFlag::Mode(MaMode::Incr),
    ///                 MaFlag::ReturnValue,
    ///                 MaFlag::ReturnKey,
    ///             ],
    ///         )
    ///         .await?;
    ///     assert_eq!(item.status, MaStatus::WithBody);
    ///     assert_eq!(item.number, Some(12));
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn ma(&mut self, key: impl AsRef<[u8]>, flags: &[MaFlag]) -> Result<MaItem> {
        match self {
            Connection::Tcp(s) => ma_cmd(s, key.as_ref(), flags).await,
            #[cfg(unix)]
            Connection::Unix(s) => ma_cmd(s, key.as_ref(), flags).await,
            Connection::Udp(s, r) => ma_cmd_udp(s, r, key.as_ref(), flags).await,
            Connection::Tls(s) => ma_cmd(s, key.as_ref(), flags).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, LruArg, LruMode};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    /// ] {
    ///     c.lru(LruArg::Mode(LruMode::Flat)).await?;
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn lru(&mut self, arg: LruArg) -> Result<()> {
        match self {
            Connection::Tcp(s) => lru_cmd(s, arg).await,
            #[cfg(unix)]
            Connection::Unix(s) => lru_cmd(s, arg).await,
            Connection::Udp(s, r) => lru_cmd_udp(s, r, arg).await,
            Connection::Tls(s) => lru_cmd(s, arg).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, VerbosityArg};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    /// ] {
    ///     assert!(c.verbosity(VerbosityArg::Off, false).await.is_ok());
    ///     assert!(c.verbosity(VerbosityArg::Off, true).await.is_ok());
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn verbosity(&mut self, arg: VerbosityArg, noreply: bool) -> Result<()> {
        match self {
            Connection::Tcp(s) => verbosity_cmd(s, arg, noreply).await,
            #[cfg(unix)]
            Connection::Unix(s) => verbosity_cmd(s, arg, noreply).await,
            Connection::Udp(s, r) => verbosity_cmd_udp(s, r, arg, noreply).await,
            Connection::Tls(s) => verbosity_cmd(s, arg, noreply).await,
        }
    }
}

pub struct WatchStream(Connection);
impl WatchStream {
    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, StorageResult, WatchArg};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for (mut c1, mut c2) in [
    ///     (Connection::default().await?, Connection::default().await?),
    ///     (
    ///         Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///         Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     ),
    ///     (
    ///         Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    ///         Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    ///     ),
    /// ] {
    ///     let mut watcher = c1.watch(&[WatchArg::Mutations]).await?;
    ///     assert_eq!(
    ///         c2.set(b"doc_watch_message", 0, 0, false, b"value").await?,
    ///         StorageResult::Stored
    ///     );
    ///     let mut found = false;
    ///     for _ in 0..100 {
    ///         match watcher.message().await? {
    ///             Some(message) if message.contains("key=doc_watch_message") => {
    ///                 found = true;
    ///                 break;
    ///             }
    ///             Some(_) => continue,
    ///             None => break,
    ///         }
    ///     }
    ///     assert!(found);
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn message(&mut self) -> Result<Option<String>> {
        let mut line = String::new();
        let n = match &mut self.0 {
            Connection::Tcp(s) => s.read_line(&mut line).await?,
            #[cfg(unix)]
            Connection::Unix(s) => s.read_line(&mut line).await?,
            Connection::Udp(_s, _r) => unreachable!("this command not work with udp connection"),
            Connection::Tls(s) => s.read_line(&mut line).await?,
        };
        if n == 0 {
            Ok(None)
        } else {
            Ok(Some(line.trim_end().to_string()))
        }
    }
}

pub struct ClientCrc32(Vec<Connection>);
impl ClientCrc32 {
    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientCrc32, Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientCrc32::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// assert_eq!(
    ///     client.set(b"doc_crc32_new", 0, 0, false, b"value").await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client.get(b"doc_crc32_new").await?.unwrap().data_block,
    ///     b"value"
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn new(conns: Vec<Connection>) -> Self {
        Self(conns)
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientCrc32, Connection, Item, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientCrc32::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// assert_eq!(
    ///     client.set(b"doc_crc32_get", 0, 0, false, b"value").await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client.get(b"doc_crc32_get").await?,
    ///     Some(Item {
    ///         key: "doc_crc32_get".to_string(),
    ///         flags: 0,
    ///         cas_unique: None,
    ///         data_block: b"value".to_vec()
    ///     })
    /// );
    /// assert_eq!(client.get(b"doc_crc32_get_missing").await?, None);
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn get(&mut self, key: impl AsRef<[u8]>) -> Result<Option<Item>> {
        let size = self.0.len();
        self.0[crc32(key.as_ref()) as usize % size]
            .get(key.as_ref())
            .await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientCrc32, Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientCrc32::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// assert_eq!(
    ///     client.set(b"doc_crc32_gets", 0, 0, false, b"value").await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// let item = client.gets(b"doc_crc32_gets").await?.unwrap();
    /// assert_eq!(item.key, "doc_crc32_gets");
    /// assert_eq!(item.data_block, b"value");
    /// assert!(item.cas_unique.is_some());
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn gets(&mut self, key: impl AsRef<[u8]>) -> Result<Option<Item>> {
        let size = self.0.len();
        self.0[crc32(key.as_ref()) as usize % size]
            .gets(key.as_ref())
            .await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientCrc32, Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientCrc32::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// assert_eq!(
    ///     client.set(b"doc_crc32_gat", 7, 0, false, b"value").await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// let item = client.gat(60, b"doc_crc32_gat").await?.unwrap();
    /// assert_eq!(item.key, "doc_crc32_gat");
    /// assert_eq!(item.flags, 7);
    /// assert_eq!(item.data_block, b"value");
    /// assert_eq!(item.cas_unique, None);
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn gat(&mut self, exptime: i64, key: impl AsRef<[u8]>) -> Result<Option<Item>> {
        let size = self.0.len();
        self.0[crc32(key.as_ref()) as usize % size]
            .gat(exptime, key.as_ref())
            .await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientCrc32, Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientCrc32::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// assert_eq!(
    ///     client.set(b"doc_crc32_gats", 7, 0, false, b"value").await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// let item = client.gats(60, b"doc_crc32_gats").await?.unwrap();
    /// assert_eq!(item.key, "doc_crc32_gats");
    /// assert_eq!(item.flags, 7);
    /// assert_eq!(item.data_block, b"value");
    /// assert!(item.cas_unique.is_some());
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn gats(&mut self, exptime: i64, key: impl AsRef<[u8]>) -> Result<Option<Item>> {
        let size = self.0.len();
        self.0[crc32(key.as_ref()) as usize % size]
            .gats(exptime, key.as_ref())
            .await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientCrc32, Connection, Item, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientCrc32::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// assert_eq!(
    ///     client.set(b"doc_crc32_set", 7, 0, false, b"value").await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client.get(b"doc_crc32_set").await?,
    ///     Some(Item {
    ///         key: "doc_crc32_set".to_string(),
    ///         flags: 7,
    ///         cas_unique: None,
    ///         data_block: b"value".to_vec()
    ///     })
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn set(
        &mut self,
        key: impl AsRef<[u8]>,
        flags: u32,
        exptime: i64,
        noreply: bool,
        data_block: impl AsRef<[u8]>,
    ) -> Result<StorageResult> {
        let size = self.0.len();
        self.0[crc32(key.as_ref()) as usize % size]
            .set(key.as_ref(), flags, exptime, noreply, data_block.as_ref())
            .await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientCrc32, Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientCrc32::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// let _ = client.delete(b"doc_crc32_add", false).await?;
    /// assert_eq!(
    ///     client.add(b"doc_crc32_add", 0, 0, false, b"first").await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client.add(b"doc_crc32_add", 0, 0, false, b"second").await?,
    ///     mcmc_rs::StorageResult::NotStored
    /// );
    /// assert_eq!(
    ///     client.get(b"doc_crc32_add").await?.unwrap().data_block,
    ///     b"first"
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn add(
        &mut self,
        key: impl AsRef<[u8]>,
        flags: u32,
        exptime: i64,
        noreply: bool,
        data_block: impl AsRef<[u8]>,
    ) -> Result<StorageResult> {
        let size = self.0.len();
        self.0[crc32(key.as_ref()) as usize % size]
            .add(key.as_ref(), flags, exptime, noreply, data_block.as_ref())
            .await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientCrc32, Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientCrc32::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// let _ = client.delete(b"doc_crc32_replace", false).await?;
    /// assert_eq!(
    ///     client
    ///         .replace(b"doc_crc32_replace", 0, 0, false, b"value")
    ///         .await?,
    ///     mcmc_rs::StorageResult::NotStored
    /// );
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_crc32_replace", 0, 0, false, b"first")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client
    ///         .replace(b"doc_crc32_replace", 0, 0, false, b"second")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client.get(b"doc_crc32_replace").await?.unwrap().data_block,
    ///     b"second"
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn replace(
        &mut self,
        key: impl AsRef<[u8]>,
        flags: u32,
        exptime: i64,
        noreply: bool,
        data_block: impl AsRef<[u8]>,
    ) -> Result<StorageResult> {
        let size = self.0.len();
        self.0[crc32(key.as_ref()) as usize % size]
            .replace(key.as_ref(), flags, exptime, noreply, data_block.as_ref())
            .await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientCrc32, Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientCrc32::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// let _ = client.delete(b"doc_crc32_append", false).await?;
    /// assert_eq!(
    ///     client
    ///         .append(b"doc_crc32_append", 0, 0, false, b"-tail")
    ///         .await?,
    ///     mcmc_rs::StorageResult::NotStored
    /// );
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_crc32_append", 0, 0, false, b"base")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client
    ///         .append(b"doc_crc32_append", 0, 0, false, b"-tail")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client.get(b"doc_crc32_append").await?.unwrap().data_block,
    ///     b"base-tail"
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn append(
        &mut self,
        key: impl AsRef<[u8]>,
        flags: u32,
        exptime: i64,
        noreply: bool,
        data_block: impl AsRef<[u8]>,
    ) -> Result<StorageResult> {
        let size = self.0.len();
        self.0[crc32(key.as_ref()) as usize % size]
            .append(key.as_ref(), flags, exptime, noreply, data_block.as_ref())
            .await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientCrc32, Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientCrc32::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// let _ = client.delete(b"doc_crc32_prepend", false).await?;
    /// assert_eq!(
    ///     client
    ///         .prepend(b"doc_crc32_prepend", 0, 0, false, b"head-")
    ///         .await?,
    ///     mcmc_rs::StorageResult::NotStored
    /// );
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_crc32_prepend", 0, 0, false, b"base")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client
    ///         .prepend(b"doc_crc32_prepend", 0, 0, false, b"head-")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client.get(b"doc_crc32_prepend").await?.unwrap().data_block,
    ///     b"head-base"
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn prepend(
        &mut self,
        key: impl AsRef<[u8]>,
        flags: u32,
        exptime: i64,
        noreply: bool,
        data_block: impl AsRef<[u8]>,
    ) -> Result<StorageResult> {
        let size = self.0.len();
        self.0[crc32(key.as_ref()) as usize % size]
            .prepend(key.as_ref(), flags, exptime, noreply, data_block.as_ref())
            .await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientCrc32, Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientCrc32::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// let _ = client.delete(b"doc_crc32_cas", false).await?;
    /// assert_eq!(
    ///     client
    ///         .cas(b"doc_crc32_cas", 0, 0, 0, false, b"value")
    ///         .await?,
    ///     mcmc_rs::StorageResult::NotFound
    /// );
    /// assert_eq!(
    ///     client.set(b"doc_crc32_cas", 0, 0, false, b"base").await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// let cas = client
    ///     .gets(b"doc_crc32_cas")
    ///     .await?
    ///     .unwrap()
    ///     .cas_unique
    ///     .unwrap();
    /// assert_eq!(
    ///     client
    ///         .cas(b"doc_crc32_cas", 0, 0, cas, false, b"new")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client
    ///         .cas(b"doc_crc32_cas", 0, 0, cas, false, b"stale")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Exists
    /// );
    /// assert_eq!(
    ///     client.get(b"doc_crc32_cas").await?.unwrap().data_block,
    ///     b"new"
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn cas(
        &mut self,
        key: impl AsRef<[u8]>,
        flags: u32,
        exptime: i64,
        cas_unique: u64,
        noreply: bool,
        data_block: impl AsRef<[u8]>,
    ) -> Result<StorageResult> {
        let size = self.0.len();
        self.0[crc32(key.as_ref()) as usize % size]
            .cas(
                key.as_ref(),
                flags,
                exptime,
                cas_unique,
                noreply,
                data_block.as_ref(),
            )
            .await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientCrc32, Connection, DeleteResult, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientCrc32::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_crc32_delete", 0, 0, false, b"value")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client.delete(b"doc_crc32_delete", false).await?,
    ///     mcmc_rs::DeleteResult::Deleted
    /// );
    /// assert_eq!(
    ///     client.delete(b"doc_crc32_delete", false).await?,
    ///     mcmc_rs::DeleteResult::NotFound
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn delete(&mut self, key: impl AsRef<[u8]>, noreply: bool) -> Result<DeleteResult> {
        let size = self.0.len();
        self.0[crc32(key.as_ref()) as usize % size]
            .delete(key.as_ref(), noreply)
            .await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientCrc32, Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientCrc32::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// let _ = client.delete(b"doc_crc32_incr", false).await?;
    /// assert_eq!(client.incr(b"doc_crc32_incr", 1, false).await?, None);
    /// assert_eq!(
    ///     client.set(b"doc_crc32_incr", 0, 0, false, b"10").await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(client.incr(b"doc_crc32_incr", 5, false).await?, Some(15));
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn incr(
        &mut self,
        key: impl AsRef<[u8]>,
        value: u64,
        noreply: bool,
    ) -> Result<Option<u64>> {
        let size = self.0.len();
        self.0[crc32(key.as_ref()) as usize % size]
            .incr(key.as_ref(), value, noreply)
            .await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientCrc32, Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientCrc32::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// let _ = client.delete(b"doc_crc32_decr", false).await?;
    /// assert_eq!(client.decr(b"doc_crc32_decr", 1, false).await?, None);
    /// assert_eq!(
    ///     client.set(b"doc_crc32_decr", 0, 0, false, b"10").await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(client.decr(b"doc_crc32_decr", 5, false).await?, Some(5));
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn decr(
        &mut self,
        key: impl AsRef<[u8]>,
        value: u64,
        noreply: bool,
    ) -> Result<Option<u64>> {
        let size = self.0.len();
        self.0[crc32(key.as_ref()) as usize % size]
            .decr(key.as_ref(), value, noreply)
            .await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientCrc32, Connection, TouchResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientCrc32::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// assert_eq!(
    ///     client.touch(b"doc_crc32_touch_missing", 60, false).await?,
    ///     mcmc_rs::TouchResult::NotFound
    /// );
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_crc32_touch", 0, 0, false, b"value")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client.touch(b"doc_crc32_touch", 60, false).await?,
    ///     mcmc_rs::TouchResult::Touched
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn touch(
        &mut self,
        key: impl AsRef<[u8]>,
        exptime: i64,
        noreply: bool,
    ) -> Result<TouchResult> {
        let size = self.0.len();
        self.0[crc32(key.as_ref()) as usize % size]
            .touch(key.as_ref(), exptime, noreply)
            .await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientCrc32, Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientCrc32::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// assert_eq!(
    ///     client.set(b"doc_crc32_me", 0, 0, false, b"value").await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// let info = client.me(b"doc_crc32_me").await?.unwrap();
    /// assert!(info.starts_with("doc_crc32_me "));
    /// assert!(info.contains("size="));
    /// assert_eq!(client.me(b"doc_crc32_me_missing").await?, None);
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn me(&mut self, key: impl AsRef<[u8]>) -> Result<Option<String>> {
        let size = self.0.len();
        self.0[crc32(key.as_ref()) as usize % size]
            .me(key.as_ref())
            .await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientCrc32, Connection, MgFlag, MgStatus, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientCrc32::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// assert_eq!(
    ///     client.set(b"doc_crc32_mg", 7, 0, false, b"hello").await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// let item = client
    ///     .mg(
    ///         b"doc_crc32_mg",
    ///         &[
    ///             MgFlag::ReturnKey,
    ///             MgFlag::ReturnFlags,
    ///             MgFlag::ReturnSize,
    ///             MgFlag::ReturnTtl,
    ///             MgFlag::ReturnValue,
    ///             MgFlag::ReturnCas,
    ///         ],
    ///     )
    ///     .await?;
    /// assert_eq!(item.status, mcmc_rs::MgStatus::WithBody);
    /// assert_eq!(item.key.as_deref(), Some("doc_crc32_mg"));
    /// assert_eq!(item.flags, Some(7));
    /// assert_eq!(item.size, Some(5));
    /// assert_eq!(item.ttl, Some(-1));
    /// assert_eq!(item.data_block.as_deref(), Some(&b"hello"[..]));
    /// assert!(item.cas.is_some());
    /// let item = client
    ///     .mg(b"doc_crc32_mg_missing", &[MgFlag::ReturnKey])
    ///     .await?;
    /// assert_eq!(item.status, mcmc_rs::MgStatus::NotFound);
    /// assert_eq!(item.key.as_deref(), Some("doc_crc32_mg_missing"));
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn mg(&mut self, key: impl AsRef<[u8]>, flags: &[MgFlag]) -> Result<MgItem> {
        let size = self.0.len();
        self.0[crc32(key.as_ref()) as usize % size]
            .mg(key.as_ref(), flags)
            .await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientCrc32, Connection, MsFlag, MsMode, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientCrc32::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// let _ = client.delete(b"doc_crc32_ms", false).await?;
    /// let item = client
    ///     .ms(
    ///         b"doc_crc32_ms",
    ///         &[MsFlag::ReturnKey, MsFlag::ReturnSize, MsFlag::ReturnCas],
    ///         b"hello",
    ///     )
    ///     .await?;
    /// assert_eq!(item.status, mcmc_rs::StorageResult::Stored);
    /// assert_eq!(item.key.as_deref(), Some("doc_crc32_ms"));
    /// assert_eq!(item.size, Some(5));
    /// assert!(item.cas.is_some());
    /// let item = client
    ///     .ms(
    ///         b"doc_crc32_ms",
    ///         &[MsFlag::Mode(MsMode::Add), MsFlag::ReturnKey],
    ///         b"again",
    ///     )
    ///     .await?;
    /// assert_eq!(item.status, mcmc_rs::StorageResult::NotStored);
    /// assert_eq!(
    ///     client.get(b"doc_crc32_ms").await?.unwrap().data_block,
    ///     b"hello"
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn ms(
        &mut self,
        key: impl AsRef<[u8]>,
        flags: &[MsFlag],
        data_block: impl AsRef<[u8]>,
    ) -> Result<MsItem> {
        let size = self.0.len();
        self.0[crc32(key.as_ref()) as usize % size]
            .ms(key.as_ref(), flags, data_block.as_ref())
            .await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientCrc32, Connection, MdFlag, MdStatus, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientCrc32::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// assert_eq!(
    ///     client.set(b"doc_crc32_md", 0, 0, false, b"value").await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// let item = client.md(b"doc_crc32_md", &[MdFlag::ReturnKey]).await?;
    /// assert_eq!(item.status, mcmc_rs::MdStatus::Deleted);
    /// assert_eq!(item.key.as_deref(), Some("doc_crc32_md"));
    /// assert_eq!(client.get(b"doc_crc32_md").await?, None);
    /// let item = client.md(b"doc_crc32_md", &[MdFlag::ReturnKey]).await?;
    /// assert_eq!(item.status, mcmc_rs::MdStatus::NotFound);
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn md(&mut self, key: impl AsRef<[u8]>, flags: &[MdFlag]) -> Result<MdItem> {
        let size = self.0.len();
        self.0[crc32(key.as_ref()) as usize % size]
            .md(key.as_ref(), flags)
            .await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientCrc32, Connection, MaFlag, MaMode, MaStatus, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientCrc32::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// let _ = client.delete(b"doc_crc32_ma", false).await?;
    /// let item = client
    ///     .ma(
    ///         b"doc_crc32_ma",
    ///         &[
    ///             MaFlag::AutoCreate(0),
    ///             MaFlag::InitValue(5),
    ///             MaFlag::Mode(MaMode::Incr),
    ///             MaFlag::ReturnValue,
    ///             MaFlag::ReturnKey,
    ///         ],
    ///     )
    ///     .await?;
    /// assert_eq!(item.status, mcmc_rs::MaStatus::WithBody);
    /// assert_eq!(item.number, Some(5));
    /// assert_eq!(item.key.as_deref(), Some("doc_crc32_ma"));
    /// let item = client
    ///     .ma(
    ///         b"doc_crc32_ma",
    ///         &[
    ///             MaFlag::DeltaApply(7),
    ///             MaFlag::Mode(MaMode::Incr),
    ///             MaFlag::ReturnValue,
    ///             MaFlag::ReturnKey,
    ///         ],
    ///     )
    ///     .await?;
    /// assert_eq!(item.status, mcmc_rs::MaStatus::WithBody);
    /// assert_eq!(item.number, Some(12));
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn ma(&mut self, key: impl AsRef<[u8]>, flags: &[MaFlag]) -> Result<MaItem> {
        let size = self.0.len();
        self.0[crc32(key.as_ref()) as usize % size]
            .ma(key.as_ref(), flags)
            .await
    }
}

pub struct ClientHashRing(Vec<Connection>, HashRing<usize>);
impl ClientHashRing {
    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientHashRing, Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientHashRing::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_hash_ring_new", 0, 0, false, b"value")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client.get(b"doc_hash_ring_new").await?.unwrap().data_block,
    ///     b"value"
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn new(conns: Vec<Connection>) -> Self {
        let mut ring = HashRing::new();
        ring.batch_add((0..conns.len()).collect());
        Self(conns, ring)
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientHashRing, Connection, Item, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientHashRing::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_hash_ring_get", 0, 0, false, b"value")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client.get(b"doc_hash_ring_get").await?,
    ///     Some(Item {
    ///         key: "doc_hash_ring_get".to_string(),
    ///         flags: 0,
    ///         cas_unique: None,
    ///         data_block: b"value".to_vec()
    ///     })
    /// );
    /// assert_eq!(client.get(b"doc_hash_ring_get_missing").await?, None);
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn get(&mut self, key: impl AsRef<[u8]>) -> Result<Option<Item>> {
        let i = *self.1.get(&key.as_ref()).unwrap();
        self.0[i].get(key.as_ref()).await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientHashRing, Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientHashRing::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_hash_ring_gets", 0, 0, false, b"value")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// let item = client.gets(b"doc_hash_ring_gets").await?.unwrap();
    /// assert_eq!(item.key, "doc_hash_ring_gets");
    /// assert_eq!(item.data_block, b"value");
    /// assert!(item.cas_unique.is_some());
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn gets(&mut self, key: impl AsRef<[u8]>) -> Result<Option<Item>> {
        let i = *self.1.get(&key.as_ref()).unwrap();
        self.0[i].gets(key.as_ref()).await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientHashRing, Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientHashRing::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_hash_ring_gat", 7, 0, false, b"value")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// let item = client.gat(60, b"doc_hash_ring_gat").await?.unwrap();
    /// assert_eq!(item.key, "doc_hash_ring_gat");
    /// assert_eq!(item.flags, 7);
    /// assert_eq!(item.data_block, b"value");
    /// assert_eq!(item.cas_unique, None);
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn gat(&mut self, exptime: i64, key: impl AsRef<[u8]>) -> Result<Option<Item>> {
        let i = *self.1.get(&key.as_ref()).unwrap();
        self.0[i].gat(exptime, key.as_ref()).await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientHashRing, Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientHashRing::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_hash_ring_gats", 7, 0, false, b"value")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// let item = client.gats(60, b"doc_hash_ring_gats").await?.unwrap();
    /// assert_eq!(item.key, "doc_hash_ring_gats");
    /// assert_eq!(item.flags, 7);
    /// assert_eq!(item.data_block, b"value");
    /// assert!(item.cas_unique.is_some());
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn gats(&mut self, exptime: i64, key: impl AsRef<[u8]>) -> Result<Option<Item>> {
        let i = *self.1.get(&key.as_ref()).unwrap();
        self.0[i].gats(exptime, key.as_ref()).await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientHashRing, Connection, Item, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientHashRing::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_hash_ring_set", 7, 0, false, b"value")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client.get(b"doc_hash_ring_set").await?,
    ///     Some(Item {
    ///         key: "doc_hash_ring_set".to_string(),
    ///         flags: 7,
    ///         cas_unique: None,
    ///         data_block: b"value".to_vec()
    ///     })
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn set(
        &mut self,
        key: impl AsRef<[u8]>,
        flags: u32,
        exptime: i64,
        noreply: bool,
        data_block: impl AsRef<[u8]>,
    ) -> Result<StorageResult> {
        let i = *self.1.get(&key.as_ref()).unwrap();
        self.0[i]
            .set(key.as_ref(), flags, exptime, noreply, data_block.as_ref())
            .await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientHashRing, Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientHashRing::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// let _ = client.delete(b"doc_hash_ring_add", false).await?;
    /// assert_eq!(
    ///     client
    ///         .add(b"doc_hash_ring_add", 0, 0, false, b"first")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client
    ///         .add(b"doc_hash_ring_add", 0, 0, false, b"second")
    ///         .await?,
    ///     mcmc_rs::StorageResult::NotStored
    /// );
    /// assert_eq!(
    ///     client.get(b"doc_hash_ring_add").await?.unwrap().data_block,
    ///     b"first"
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn add(
        &mut self,
        key: impl AsRef<[u8]>,
        flags: u32,
        exptime: i64,
        noreply: bool,
        data_block: impl AsRef<[u8]>,
    ) -> Result<StorageResult> {
        let i = *self.1.get(&key.as_ref()).unwrap();
        self.0[i]
            .add(key.as_ref(), flags, exptime, noreply, data_block.as_ref())
            .await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientHashRing, Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientHashRing::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// let _ = client.delete(b"doc_hash_ring_replace", false).await?;
    /// assert_eq!(
    ///     client
    ///         .replace(b"doc_hash_ring_replace", 0, 0, false, b"value")
    ///         .await?,
    ///     mcmc_rs::StorageResult::NotStored
    /// );
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_hash_ring_replace", 0, 0, false, b"first")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client
    ///         .replace(b"doc_hash_ring_replace", 0, 0, false, b"second")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client
    ///         .get(b"doc_hash_ring_replace")
    ///         .await?
    ///         .unwrap()
    ///         .data_block,
    ///     b"second"
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn replace(
        &mut self,
        key: impl AsRef<[u8]>,
        flags: u32,
        exptime: i64,
        noreply: bool,
        data_block: impl AsRef<[u8]>,
    ) -> Result<StorageResult> {
        let i = *self.1.get(&key.as_ref()).unwrap();
        self.0[i]
            .replace(key.as_ref(), flags, exptime, noreply, data_block.as_ref())
            .await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientHashRing, Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientHashRing::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// let _ = client.delete(b"doc_hash_ring_append", false).await?;
    /// assert_eq!(
    ///     client
    ///         .append(b"doc_hash_ring_append", 0, 0, false, b"-tail")
    ///         .await?,
    ///     mcmc_rs::StorageResult::NotStored
    /// );
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_hash_ring_append", 0, 0, false, b"base")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client
    ///         .append(b"doc_hash_ring_append", 0, 0, false, b"-tail")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client
    ///         .get(b"doc_hash_ring_append")
    ///         .await?
    ///         .unwrap()
    ///         .data_block,
    ///     b"base-tail"
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn append(
        &mut self,
        key: impl AsRef<[u8]>,
        flags: u32,
        exptime: i64,
        noreply: bool,
        data_block: impl AsRef<[u8]>,
    ) -> Result<StorageResult> {
        let i = *self.1.get(&key.as_ref()).unwrap();
        self.0[i]
            .append(key.as_ref(), flags, exptime, noreply, data_block.as_ref())
            .await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientHashRing, Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientHashRing::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// let _ = client.delete(b"doc_hash_ring_prepend", false).await?;
    /// assert_eq!(
    ///     client
    ///         .prepend(b"doc_hash_ring_prepend", 0, 0, false, b"head-")
    ///         .await?,
    ///     mcmc_rs::StorageResult::NotStored
    /// );
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_hash_ring_prepend", 0, 0, false, b"base")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client
    ///         .prepend(b"doc_hash_ring_prepend", 0, 0, false, b"head-")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client
    ///         .get(b"doc_hash_ring_prepend")
    ///         .await?
    ///         .unwrap()
    ///         .data_block,
    ///     b"head-base"
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn prepend(
        &mut self,
        key: impl AsRef<[u8]>,
        flags: u32,
        exptime: i64,
        noreply: bool,
        data_block: impl AsRef<[u8]>,
    ) -> Result<StorageResult> {
        let i = *self.1.get(&key.as_ref()).unwrap();
        self.0[i]
            .prepend(key.as_ref(), flags, exptime, noreply, data_block.as_ref())
            .await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientHashRing, Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientHashRing::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// let _ = client.delete(b"doc_hash_ring_cas", false).await?;
    /// assert_eq!(
    ///     client
    ///         .cas(b"doc_hash_ring_cas", 0, 0, 0, false, b"value")
    ///         .await?,
    ///     mcmc_rs::StorageResult::NotFound
    /// );
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_hash_ring_cas", 0, 0, false, b"base")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// let cas = client
    ///     .gets(b"doc_hash_ring_cas")
    ///     .await?
    ///     .unwrap()
    ///     .cas_unique
    ///     .unwrap();
    /// assert_eq!(
    ///     client
    ///         .cas(b"doc_hash_ring_cas", 0, 0, cas, false, b"new")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client
    ///         .cas(b"doc_hash_ring_cas", 0, 0, cas, false, b"stale")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Exists
    /// );
    /// assert_eq!(
    ///     client.get(b"doc_hash_ring_cas").await?.unwrap().data_block,
    ///     b"new"
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn cas(
        &mut self,
        key: impl AsRef<[u8]>,
        flags: u32,
        exptime: i64,
        cas_unique: u64,
        noreply: bool,
        data_block: impl AsRef<[u8]>,
    ) -> Result<StorageResult> {
        let i = *self.1.get(&key.as_ref()).unwrap();
        self.0[i]
            .cas(
                key.as_ref(),
                flags,
                exptime,
                cas_unique,
                noreply,
                data_block.as_ref(),
            )
            .await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientHashRing, Connection, DeleteResult, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientHashRing::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_hash_ring_delete", 0, 0, false, b"value")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client.delete(b"doc_hash_ring_delete", false).await?,
    ///     mcmc_rs::DeleteResult::Deleted
    /// );
    /// assert_eq!(
    ///     client.delete(b"doc_hash_ring_delete", false).await?,
    ///     mcmc_rs::DeleteResult::NotFound
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn delete(&mut self, key: impl AsRef<[u8]>, noreply: bool) -> Result<DeleteResult> {
        let i = *self.1.get(&key.as_ref()).unwrap();
        self.0[i].delete(key.as_ref(), noreply).await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientHashRing, Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientHashRing::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// let _ = client.delete(b"doc_hash_ring_incr", false).await?;
    /// assert_eq!(client.incr(b"doc_hash_ring_incr", 1, false).await?, None);
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_hash_ring_incr", 0, 0, false, b"10")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client.incr(b"doc_hash_ring_incr", 5, false).await?,
    ///     Some(15)
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn incr(
        &mut self,
        key: impl AsRef<[u8]>,
        value: u64,
        noreply: bool,
    ) -> Result<Option<u64>> {
        let i = *self.1.get(&key.as_ref()).unwrap();
        self.0[i].incr(key.as_ref(), value, noreply).await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientHashRing, Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientHashRing::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// let _ = client.delete(b"doc_hash_ring_decr", false).await?;
    /// assert_eq!(client.decr(b"doc_hash_ring_decr", 1, false).await?, None);
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_hash_ring_decr", 0, 0, false, b"10")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(client.decr(b"doc_hash_ring_decr", 5, false).await?, Some(5));
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn decr(
        &mut self,
        key: impl AsRef<[u8]>,
        value: u64,
        noreply: bool,
    ) -> Result<Option<u64>> {
        let i = *self.1.get(&key.as_ref()).unwrap();
        self.0[i].decr(key.as_ref(), value, noreply).await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientHashRing, Connection, TouchResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientHashRing::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// assert_eq!(
    ///     client
    ///         .touch(b"doc_hash_ring_touch_missing", 60, false)
    ///         .await?,
    ///     mcmc_rs::TouchResult::NotFound
    /// );
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_hash_ring_touch", 0, 0, false, b"value")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client.touch(b"doc_hash_ring_touch", 60, false).await?,
    ///     mcmc_rs::TouchResult::Touched
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn touch(
        &mut self,
        key: impl AsRef<[u8]>,
        exptime: i64,
        noreply: bool,
    ) -> Result<TouchResult> {
        let i = *self.1.get(&key.as_ref()).unwrap();
        self.0[i].touch(key.as_ref(), exptime, noreply).await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientHashRing, Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientHashRing::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_hash_ring_me", 0, 0, false, b"value")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// let info = client.me(b"doc_hash_ring_me").await?.unwrap();
    /// assert!(info.starts_with("doc_hash_ring_me "));
    /// assert!(info.contains("size="));
    /// assert_eq!(client.me(b"doc_hash_ring_me_missing").await?, None);
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn me(&mut self, key: impl AsRef<[u8]>) -> Result<Option<String>> {
        let i = *self.1.get(&key.as_ref()).unwrap();
        self.0[i].me(key.as_ref()).await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientHashRing, Connection, MgFlag, MgStatus, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientHashRing::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_hash_ring_mg", 7, 0, false, b"hello")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// let item = client
    ///     .mg(
    ///         b"doc_hash_ring_mg",
    ///         &[
    ///             MgFlag::ReturnKey,
    ///             MgFlag::ReturnFlags,
    ///             MgFlag::ReturnSize,
    ///             MgFlag::ReturnTtl,
    ///             MgFlag::ReturnValue,
    ///             MgFlag::ReturnCas,
    ///         ],
    ///     )
    ///     .await?;
    /// assert_eq!(item.status, mcmc_rs::MgStatus::WithBody);
    /// assert_eq!(item.key.as_deref(), Some("doc_hash_ring_mg"));
    /// assert_eq!(item.flags, Some(7));
    /// assert_eq!(item.size, Some(5));
    /// assert_eq!(item.ttl, Some(-1));
    /// assert_eq!(item.data_block.as_deref(), Some(&b"hello"[..]));
    /// assert!(item.cas.is_some());
    /// let item = client
    ///     .mg(b"doc_hash_ring_mg_missing", &[MgFlag::ReturnKey])
    ///     .await?;
    /// assert_eq!(item.status, mcmc_rs::MgStatus::NotFound);
    /// assert_eq!(item.key.as_deref(), Some("doc_hash_ring_mg_missing"));
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn mg(&mut self, key: impl AsRef<[u8]>, flags: &[MgFlag]) -> Result<MgItem> {
        let i = *self.1.get(&key.as_ref()).unwrap();
        self.0[i].mg(key.as_ref(), flags).await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientHashRing, Connection, MsFlag, MsMode, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientHashRing::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// let _ = client.delete(b"doc_hash_ring_ms", false).await?;
    /// let item = client
    ///     .ms(
    ///         b"doc_hash_ring_ms",
    ///         &[MsFlag::ReturnKey, MsFlag::ReturnSize, MsFlag::ReturnCas],
    ///         b"hello",
    ///     )
    ///     .await?;
    /// assert_eq!(item.status, mcmc_rs::StorageResult::Stored);
    /// assert_eq!(item.key.as_deref(), Some("doc_hash_ring_ms"));
    /// assert_eq!(item.size, Some(5));
    /// assert!(item.cas.is_some());
    /// let item = client
    ///     .ms(
    ///         b"doc_hash_ring_ms",
    ///         &[MsFlag::Mode(MsMode::Add), MsFlag::ReturnKey],
    ///         b"again",
    ///     )
    ///     .await?;
    /// assert_eq!(item.status, mcmc_rs::StorageResult::NotStored);
    /// assert_eq!(
    ///     client.get(b"doc_hash_ring_ms").await?.unwrap().data_block,
    ///     b"hello"
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn ms(
        &mut self,
        key: impl AsRef<[u8]>,
        flags: &[MsFlag],
        data_block: impl AsRef<[u8]>,
    ) -> Result<MsItem> {
        let i = *self.1.get(&key.as_ref()).unwrap();
        self.0[i].ms(key.as_ref(), flags, data_block.as_ref()).await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientHashRing, Connection, MdFlag, MdStatus, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientHashRing::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_hash_ring_md", 0, 0, false, b"value")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// let item = client.md(b"doc_hash_ring_md", &[MdFlag::ReturnKey]).await?;
    /// assert_eq!(item.status, mcmc_rs::MdStatus::Deleted);
    /// assert_eq!(item.key.as_deref(), Some("doc_hash_ring_md"));
    /// assert_eq!(client.get(b"doc_hash_ring_md").await?, None);
    /// let item = client.md(b"doc_hash_ring_md", &[MdFlag::ReturnKey]).await?;
    /// assert_eq!(item.status, mcmc_rs::MdStatus::NotFound);
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn md(&mut self, key: impl AsRef<[u8]>, flags: &[MdFlag]) -> Result<MdItem> {
        let i = *self.1.get(&key.as_ref()).unwrap();
        self.0[i].md(key.as_ref(), flags).await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientHashRing, Connection, MaFlag, MaMode, MaStatus, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientHashRing::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// let _ = client.delete(b"doc_hash_ring_ma", false).await?;
    /// let item = client
    ///     .ma(
    ///         b"doc_hash_ring_ma",
    ///         &[
    ///             MaFlag::AutoCreate(0),
    ///             MaFlag::InitValue(5),
    ///             MaFlag::Mode(MaMode::Incr),
    ///             MaFlag::ReturnValue,
    ///             MaFlag::ReturnKey,
    ///         ],
    ///     )
    ///     .await?;
    /// assert_eq!(item.status, mcmc_rs::MaStatus::WithBody);
    /// assert_eq!(item.number, Some(5));
    /// assert_eq!(item.key.as_deref(), Some("doc_hash_ring_ma"));
    /// let item = client
    ///     .ma(
    ///         b"doc_hash_ring_ma",
    ///         &[
    ///             MaFlag::DeltaApply(7),
    ///             MaFlag::Mode(MaMode::Incr),
    ///             MaFlag::ReturnValue,
    ///             MaFlag::ReturnKey,
    ///         ],
    ///     )
    ///     .await?;
    /// assert_eq!(item.status, mcmc_rs::MaStatus::WithBody);
    /// assert_eq!(item.number, Some(12));
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn ma(&mut self, key: impl AsRef<[u8]>, flags: &[MaFlag]) -> Result<MaItem> {
        let i = *self.1.get(&key.as_ref()).unwrap();
        self.0[i].ma(key.as_ref(), flags).await
    }
}

pub struct ClientRendezvous(Vec<Connection>, HrwNodes<usize>);
impl ClientRendezvous {
    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientRendezvous, Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientRendezvous::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_rendezvous_new", 0, 0, false, b"value")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client.get(b"doc_rendezvous_new").await?.unwrap().data_block,
    ///     b"value"
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn new(conns: Vec<Connection>) -> Self {
        let hrw = HrwNodes::new(0..conns.len());
        Self(conns, hrw)
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientRendezvous, Connection, Item, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientRendezvous::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_rendezvous_get", 0, 0, false, b"value")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client.get(b"doc_rendezvous_get").await?,
    ///     Some(Item {
    ///         key: "doc_rendezvous_get".to_string(),
    ///         flags: 0,
    ///         cas_unique: None,
    ///         data_block: b"value".to_vec()
    ///     })
    /// );
    /// assert_eq!(client.get(b"doc_rendezvous_get_missing").await?, None);
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn get(&mut self, key: impl AsRef<[u8]>) -> Result<Option<Item>> {
        let i = *self.1.sorted(&key.as_ref()).next().unwrap();
        self.0[i].get(key.as_ref()).await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientRendezvous, Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientRendezvous::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_rendezvous_gets", 0, 0, false, b"value")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// let item = client.gets(b"doc_rendezvous_gets").await?.unwrap();
    /// assert_eq!(item.key, "doc_rendezvous_gets");
    /// assert_eq!(item.data_block, b"value");
    /// assert!(item.cas_unique.is_some());
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn gets(&mut self, key: impl AsRef<[u8]>) -> Result<Option<Item>> {
        let i = *self.1.sorted(&key.as_ref()).next().unwrap();
        self.0[i].gets(key.as_ref()).await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientRendezvous, Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientRendezvous::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_rendezvous_gat", 7, 0, false, b"value")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// let item = client.gat(60, b"doc_rendezvous_gat").await?.unwrap();
    /// assert_eq!(item.key, "doc_rendezvous_gat");
    /// assert_eq!(item.flags, 7);
    /// assert_eq!(item.data_block, b"value");
    /// assert_eq!(item.cas_unique, None);
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn gat(&mut self, exptime: i64, key: impl AsRef<[u8]>) -> Result<Option<Item>> {
        let i = *self.1.sorted(&key.as_ref()).next().unwrap();
        self.0[i].gat(exptime, key.as_ref()).await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientRendezvous, Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientRendezvous::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_rendezvous_gats", 7, 0, false, b"value")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// let item = client.gats(60, b"doc_rendezvous_gats").await?.unwrap();
    /// assert_eq!(item.key, "doc_rendezvous_gats");
    /// assert_eq!(item.flags, 7);
    /// assert_eq!(item.data_block, b"value");
    /// assert!(item.cas_unique.is_some());
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn gats(&mut self, exptime: i64, key: impl AsRef<[u8]>) -> Result<Option<Item>> {
        let i = *self.1.sorted(&key.as_ref()).next().unwrap();
        self.0[i].gats(exptime, key.as_ref()).await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientRendezvous, Connection, Item, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientRendezvous::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_rendezvous_set", 7, 0, false, b"value")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client.get(b"doc_rendezvous_set").await?,
    ///     Some(Item {
    ///         key: "doc_rendezvous_set".to_string(),
    ///         flags: 7,
    ///         cas_unique: None,
    ///         data_block: b"value".to_vec()
    ///     })
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn set(
        &mut self,
        key: impl AsRef<[u8]>,
        flags: u32,
        exptime: i64,
        noreply: bool,
        data_block: impl AsRef<[u8]>,
    ) -> Result<StorageResult> {
        let i = *self.1.sorted(&key.as_ref()).next().unwrap();
        self.0[i]
            .set(key.as_ref(), flags, exptime, noreply, data_block.as_ref())
            .await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientRendezvous, Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientRendezvous::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// let _ = client.delete(b"doc_rendezvous_add", false).await?;
    /// assert_eq!(
    ///     client
    ///         .add(b"doc_rendezvous_add", 0, 0, false, b"first")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client
    ///         .add(b"doc_rendezvous_add", 0, 0, false, b"second")
    ///         .await?,
    ///     mcmc_rs::StorageResult::NotStored
    /// );
    /// assert_eq!(
    ///     client.get(b"doc_rendezvous_add").await?.unwrap().data_block,
    ///     b"first"
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn add(
        &mut self,
        key: impl AsRef<[u8]>,
        flags: u32,
        exptime: i64,
        noreply: bool,
        data_block: impl AsRef<[u8]>,
    ) -> Result<StorageResult> {
        let i = *self.1.sorted(&key.as_ref()).next().unwrap();
        self.0[i]
            .add(key.as_ref(), flags, exptime, noreply, data_block.as_ref())
            .await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientRendezvous, Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientRendezvous::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// let _ = client.delete(b"doc_rendezvous_replace", false).await?;
    /// assert_eq!(
    ///     client
    ///         .replace(b"doc_rendezvous_replace", 0, 0, false, b"value")
    ///         .await?,
    ///     mcmc_rs::StorageResult::NotStored
    /// );
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_rendezvous_replace", 0, 0, false, b"first")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client
    ///         .replace(b"doc_rendezvous_replace", 0, 0, false, b"second")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client
    ///         .get(b"doc_rendezvous_replace")
    ///         .await?
    ///         .unwrap()
    ///         .data_block,
    ///     b"second"
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn replace(
        &mut self,
        key: impl AsRef<[u8]>,
        flags: u32,
        exptime: i64,
        noreply: bool,
        data_block: impl AsRef<[u8]>,
    ) -> Result<StorageResult> {
        let i = *self.1.sorted(&key.as_ref()).next().unwrap();
        self.0[i]
            .replace(key.as_ref(), flags, exptime, noreply, data_block.as_ref())
            .await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientRendezvous, Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientRendezvous::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// let _ = client.delete(b"doc_rendezvous_append", false).await?;
    /// assert_eq!(
    ///     client
    ///         .append(b"doc_rendezvous_append", 0, 0, false, b"-tail")
    ///         .await?,
    ///     mcmc_rs::StorageResult::NotStored
    /// );
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_rendezvous_append", 0, 0, false, b"base")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client
    ///         .append(b"doc_rendezvous_append", 0, 0, false, b"-tail")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client
    ///         .get(b"doc_rendezvous_append")
    ///         .await?
    ///         .unwrap()
    ///         .data_block,
    ///     b"base-tail"
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn append(
        &mut self,
        key: impl AsRef<[u8]>,
        flags: u32,
        exptime: i64,
        noreply: bool,
        data_block: impl AsRef<[u8]>,
    ) -> Result<StorageResult> {
        let i = *self.1.sorted(&key.as_ref()).next().unwrap();
        self.0[i]
            .append(key.as_ref(), flags, exptime, noreply, data_block.as_ref())
            .await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientRendezvous, Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientRendezvous::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// let _ = client.delete(b"doc_rendezvous_prepend", false).await?;
    /// assert_eq!(
    ///     client
    ///         .prepend(b"doc_rendezvous_prepend", 0, 0, false, b"head-")
    ///         .await?,
    ///     mcmc_rs::StorageResult::NotStored
    /// );
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_rendezvous_prepend", 0, 0, false, b"base")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client
    ///         .prepend(b"doc_rendezvous_prepend", 0, 0, false, b"head-")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client
    ///         .get(b"doc_rendezvous_prepend")
    ///         .await?
    ///         .unwrap()
    ///         .data_block,
    ///     b"head-base"
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn prepend(
        &mut self,
        key: impl AsRef<[u8]>,
        flags: u32,
        exptime: i64,
        noreply: bool,
        data_block: impl AsRef<[u8]>,
    ) -> Result<StorageResult> {
        let i = *self.1.sorted(&key.as_ref()).next().unwrap();
        self.0[i]
            .prepend(key.as_ref(), flags, exptime, noreply, data_block.as_ref())
            .await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientRendezvous, Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientRendezvous::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// let _ = client.delete(b"doc_rendezvous_cas", false).await?;
    /// assert_eq!(
    ///     client
    ///         .cas(b"doc_rendezvous_cas", 0, 0, 0, false, b"value")
    ///         .await?,
    ///     mcmc_rs::StorageResult::NotFound
    /// );
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_rendezvous_cas", 0, 0, false, b"base")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// let cas = client
    ///     .gets(b"doc_rendezvous_cas")
    ///     .await?
    ///     .unwrap()
    ///     .cas_unique
    ///     .unwrap();
    /// assert_eq!(
    ///     client
    ///         .cas(b"doc_rendezvous_cas", 0, 0, cas, false, b"new")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client
    ///         .cas(b"doc_rendezvous_cas", 0, 0, cas, false, b"stale")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Exists
    /// );
    /// assert_eq!(
    ///     client.get(b"doc_rendezvous_cas").await?.unwrap().data_block,
    ///     b"new"
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn cas(
        &mut self,
        key: impl AsRef<[u8]>,
        flags: u32,
        exptime: i64,
        cas_unique: u64,
        noreply: bool,
        data_block: impl AsRef<[u8]>,
    ) -> Result<StorageResult> {
        let i = *self.1.sorted(&key.as_ref()).next().unwrap();
        self.0[i]
            .cas(
                key.as_ref(),
                flags,
                exptime,
                cas_unique,
                noreply,
                data_block.as_ref(),
            )
            .await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientRendezvous, Connection, DeleteResult, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientRendezvous::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_rendezvous_delete", 0, 0, false, b"value")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client.delete(b"doc_rendezvous_delete", false).await?,
    ///     mcmc_rs::DeleteResult::Deleted
    /// );
    /// assert_eq!(
    ///     client.delete(b"doc_rendezvous_delete", false).await?,
    ///     mcmc_rs::DeleteResult::NotFound
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn delete(&mut self, key: impl AsRef<[u8]>, noreply: bool) -> Result<DeleteResult> {
        let i = *self.1.sorted(&key.as_ref()).next().unwrap();
        self.0[i].delete(key.as_ref(), noreply).await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientRendezvous, Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientRendezvous::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// let _ = client.delete(b"doc_rendezvous_incr", false).await?;
    /// assert_eq!(client.incr(b"doc_rendezvous_incr", 1, false).await?, None);
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_rendezvous_incr", 0, 0, false, b"10")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client.incr(b"doc_rendezvous_incr", 5, false).await?,
    ///     Some(15)
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn incr(
        &mut self,
        key: impl AsRef<[u8]>,
        value: u64,
        noreply: bool,
    ) -> Result<Option<u64>> {
        let i = *self.1.sorted(&key.as_ref()).next().unwrap();
        self.0[i].incr(key.as_ref(), value, noreply).await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientRendezvous, Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientRendezvous::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// let _ = client.delete(b"doc_rendezvous_decr", false).await?;
    /// assert_eq!(client.decr(b"doc_rendezvous_decr", 1, false).await?, None);
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_rendezvous_decr", 0, 0, false, b"10")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client.decr(b"doc_rendezvous_decr", 5, false).await?,
    ///     Some(5)
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn decr(
        &mut self,
        key: impl AsRef<[u8]>,
        value: u64,
        noreply: bool,
    ) -> Result<Option<u64>> {
        let i = *self.1.sorted(&key.as_ref()).next().unwrap();
        self.0[i].decr(key.as_ref(), value, noreply).await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientRendezvous, Connection, TouchResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientRendezvous::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// assert_eq!(
    ///     client
    ///         .touch(b"doc_rendezvous_touch_missing", 60, false)
    ///         .await?,
    ///     mcmc_rs::TouchResult::NotFound
    /// );
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_rendezvous_touch", 0, 0, false, b"value")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client.touch(b"doc_rendezvous_touch", 60, false).await?,
    ///     mcmc_rs::TouchResult::Touched
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn touch(
        &mut self,
        key: impl AsRef<[u8]>,
        exptime: i64,
        noreply: bool,
    ) -> Result<TouchResult> {
        let i = *self.1.sorted(&key.as_ref()).next().unwrap();
        self.0[i].touch(key.as_ref(), exptime, noreply).await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientRendezvous, Connection, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientRendezvous::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_rendezvous_me", 0, 0, false, b"value")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// let info = client.me(b"doc_rendezvous_me").await?.unwrap();
    /// assert!(info.starts_with("doc_rendezvous_me "));
    /// assert!(info.contains("size="));
    /// assert_eq!(client.me(b"doc_rendezvous_me_missing").await?, None);
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn me(&mut self, key: impl AsRef<[u8]>) -> Result<Option<String>> {
        let i = *self.1.sorted(&key.as_ref()).next().unwrap();
        self.0[i].me(key.as_ref()).await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientRendezvous, Connection, MgFlag, MgStatus, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientRendezvous::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_rendezvous_mg", 7, 0, false, b"hello")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// let item = client
    ///     .mg(
    ///         b"doc_rendezvous_mg",
    ///         &[
    ///             MgFlag::ReturnKey,
    ///             MgFlag::ReturnFlags,
    ///             MgFlag::ReturnSize,
    ///             MgFlag::ReturnTtl,
    ///             MgFlag::ReturnValue,
    ///             MgFlag::ReturnCas,
    ///         ],
    ///     )
    ///     .await?;
    /// assert_eq!(item.status, mcmc_rs::MgStatus::WithBody);
    /// assert_eq!(item.key.as_deref(), Some("doc_rendezvous_mg"));
    /// assert_eq!(item.flags, Some(7));
    /// assert_eq!(item.size, Some(5));
    /// assert_eq!(item.ttl, Some(-1));
    /// assert_eq!(item.data_block.as_deref(), Some(&b"hello"[..]));
    /// assert!(item.cas.is_some());
    /// let item = client
    ///     .mg(b"doc_rendezvous_mg_missing", &[MgFlag::ReturnKey])
    ///     .await?;
    /// assert_eq!(item.status, mcmc_rs::MgStatus::NotFound);
    /// assert_eq!(item.key.as_deref(), Some("doc_rendezvous_mg_missing"));
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn mg(&mut self, key: impl AsRef<[u8]>, flags: &[MgFlag]) -> Result<MgItem> {
        let i = *self.1.sorted(&key.as_ref()).next().unwrap();
        self.0[i].mg(key.as_ref(), flags).await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientRendezvous, Connection, MsFlag, MsMode, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientRendezvous::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// let _ = client.delete(b"doc_rendezvous_ms", false).await?;
    /// let item = client
    ///     .ms(
    ///         b"doc_rendezvous_ms",
    ///         &[MsFlag::ReturnKey, MsFlag::ReturnSize, MsFlag::ReturnCas],
    ///         b"hello",
    ///     )
    ///     .await?;
    /// assert_eq!(item.status, mcmc_rs::StorageResult::Stored);
    /// assert_eq!(item.key.as_deref(), Some("doc_rendezvous_ms"));
    /// assert_eq!(item.size, Some(5));
    /// assert!(item.cas.is_some());
    /// let item = client
    ///     .ms(
    ///         b"doc_rendezvous_ms",
    ///         &[MsFlag::Mode(MsMode::Add), MsFlag::ReturnKey],
    ///         b"again",
    ///     )
    ///     .await?;
    /// assert_eq!(item.status, mcmc_rs::StorageResult::NotStored);
    /// assert_eq!(
    ///     client.get(b"doc_rendezvous_ms").await?.unwrap().data_block,
    ///     b"hello"
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn ms(
        &mut self,
        key: impl AsRef<[u8]>,
        flags: &[MsFlag],
        data_block: impl AsRef<[u8]>,
    ) -> Result<MsItem> {
        let i = *self.1.sorted(&key.as_ref()).next().unwrap();
        self.0[i].ms(key.as_ref(), flags, data_block.as_ref()).await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientRendezvous, Connection, MdFlag, MdStatus, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientRendezvous::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_rendezvous_md", 0, 0, false, b"value")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// let item = client
    ///     .md(b"doc_rendezvous_md", &[MdFlag::ReturnKey])
    ///     .await?;
    /// assert_eq!(item.status, mcmc_rs::MdStatus::Deleted);
    /// assert_eq!(item.key.as_deref(), Some("doc_rendezvous_md"));
    /// assert_eq!(client.get(b"doc_rendezvous_md").await?, None);
    /// let item = client
    ///     .md(b"doc_rendezvous_md", &[MdFlag::ReturnKey])
    ///     .await?;
    /// assert_eq!(item.status, mcmc_rs::MdStatus::NotFound);
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn md(&mut self, key: impl AsRef<[u8]>, flags: &[MdFlag]) -> Result<MdItem> {
        let i = *self.1.sorted(&key.as_ref()).next().unwrap();
        self.0[i].md(key.as_ref(), flags).await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientRendezvous, Connection, MaFlag, MaMode, MaStatus, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientRendezvous::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// let _ = client.delete(b"doc_rendezvous_ma", false).await?;
    /// let item = client
    ///     .ma(
    ///         b"doc_rendezvous_ma",
    ///         &[
    ///             MaFlag::AutoCreate(0),
    ///             MaFlag::InitValue(5),
    ///             MaFlag::Mode(MaMode::Incr),
    ///             MaFlag::ReturnValue,
    ///             MaFlag::ReturnKey,
    ///         ],
    ///     )
    ///     .await?;
    /// assert_eq!(item.status, mcmc_rs::MaStatus::WithBody);
    /// assert_eq!(item.number, Some(5));
    /// assert_eq!(item.key.as_deref(), Some("doc_rendezvous_ma"));
    /// let item = client
    ///     .ma(
    ///         b"doc_rendezvous_ma",
    ///         &[
    ///             MaFlag::DeltaApply(7),
    ///             MaFlag::Mode(MaMode::Incr),
    ///             MaFlag::ReturnValue,
    ///             MaFlag::ReturnKey,
    ///         ],
    ///     )
    ///     .await?;
    /// assert_eq!(item.status, mcmc_rs::MaStatus::WithBody);
    /// assert_eq!(item.number, Some(12));
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn ma(&mut self, key: impl AsRef<[u8]>, flags: &[MaFlag]) -> Result<MaItem> {
        let i = *self.1.sorted(&key.as_ref()).next().unwrap();
        self.0[i].ma(key.as_ref(), flags).await
    }
}

pub struct Pipeline<'a>(&'a mut Connection, Vec<Vec<u8>>);
impl<'a> Pipeline<'a> {
    /// # Example
    ///
    /// ```
    /// # use std::assert_matches;
    /// # use mcmc_rs::{Connection, PipelineResponse};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// let result = conn.pipeline().version().execute().await?;
    /// assert_eq!(result.len(), 1);
    /// assert_matches!(result[0], PipelineResponse::String(_));
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    fn new(conn: &'a mut Connection) -> Self {
        Self(conn, Vec::new())
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, Item, PipelineResponse, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::tls_connect("localhost", 11216, "cert.pem").await?,
    /// ] {
    ///     let result = c
    ///         .pipeline()
    ///         .set(b"doc_pipeline_execute", 0, 0, false, b"value")
    ///         .get(b"doc_pipeline_execute")
    ///         .execute()
    ///         .await?;
    ///     assert_eq!(result.len(), 2);
    ///     assert_eq!(
    ///         result[0],
    ///         PipelineResponse::StorageResult(StorageResult::Stored)
    ///     );
    ///     assert_eq!(
    ///         result[1],
    ///         PipelineResponse::OptionItem(Some(Item {
    ///             key: "doc_pipeline_execute".to_string(),
    ///             flags: 0,
    ///             cas_unique: None,
    ///             data_block: b"value".to_vec()
    ///         }))
    ///     );
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn execute(self) -> Result<Vec<PipelineResponse>> {
        if self.1.is_empty() {
            return Ok(Vec::new());
        };
        match self.0 {
            Connection::Tcp(s) => execute_cmd(s, &self.1).await,
            #[cfg(unix)]
            Connection::Unix(s) => execute_cmd(s, &self.1).await,
            Connection::Udp(_s, _r) => unreachable!("pipeline not work with udp!"),
            Connection::Tls(s) => execute_cmd(s, &self.1).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, PipelineResponse};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// let result = conn.pipeline().version().execute().await?;
    /// match &result[0] {
    ///     PipelineResponse::String(version) => assert!(!version.is_empty()),
    ///     other => panic!("unexpected response: {other:?}"),
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn version(mut self) -> Self {
        self.1.push(build_version_cmd().to_vec());
        self
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, PipelineResponse};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// let result = conn.pipeline().quit().execute().await?;
    /// assert_eq!(result, [PipelineResponse::Unit(())]);
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn quit(mut self) -> Self {
        self.1.push(build_quit_cmd().to_vec());
        self
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, PipelineResponse};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// let _pipeline = conn.pipeline().shutdown(false);
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn shutdown(mut self, graceful: bool) -> Self {
        self.1.push(build_shutdown_cmd(graceful).to_vec());
        self
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, PipelineResponse};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// let result = conn.pipeline().cache_memlimit(64, false).execute().await?;
    /// assert_eq!(result, [PipelineResponse::Unit(())]);
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn cache_memlimit(mut self, limit: usize, noreply: bool) -> Self {
        self.1
            .push(build_cache_memlimit_cmd(limit, noreply).to_vec());
        self
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, PipelineResponse, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::tls_connect("localhost", 11216, "cert.pem").await?;
    /// assert_eq!(
    ///     conn.set(b"doc_pipeline_flush_all", 0, 0, false, b"value")
    ///         .await?,
    ///     StorageResult::Stored
    /// );
    /// let result = conn.pipeline().flush_all(Some(999), false).execute().await?;
    /// assert_eq!(result, [PipelineResponse::Unit(())]);
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn flush_all(mut self, exptime: Option<i64>, noreply: bool) -> Self {
        self.1.push(build_flush_all_cmd(exptime, noreply).to_vec());
        self
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, PipelineResponse, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// let result = conn
    ///     .pipeline()
    ///     .set(b"doc_pipeline_set", 0, 0, false, b"value")
    ///     .execute()
    ///     .await?;
    /// assert_eq!(
    ///     result,
    ///     [PipelineResponse::StorageResult(StorageResult::Stored)]
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn set(
        mut self,
        key: impl AsRef<[u8]>,
        flags: u32,
        exptime: i64,
        noreply: bool,
        data_block: impl AsRef<[u8]>,
    ) -> Self {
        self.1.push(build_storage_cmd(
            b"set",
            key.as_ref(),
            flags,
            exptime,
            None,
            noreply,
            data_block.as_ref(),
        ));
        self
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, PipelineResponse, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// let _ = conn.delete(b"doc_pipeline_add", false).await?;
    /// let result = conn
    ///     .pipeline()
    ///     .add(b"doc_pipeline_add", 0, 0, false, b"first")
    ///     .add(b"doc_pipeline_add", 0, 0, false, b"second")
    ///     .execute()
    ///     .await?;
    /// assert_eq!(
    ///     result,
    ///     [
    ///         PipelineResponse::StorageResult(StorageResult::Stored),
    ///         PipelineResponse::StorageResult(StorageResult::NotStored),
    ///     ]
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn add(
        mut self,
        key: impl AsRef<[u8]>,
        flags: u32,
        exptime: i64,
        noreply: bool,
        data_block: impl AsRef<[u8]>,
    ) -> Self {
        self.1.push(build_storage_cmd(
            b"add",
            key.as_ref(),
            flags,
            exptime,
            None,
            noreply,
            data_block.as_ref(),
        ));
        self
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, PipelineResponse, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// let _ = conn.delete(b"doc_pipeline_replace", false).await?;
    /// let result = conn
    ///     .pipeline()
    ///     .replace(b"doc_pipeline_replace", 0, 0, false, b"value")
    ///     .execute()
    ///     .await?;
    /// assert_eq!(
    ///     result,
    ///     [PipelineResponse::StorageResult(StorageResult::NotStored)]
    /// );
    /// conn.set(b"doc_pipeline_replace", 0, 0, false, b"first")
    ///     .await?;
    /// let result = conn
    ///     .pipeline()
    ///     .replace(b"doc_pipeline_replace", 0, 0, false, b"second")
    ///     .execute()
    ///     .await?;
    /// assert_eq!(
    ///     result,
    ///     [PipelineResponse::StorageResult(StorageResult::Stored)]
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn replace(
        mut self,
        key: impl AsRef<[u8]>,
        flags: u32,
        exptime: i64,
        noreply: bool,
        data_block: impl AsRef<[u8]>,
    ) -> Self {
        self.1.push(build_storage_cmd(
            b"replace",
            key.as_ref(),
            flags,
            exptime,
            None,
            noreply,
            data_block.as_ref(),
        ));
        self
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, Item, PipelineResponse, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// let _ = conn.delete(b"doc_pipeline_append", false).await?;
    /// let result = conn
    ///     .pipeline()
    ///     .append(b"doc_pipeline_append", 0, 0, false, b"-tail")
    ///     .execute()
    ///     .await?;
    /// assert_eq!(
    ///     result,
    ///     [PipelineResponse::StorageResult(StorageResult::NotStored)]
    /// );
    /// conn.set(b"doc_pipeline_append", 0, 0, false, b"base")
    ///     .await?;
    /// let result = conn
    ///     .pipeline()
    ///     .append(b"doc_pipeline_append", 0, 0, false, b"-tail")
    ///     .get(b"doc_pipeline_append")
    ///     .execute()
    ///     .await?;
    /// assert_eq!(
    ///     result[0],
    ///     PipelineResponse::StorageResult(StorageResult::Stored)
    /// );
    /// assert_eq!(
    ///     result[1],
    ///     PipelineResponse::OptionItem(Some(Item {
    ///         key: "doc_pipeline_append".to_string(),
    ///         flags: 0,
    ///         cas_unique: None,
    ///         data_block: b"base-tail".to_vec()
    ///     }))
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn append(
        mut self,
        key: impl AsRef<[u8]>,
        flags: u32,
        exptime: i64,
        noreply: bool,
        data_block: impl AsRef<[u8]>,
    ) -> Self {
        self.1.push(build_storage_cmd(
            b"append",
            key.as_ref(),
            flags,
            exptime,
            None,
            noreply,
            data_block.as_ref(),
        ));
        self
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, Item, PipelineResponse, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// let _ = conn.delete(b"doc_pipeline_prepend", false).await?;
    /// let result = conn
    ///     .pipeline()
    ///     .prepend(b"doc_pipeline_prepend", 0, 0, false, b"head-")
    ///     .execute()
    ///     .await?;
    /// assert_eq!(
    ///     result,
    ///     [PipelineResponse::StorageResult(StorageResult::NotStored)]
    /// );
    /// conn.set(b"doc_pipeline_prepend", 0, 0, false, b"base")
    ///     .await?;
    /// let result = conn
    ///     .pipeline()
    ///     .prepend(b"doc_pipeline_prepend", 0, 0, false, b"head-")
    ///     .get(b"doc_pipeline_prepend")
    ///     .execute()
    ///     .await?;
    /// assert_eq!(
    ///     result[0],
    ///     PipelineResponse::StorageResult(StorageResult::Stored)
    /// );
    /// assert_eq!(
    ///     result[1],
    ///     PipelineResponse::OptionItem(Some(Item {
    ///         key: "doc_pipeline_prepend".to_string(),
    ///         flags: 0,
    ///         cas_unique: None,
    ///         data_block: b"head-base".to_vec()
    ///     }))
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn prepend(
        mut self,
        key: impl AsRef<[u8]>,
        flags: u32,
        exptime: i64,
        noreply: bool,
        data_block: impl AsRef<[u8]>,
    ) -> Self {
        self.1.push(build_storage_cmd(
            b"prepend",
            key.as_ref(),
            flags,
            exptime,
            None,
            noreply,
            data_block.as_ref(),
        ));
        self
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, PipelineResponse, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// let _ = conn.delete(b"doc_pipeline_cas", false).await?;
    /// let result = conn
    ///     .pipeline()
    ///     .cas(b"doc_pipeline_cas", 0, 0, 0, false, b"value")
    ///     .execute()
    ///     .await?;
    /// assert_eq!(
    ///     result,
    ///     [PipelineResponse::StorageResult(StorageResult::NotFound)]
    /// );
    /// conn.set(b"doc_pipeline_cas", 0, 0, false, b"base").await?;
    /// let cas = conn
    ///     .gets(b"doc_pipeline_cas")
    ///     .await?
    ///     .unwrap()
    ///     .cas_unique
    ///     .unwrap();
    /// let result = conn
    ///     .pipeline()
    ///     .cas(b"doc_pipeline_cas", 0, 0, cas, false, b"new")
    ///     .execute()
    ///     .await?;
    /// assert_eq!(
    ///     result,
    ///     [PipelineResponse::StorageResult(StorageResult::Stored)]
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn cas(
        mut self,
        key: impl AsRef<[u8]>,
        flags: u32,
        exptime: i64,
        cas_unique: u64,
        noreply: bool,
        data_block: impl AsRef<[u8]>,
    ) -> Self {
        self.1.push(build_storage_cmd(
            b"cas",
            key.as_ref(),
            flags,
            exptime,
            Some(cas_unique),
            noreply,
            data_block.as_ref(),
        ));
        self
    }

    /// # Example
    ///
    /// ```
    /// # use std::assert_matches;
    /// # use mcmc_rs::{Connection, PipelineResponse};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::tcp_connect("127.0.0.1:11212").await?;
    /// let result = conn.pipeline().auth(b"a", b"a").version().execute().await?;
    /// assert_eq!(result[0], PipelineResponse::Unit(()));
    /// assert_matches!(&result[1], PipelineResponse::String(version) if !version.is_empty());
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn auth(mut self, username: impl AsRef<[u8]>, password: impl AsRef<[u8]>) -> Self {
        self.1
            .push(build_auth_cmd(username.as_ref(), password.as_ref()));
        self
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, DeleteResult, PipelineResponse, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.set(b"doc_pipeline_delete", 0, 0, false, b"value")
    ///     .await?;
    /// let result = conn
    ///     .pipeline()
    ///     .delete(b"doc_pipeline_delete", false)
    ///     .delete(b"doc_pipeline_delete", false)
    ///     .execute()
    ///     .await?;
    /// assert_eq!(
    ///     result,
    ///     [
    ///         PipelineResponse::DeleteResult(DeleteResult::Deleted),
    ///         PipelineResponse::DeleteResult(DeleteResult::NotFound),
    ///     ]
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn delete(mut self, key: impl AsRef<[u8]>, noreply: bool) -> Self {
        self.1.push(build_delete_cmd(key.as_ref(), noreply));
        self
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, PipelineResponse, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// let _ = conn.delete(b"doc_pipeline_incr", false).await?;
    /// let result = conn
    ///     .pipeline()
    ///     .incr(b"doc_pipeline_incr", 1, false)
    ///     .execute()
    ///     .await?;
    /// assert_eq!(result, [PipelineResponse::Value(None)]);
    /// conn.set(b"doc_pipeline_incr", 0, 0, false, b"10").await?;
    /// let result = conn
    ///     .pipeline()
    ///     .incr(b"doc_pipeline_incr", 5, false)
    ///     .execute()
    ///     .await?;
    /// assert_eq!(result, [PipelineResponse::Value(Some(15))]);
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn incr(mut self, key: impl AsRef<[u8]>, value: u64, noreply: bool) -> Self {
        self.1
            .push(build_incr_decr_cmd(b"incr", key.as_ref(), value, noreply));
        self
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, PipelineResponse, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// let _ = conn.delete(b"doc_pipeline_decr", false).await?;
    /// let result = conn
    ///     .pipeline()
    ///     .decr(b"doc_pipeline_decr", 1, false)
    ///     .execute()
    ///     .await?;
    /// assert_eq!(result, [PipelineResponse::Value(None)]);
    /// conn.set(b"doc_pipeline_decr", 0, 0, false, b"10").await?;
    /// let result = conn
    ///     .pipeline()
    ///     .decr(b"doc_pipeline_decr", 5, false)
    ///     .execute()
    ///     .await?;
    /// assert_eq!(result, [PipelineResponse::Value(Some(5))]);
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn decr(mut self, key: impl AsRef<[u8]>, value: u64, noreply: bool) -> Self {
        self.1
            .push(build_incr_decr_cmd(b"decr", key.as_ref(), value, noreply));
        self
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, PipelineResponse, StorageResult, TouchResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// let _ = conn.delete(b"doc_pipeline_touch", false).await?;
    /// let result = conn
    ///     .pipeline()
    ///     .touch(b"doc_pipeline_touch", 60, false)
    ///     .execute()
    ///     .await?;
    /// assert_eq!(
    ///     result,
    ///     [PipelineResponse::TouchResult(TouchResult::NotFound)]
    /// );
    /// conn.set(b"doc_pipeline_touch", 0, 0, false, b"value")
    ///     .await?;
    /// let result = conn
    ///     .pipeline()
    ///     .touch(b"doc_pipeline_touch", 60, false)
    ///     .execute()
    ///     .await?;
    /// assert_eq!(
    ///     result,
    ///     [PipelineResponse::TouchResult(TouchResult::Touched)]
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn touch(mut self, key: impl AsRef<[u8]>, exptime: i64, noreply: bool) -> Self {
        self.1.push(build_touch_cmd(key.as_ref(), exptime, noreply));
        self
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, Item, PipelineResponse, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// let _ = conn.delete(b"doc_pipeline_get", false).await?;
    /// let result = conn.pipeline().get(b"doc_pipeline_get").execute().await?;
    /// assert_eq!(result, [PipelineResponse::OptionItem(None)]);
    /// conn.set(b"doc_pipeline_get", 0, 0, false, b"value").await?;
    /// let result = conn.pipeline().get(b"doc_pipeline_get").execute().await?;
    /// assert_eq!(
    ///     result,
    ///     [PipelineResponse::OptionItem(Some(Item {
    ///         key: "doc_pipeline_get".to_string(),
    ///         flags: 0,
    ///         cas_unique: None,
    ///         data_block: b"value".to_vec()
    ///     }))]
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn get(mut self, key: impl AsRef<[u8]>) -> Self {
        self.1
            .push(build_retrieval_cmd(b"get", None, &[key.as_ref()]));
        self
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, PipelineResponse, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.set(b"doc_pipeline_gets", 0, 0, false, b"value")
    ///     .await?;
    /// let result = conn.pipeline().gets(b"doc_pipeline_gets").execute().await?;
    /// match &result[0] {
    ///     PipelineResponse::OptionItem(Some(item)) => {
    ///         assert_eq!(item.key, "doc_pipeline_gets");
    ///         assert_eq!(item.data_block, b"value");
    ///         assert!(item.cas_unique.is_some());
    ///     }
    ///     other => panic!("unexpected response: {other:?}"),
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn gets(mut self, key: impl AsRef<[u8]>) -> Self {
        self.1
            .push(build_retrieval_cmd(b"gets", None, &[key.as_ref()]));
        self
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, PipelineResponse, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.set(b"doc_pipeline_gat", 7, 0, false, b"value").await?;
    /// let result = conn
    ///     .pipeline()
    ///     .gat(60, b"doc_pipeline_gat")
    ///     .execute()
    ///     .await?;
    /// match &result[0] {
    ///     PipelineResponse::OptionItem(Some(item)) => {
    ///         assert_eq!(item.key, "doc_pipeline_gat");
    ///         assert_eq!(item.flags, 7);
    ///         assert_eq!(item.data_block, b"value");
    ///         assert_eq!(item.cas_unique, None);
    ///     }
    ///     other => panic!("unexpected response: {other:?}"),
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn gat(mut self, exptime: i64, key: impl AsRef<[u8]>) -> Self {
        self.1
            .push(build_retrieval_cmd(b"gat", Some(exptime), &[key.as_ref()]));
        self
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, PipelineResponse, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.set(b"doc_pipeline_gats", 7, 0, false, b"value")
    ///     .await?;
    /// let result = conn
    ///     .pipeline()
    ///     .gats(60, b"doc_pipeline_gats")
    ///     .execute()
    ///     .await?;
    /// match &result[0] {
    ///     PipelineResponse::OptionItem(Some(item)) => {
    ///         assert_eq!(item.key, "doc_pipeline_gats");
    ///         assert_eq!(item.flags, 7);
    ///         assert_eq!(item.data_block, b"value");
    ///         assert!(item.cas_unique.is_some());
    ///     }
    ///     other => panic!("unexpected response: {other:?}"),
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn gats(mut self, exptime: i64, key: impl AsRef<[u8]>) -> Self {
        self.1
            .push(build_retrieval_cmd(b"gats", Some(exptime), &[key.as_ref()]));
        self
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, Item, PipelineResponse, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// let _ = conn.delete(b"doc_pipeline_get_multi_a", false).await?;
    /// let _ = conn.delete(b"doc_pipeline_get_multi_b", false).await?;
    /// let result = conn
    ///     .pipeline()
    ///     .get_multi(&[b"doc_pipeline_get_multi_a", b"doc_pipeline_get_multi_b"])
    ///     .execute()
    ///     .await?;
    /// assert_eq!(result, [PipelineResponse::VecItem(Vec::new())]);
    /// conn.set(b"doc_pipeline_get_multi_a", 0, 0, false, b"a")
    ///     .await?;
    /// conn.set(b"doc_pipeline_get_multi_b", 0, 0, false, b"b")
    ///     .await?;
    /// let result = conn
    ///     .pipeline()
    ///     .get_multi(&[b"doc_pipeline_get_multi_a", b"doc_pipeline_get_multi_b"])
    ///     .execute()
    ///     .await?;
    /// assert_eq!(
    ///     result,
    ///     [PipelineResponse::VecItem(vec![
    ///         Item {
    ///             key: "doc_pipeline_get_multi_a".to_string(),
    ///             flags: 0,
    ///             cas_unique: None,
    ///             data_block: b"a".to_vec()
    ///         },
    ///         Item {
    ///             key: "doc_pipeline_get_multi_b".to_string(),
    ///             flags: 0,
    ///             cas_unique: None,
    ///             data_block: b"b".to_vec()
    ///         }
    ///     ])]
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn get_multi(mut self, keys: &[impl AsRef<[u8]>]) -> Self {
        self.1.push(build_retrieval_cmd(
            b"get",
            None,
            &keys.iter().map(|x| x.as_ref()).collect::<Vec<&[u8]>>(),
        ));
        self
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, PipelineResponse, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.set(b"doc_pipeline_gets_multi_a", 0, 0, false, b"a")
    ///     .await?;
    /// conn.set(b"doc_pipeline_gets_multi_b", 0, 0, false, b"b")
    ///     .await?;
    /// let result = conn
    ///     .pipeline()
    ///     .gets_multi(&[b"doc_pipeline_gets_multi_a", b"doc_pipeline_gets_multi_b"])
    ///     .execute()
    ///     .await?;
    /// match &result[0] {
    ///     PipelineResponse::VecItem(items) => {
    ///         assert_eq!(items.len(), 2);
    ///         assert_eq!(items[0].key, "doc_pipeline_gets_multi_a");
    ///         assert_eq!(items[1].key, "doc_pipeline_gets_multi_b");
    ///         assert!(items.iter().all(|item| item.cas_unique.is_some()));
    ///     }
    ///     other => panic!("unexpected response: {other:?}"),
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn gets_multi(mut self, keys: &[impl AsRef<[u8]>]) -> Self {
        self.1.push(build_retrieval_cmd(
            b"gets",
            None,
            &keys.iter().map(|x| x.as_ref()).collect::<Vec<&[u8]>>(),
        ));
        self
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, Item, PipelineResponse, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.set(b"doc_pipeline_gat_multi_a", 0, 0, false, b"a")
    ///     .await?;
    /// conn.set(b"doc_pipeline_gat_multi_b", 0, 0, false, b"b")
    ///     .await?;
    /// let result = conn
    ///     .pipeline()
    ///     .gat_multi(
    ///         60,
    ///         &[b"doc_pipeline_gat_multi_a", b"doc_pipeline_gat_multi_b"],
    ///     )
    ///     .execute()
    ///     .await?;
    /// assert_eq!(
    ///     result,
    ///     [PipelineResponse::VecItem(vec![
    ///         Item {
    ///             key: "doc_pipeline_gat_multi_a".to_string(),
    ///             flags: 0,
    ///             cas_unique: None,
    ///             data_block: b"a".to_vec()
    ///         },
    ///         Item {
    ///             key: "doc_pipeline_gat_multi_b".to_string(),
    ///             flags: 0,
    ///             cas_unique: None,
    ///             data_block: b"b".to_vec()
    ///         }
    ///     ])]
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn gat_multi(mut self, exptime: i64, keys: &[impl AsRef<[u8]>]) -> Self {
        self.1.push(build_retrieval_cmd(
            b"gat",
            Some(exptime),
            &keys.iter().map(|x| x.as_ref()).collect::<Vec<&[u8]>>(),
        ));
        self
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, PipelineResponse, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.set(b"doc_pipeline_gats_multi_a", 0, 0, false, b"a")
    ///     .await?;
    /// conn.set(b"doc_pipeline_gats_multi_b", 0, 0, false, b"b")
    ///     .await?;
    /// let result = conn
    ///     .pipeline()
    ///     .gats_multi(
    ///         60,
    ///         &[b"doc_pipeline_gats_multi_a", b"doc_pipeline_gats_multi_b"],
    ///     )
    ///     .execute()
    ///     .await?;
    /// match &result[0] {
    ///     PipelineResponse::VecItem(items) => {
    ///         assert_eq!(items.len(), 2);
    ///         assert_eq!(items[0].key, "doc_pipeline_gats_multi_a");
    ///         assert_eq!(items[1].key, "doc_pipeline_gats_multi_b");
    ///         assert!(items.iter().all(|item| item.cas_unique.is_some()));
    ///     }
    ///     other => panic!("unexpected response: {other:?}"),
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn gats_multi(mut self, exptime: i64, keys: &[impl AsRef<[u8]>]) -> Self {
        self.1.push(build_retrieval_cmd(
            b"gats",
            Some(exptime),
            &keys.iter().map(|x| x.as_ref()).collect::<Vec<&[u8]>>(),
        ));
        self
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, PipelineResponse, StatsArg};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// let result = conn.pipeline().stats(None).execute().await?;
    /// match &result[0] {
    ///     PipelineResponse::HashMap(stats) => {
    ///         assert!(stats.contains_key("pid"));
    ///         assert!(stats.contains_key("uptime"));
    ///         assert!(stats["uptime"].parse::<u64>().is_ok());
    ///     }
    ///     other => panic!("unexpected response: {other:?}"),
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn stats(mut self, arg: Option<StatsArg>) -> Self {
        self.1.push(build_stats_cmd(arg).to_vec());
        self
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, PipelineResponse, SlabsAutomoveArg, StatsArg};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// let result = conn
    ///     .pipeline()
    ///     .slabs_automove(SlabsAutomoveArg::Zero)
    ///     .execute()
    ///     .await?;
    /// assert_eq!(result, [PipelineResponse::Unit(())]);
    /// let settings = conn.stats(Some(StatsArg::Settings)).await?;
    /// assert_eq!(settings.get("slab_automove").map(String::as_str), Some("0"));
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn slabs_automove(mut self, arg: SlabsAutomoveArg) -> Self {
        self.1.push(build_slabs_automove_cmd(arg).to_vec());
        self
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, LruCrawlerArg, PipelineResponse, StatsArg};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// let _ = conn
    ///     .pipeline()
    ///     .lru_crawler(LruCrawlerArg::Enable)
    ///     .execute()
    ///     .await;
    /// let settings = conn.stats(Some(StatsArg::Settings)).await?;
    /// assert_eq!(settings.get("lru_crawler").map(String::as_str), Some("yes"));
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn lru_crawler(mut self, arg: LruCrawlerArg) -> Self {
        self.1.push(build_lru_crawler_cmd(arg).to_vec());
        self
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, LruCrawlerArg, PipelineResponse, StatsArg};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// let _ = conn
    ///     .pipeline()
    ///     .lru_crawler(LruCrawlerArg::Enable)
    ///     .execute()
    ///     .await;
    /// let result = conn
    ///     .pipeline()
    ///     .lru_crawler_sleep(1_000_000)
    ///     .execute()
    ///     .await?;
    /// assert_eq!(result, [PipelineResponse::Unit(())]);
    /// let settings = conn.stats(Some(StatsArg::Settings)).await?;
    /// assert_eq!(
    ///     settings.get("lru_crawler_sleep").map(String::as_str),
    ///     Some("1000000")
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn lru_crawler_sleep(mut self, microseconds: usize) -> Self {
        self.1.push(build_lru_clawler_sleep_cmd(microseconds));
        self
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, LruCrawlerArg, PipelineResponse, StatsArg};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// let _ = conn
    ///     .pipeline()
    ///     .lru_crawler(LruCrawlerArg::Enable)
    ///     .execute()
    ///     .await;
    /// let result = conn.pipeline().lru_crawler_tocrawl(1).execute().await?;
    /// assert_eq!(result, [PipelineResponse::Unit(())]);
    /// let settings = conn.stats(Some(StatsArg::Settings)).await?;
    /// assert_eq!(
    ///     settings.get("lru_crawler_tocrawl").map(String::as_str),
    ///     Some("1")
    /// );
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn lru_crawler_tocrawl(mut self, arg: u32) -> Self {
        self.1.push(build_lru_crawler_tocrawl_cmd(arg));
        self
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, LruCrawlerArg, LruCrawlerCrawlArg, PipelineResponse};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn
    ///     .pipeline()
    ///     .lru_crawler_crawl(LruCrawlerCrawlArg::All)
    ///     .execute()
    ///     .await?;
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn lru_crawler_crawl(mut self, arg: LruCrawlerCrawlArg<'_>) -> Self {
        self.1.push(build_lru_clawler_crawl_cmd(arg));
        self
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, PipelineResponse};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.pipeline().slabs_reassign(1, 2).execute().await?;
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn slabs_reassign(mut self, source_class: isize, dest_class: isize) -> Self {
        self.1
            .push(build_slabs_reassign_cmd(source_class, dest_class));
        self
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, LruCrawlerArg, LruCrawlerMetadumpArg, PipelineResponse, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.pipeline().lru_crawler_metadump(LruCrawlerMetadumpArg::All).execute().await?;
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn lru_crawler_metadump(mut self, arg: LruCrawlerMetadumpArg<'_>) -> Self {
        self.1.push(build_lru_clawler_metadump_cmd(arg));
        self
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, LruCrawlerArg, LruCrawlerMgdumpArg, PipelineResponse, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.set(b"doc_pipeline_mgdump", 0, 0, false, b"value").await?;
    /// conn.pipeline().lru_crawler_mgdump(LruCrawlerMgdumpArg::All).execute().await?;
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn lru_crawler_mgdump(mut self, arg: LruCrawlerMgdumpArg<'_>) -> Self {
        self.1.push(build_lru_clawler_mgdump_cmd(arg));
        self
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, PipelineResponse};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// let result = conn.pipeline().mn().execute().await?;
    /// assert_eq!(result, [PipelineResponse::Unit(())]);
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn mn(mut self) -> Self {
        self.1.push(build_mn_cmd().to_vec());
        self
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, PipelineResponse, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.set(b"doc_pipeline_me", 0, 0, false, b"value").await?;
    /// let result = conn.pipeline().me(b"doc_pipeline_me").execute().await?;
    /// match &result[0] {
    ///     PipelineResponse::OptionString(Some(info)) => assert!(info.starts_with("doc_pipeline_me ")),
    ///     other => panic!("unexpected response: {other:?}"),
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn me(mut self, key: impl AsRef<[u8]>) -> Self {
        self.1.push(build_me_cmd(key.as_ref()));
        self
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, MgFlag, MgStatus, PipelineResponse, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.set(b"doc_pipeline_mg", 7, 0, false, b"hello").await?;
    /// let result = conn
    ///     .pipeline()
    ///     .mg(
    ///         b"doc_pipeline_mg",
    ///         &[
    ///             MgFlag::ReturnKey,
    ///             MgFlag::ReturnFlags,
    ///             MgFlag::ReturnSize,
    ///             MgFlag::ReturnValue,
    ///         ],
    ///     )
    ///     .execute()
    ///     .await?;
    /// match &result[0] {
    ///     PipelineResponse::MetaGet(item) => {
    ///         assert_eq!(item.status, MgStatus::WithBody);
    ///         assert_eq!(item.key, Some("doc_pipeline_mg".to_string()));
    ///         assert_eq!(item.flags, Some(7));
    ///         assert_eq!(item.size, Some(5));
    ///         assert_eq!(item.data_block, Some(b"hello".to_vec()));
    ///     }
    ///     other => panic!("unexpected response: {other:?}"),
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn mg(mut self, key: impl AsRef<[u8]>, flags: &[MgFlag]) -> Self {
        self.1.push(build_mc_cmd(
            b"mg",
            key.as_ref(),
            &build_mg_flags(flags),
            None,
        ));
        self
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, MsFlag, MsMode, PipelineResponse, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// let result = conn
    ///     .pipeline()
    ///     .ms(
    ///         b"doc_pipeline_ms",
    ///         &[MsFlag::ReturnKey, MsFlag::ReturnSize],
    ///         b"hello",
    ///     )
    ///     .execute()
    ///     .await?;
    /// match &result[0] {
    ///     PipelineResponse::MetaSet(item) => {
    ///         assert_eq!(item.status, StorageResult::Stored);
    ///         assert_eq!(item.key, Some("doc_pipeline_ms".to_string()));
    ///         assert_eq!(item.size, Some(5));
    ///     }
    ///     other => panic!("unexpected response: {other:?}"),
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn ms(
        mut self,
        key: impl AsRef<[u8]>,
        flags: &[MsFlag],
        data_block: impl AsRef<[u8]>,
    ) -> Self {
        self.1.push(build_mc_cmd(
            b"ms",
            key.as_ref(),
            &build_ms_flags(flags),
            Some(data_block.as_ref()),
        ));
        self
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, MdFlag, MdStatus, PipelineResponse, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.set(b"doc_pipeline_md", 0, 0, false, b"value").await?;
    /// let result = conn
    ///     .pipeline()
    ///     .md(b"doc_pipeline_md", &[MdFlag::ReturnKey])
    ///     .execute()
    ///     .await?;
    /// match &result[0] {
    ///     PipelineResponse::MetaDelete(item) => {
    ///         assert_eq!(item.status, MdStatus::Deleted);
    ///         assert_eq!(item.key, Some("doc_pipeline_md".to_string()));
    ///     }
    ///     other => panic!("unexpected response: {other:?}"),
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn md(mut self, key: impl AsRef<[u8]>, flags: &[MdFlag]) -> Self {
        self.1.push(build_mc_cmd(
            b"md",
            key.as_ref(),
            &build_md_flags(flags),
            None,
        ));
        self
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, MaFlag, MaMode, MaStatus, PipelineResponse, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// let _ = conn.delete(b"doc_pipeline_ma", false).await?;
    /// let result = conn
    ///     .pipeline()
    ///     .ma(
    ///         b"doc_pipeline_ma",
    ///         &[
    ///             MaFlag::AutoCreate(0),
    ///             MaFlag::InitValue(5),
    ///             MaFlag::Mode(MaMode::Incr),
    ///             MaFlag::ReturnValue,
    ///             MaFlag::ReturnKey,
    ///         ],
    ///     )
    ///     .execute()
    ///     .await?;
    /// match &result[0] {
    ///     PipelineResponse::MetaArithmetic(item) => {
    ///         assert_eq!(item.status, MaStatus::WithBody);
    ///         assert_eq!(item.number, Some(5));
    ///         assert_eq!(item.key, Some("doc_pipeline_ma".to_string()));
    ///     }
    ///     other => panic!("unexpected response: {other:?}"),
    /// }
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn ma(mut self, key: impl AsRef<[u8]>, flags: &[MaFlag]) -> Self {
        self.1.push(build_mc_cmd(
            b"ma",
            key.as_ref(),
            &build_ma_flags(flags),
            None,
        ));
        self
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, LruArg, LruMode, PipelineResponse};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// let result = conn
    ///     .pipeline()
    ///     .lru(LruArg::Mode(LruMode::Flat))
    ///     .execute()
    ///     .await;
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn lru(mut self, arg: LruArg) -> Self {
        self.1.push(build_lru_cmd(arg));
        self
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, VerbosityArg, PipelineResponse};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// let result = conn
    ///     .pipeline()
    ///     .verbosity(VerbosityArg::Off, false)
    ///     .execute()
    ///     .await?;
    /// assert_eq!(result, [PipelineResponse::Unit(())]);
    /// #     Ok::<(), mcmc_rs::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn verbosity(mut self, arg: VerbosityArg, noreply: bool) -> Self {
        self.1.push(build_verbosity_cmd(arg, noreply));
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::assert_matches;

    use pretty_assertions::assert_eq;
    use smol::block_on;

    #[test]
    fn test_version() {
        block_on(async {
            let mut c = Cursor::new(b"version\r\nVERSION 1.2.3\r\n".to_vec());
            assert_eq!("1.2.3", version_cmd(&mut c).await.unwrap());

            let mut c = Cursor::new(b"version\r\nERROR\r\n".to_vec());
            assert_matches!(version_cmd(&mut c).await, Err(Error::Error));

            let mut c = Cursor::new(b"version\r\nCLIENT_ERROR client msg error\r\n".to_vec());
            assert_matches!(
                version_cmd(&mut c).await,
                Err(Error::ClientError(e)) if e == "client msg error"
            );

            let mut c = Cursor::new(b"version\r\nSERVER_ERROR server msg error\r\n".to_vec());
            assert_matches!(
                version_cmd(&mut c).await,
                Err(Error::ServerError(e)) if e == "server msg error"
            );

            let mut c = Cursor::new(b"version\r\nUNEXPECTED_ERROR msg error\r\n".to_vec());
            assert_matches!(
                version_cmd(&mut c).await,
                Err(Error::UnexpectedError(e)) if e == "UNEXPECTED_ERROR msg error\r\n"
            );
        })
    }

    #[test]
    fn test_quit() {
        block_on(async {
            let mut c = Cursor::new(b"quit\r\n".to_vec());
            assert!(quit_cmd(&mut c).await.is_ok())
        })
    }

    #[test]
    fn test_shutdown() {
        block_on(async {
            let mut c = Cursor::new(b"shutdown\r\n".to_vec());
            assert!(shutdown_cmd(&mut c, false).await.is_ok());

            let mut c = Cursor::new(b"shutdown graceful\r\n".to_vec());
            assert!(shutdown_cmd(&mut c, true).await.is_ok())
        })
    }

    #[test]
    fn test_cache_memlimit() {
        block_on(async {
            let mut c = Cursor::new(b"cache_memlimit 1\r\nOK\r\n".to_vec());
            assert!(cache_memlimit_cmd(&mut c, 1, false).await.is_ok());

            let mut c = Cursor::new(b"cache_memlimit 1 noreply\r\n".to_vec());
            assert!(cache_memlimit_cmd(&mut c, 1, true).await.is_ok());

            let mut c = Cursor::new(b"cache_memlimit 1\r\nERROR\r\n".to_vec());
            assert_matches!(
                cache_memlimit_cmd(&mut c, 1, false).await,
                Err(Error::Error)
            );
        })
    }

    #[test]
    fn test_flush_all() {
        block_on(async {
            let mut c = Cursor::new(b"flush_all\r\nOK\r\n".to_vec());
            assert!(flush_all_cmd(&mut c, None, false).await.is_ok());

            let mut c = Cursor::new(b"flush_all 1 noreply\r\n".to_vec());
            assert!(flush_all_cmd(&mut c, Some(1), true).await.is_ok());

            let mut c = Cursor::new(b"flush_all\r\nERROR\r\n".to_vec());
            assert_matches!(flush_all_cmd(&mut c, None, false).await, Err(Error::Error));
        })
    }

    #[test]
    fn test_storage() {
        block_on(async {
            let mut c = Cursor::new(b"cas key 0 0 0 0\r\nvalue\r\nSTORED\r\n".to_vec());
            assert_eq!(
                storage_cmd(&mut c, b"cas", b"key", 0, 0, Some(0), false, b"value")
                    .await
                    .unwrap(),
                StorageResult::Stored
            );

            let mut c = Cursor::new(b"append key 0 0 0 noreply\r\nvalue\r\n".to_vec());
            assert_eq!(
                storage_cmd(&mut c, b"append", b"key", 0, 0, None, true, b"value")
                    .await
                    .unwrap(),
                StorageResult::Stored
            );

            let mut c = Cursor::new(b"prepend key 0 0 0\r\nvalue\r\nNOT_STORED\r\n".to_vec());
            assert_eq!(
                storage_cmd(&mut c, b"prepend", b"key", 0, 0, None, false, b"value")
                    .await
                    .unwrap(),
                StorageResult::NotStored
            );

            let mut c = Cursor::new(b"prepend key 0 0 0\r\nvalue\r\nEXISTS\r\n".to_vec());
            assert_eq!(
                storage_cmd(&mut c, b"prepend", b"key", 0, 0, None, false, b"value")
                    .await
                    .unwrap(),
                StorageResult::Exists
            );

            let mut c = Cursor::new(b"prepend key 0 0 0\r\nvalue\r\nNOT_FOUND\r\n".to_vec());
            assert_eq!(
                storage_cmd(&mut c, b"prepend", b"key", 0, 0, None, false, b"value")
                    .await
                    .unwrap(),
                StorageResult::NotFound
            );

            let mut c = Cursor::new(b"add key 0 0 0\r\nvalue\r\nERROR\r\n".to_vec());
            assert_matches!(
                storage_cmd(&mut c, b"add", b"key", 0, 0, None, false, b"value").await,
                Err(Error::Error)
            )
        })
    }

    #[test]
    fn test_delete() {
        block_on(async {
            let mut c = Cursor::new(b"delete key\r\nDELETED\r\n".to_vec());
            assert_eq!(
                delete_cmd(&mut c, b"key", false).await.unwrap(),
                DeleteResult::Deleted
            );

            let mut c = Cursor::new(b"delete key\r\nNOT_FOUND\r\n".to_vec());
            assert_eq!(
                delete_cmd(&mut c, b"key", false).await.unwrap(),
                DeleteResult::NotFound
            );

            let mut c = Cursor::new(b"delete key noreply\r\n".to_vec());
            assert_eq!(
                delete_cmd(&mut c, b"key", true).await.unwrap(),
                DeleteResult::Deleted
            );

            let mut c = Cursor::new(b"delete key\r\nERROR\r\n".to_vec());
            assert_matches!(delete_cmd(&mut c, b"key", false).await, Err(Error::Error));
        })
    }

    #[test]
    fn test_auth() {
        block_on(async {
            let mut c = Cursor::new(b"set _ _ _ 3\r\na b\r\nSTORED\r\n".to_vec());
            assert!(auth_cmd(&mut c, b"a", b"b").await.is_ok());

            let mut c = Cursor::new(b"set _ _ _ 3\r\na b\r\nERROR\r\n".to_vec());
            assert_matches!(auth_cmd(&mut c, b"a", b"b").await, Err(Error::Error));
        })
    }

    #[test]
    fn test_incr_decr() {
        block_on(async {
            let mut c = Cursor::new(b"incr key 1\r\n2\r\n".to_vec());
            assert_eq!(
                incr_decr_cmd(&mut c, b"incr", b"key", 1, false)
                    .await
                    .unwrap(),
                Some(2)
            );

            let mut c = Cursor::new(b"incr key 1 noreply\r\n".to_vec());
            assert!(
                incr_decr_cmd(&mut c, b"incr", b"key", 1, true)
                    .await
                    .unwrap()
                    .is_none(),
            );

            let mut c = Cursor::new(b"incr key 1\r\nNOT_FOUND\r\n".to_vec());
            assert!(
                incr_decr_cmd(&mut c, b"incr", b"key", 1, false)
                    .await
                    .unwrap()
                    .is_none()
            );

            let mut c = Cursor::new(b"incr key 1\r\nERROR\r\n".to_vec());
            assert_matches!(
                incr_decr_cmd(&mut c, b"incr", b"key", 1, false).await,
                Err(Error::Error)
            );
        })
    }

    #[test]
    fn test_touch() {
        block_on(async {
            let mut c = Cursor::new(b"touch key 0\r\nTOUCHED\r\n".to_vec());
            assert_eq!(
                touch_cmd(&mut c, b"key", 0, false).await.unwrap(),
                TouchResult::Touched
            );

            let mut c = Cursor::new(b"touch key 0\r\nNOT_FOUND\r\n".to_vec());
            assert_eq!(
                touch_cmd(&mut c, b"key", 0, false).await.unwrap(),
                TouchResult::NotFound
            );

            let mut c = Cursor::new(b"touch key 0 noreply\r\n".to_vec());
            assert_eq!(
                touch_cmd(&mut c, b"key", 0, true).await.unwrap(),
                TouchResult::Touched
            );

            let mut c = Cursor::new(b"touch key 0\r\nERROR\r\n".to_vec());
            assert_matches!(touch_cmd(&mut c, b"key", 0, false).await, Err(Error::Error))
        })
    }

    #[test]
    fn test_retrieval() {
        block_on(async {
            let mut c = Cursor::new(b"gets key\r\nEND\r\n".to_vec());
            assert_eq!(
                retrieval_cmd(&mut c, b"gets", None, &[b"key"])
                    .await
                    .unwrap(),
                vec![]
            );

            let mut c = Cursor::new(b"gat 0 key\r\nVALUE key 0 1\r\na\r\nEND\r\n".to_vec());
            assert_eq!(
                retrieval_cmd(&mut c, b"gat", Some(0), &[b"key"])
                    .await
                    .unwrap(),
                vec![Item {
                    key: "key".to_string(),
                    flags: 0,
                    cas_unique: None,
                    data_block: b"a".to_vec(),
                }]
            );

            let mut c = Cursor::new(
                b"gats 0 key key2\r\nVALUE key 0 1 0\r\na\r\nVALUE key2 0 1 0\r\na\r\nEND\r\n"
                    .to_vec(),
            );
            assert_eq!(
                retrieval_cmd(&mut c, b"gats", Some(0), &[b"key", b"key2"])
                    .await
                    .unwrap(),
                vec![
                    Item {
                        key: "key".to_string(),
                        flags: 0,
                        cas_unique: Some(0),
                        data_block: b"a".to_vec()
                    },
                    Item {
                        key: "key2".to_string(),
                        flags: 0,
                        cas_unique: Some(0),
                        data_block: b"a".to_vec()
                    }
                ]
            );

            let mut c = Cursor::new(b"get key\r\nERROR\r\n".to_vec());
            assert_matches!(
                retrieval_cmd(&mut c, b"get", None, &[b"key"]).await,
                Err(Error::Error)
            )
        })
    }

    #[test]
    fn test_stats() {
        block_on(async {
            let mut c =
                Cursor::new(b"stats\r\nSTAT version 1.2.3\r\nSTAT threads 4\r\nEND\r\n".to_vec());
            assert_eq!(
                stats_cmd(&mut c, None).await.unwrap(),
                HashMap::from([
                    ("version".to_string(), "1.2.3".to_string()),
                    ("threads".to_string(), "4".to_string()),
                ])
            );

            let mut c = Cursor::new(b"stats settings\r\nERROR\r\n".to_vec());
            assert_matches!(
                stats_cmd(&mut c, Some(StatsArg::Settings)).await,
                Err(Error::Error)
            );

            let mut c = Cursor::new(b"stats items\r\nERROR\r\n".to_vec());
            assert_matches!(
                stats_cmd(&mut c, Some(StatsArg::Items)).await,
                Err(Error::Error)
            );

            let mut c = Cursor::new(b"stats sizes\r\nERROR\r\n".to_vec());
            assert_matches!(
                stats_cmd(&mut c, Some(StatsArg::Sizes)).await,
                Err(Error::Error)
            );

            let mut c = Cursor::new(b"stats slabs\r\nERROR\r\n".to_vec());
            assert_matches!(
                stats_cmd(&mut c, Some(StatsArg::Slabs)).await,
                Err(Error::Error)
            );

            let mut c = Cursor::new(b"stats conns\r\nERROR\r\n".to_vec());
            assert_matches!(
                stats_cmd(&mut c, Some(StatsArg::Conns)).await,
                Err(Error::Error)
            )
        })
    }

    #[test]
    fn test_slabs_automove() {
        block_on(async {
            let mut c = Cursor::new(b"slabs automove 0\r\nOK\r\n".to_vec());
            assert!(
                slabs_automove_cmd(&mut c, SlabsAutomoveArg::Zero)
                    .await
                    .is_ok()
            );

            let mut c = Cursor::new(b"slabs automove 1\r\nERROR\r\n".to_vec());
            assert_matches!(
                slabs_automove_cmd(&mut c, SlabsAutomoveArg::One).await,
                Err(Error::Error)
            );

            let mut c = Cursor::new(b"slabs automove 2\r\nERROR\r\n".to_vec());
            assert_matches!(
                slabs_automove_cmd(&mut c, SlabsAutomoveArg::Two).await,
                Err(Error::Error)
            )
        })
    }

    #[test]
    fn test_lru_crawler() {
        block_on(async {
            let mut c = Cursor::new(b"lru_crawler enable\r\nOK\r\n".to_vec());
            assert!(lru_crawler_cmd(&mut c, LruCrawlerArg::Enable).await.is_ok());

            let mut c = Cursor::new(b"lru_crawler disable\r\nERROR\r\n".to_vec());
            assert_matches!(
                lru_crawler_cmd(&mut c, LruCrawlerArg::Disable).await,
                Err(Error::Error)
            )
        })
    }

    #[test]
    fn test_lru_crawler_sleep() {
        block_on(async {
            let mut c = Cursor::new(b"lru_crawler sleep 1000000\r\nOK\r\n".to_vec());
            assert!(lru_crawler_sleep_cmd(&mut c, 1_000_000).await.is_ok());

            let mut c = Cursor::new(b"lru_crawler sleep 0\r\nERROR\r\n".to_vec());
            assert_matches!(lru_crawler_sleep_cmd(&mut c, 0).await, Err(Error::Error))
        })
    }

    #[test]
    fn test_lru_crawler_tocrawl() {
        block_on(async {
            let mut c = Cursor::new(b"lru_crawler tocrawl 0\r\nOK\r\n".to_vec());
            assert!(lru_crawler_tocrawl_cmd(&mut c, 0).await.is_ok());

            let mut c = Cursor::new(b"lru_crawler tocrawl 0\r\nERROR\r\n".to_vec());
            assert_matches!(lru_crawler_tocrawl_cmd(&mut c, 0).await, Err(Error::Error))
        })
    }

    #[test]
    fn test_lru_crawler_crawl() {
        block_on(async {
            let mut c = Cursor::new(b"lru_crawler crawl 1,2,3\r\nOK\r\n".to_vec());
            assert_eq!(
                lru_crawler_crawl_cmd(&mut c, LruCrawlerCrawlArg::Classids(&[1, 2, 3]))
                    .await
                    .unwrap(),
                LruCrawlerResult::Ok
            );

            let mut c = Cursor::new(b"lru_crawler crawl 1,2,3\r\nBUSY msg\r\n".to_vec());
            assert_eq!(
                lru_crawler_crawl_cmd(&mut c, LruCrawlerCrawlArg::Classids(&[1, 2, 3]))
                    .await
                    .unwrap(),
                LruCrawlerResult::Busy("msg".to_string())
            );

            let mut c = Cursor::new(b"lru_crawler crawl 1,2,3\r\nBADCLASS msg\r\n".to_vec());
            assert_eq!(
                lru_crawler_crawl_cmd(&mut c, LruCrawlerCrawlArg::Classids(&[1, 2, 3]))
                    .await
                    .unwrap(),
                LruCrawlerResult::BadClass("msg".to_string())
            );

            let mut c = Cursor::new(b"lru_crawler crawl all\r\nERROR\r\n".to_vec());
            assert_matches!(
                lru_crawler_crawl_cmd(&mut c, LruCrawlerCrawlArg::All).await,
                Err(Error::Error)
            )
        })
    }

    #[test]
    fn test_slabs_reassign() {
        block_on(async {
            let mut c = Cursor::new(b"slabs reassign 1 10\r\nOK\r\n".to_vec());
            assert_eq!(
                slabs_reassign_cmd(&mut c, 1, 10).await.unwrap(),
                SlabsReassignResult::Ok
            );

            let mut c = Cursor::new(b"slabs reassign 1 10\r\nBUSY msg\r\n".to_vec());
            assert_eq!(
                slabs_reassign_cmd(&mut c, 1, 10).await.unwrap(),
                SlabsReassignResult::Busy("msg".to_string())
            );

            let mut c = Cursor::new(b"slabs reassign 1 10\r\nBADCLASS msg\r\n".to_vec());
            assert_eq!(
                slabs_reassign_cmd(&mut c, 1, 10).await.unwrap(),
                SlabsReassignResult::BadClass("msg".to_string())
            );

            let mut c = Cursor::new(b"slabs reassign 1 10\r\nNOSPARE msg\r\n".to_vec());
            assert_eq!(
                slabs_reassign_cmd(&mut c, 1, 10).await.unwrap(),
                SlabsReassignResult::NoSpare("msg".to_string())
            );

            let mut c = Cursor::new(b"slabs reassign 1 10\r\nNOTFULL msg\r\n".to_vec());
            assert_eq!(
                slabs_reassign_cmd(&mut c, 1, 10).await.unwrap(),
                SlabsReassignResult::NotFull("msg".to_string())
            );

            let mut c = Cursor::new(b"slabs reassign 1 10\r\nUNSAFE msg\r\n".to_vec());
            assert_eq!(
                slabs_reassign_cmd(&mut c, 1, 10).await.unwrap(),
                SlabsReassignResult::UnSafe("msg".to_string())
            );

            let mut c = Cursor::new(b"slabs reassign 1 10\r\nSAME msg\r\n".to_vec());
            assert_eq!(
                slabs_reassign_cmd(&mut c, 1, 10).await.unwrap(),
                SlabsReassignResult::Same("msg".to_string())
            );

            let mut c = Cursor::new(b"slabs reassign 1 10\r\nERROR\r\n".to_vec());
            assert_matches!(slabs_reassign_cmd(&mut c, 1, 10).await, Err(Error::Error))
        })
    }

    #[test]
    fn test_lru_crawler_metadump() {
        block_on(async {
            let mut c = Cursor::new(b"lru_crawler metadump all\r\nOK\r\nkey=key exp=-1 la=1745299782 cas=2 fetch=no cls=1 size=63 flags=0\r\nkey=key2 exp=-1 la=1745299782 cas=2 fetch=no cls=1 size=63 flags=0\r\nEND\r\n".to_vec());
            assert_eq!(
                lru_crawler_metadump_cmd(&mut c, LruCrawlerMetadumpArg::All)
                    .await
                    .unwrap(),
                LruCrawlerDumpResult::Ok(vec![
                    "key=key exp=-1 la=1745299782 cas=2 fetch=no cls=1 size=63 flags=0".to_string(),
                    "key=key2 exp=-1 la=1745299782 cas=2 fetch=no cls=1 size=63 flags=0"
                        .to_string()
                ])
            );

            let mut c = Cursor::new(b"lru_crawler metadump all\r\nBUSY msg\r\n".to_vec());
            assert_eq!(
                lru_crawler_metadump_cmd(&mut c, LruCrawlerMetadumpArg::All)
                    .await
                    .unwrap(),
                LruCrawlerDumpResult::Busy("msg".to_string())
            );

            let mut c = Cursor::new(b"lru_crawler metadump all\r\nBADCLASS msg\r\n".to_vec());
            assert_eq!(
                lru_crawler_metadump_cmd(&mut c, LruCrawlerMetadumpArg::All)
                    .await
                    .unwrap(),
                LruCrawlerDumpResult::BadClass("msg".to_string())
            );

            let mut c = Cursor::new(b"lru_crawler metadump 1,2,3\r\nERROR\r\n".to_vec());
            assert_matches!(
                lru_crawler_metadump_cmd(&mut c, LruCrawlerMetadumpArg::Classids(&[1, 2, 3])).await,
                Err(Error::Error)
            );

            let mut c = Cursor::new(b"lru_crawler metadump hash\r\nERROR\r\n".to_vec());
            assert_matches!(
                lru_crawler_metadump_cmd(&mut c, LruCrawlerMetadumpArg::Hash).await,
                Err(Error::Error)
            )
        })
    }

    #[test]
    fn test_lru_crawler_mgdump() {
        block_on(async {
            let mut c = Cursor::new(
                b"lru_crawler mgdump 1,2,3\r\nOK\r\nmg key\r\nmg key2\r\nEN\r\n".to_vec(),
            );
            assert_eq!(
                lru_crawler_mgdump_cmd(&mut c, LruCrawlerMgdumpArg::Classids(&[1, 2, 3]))
                    .await
                    .unwrap(),
                LruCrawlerDumpResult::Ok(vec!["mg key\r\n".to_string(), "mg key2\r\n".to_string()])
            );

            let mut c = Cursor::new(b"lru_crawler mgdump all\r\nBUSY msg\r\n".to_vec());
            assert_eq!(
                lru_crawler_mgdump_cmd(&mut c, LruCrawlerMgdumpArg::All)
                    .await
                    .unwrap(),
                LruCrawlerDumpResult::Busy("msg".to_string())
            );

            let mut c = Cursor::new(b"lru_crawler mgdump 1,2,3\r\nBADCLASS msg\r\n".to_vec());
            assert_eq!(
                lru_crawler_mgdump_cmd(&mut c, LruCrawlerMgdumpArg::Classids(&[1, 2, 3]))
                    .await
                    .unwrap(),
                LruCrawlerDumpResult::BadClass("msg".to_string())
            );

            let mut c = Cursor::new(b"lru_crawler mgdump hash\r\nERROR\r\n".to_vec());
            assert_matches!(
                lru_crawler_mgdump_cmd(&mut c, LruCrawlerMgdumpArg::Hash).await,
                Err(Error::Error)
            )
        })
    }

    #[test]
    fn test_mn() {
        block_on(async {
            let mut c = Cursor::new(b"mn\r\nMN\r\n".to_vec());
            assert!(mn_cmd(&mut c).await.is_ok());

            let mut c = Cursor::new(b"mn\r\nERROR\r\n".to_vec());
            assert_matches!(mn_cmd(&mut c).await, Err(Error::Error))
        })
    }

    #[test]
    fn test_me() {
        block_on(async {
            let mut c = Cursor::new(b"me key\r\nEN\r\n".to_vec());
            assert!(me_cmd(&mut c, b"key").await.unwrap().is_none());

            let mut c = Cursor::new(
                b"me key\r\nME key exp=-1 la=3 cas=2 fetch=no cls=1 size=63\r\n".to_vec(),
            );
            assert_eq!(
                me_cmd(&mut c, b"key").await.unwrap().unwrap(),
                "key exp=-1 la=3 cas=2 fetch=no cls=1 size=63"
            );

            let mut c = Cursor::new(b"me key\r\nERROR\r\n".to_vec());
            assert_matches!(me_cmd(&mut c, b"key").await, Err(Error::Error));
        })
    }

    #[test]
    fn test_pipeline() {
        block_on(async {
            let cmds = [
                b"version\r\n".to_vec(),
                b"quit\r\n".to_vec(),
                b"shutdown\r\n".to_vec(),
                b"cache_memlimit 1\r\n".to_vec(),
                b"cache_memlimit 1 noreply\r\n".to_vec(),
                b"flush_all\r\n".to_vec(),
                b"flush_all 1 noreply\r\n".to_vec(),
                b"cas key 0 0 5 0\r\nvalue\r\n".to_vec(),
                b"append key 0 0 5 noreply\r\nvalue\r\n".to_vec(),
                b"delete key\r\n".to_vec(),
                b"delete key noreply\r\n".to_vec(),
                b"set _ _ _ 3\r\na b\r\n".to_vec(),
                b"incr key 1\r\n".to_vec(),
                b"incr key 1 noreply\r\n".to_vec(),
                b"touch key 0\r\n".to_vec(),
                b"touch key 0 noreply\r\n".to_vec(),
                b"gets key\r\n".to_vec(),
                b"get key key2\r\n".to_vec(),
                b"gat 0 key key2\r\n".to_vec(),
                b"gats 0 key\r\n".to_vec(),
                b"stats\r\n".to_vec(),
                b"slabs automove 0\r\n".to_vec(),
                b"lru_crawler enable\r\n".to_vec(),
                b"lru_crawler disable\r\n".to_vec(),
                b"lru_crawler sleep 1000000\r\n".to_vec(),
                b"lru_crawler tocrawl 0\r\n".to_vec(),
                b"lru_crawler crawl 1,2,3\r\n".to_vec(),
                b"slabs reassign 1 10\r\n".to_vec(),
                b"lru_crawler metadump all\r\n".to_vec(),
                b"lru_crawler mgdump 3\r\n".to_vec(),
                b"mn\r\n".to_vec(),
                b"me key\r\n".to_vec(),
                b"mg 44OG44K544OI b c f h k l Oopaque s t u E0 N0 R0 T0 v\r\n".to_vec(),
                b"ms 44OG44K544OI 2 b c C0 E0 F0 I k Oopaque s T0 MS N0\r\nhi\r\n".to_vec(),
                b"md 44OG44K544OI b C0 E0 I k Oopaque T0 x\r\n".to_vec(),
                b"ma 44OG44K544OI b C0 E0 N0 J0 D0 T0 M+ Oopaque t c v k\r\n".to_vec(),
                b"lru mode flat\r\n".to_vec(),
                b"verbosity 0\r\n".to_vec(),
            ];
            let rps = [
                b"VERSION 1.2.3\r\n".to_vec(),
                b"OK\r\n".to_vec(),
                b"OK\r\n".to_vec(),
                b"STORED\r\n".to_vec(),
                b"DELETED\r\n".to_vec(),
                b"STORED\r\n".to_vec(),
                b"2\r\n".to_vec(),
                b"TOUCHED\r\n".to_vec(),
                b"END\r\n".to_vec(),
                b"END\r\n".to_vec(),
                b"VALUE key 0 1 0\r\na\r\nVALUE key2 0 1 0\r\na\r\nEND\r\n".to_vec(),
                b"VALUE key 0 1 0\r\na\r\nEND\r\n".to_vec(),
                b"STAT version 1.2.3\r\nSTAT threads 4\r\nEND\r\n".to_vec(),
                b"OK\r\n".to_vec(),
                b"OK\r\n".to_vec(),
                b"OK\r\n".to_vec(),
                b"OK\r\n".to_vec(),
                b"OK\r\n".to_vec(),
                b"OK\r\n".to_vec(),
                b"OK\r\n".to_vec(),
                b"OK\r\nkey=key exp=-1 la=1745299782 cas=2 fetch=no cls=1 size=63 flags=0\r\nkey=key2 exp=-1 la=1745299782 cas=2 fetch=no cls=1 size=63 flags=0\r\nEND\r\n".to_vec(),
                b"OK\r\nmg key\r\nmg key2\r\nEN\r\n".to_vec(),
                b"MN\r\n".to_vec(),
                b"ME key exp=-1 la=3 cas=2 fetch=no cls=1 size=63\r\n".to_vec(),
                b"VA 1 b c0 f0 h0 k44OG44K544OI l0 Oopaque s0 t0 W X Z\r\nA\r\n".to_vec(),
                b"HD b c0 k44OG44K544OI Oopaque s0\r\n".to_vec(),
                b"HD k44OG44K544OI Oopaque b\r\n".to_vec(),
                b"VA 2 Oopaque t0 c0 k44OG44K544OI b\r\n10\r\n".to_vec(),
                b"OK\r\n".to_vec(),
                b"OK\r\n".to_vec()
            ];
            let mut c = Cursor::new([cmds.concat(), rps.concat()].concat().to_vec());
            assert_eq!(
                execute_cmd(&mut c, &cmds).await.unwrap(),
                [
                    PipelineResponse::String("1.2.3".to_string()),
                    PipelineResponse::Unit(()),
                    PipelineResponse::Unit(()),
                    PipelineResponse::Unit(()),
                    PipelineResponse::Unit(()),
                    PipelineResponse::Unit(()),
                    PipelineResponse::Unit(()),
                    PipelineResponse::StorageResult(StorageResult::Stored),
                    PipelineResponse::StorageResult(StorageResult::Stored),
                    PipelineResponse::DeleteResult(DeleteResult::Deleted),
                    PipelineResponse::DeleteResult(DeleteResult::Deleted),
                    PipelineResponse::Unit(()),
                    PipelineResponse::Value(Some(2)),
                    PipelineResponse::Value(None),
                    PipelineResponse::TouchResult(TouchResult::Touched),
                    PipelineResponse::TouchResult(TouchResult::Touched),
                    PipelineResponse::OptionItem(None),
                    PipelineResponse::VecItem(Vec::new()),
                    PipelineResponse::VecItem(vec![
                        Item {
                            key: "key".to_string(),
                            flags: 0,
                            cas_unique: Some(0),
                            data_block: b"a".to_vec()
                        },
                        Item {
                            key: "key2".to_string(),
                            flags: 0,
                            cas_unique: Some(0),
                            data_block: b"a".to_vec()
                        }
                    ]),
                    PipelineResponse::OptionItem(Some(Item {
                        key: "key".to_string(),
                        flags: 0,
                        cas_unique: Some(0),
                        data_block: b"a".to_vec()
                    })),
                    PipelineResponse::HashMap(HashMap::from([
                        ("threads".to_string(), "4".to_string()),
                        ("version".to_string(), "1.2.3".to_string())
                    ])),
                    PipelineResponse::Unit(()),
                    PipelineResponse::Unit(()),
                    PipelineResponse::Unit(()),
                    PipelineResponse::Unit(()),
                    PipelineResponse::Unit(()),
                    PipelineResponse::LruCrawlerResult(LruCrawlerResult::Ok),
                    PipelineResponse::SlabsReassignResult(SlabsReassignResult::Ok),
                    PipelineResponse::LruCrawlerDumpResult(LruCrawlerDumpResult::Ok(vec![
                        "key=key exp=-1 la=1745299782 cas=2 fetch=no cls=1 size=63 flags=0"
                            .to_string(),
                        "key=key2 exp=-1 la=1745299782 cas=2 fetch=no cls=1 size=63 flags=0"
                            .to_string()
                    ])),
                    PipelineResponse::LruCrawlerDumpResult(LruCrawlerDumpResult::Ok(vec![
                        "mg key\r\n".to_string(),
                        "mg key2\r\n".to_string()
                    ])),
                    PipelineResponse::Unit(()),
                    PipelineResponse::OptionString(Some(
                        "key exp=-1 la=3 cas=2 fetch=no cls=1 size=63".to_string()
                    )),
                    PipelineResponse::MetaGet(MgItem {
                        status: MgStatus::WithBody,
                        base64_key: true,
                        cas: Some(0),
                        flags: Some(0),
                        hit: Some(0),
                        key: Some("44OG44K544OI".to_string()),
                        last_access_ttl: Some(0),
                        opaque: Some("opaque".to_string()),
                        size: Some(0),
                        ttl: Some(0),
                        data_block: Some(b"A".to_vec()),
                        won_recache: true,
                        stale: true,
                        already_win: true
                    }),
                    PipelineResponse::MetaSet(MsItem {
                        status: StorageResult::Stored,
                        cas: Some(0),
                        key: Some("44OG44K544OI".to_string()),
                        opaque: Some("opaque".to_string()),
                        size: Some(0),
                        base64_key: true
                    }),
                    PipelineResponse::MetaDelete(MdItem {
                        status: MdStatus::Deleted,
                        key: Some("44OG44K544OI".to_string()),
                        opaque: Some("opaque".to_string()),
                        base64_key: true
                    }),
                    PipelineResponse::MetaArithmetic(MaItem {
                        status: MaStatus::WithBody,
                        opaque: Some("opaque".to_string()),
                        ttl: Some(0),
                        cas: Some(0),
                        number: Some(10),
                        key: Some("44OG44K544OI".to_string()),
                        base64_key: true
                    }),
                    PipelineResponse::Unit(()),
                    PipelineResponse::Unit(())
                ]
            );

            let cmds = [
                b"version\r\n".to_vec(),
                b"version\r\n".to_vec(),
                b"version\r\n".to_vec(),
            ];
            let rps = [
                b"VERSION 1.2.3\r\n".to_vec(),
                b"ERROR\r\n".to_vec(),
                b"VERSION 1.2.3\r\n".to_vec(),
            ];
            let mut c = Cursor::new([cmds.concat(), rps.concat()].concat().to_vec());
            assert_matches!(execute_cmd(&mut c, &cmds).await, Err(Error::Error));
        })
    }

    #[test]
    fn test_watch() {
        block_on(async {
            let mut c = Cursor::new(b"watch fetchers mutations evictions connevents proxyreqs proxyevents proxyuser deletions\r\nOK\r\n".to_vec());
            assert!(
                watch_cmd(
                    &mut c,
                    &[
                        WatchArg::Fetchers,
                        WatchArg::Mutations,
                        WatchArg::Evictions,
                        WatchArg::Connevents,
                        WatchArg::Proxyreqs,
                        WatchArg::Proxyevents,
                        WatchArg::Proxyuser,
                        WatchArg::Deletions
                    ]
                )
                .await
                .is_ok()
            );

            let mut c = Cursor::new(b"watch fetchers mutations\r\nERROR\r\n".to_vec());
            assert_matches!(
                watch_cmd(&mut c, &[WatchArg::Fetchers, WatchArg::Mutations]).await,
                Err(Error::Error)
            );
        })
    }

    #[test]
    fn test_mg() {
        block_on(async {
            let mut c = Cursor::new(b"mg key b Pauth-pool Leu-central\r\nEN b\r\n".to_vec());
            assert_eq!(
                mg_cmd(
                    &mut c,
                    b"key",
                    &[
                        MgFlag::Base64Key,
                        MgFlag::ProxyHint("auth-pool".to_string()),
                        MgFlag::LocationHint("eu-central".to_string())
                    ]
                )
                .await
                .unwrap(),
                MgItem {
                    status: MgStatus::NotFound,
                    base64_key: true,
                    cas: None,
                    flags: None,
                    hit: None,
                    key: None,
                    last_access_ttl: None,
                    opaque: None,
                    size: None,
                    ttl: None,
                    data_block: None,
                    already_win: false,
                    won_recache: false,
                    stale: false,
                }
            );

            let mut c = Cursor::new(b"mg 44OG44K544OI b c C0 f h k l Oopaque s t u E0 N0 R0 T0\r\nHD b c0 f0 h0 k44OG44K544OI l0 Oopaque s0 t0 W X Z\r\n".to_vec());
            assert_eq!(
                mg_cmd(
                    &mut c,
                    b"44OG44K544OI",
                    &[
                        MgFlag::Base64Key,
                        MgFlag::ReturnCas,
                        MgFlag::CheckCas(0),
                        MgFlag::ReturnFlags,
                        MgFlag::ReturnHit,
                        MgFlag::ReturnKey,
                        MgFlag::ReturnLastAccess,
                        MgFlag::Opaque("opaque".to_string()),
                        MgFlag::ReturnSize,
                        MgFlag::ReturnTtl,
                        MgFlag::UnBump,
                        MgFlag::NewCas(0),
                        MgFlag::Autovivify(0),
                        MgFlag::RecacheTtl(0),
                        MgFlag::UpdateTtl(0),
                    ]
                )
                .await
                .unwrap(),
                MgItem {
                    status: MgStatus::WithoutBody,
                    base64_key: true,
                    cas: Some(0),
                    flags: Some(0),
                    hit: Some(0),
                    key: Some("44OG44K544OI".to_string()),
                    last_access_ttl: Some(0),
                    opaque: Some("opaque".to_string()),
                    size: Some(0),
                    ttl: Some(0),
                    data_block: None,
                    already_win: true,
                    won_recache: true,
                    stale: true,
                }
            );

            let mut c = Cursor::new(b"mg 44OG44K544OI b c C0 f h k l Oopaque s t u E0 N0 R0 T0 v\r\nVA 1 b c0 f0 h0 k44OG44K544OI l0 Oopaque s0 t0 W X Z\r\nA\r\n".to_vec());
            assert_eq!(
                mg_cmd(
                    &mut c,
                    b"44OG44K544OI",
                    &[
                        MgFlag::Base64Key,
                        MgFlag::ReturnCas,
                        MgFlag::CheckCas(0),
                        MgFlag::ReturnFlags,
                        MgFlag::ReturnHit,
                        MgFlag::ReturnKey,
                        MgFlag::ReturnLastAccess,
                        MgFlag::Opaque("opaque".to_string()),
                        MgFlag::ReturnSize,
                        MgFlag::ReturnTtl,
                        MgFlag::UnBump,
                        MgFlag::ReturnValue,
                        MgFlag::NewCas(0),
                        MgFlag::Autovivify(0),
                        MgFlag::RecacheTtl(0),
                        MgFlag::UpdateTtl(0),
                    ]
                )
                .await
                .unwrap(),
                MgItem {
                    status: MgStatus::WithBody,
                    base64_key: true,
                    cas: Some(0),
                    flags: Some(0),
                    hit: Some(0),
                    key: Some("44OG44K544OI".to_string()),
                    last_access_ttl: Some(0),
                    opaque: Some("opaque".to_string()),
                    size: Some(0),
                    ttl: Some(0),
                    data_block: Some(b"A".to_vec()),
                    already_win: true,
                    won_recache: true,
                    stale: true,
                }
            );

            let mut c = Cursor::new(
                b"mg 44OG44K544OI b c f h k l Oopaque s t u E0 N0 R0 T0 v\r\nERROR\r\n".to_vec(),
            );
            assert_matches!(
                mg_cmd(
                    &mut c,
                    b"44OG44K544OI",
                    &[
                        MgFlag::Base64Key,
                        MgFlag::ReturnCas,
                        MgFlag::ReturnFlags,
                        MgFlag::ReturnHit,
                        MgFlag::ReturnKey,
                        MgFlag::ReturnLastAccess,
                        MgFlag::Opaque("opaque".to_string()),
                        MgFlag::ReturnSize,
                        MgFlag::ReturnTtl,
                        MgFlag::UnBump,
                        MgFlag::ReturnValue,
                        MgFlag::NewCas(0),
                        MgFlag::Autovivify(0),
                        MgFlag::RecacheTtl(0),
                        MgFlag::UpdateTtl(0),
                    ]
                )
                .await,
                Err(Error::Error)
            );
        })
    }

    #[test]
    fn test_ms() {
        block_on(async {
            let mut c = Cursor::new(
                b"ms 44OG44K544OI 2 b c C0 E0 F0 I k Oopaque s T0 MP N0 Pauth-pool Leu-central\r\nhi\r\nNF\r\n".to_vec(),
            );
            assert_eq!(
                ms_cmd(
                    &mut c,
                    b"44OG44K544OI",
                    &[
                        MsFlag::Base64Key,
                        MsFlag::ReturnCas,
                        MsFlag::CompareCas(0),
                        MsFlag::NewCas(0),
                        MsFlag::SetFlags(0),
                        MsFlag::Invalidate,
                        MsFlag::ReturnKey,
                        MsFlag::Opaque("opaque".to_string()),
                        MsFlag::ReturnSize,
                        MsFlag::Ttl(0),
                        MsFlag::Mode(MsMode::Prepend),
                        MsFlag::Autovivify(0),
                        MsFlag::ProxyHint("auth-pool".to_string()),
                        MsFlag::LocationHint("eu-central".to_string())
                    ],
                    b"hi"
                )
                .await
                .unwrap(),
                MsItem {
                    status: StorageResult::NotFound,
                    cas: None,
                    key: None,
                    opaque: None,
                    size: None,
                    base64_key: false
                }
            );

            let mut c = Cursor::new(b"ms 44OG44K544OI 2 MR\r\nhi\r\nEX\r\n".to_vec());
            assert_eq!(
                ms_cmd(
                    &mut c,
                    b"44OG44K544OI",
                    &[MsFlag::Mode(MsMode::Replace)],
                    b"hi"
                )
                .await
                .unwrap(),
                MsItem {
                    status: StorageResult::Exists,
                    cas: None,
                    key: None,
                    opaque: None,
                    size: None,
                    base64_key: false
                }
            );

            let mut c = Cursor::new(
                b"ms 44OG44K544OI 2 b c C0 E0 F0 I k Oopaque s T0 ME N0\r\nhi\r\nNS\r\n".to_vec(),
            );
            assert_eq!(
                ms_cmd(
                    &mut c,
                    b"44OG44K544OI",
                    &[
                        MsFlag::Base64Key,
                        MsFlag::ReturnCas,
                        MsFlag::CompareCas(0),
                        MsFlag::NewCas(0),
                        MsFlag::SetFlags(0),
                        MsFlag::Invalidate,
                        MsFlag::ReturnKey,
                        MsFlag::Opaque("opaque".to_string()),
                        MsFlag::ReturnSize,
                        MsFlag::Ttl(0),
                        MsFlag::Mode(MsMode::Add),
                        MsFlag::Autovivify(0)
                    ],
                    b"hi"
                )
                .await
                .unwrap(),
                MsItem {
                    status: StorageResult::NotStored,
                    cas: None,
                    key: None,
                    opaque: None,
                    size: None,
                    base64_key: false
                }
            );

            let mut c = Cursor::new(
                b"ms 44OG44K544OI 2 b c C0 E0 F0 I k Oopaque s T0 ME N0\r\nhi\r\nERROR\r\n"
                    .to_vec(),
            );
            assert_matches!(
                ms_cmd(
                    &mut c,
                    b"44OG44K544OI",
                    &[
                        MsFlag::Base64Key,
                        MsFlag::ReturnCas,
                        MsFlag::CompareCas(0),
                        MsFlag::NewCas(0),
                        MsFlag::SetFlags(0),
                        MsFlag::Invalidate,
                        MsFlag::ReturnKey,
                        MsFlag::Opaque("opaque".to_string()),
                        MsFlag::ReturnSize,
                        MsFlag::Ttl(0),
                        MsFlag::Mode(MsMode::Append),
                        MsFlag::Autovivify(0)
                    ],
                    b"hi"
                )
                .await,
                Err(Error::Error)
            );

            let mut c = Cursor::new(
                b"ms 44OG44K544OI 2 b c C0 E0 F0 I k Oopaque s T0 MS N0\r\nhi\r\nHD b c0 k44OG44K544OI Oopaque s0\r\n".to_vec(),
            );
            assert_eq!(
                ms_cmd(
                    &mut c,
                    b"44OG44K544OI",
                    &[
                        MsFlag::Base64Key,
                        MsFlag::ReturnCas,
                        MsFlag::CompareCas(0),
                        MsFlag::NewCas(0),
                        MsFlag::SetFlags(0),
                        MsFlag::Invalidate,
                        MsFlag::ReturnKey,
                        MsFlag::Opaque("opaque".to_string()),
                        MsFlag::ReturnSize,
                        MsFlag::Ttl(0),
                        MsFlag::Mode(MsMode::Set),
                        MsFlag::Autovivify(0)
                    ],
                    b"hi"
                )
                .await
                .unwrap(),
                MsItem {
                    status: StorageResult::Stored,
                    cas: Some(0),
                    key: Some("44OG44K544OI".to_string()),
                    opaque: Some("opaque".to_string()),
                    size: Some(0),
                    base64_key: true
                }
            );
        })
    }

    #[test]
    fn test_md() {
        block_on(async {
            let mut c = Cursor::new(
                b"md 44OG44K544OI b C0 E0 I k Oopaque T0 x Pauth-pool Leu-central\r\nNF\r\n"
                    .to_vec(),
            );
            assert_eq!(
                md_cmd(
                    &mut c,
                    b"44OG44K544OI",
                    &[
                        MdFlag::Base64Key,
                        MdFlag::CompareCas(0),
                        MdFlag::NewCas(0),
                        MdFlag::Invalidate,
                        MdFlag::ReturnKey,
                        MdFlag::Opaque("opaque".to_string()),
                        MdFlag::UpdateTtl(0),
                        MdFlag::LeaveKey,
                        MdFlag::ProxyHint("auth-pool".to_string()),
                        MdFlag::LocationHint("eu-central".to_string())
                    ]
                )
                .await
                .unwrap(),
                MdItem {
                    status: MdStatus::NotFound,
                    key: None,
                    opaque: None,
                    base64_key: false,
                }
            );

            let mut c = Cursor::new(b"md 44OG44K544OI\r\nEX\r\n".to_vec());
            assert_eq!(
                md_cmd(&mut c, b"44OG44K544OI", &[]).await.unwrap(),
                MdItem {
                    status: MdStatus::Exists,
                    key: None,
                    opaque: None,
                    base64_key: false,
                }
            );

            let mut c = Cursor::new(
                b"md 44OG44K544OI b C0 E0 I k Oopaque T0 x\r\nHD k44OG44K544OI Oopaque b\r\n"
                    .to_vec(),
            );
            assert_eq!(
                md_cmd(
                    &mut c,
                    b"44OG44K544OI",
                    &[
                        MdFlag::Base64Key,
                        MdFlag::CompareCas(0),
                        MdFlag::NewCas(0),
                        MdFlag::Invalidate,
                        MdFlag::ReturnKey,
                        MdFlag::Opaque("opaque".to_string()),
                        MdFlag::UpdateTtl(0),
                        MdFlag::LeaveKey,
                    ]
                )
                .await
                .unwrap(),
                MdItem {
                    status: MdStatus::Deleted,
                    key: Some("44OG44K544OI".to_string()),
                    opaque: Some("opaque".to_string()),
                    base64_key: true
                }
            );

            let mut c =
                Cursor::new(b"md 44OG44K544OI b C0 E0 I k Oopaque T0 x\r\nERROR\r\n".to_vec());
            assert_matches!(
                md_cmd(
                    &mut c,
                    b"44OG44K544OI",
                    &[
                        MdFlag::Base64Key,
                        MdFlag::CompareCas(0),
                        MdFlag::NewCas(0),
                        MdFlag::Invalidate,
                        MdFlag::ReturnKey,
                        MdFlag::Opaque("opaque".to_string()),
                        MdFlag::UpdateTtl(0),
                        MdFlag::LeaveKey,
                    ]
                )
                .await,
                Err(Error::Error)
            )
        })
    }

    #[test]
    fn test_ma() {
        block_on(async {
            let mut c = Cursor::new(
                b"ma 44OG44K544OI b C0 E0 N0 J0 D0 T0 M+ Oopaque t c v k Pauth-pool Leu-central\r\nNF\r\n".to_vec(),
            );
            assert_eq!(
                ma_cmd(
                    &mut c,
                    b"44OG44K544OI",
                    &[
                        MaFlag::Base64Key,
                        MaFlag::CompareCas(0),
                        MaFlag::NewCas(0),
                        MaFlag::AutoCreate(0),
                        MaFlag::InitValue(0),
                        MaFlag::DeltaApply(0),
                        MaFlag::UpdateTtl(0),
                        MaFlag::Mode(MaMode::Incr),
                        MaFlag::Opaque("opaque".to_string()),
                        MaFlag::ReturnTtl,
                        MaFlag::ReturnCas,
                        MaFlag::ReturnValue,
                        MaFlag::ReturnKey,
                        MaFlag::ProxyHint("auth-pool".to_string()),
                        MaFlag::LocationHint("eu-central".to_string())
                    ],
                )
                .await
                .unwrap(),
                MaItem {
                    status: MaStatus::NotFound,
                    opaque: None,
                    ttl: None,
                    cas: None,
                    number: None,
                    key: None,
                    base64_key: false,
                }
            );

            let mut c = Cursor::new(
            b"ma 44OG44K544OI b C0 E0 N0 J0 D0 T0 M+ Oopaque t c v k\r\nNS Oopaque t0 c0 k44OG44K544OI b\r\n"
                .to_vec(),
        );
            assert_eq!(
                ma_cmd(
                    &mut c,
                    b"44OG44K544OI",
                    &[
                        MaFlag::Base64Key,
                        MaFlag::CompareCas(0),
                        MaFlag::NewCas(0),
                        MaFlag::AutoCreate(0),
                        MaFlag::InitValue(0),
                        MaFlag::DeltaApply(0),
                        MaFlag::UpdateTtl(0),
                        MaFlag::Mode(MaMode::Incr),
                        MaFlag::Opaque("opaque".to_string()),
                        MaFlag::ReturnTtl,
                        MaFlag::ReturnCas,
                        MaFlag::ReturnValue,
                        MaFlag::ReturnKey,
                    ],
                )
                .await
                .unwrap(),
                MaItem {
                    status: MaStatus::NotStored,
                    opaque: Some("opaque".to_string()),
                    ttl: Some(0),
                    cas: Some(0),
                    number: None,
                    key: Some("44OG44K544OI".to_string()),
                    base64_key: true,
                }
            );

            let mut c = Cursor::new(b"ma 44OG44K544OI\r\nEX\r\n".to_vec());
            assert_eq!(
                ma_cmd(&mut c, b"44OG44K544OI", &[],).await.unwrap(),
                MaItem {
                    status: MaStatus::Exists,
                    opaque: None,
                    ttl: None,
                    cas: None,
                    number: None,
                    key: None,
                    base64_key: false,
                }
            );
            let mut c = Cursor::new(b"ma 44OG44K544OI\r\nHD\r\n".to_vec());
            assert_eq!(
                ma_cmd(&mut c, b"44OG44K544OI", &[],).await.unwrap(),
                MaItem {
                    status: MaStatus::WithoutBody,
                    opaque: None,
                    ttl: None,
                    cas: None,
                    number: None,
                    key: None,
                    base64_key: false,
                }
            );

            let mut c = Cursor::new(
            b"ma 44OG44K544OI b C0 E0 N0 J0 D0 T0 M+ Oopaque t c v k\r\nVA 2 Oopaque t0 c0 k44OG44K544OI b\r\n10\r\n"
                .to_vec(),
        );
            assert_eq!(
                ma_cmd(
                    &mut c,
                    b"44OG44K544OI",
                    &[
                        MaFlag::Base64Key,
                        MaFlag::CompareCas(0),
                        MaFlag::NewCas(0),
                        MaFlag::AutoCreate(0),
                        MaFlag::InitValue(0),
                        MaFlag::DeltaApply(0),
                        MaFlag::UpdateTtl(0),
                        MaFlag::Mode(MaMode::Incr),
                        MaFlag::Opaque("opaque".to_string()),
                        MaFlag::ReturnTtl,
                        MaFlag::ReturnCas,
                        MaFlag::ReturnValue,
                        MaFlag::ReturnKey,
                    ],
                )
                .await
                .unwrap(),
                MaItem {
                    status: MaStatus::WithBody,
                    opaque: Some("opaque".to_string()),
                    ttl: Some(0),
                    cas: Some(0),
                    number: Some(10),
                    key: Some("44OG44K544OI".to_string()),
                    base64_key: true,
                }
            );

            let mut c = Cursor::new(
                b"ma 44OG44K544OI b C0 E0 N0 J0 D0 T0 M+ Oopaque t c v k\r\nERROR\r\n".to_vec(),
            );
            assert_matches!(
                ma_cmd(
                    &mut c,
                    b"44OG44K544OI",
                    &[
                        MaFlag::Base64Key,
                        MaFlag::CompareCas(0),
                        MaFlag::NewCas(0),
                        MaFlag::AutoCreate(0),
                        MaFlag::InitValue(0),
                        MaFlag::DeltaApply(0),
                        MaFlag::UpdateTtl(0),
                        MaFlag::Mode(MaMode::Decr),
                        MaFlag::Opaque("opaque".to_string()),
                        MaFlag::ReturnTtl,
                        MaFlag::ReturnCas,
                        MaFlag::ReturnValue,
                        MaFlag::ReturnKey,
                    ],
                )
                .await,
                Err(Error::Error)
            )
        })
    }

    #[test]
    fn test_lru() {
        block_on(async {
            let mut c = Cursor::new(b"lru mode flat\r\nERROR\r\n".to_vec());
            assert_matches!(
                lru_cmd(&mut c, LruArg::Mode(LruMode::Flat)).await,
                Err(Error::Error)
            );

            let mut c = Cursor::new(b"lru mode segmented\r\nOK\r\n".to_vec());
            assert!(
                lru_cmd(&mut c, LruArg::Mode(LruMode::Segmented))
                    .await
                    .is_ok()
            );

            let mut c = Cursor::new(b"lru tune 10 25 0.1 2\r\nOK\r\n".to_vec());
            assert!(
                lru_cmd(
                    &mut c,
                    LruArg::Tune {
                        percent_hot: 10,
                        percent_warm: 25,
                        max_hot_factor: 0.1,
                        max_warm_factor: 2.0
                    }
                )
                .await
                .is_ok()
            );

            let mut c = Cursor::new(b"lru temp_ttl 0\r\nOK\r\n".to_vec());
            assert!(lru_cmd(&mut c, LruArg::TempTtl(0)).await.is_ok())
        })
    }

    #[test]
    fn test_verbosity() {
        block_on(async {
            let mut c = Cursor::new(b"verbosity 0\r\nERROR\r\n".to_vec());
            assert_matches!(
                verbosity_cmd(&mut c, VerbosityArg::Off, false).await,
                Err(Error::Error)
            );

            let mut c = Cursor::new(b"verbosity 0 noreply\r\n".to_vec());
            assert_eq!(
                verbosity_cmd(&mut c, VerbosityArg::Off, true)
                    .await
                    .unwrap(),
                ()
            );

            let mut c = Cursor::new(b"verbosity 1\r\nOK\r\n".to_vec());
            assert_eq!(
                verbosity_cmd(&mut c, VerbosityArg::Error, false)
                    .await
                    .unwrap(),
                ()
            );

            let mut c = Cursor::new(b"verbosity 2\r\nOK\r\n".to_vec());
            assert_eq!(
                verbosity_cmd(&mut c, VerbosityArg::Info, false)
                    .await
                    .unwrap(),
                ()
            );

            let mut c = Cursor::new(b"verbosity 3\r\nOK\r\n".to_vec());
            assert_eq!(
                verbosity_cmd(&mut c, VerbosityArg::Debug, false)
                    .await
                    .unwrap(),
                ()
            );
        })
    }
}
