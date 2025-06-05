use std::collections::HashMap;
use std::io::Write;

use crc32fast;
use deadpool::managed;
use smol::io::{self, BufReader};
use smol::net::{TcpStream, UdpSocket, unix::UnixStream};
use smol::prelude::*;

pub enum AddrArg {
    Tcp(String),
    Unix(String),
    Udp(String),
}

pub struct Manager(AddrArg);
impl Manager {
    /// # Example
    ///
    /// ```
    /// use mcmc_rs::{AddrArg, Manager, Pool};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mgr = Manager::new(AddrArg::Tcp("127.0.0.1:11211".to_string()));
    /// let pool = Pool::builder(mgr).build().unwrap();
    /// let mut conn = pool.get().await.unwrap();
    /// let result = conn.version().await?;
    /// assert!(result.chars().any(|x| x.is_numeric()));
    /// #     Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn new(addr: AddrArg) -> Self {
        Self(addr)
    }
}

impl managed::Manager for Manager {
    type Type = Connection;
    type Error = io::Error;

    async fn create(&self) -> Result<Connection, io::Error> {
        match &self.0 {
            AddrArg::Tcp(addr) => Connection::tcp_connect(addr).await,
            AddrArg::Unix(addr) => Connection::unix_connect(addr).await,
            AddrArg::Udp(_addr) => todo!(),
        }
    }

    async fn recycle(
        &self,
        conn: &mut Connection,
        _: &managed::Metrics,
    ) -> managed::RecycleResult<io::Error> {
        match conn.version().await {
            Ok(_) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

pub type Pool = managed::Pool<Manager>;

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

#[derive(Debug, PartialEq)]
pub struct Item {
    pub key: String,
    pub flags: u32,
    pub cas_unique: Option<u64>,
    pub data_block: Vec<u8>,
}

#[derive(Debug, PartialEq)]
pub enum PipelineResponse {
    Bool(bool),
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
}

pub enum MgFlag {
    Base64Key,
    ReturnCas,
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
}

#[derive(Debug, PartialEq)]
pub struct MgItem {
    pub success: bool,
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
    pub success: bool,
    pub cas: Option<u64>,
    pub key: Option<String>,
    pub opaque: Option<String>,
    pub size: Option<usize>,
    pub base64_key: bool,
}

#[derive(Debug, PartialEq)]
pub struct MdItem {
    pub success: bool,
    pub key: Option<String>,
    pub opaque: Option<String>,
    pub base64_key: bool,
}

#[derive(Debug, PartialEq)]
pub struct MaItem {
    pub success: bool,
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
) -> io::Result<bool> {
    if noreply {
        return Ok(true);
    }
    let mut line = String::new();
    s.read_line(&mut line).await?;
    match line.as_str() {
        "STORED\r\n" => Ok(true),
        "NOT_STORED\r\n" | "EXISTS\r\n" | "NOT_FOUND\r\n" => Ok(false),
        _ => Err(io::Error::other(line)),
    }
}

async fn parse_retrieval_rp<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
) -> io::Result<Vec<Item>> {
    let mut line = String::new();
    s.read_line(&mut line).await?;
    let mut items = Vec::new();
    while line.starts_with("VALUE") {
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
        s.read_line(&mut line).await?;
    }
    if line == "END\r\n" {
        Ok(items)
    } else {
        Err(io::Error::other(line))
    }
}

async fn parse_version_rp<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> io::Result<String> {
    let mut line = String::new();
    let n = s.read_line(&mut line).await?;
    if line.starts_with("VERSION") {
        Ok(line[8..n - 2].to_string())
    } else {
        Err(io::Error::other(line))
    }
}

async fn parse_ok_rp<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    noreply: bool,
) -> io::Result<()> {
    if noreply {
        return Ok(());
    }
    let mut line = String::new();
    s.read_line(&mut line).await?;
    if line == "OK\r\n" {
        Ok(())
    } else {
        Err(io::Error::other(line))
    }
}

async fn parse_delete_rp<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    noreply: bool,
) -> io::Result<bool> {
    if noreply {
        return Ok(true);
    }
    let mut line = String::new();
    s.read_line(&mut line).await?;
    match line.as_str() {
        "DELETED\r\n" => Ok(true),
        "NOT_FOUND\r\n" => Ok(false),
        _ => Err(io::Error::other(line)),
    }
}

async fn parse_auth_rp<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> io::Result<()> {
    let mut line = String::new();
    s.read_line(&mut line).await?;
    match line.as_str() {
        "STORED\r\n" => Ok(()),
        _ => Err(io::Error::other(line)),
    }
}

async fn parse_incr_decr_rp<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    noreply: bool,
) -> io::Result<Option<u64>> {
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
        Err(_) => Err(io::Error::other(line)),
    }
}

async fn parse_touch_rp<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    noreply: bool,
) -> io::Result<bool> {
    if noreply {
        return Ok(true);
    }
    let mut line = String::new();
    s.read_line(&mut line).await?;
    if line == "TOUCHED\r\n" {
        Ok(true)
    } else if line == "NOT_FOUND\r\n" {
        Ok(false)
    } else {
        Err(io::Error::other(line))
    }
}

async fn parse_stats_rp<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
) -> io::Result<HashMap<String, String>> {
    let mut lines = s.lines();
    let mut items = HashMap::new();
    while let Some(line) = lines.next().await {
        let data = line?;
        if data.starts_with("STAT") {
            let mut split = data.split(' ');
            split.next();
            let (k, v) = (
                split.next().unwrap().to_string(),
                split.next().unwrap().trim_end().to_string(),
            );
            items.insert(k, v);
        } else if data == "END" {
            break;
        } else {
            return Err(io::Error::other(data));
        }
    }
    Ok(items)
}

async fn parse_lru_crawler_metadump_rp<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
) -> io::Result<Vec<String>> {
    let mut line = String::new();
    s.read_line(&mut line).await?;
    let mut items = Vec::new();
    while line.starts_with("key=") {
        items.push(line.trim_end().to_string());
        line.clear();
        s.read_line(&mut line).await?;
    }
    if line == "END\r\n" {
        Ok(items)
    } else {
        Err(io::Error::other(line))
    }
}

async fn parse_lru_crawler_mgdump_rp<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
) -> io::Result<Vec<String>> {
    let mut line = String::new();
    s.read_line(&mut line).await?;
    let mut items = Vec::new();
    while line.starts_with("mg ") {
        let mut split = line.split(' ');
        split.next();
        items.push(split.next().unwrap().trim_end().to_string());
        line.clear();
        s.read_line(&mut line).await?;
    }
    if line == "EN\r\n" {
        Ok(items)
    } else {
        Err(io::Error::other(line))
    }
}

async fn parse_mn_rp<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> io::Result<()> {
    let mut line = String::new();
    s.read_line(&mut line).await?;
    if line == "MN\r\n" {
        Ok(())
    } else {
        Err(io::Error::other(line))
    }
}

async fn parse_me_rp<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
) -> io::Result<Option<String>> {
    let mut line = String::new();
    s.read_line(&mut line).await?;
    if line == "EN\r\n" {
        Ok(None)
    } else if line.starts_with("ME") {
        Ok(Some(line[3..line.len() - 2].to_string()))
    } else {
        Err(io::Error::other(line))
    }
}

async fn parse_mg_rp<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> io::Result<MgItem> {
    let mut line = String::new();
    s.read_line(&mut line).await?;
    let success;
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
        success = true;
        split.next();
        Some(split.next().unwrap().parse().unwrap())
    } else if line.starts_with("HD") {
        success = true;
        split.next();
        None
    } else if line.starts_with("EN") {
        success = false;
        split.next();
        None
    } else {
        return Err(io::Error::other(line));
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
        let mut buf = vec![0u8; a + 2];
        s.read_exact(&mut buf).await?;
        buf.truncate(a);
        data_block = Some(buf);
    }
    Ok(MgItem {
        success,
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

async fn parse_ms_rp<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> io::Result<MsItem> {
    let mut line = String::new();
    s.read_line(&mut line).await?;
    let success;
    let (mut cas, mut key, mut opaque, mut size, mut base64_key) = (None, None, None, None, false);
    if line.starts_with("HD") {
        success = true
    } else if line.starts_with("NS") || line.starts_with("EX") || line.starts_with("NF") {
        success = false
    } else {
        return Err(io::Error::other(line));
    }
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
        success,
        cas,
        opaque,
        key,
        size,
        base64_key,
    })
}

async fn parse_md_rp<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> io::Result<MdItem> {
    let mut line = String::new();
    s.read_line(&mut line).await?;
    let success;
    let (mut key, mut opaque, mut base64_key) = (None, None, false);
    if line.starts_with("HD") {
        success = true
    } else if line.starts_with("NF") || line.starts_with("EX") {
        success = false
    } else {
        return Err(io::Error::other(line));
    }
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
        success,
        key,
        opaque,
        base64_key,
    })
}

async fn parse_ma_rp<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> io::Result<MaItem> {
    let mut line = String::new();
    s.read_line(&mut line).await?;
    let success;
    let (mut opaque, mut ttl, mut cas, mut number, mut key, mut base64_key) =
        (None, None, None, None, None, false);
    let mut split = line.trim_end().split(' ');
    let data_len = if line.starts_with("VA") {
        split.next();
        success = true;
        Some(split.next().unwrap().parse().unwrap())
    } else if line.starts_with("HD") {
        split.next();
        success = true;
        None
    } else if line.starts_with("NS") || line.starts_with("EX") || line.starts_with("NF") {
        split.next();
        success = false;
        None
    } else {
        return Err(io::Error::other(line));
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
        success,
        opaque,
        ttl,
        cas,
        number,
        key,
        base64_key,
    })
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
    let n = if noreply { b" noreply".as_slice() } else { b"" };
    let cas = match cas_unique {
        Some(x) => [b" ", x.to_string().as_bytes()].concat(),
        None => b"".to_vec(),
    };
    [
        command_name,
        b" ",
        key,
        b" ",
        flags.to_string().as_bytes(),
        b" ",
        exptime.to_string().as_bytes(),
        b" ",
        data_block.len().to_string().as_bytes(),
        cas.as_slice(),
        n,
        b"\r\n",
        data_block,
        b"\r\n",
    ]
    .concat()
}

fn build_retrieval_cmd(command_name: &[u8], exptime: Option<i64>, keys: &[&[u8]]) -> Vec<u8> {
    let t = match exptime {
        Some(x) => [x.to_string().as_bytes(), b" "].concat(),
        None => b"".to_vec(),
    };
    [
        command_name,
        b" ",
        t.as_slice(),
        keys.join(b" ".as_slice()).as_slice(),
        b"\r\n",
    ]
    .concat()
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
    let mut w = Vec::new();
    write!(
        &mut w,
        "cache_memlimit {limit}{}\r\n",
        if noreply { " noreply" } else { "" }
    )
    .unwrap();
    w
}

fn build_flush_all_cmd(exptime: Option<i64>, noreply: bool) -> Vec<u8> {
    let d = match exptime {
        Some(x) => [b" ", x.to_string().as_bytes()].concat(),
        None => b"".to_vec(),
    };
    let n = if noreply { b" noreply".as_slice() } else { b"" };
    [b"flush_all", d.as_slice(), n, b"\r\n"].concat()
}

fn build_delete_cmd(key: &[u8], noreply: bool) -> Vec<u8> {
    let n = if noreply { b" noreply".as_slice() } else { b"" };
    [b"delete ", key, n, b"\r\n"].concat()
}

fn build_auth_cmd(username: &[u8], password: &[u8]) -> Vec<u8> {
    [
        b"set _ _ _ ",
        (username.len() + password.len() + 1).to_string().as_bytes(),
        b"\r\n",
        username,
        b" ",
        password,
        b"\r\n",
    ]
    .concat()
}

fn build_incr_decr_cmd(command_name: &[u8], key: &[u8], value: u64, noreply: bool) -> Vec<u8> {
    let n = if noreply { b" noreply".as_slice() } else { b"" };
    [
        command_name,
        b" ",
        key,
        b" ",
        value.to_string().as_bytes(),
        n,
        b"\r\n",
    ]
    .concat()
}

fn build_touch_cmd(key: &[u8], exptime: i64, noreply: bool) -> Vec<u8> {
    let n = if noreply { b" noreply".as_slice() } else { b"" };
    [
        b"touch ",
        key,
        b" ",
        exptime.to_string().as_bytes(),
        n,
        b"\r\n",
    ]
    .concat()
}

fn build_stats_cmd(arg: Option<StatsArg>) -> Vec<u8> {
    let a = match arg {
        Some(a) => match a {
            StatsArg::Settings => " settings",
            StatsArg::Items => " items",
            StatsArg::Sizes => " sizes",
            StatsArg::Slabs => " slabs",
            StatsArg::Conns => " conns",
        },
        None => "",
    };
    let mut w = Vec::new();
    write!(&mut w, "stats{a}\r\n").unwrap();
    w
}

fn build_slabs_automove_cmd(arg: SlabsAutomoveArg) -> Vec<u8> {
    let a = match arg {
        SlabsAutomoveArg::Zero => 0,
        SlabsAutomoveArg::One => 1,
        SlabsAutomoveArg::Two => 2,
    };
    let mut w = Vec::new();
    write!(&mut w, "slabs automove {a}\r\n").unwrap();
    w
}

fn build_lru_crawler_cmd(arg: LruCrawlerArg) -> &'static [u8] {
    match arg {
        LruCrawlerArg::Enable => b"lru_crawler enable\r\n",
        LruCrawlerArg::Disable => b"lru_crawler disable\r\n",
    }
}

fn build_lru_clawler_sleep_cmd(microseconds: usize) -> Vec<u8> {
    let mut w = Vec::new();
    write!(&mut w, "lru_crawler sleep {microseconds}\r\n").unwrap();
    w
}

fn build_lru_crawler_tocrawl_cmd(arg: u32) -> Vec<u8> {
    let mut w = Vec::new();
    write!(&mut w, "lru_crawler tocrawl {arg}\r\n").unwrap();
    w
}

fn build_lru_clawler_crawl_cmd(arg: LruCrawlerCrawlArg) -> Vec<u8> {
    let a = match arg {
        LruCrawlerCrawlArg::Classids(ids) => ids
            .iter()
            .map(|x| x.to_string().into_bytes())
            .collect::<Vec<_>>()
            .join(b",".as_slice()),
        LruCrawlerCrawlArg::All => b"all".to_vec(),
    };
    [b"lru_crawler crawl ", a.as_slice(), b"\r\n"].concat()
}

fn build_slabs_reassign_cmd(source_class: usize, dest_class: usize) -> Vec<u8> {
    let mut w = Vec::new();
    write!(&mut w, "slabs reassign {source_class} {dest_class}\r\n").unwrap();
    w
}

fn build_lru_clawler_metadump_cmd(arg: LruCrawlerMetadumpArg) -> Vec<u8> {
    let a = match arg {
        LruCrawlerMetadumpArg::Classids(ids) => ids
            .iter()
            .map(|x| x.to_string().into_bytes())
            .collect::<Vec<_>>()
            .join(b",".as_slice()),
        LruCrawlerMetadumpArg::All => b"all".to_vec(),
        LruCrawlerMetadumpArg::Hash => b"hash".to_vec(),
    };
    [b"lru_crawler metadump ", a.as_slice(), b"\r\n"].concat()
}

fn build_lru_clawler_mgdump_cmd(arg: LruCrawlerMgdumpArg) -> Vec<u8> {
    let a = match arg {
        LruCrawlerMgdumpArg::Classids(ids) => ids
            .iter()
            .map(|x| x.to_string().into_bytes())
            .collect::<Vec<_>>()
            .join(b",".as_slice()),
        LruCrawlerMgdumpArg::All => b"all".to_vec(),
        LruCrawlerMgdumpArg::Hash => b"hash".to_vec(),
    };
    [b"lru_crawler mgdump ", a.as_slice(), b"\r\n"].concat()
}

fn build_mn_cmd() -> &'static [u8] {
    b"mn\r\n"
}

fn build_me_cmd(key: &[u8]) -> Vec<u8> {
    [b"me ", key, b"\r\n"].concat()
}

fn build_watch_cmd(arg: &[WatchArg]) -> Vec<u8> {
    let mut w = b"watch".to_vec();
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
    let (data_len, data, end) = if let Some(a) = data_block {
        (
            [b" ", a.len().to_string().as_bytes()].concat(),
            a,
            b"\r\n".as_slice(),
        )
    } else {
        (b"".to_vec(), b"".as_slice(), b"".as_slice())
    };
    [
        command_name,
        b" ",
        key,
        data_len.as_slice(),
        flags,
        b"\r\n",
        data,
        end,
    ]
    .concat()
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
    });
    w
}

fn build_mg_flags(flags: &[MgFlag]) -> Vec<u8> {
    let mut w = Vec::new();
    flags.iter().for_each(|x| match x {
        MgFlag::Base64Key => w.extend(b" b"),
        MgFlag::ReturnCas => w.extend(b" c"),
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
    };
    w
}

async fn version_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> io::Result<String> {
    s.write_all(build_version_cmd()).await?;
    s.flush().await?;
    parse_version_rp(s).await
}

async fn quit_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> io::Result<()> {
    s.write_all(build_quit_cmd()).await?;
    s.flush().await?;
    Ok(())
}

async fn shutdown_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    graceful: bool,
) -> io::Result<()> {
    s.write_all(build_shutdown_cmd(graceful)).await?;
    s.flush().await?;
    Ok(())
}

async fn cache_memlimit_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    limit: usize,
    noreply: bool,
) -> io::Result<()> {
    s.write_all(&build_cache_memlimit_cmd(limit, noreply))
        .await?;
    s.flush().await?;
    parse_ok_rp(s, noreply).await
}

async fn flush_all_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    exptime: Option<i64>,
    noreply: bool,
) -> io::Result<()> {
    s.write_all(&build_flush_all_cmd(exptime, noreply)).await?;
    s.flush().await?;
    parse_ok_rp(s, noreply).await
}

async fn storage_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    command_name: &[u8],
    key: &[u8],
    flags: u32,
    exptime: i64,
    cas_unique: Option<u64>,
    noreply: bool,
    data_block: &[u8],
) -> io::Result<bool> {
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
    s.flush().await?;
    parse_storage_rp(s, noreply).await
}

async fn delete_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    key: &[u8],
    noreply: bool,
) -> io::Result<bool> {
    s.write_all(&build_delete_cmd(key, noreply)).await?;
    s.flush().await?;
    parse_delete_rp(s, noreply).await
}

async fn auth_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    username: &[u8],
    password: &[u8],
) -> io::Result<()> {
    s.write_all(&build_auth_cmd(username, password)).await?;
    s.flush().await?;
    parse_auth_rp(s).await
}

async fn incr_decr_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    command_name: &[u8],
    key: &[u8],
    value: u64,
    noreply: bool,
) -> io::Result<Option<u64>> {
    s.write_all(&build_incr_decr_cmd(command_name, key, value, noreply))
        .await?;
    s.flush().await?;
    parse_incr_decr_rp(s, noreply).await
}

async fn touch_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    key: &[u8],
    exptime: i64,
    noreply: bool,
) -> io::Result<bool> {
    s.write_all(&build_touch_cmd(key, exptime, noreply)).await?;
    s.flush().await?;
    parse_touch_rp(s, noreply).await
}

async fn retrieval_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    command_name: &[u8],
    exptime: Option<i64>,
    keys: &[&[u8]],
) -> io::Result<Vec<Item>> {
    s.write_all(&build_retrieval_cmd(command_name, exptime, keys))
        .await?;
    s.flush().await?;
    parse_retrieval_rp(s).await
}

async fn stats_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    arg: Option<StatsArg>,
) -> io::Result<HashMap<String, String>> {
    s.write_all(&build_stats_cmd(arg)).await?;
    s.flush().await?;
    parse_stats_rp(s).await
}

async fn slabs_automove_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    arg: SlabsAutomoveArg,
) -> io::Result<()> {
    s.write_all(&build_slabs_automove_cmd(arg)).await?;
    s.flush().await?;
    parse_ok_rp(s, false).await
}

async fn lru_crawler_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    arg: LruCrawlerArg,
) -> io::Result<()> {
    s.write_all(build_lru_crawler_cmd(arg)).await?;
    s.flush().await?;
    parse_ok_rp(s, false).await
}

async fn lru_crawler_sleep_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    microseconds: usize,
) -> io::Result<()> {
    s.write_all(&build_lru_clawler_sleep_cmd(microseconds))
        .await?;
    s.flush().await?;
    parse_ok_rp(s, false).await
}

async fn lru_crawler_tocrawl_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    arg: u32,
) -> io::Result<()> {
    s.write_all(&build_lru_crawler_tocrawl_cmd(arg)).await?;
    s.flush().await?;
    parse_ok_rp(s, false).await
}

async fn lru_crawler_crawl_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    arg: LruCrawlerCrawlArg<'_>,
) -> io::Result<()> {
    s.write_all(&build_lru_clawler_crawl_cmd(arg)).await?;
    s.flush().await?;
    parse_ok_rp(s, false).await
}

async fn slabs_reassign_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    source_class: usize,
    dest_class: usize,
) -> io::Result<()> {
    s.write_all(&build_slabs_reassign_cmd(source_class, dest_class))
        .await?;
    s.flush().await?;
    parse_ok_rp(s, false).await
}

async fn lru_crawler_metadump_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    arg: LruCrawlerMetadumpArg<'_>,
) -> io::Result<Vec<String>> {
    s.write_all(&build_lru_clawler_metadump_cmd(arg)).await?;
    s.flush().await?;
    parse_lru_crawler_metadump_rp(s).await
}

async fn lru_crawler_mgdump_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    arg: LruCrawlerMgdumpArg<'_>,
) -> io::Result<Vec<String>> {
    s.write_all(&build_lru_clawler_mgdump_cmd(arg)).await?;
    s.flush().await?;
    parse_lru_crawler_mgdump_rp(s).await
}

async fn mn_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> io::Result<()> {
    s.write_all(build_mn_cmd()).await?;
    s.flush().await?;
    parse_mn_rp(s).await
}

async fn me_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    key: &[u8],
) -> io::Result<Option<String>> {
    s.write_all(&build_me_cmd(key)).await?;
    s.flush().await?;
    parse_me_rp(s).await
}

async fn execute_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    cmds: &[Vec<u8>],
) -> io::Result<Vec<PipelineResponse>> {
    s.write_all(&cmds.concat()).await?;
    s.flush().await?;
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
            result.push(PipelineResponse::Bool(
                parse_storage_rp(s, n.ends_with(b"noreply")).await?,
            ))
        } else if cmd == build_version_cmd() {
            result.push(PipelineResponse::String(parse_version_rp(s).await?))
        } else if cmd.starts_with(b"delete ") {
            result.push(PipelineResponse::Bool(
                parse_delete_rp(s, cmd.ends_with(b"noreply\r\n")).await?,
            ))
        } else if cmd.starts_with(b"incr ") || cmd.starts_with(b"decr ") {
            result.push(PipelineResponse::Value(
                parse_incr_decr_rp(s, cmd.ends_with(b"noreply\r\n")).await?,
            ))
        } else if cmd.starts_with(b"touch ") {
            result.push(PipelineResponse::Bool(
                parse_touch_rp(s, cmd.ends_with(b"noreply\r\n")).await?,
            ))
        } else if cmd == build_quit_cmd() || cmd.starts_with(b"shutdown") {
            result.push(PipelineResponse::Unit(()))
        } else if cmd.starts_with(b"flush_all") || cmd.starts_with(b"cache_memlimit ") {
            result.push(PipelineResponse::Unit(
                parse_ok_rp(s, cmd.ends_with(b"noreply\r\n")).await?,
            ))
        } else if cmd.starts_with(b"slabs automove ")
            || cmd.starts_with(b"slabs reassign ")
            || cmd.starts_with(b"lru_crawler sleep ")
            || cmd.starts_with(b"lru_crawler crawl ")
            || cmd.starts_with(b"lru_crawler tocrawl ")
            || cmd == build_lru_crawler_cmd(LruCrawlerArg::Enable)
            || cmd == build_lru_crawler_cmd(LruCrawlerArg::Disable)
        {
            result.push(PipelineResponse::Unit(parse_ok_rp(s, false).await?))
        } else if cmd == build_mn_cmd() {
            result.push(PipelineResponse::Unit(parse_mn_rp(s).await?))
        } else if cmd.starts_with(b"stats") {
            result.push(PipelineResponse::HashMap(parse_stats_rp(s).await?))
        } else if cmd.starts_with(b"lru_crawler metadump ") {
            result.push(PipelineResponse::VecString(
                parse_lru_crawler_metadump_rp(s).await?,
            ))
        } else if cmd.starts_with(b"lru_crawler mgdump ") {
            result.push(PipelineResponse::VecString(
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
        } else {
            assert!(cmd.starts_with(b"me "));
            result.push(PipelineResponse::OptionString(parse_me_rp(s).await?))
        }
    }
    Ok(result)
}

async fn watch_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    arg: &[WatchArg],
) -> io::Result<()> {
    s.write_all(&build_watch_cmd(arg)).await?;
    s.flush().await?;
    parse_ok_rp(s, false).await
}

async fn ms_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    key: &[u8],
    flags: &[MsFlag],
    data_block: &[u8],
) -> io::Result<MsItem> {
    s.write_all(&build_mc_cmd(
        b"ms",
        key,
        &build_ms_flags(flags),
        Some(data_block),
    ))
    .await?;
    s.flush().await?;
    parse_ms_rp(s).await
}

async fn mg_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    key: &[u8],
    flags: &[MgFlag],
) -> io::Result<MgItem> {
    s.write_all(&build_mc_cmd(b"mg", key, &build_mg_flags(flags), None))
        .await?;
    s.flush().await?;
    parse_mg_rp(s).await
}

async fn md_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    key: &[u8],
    flags: &[MdFlag],
) -> io::Result<MdItem> {
    s.write_all(&build_mc_cmd(b"md", key, &build_md_flags(flags), None))
        .await?;
    s.flush().await?;
    parse_md_rp(s).await
}

async fn ma_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    key: &[u8],
    flags: &[MaFlag],
) -> io::Result<MaItem> {
    s.write_all(&build_mc_cmd(b"ma", key, &build_ma_flags(flags), None))
        .await?;
    s.flush().await?;
    parse_ma_rp(s).await
}

async fn lru_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S, arg: LruArg) -> io::Result<()> {
    s.write_all(&build_lru_cmd(arg)).await?;
    s.flush().await?;
    parse_ok_rp(s, false).await
}

pub enum Connection {
    Tcp(BufReader<TcpStream>),
    Unix(BufReader<UnixStream>),
    Udp(UdpSocket),
}
impl Connection {
    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// #     Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn default() -> io::Result<Self> {
        Ok(Connection::Tcp(BufReader::new(
            TcpStream::connect("127.0.0.1:11211").await?,
        )))
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::tcp_connect("127.0.0.1:11211").await?;
    /// #     Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn tcp_connect(addr: &str) -> io::Result<Self> {
        Ok(Connection::Tcp(BufReader::new(
            TcpStream::connect(addr).await?,
        )))
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::unix_connect("/tmp/memcached.sock").await?;
    /// #     Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn unix_connect(path: &str) -> io::Result<Self> {
        Ok(Connection::Unix(BufReader::new(
            UnixStream::connect(path).await?,
        )))
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// #     let mut conn = Connection::default().await?;
    /// let result = conn.version().await?;
    /// assert!(result.chars().any(|x| x.is_numeric()));
    /// #     Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn version(&mut self) -> io::Result<String> {
        match self {
            Connection::Tcp(s) => version_cmd(s).await,
            Connection::Unix(s) => version_cmd(s).await,
            Connection::Udp(_s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.quit().await?;
    /// #     Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn quit(mut self) -> io::Result<()> {
        match &mut self {
            Connection::Tcp(s) => quit_cmd(s).await,
            Connection::Unix(s) => quit_cmd(s).await,
            Connection::Udp(_s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::tcp_connect("127.0.0.1:11213").await?;
    /// conn.shutdown(true).await?;
    /// #     Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn shutdown(mut self, graceful: bool) -> io::Result<()> {
        match &mut self {
            Connection::Tcp(s) => shutdown_cmd(s, graceful).await,
            Connection::Unix(s) => shutdown_cmd(s, graceful).await,
            Connection::Udp(_s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.cache_memlimit(10, true).await?;
    /// #     Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn cache_memlimit(&mut self, limit: usize, noreply: bool) -> io::Result<()> {
        match self {
            Connection::Tcp(s) => cache_memlimit_cmd(s, limit, noreply).await,
            Connection::Unix(s) => cache_memlimit_cmd(s, limit, noreply).await,
            Connection::Udp(_s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.flush_all(Some(999), true).await?;
    /// #     Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn flush_all(&mut self, exptime: Option<i64>, noreply: bool) -> io::Result<()> {
        match self {
            Connection::Tcp(s) => flush_all_cmd(s, exptime, noreply).await,
            Connection::Unix(s) => flush_all_cmd(s, exptime, noreply).await,
            Connection::Udp(_s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// let result = conn.set(b"key", 0, -1, true, b"value").await?;
    /// assert!(result);
    /// #     Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn set(
        &mut self,
        key: impl AsRef<[u8]>,
        flags: u32,
        exptime: i64,
        noreply: bool,
        data_block: impl AsRef<[u8]>,
    ) -> io::Result<bool> {
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
            Connection::Udp(_s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// #     let mut conn = Connection::default().await?;
    /// let result = conn.add(b"key", 0, -1, true, b"value").await?;
    /// assert!(result);
    /// #     Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn add(
        &mut self,
        key: impl AsRef<[u8]>,
        flags: u32,
        exptime: i64,
        noreply: bool,
        data_block: impl AsRef<[u8]>,
    ) -> io::Result<bool> {
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
            Connection::Udp(_s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// #     let mut conn = Connection::default().await?;
    /// let result = conn.replace(b"key", 0, -1, true, b"value").await?;
    /// assert!(result);
    /// #     Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn replace(
        &mut self,
        key: impl AsRef<[u8]>,
        flags: u32,
        exptime: i64,
        noreply: bool,
        data_block: impl AsRef<[u8]>,
    ) -> io::Result<bool> {
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
            Connection::Udp(_s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// #     let mut conn = Connection::default().await?;
    /// let result = conn.append(b"key", 0, -1, true, b"value").await?;
    /// assert!(result);
    /// #     Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn append(
        &mut self,
        key: impl AsRef<[u8]>,
        flags: u32,
        exptime: i64,
        noreply: bool,
        data_block: impl AsRef<[u8]>,
    ) -> io::Result<bool> {
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
            Connection::Udp(_s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// # let mut conn = Connection::default().await?;
    /// let result = conn.prepend(b"key", 0, -1, true, b"value").await?;
    /// assert!(result);
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn prepend(
        &mut self,
        key: impl AsRef<[u8]>,
        flags: u32,
        exptime: i64,
        noreply: bool,
        data_block: impl AsRef<[u8]>,
    ) -> io::Result<bool> {
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
            Connection::Udp(_s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// # let mut conn = Connection::default().await?;
    /// let result = conn.cas(b"key", 0, -1, 0, true, b"value").await?;
    /// assert!(result);
    /// # Ok::<(), io::Error>(())
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
    ) -> io::Result<bool> {
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
            Connection::Udp(_s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// #     let mut conn = Connection::tcp_connect("127.0.0.1:11212").await?;
    /// conn.auth(b"a", b"a").await?;
    /// #     Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn auth(
        &mut self,
        username: impl AsRef<[u8]>,
        password: impl AsRef<[u8]>,
    ) -> io::Result<()> {
        match self {
            Connection::Tcp(s) => auth_cmd(s, username.as_ref(), password.as_ref()).await,
            Connection::Unix(s) => auth_cmd(s, username.as_ref(), password.as_ref()).await,
            Connection::Udp(_s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// # let mut conn = Connection::default().await?;
    /// let result = conn.delete(b"key", true).await?;
    /// assert!(result);
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn delete(&mut self, key: impl AsRef<[u8]>, noreply: bool) -> io::Result<bool> {
        match self {
            Connection::Tcp(s) => delete_cmd(s, key.as_ref(), noreply).await,
            Connection::Unix(s) => delete_cmd(s, key.as_ref(), noreply).await,
            Connection::Udp(_s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// # block_on(async {
    /// # let mut conn = Connection::default().await?;
    /// let result = conn.incr(b"key", 1, true).await?;
    /// assert!(result.is_none());
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn incr(
        &mut self,
        key: impl AsRef<[u8]>,
        value: u64,
        noreply: bool,
    ) -> io::Result<Option<u64>> {
        match self {
            Connection::Tcp(s) => incr_decr_cmd(s, b"incr", key.as_ref(), value, noreply).await,
            Connection::Unix(s) => incr_decr_cmd(s, b"incr", key.as_ref(), value, noreply).await,
            Connection::Udp(_s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// # let mut conn = Connection::default().await?;
    /// let result = conn.decr(b"key", 1, true).await?;
    /// assert!(result.is_none());
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn decr(
        &mut self,
        key: impl AsRef<[u8]>,
        value: u64,
        noreply: bool,
    ) -> io::Result<Option<u64>> {
        match self {
            Connection::Tcp(s) => incr_decr_cmd(s, b"decr", key.as_ref(), value, noreply).await,
            Connection::Unix(s) => incr_decr_cmd(s, b"decr", key.as_ref(), value, noreply).await,
            Connection::Udp(_s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// # let mut conn = Connection::default().await?;
    /// let result = conn.touch(b"key", -1, true).await?;
    /// assert!(result);
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn touch(
        &mut self,
        key: impl AsRef<[u8]>,
        exptime: i64,
        noreply: bool,
    ) -> io::Result<bool> {
        match self {
            Connection::Tcp(s) => touch_cmd(s, key.as_ref(), exptime, noreply).await,
            Connection::Unix(s) => touch_cmd(s, key.as_ref(), exptime, noreply).await,
            Connection::Udp(_s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// # let mut conn = Connection::default().await?;
    /// # assert!(conn.set(b"k1", 0, 0, false, b"v1").await?);
    /// let result = conn.get(b"k1").await?;
    /// assert_eq!(result.unwrap().key, "k1");
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn get(&mut self, key: impl AsRef<[u8]>) -> io::Result<Option<Item>> {
        match self {
            Connection::Tcp(s) => Ok(retrieval_cmd(s, b"get", None, &[key.as_ref()]).await?.pop()),
            Connection::Unix(s) => Ok(retrieval_cmd(s, b"get", None, &[key.as_ref()]).await?.pop()),
            Connection::Udp(_s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, Item};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// # let mut conn = Connection::default().await?;
    /// assert!(conn.set(b"k2", 0, 0, false, b"v2").await?);
    /// let result = conn.gets(b"k2").await?;
    /// assert_eq!(result.unwrap().key, "k2");
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn gets(&mut self, key: impl AsRef<[u8]>) -> io::Result<Option<Item>> {
        match self {
            Connection::Tcp(s) => Ok(retrieval_cmd(s, b"gets", None, &[key.as_ref()])
                .await?
                .pop()),
            Connection::Unix(s) => Ok(retrieval_cmd(s, b"gets", None, &[key.as_ref()])
                .await?
                .pop()),
            Connection::Udp(_s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, Item};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// # let mut conn = Connection::default().await?;
    /// assert!(conn.set(b"k3", 0, 0, false, b"v3").await?);
    /// let result = conn.gat(0, b"k3").await?;
    /// assert_eq!(result.unwrap().key, "k3");
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn gat(&mut self, exptime: i64, key: impl AsRef<[u8]>) -> io::Result<Option<Item>> {
        match self {
            Connection::Tcp(s) => Ok(retrieval_cmd(s, b"gat", Some(exptime), &[key.as_ref()])
                .await?
                .pop()),
            Connection::Unix(s) => Ok(retrieval_cmd(s, b"gat", Some(exptime), &[key.as_ref()])
                .await?
                .pop()),
            Connection::Udp(_s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, Item};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// # let mut conn = Connection::default().await?;
    /// assert!(conn.set(b"k4", 0, 0, false, b"v4").await?);
    /// let result = conn.gats(0, b"k4").await?;
    /// assert_eq!(result.unwrap().key, "k4");
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn gats(&mut self, exptime: i64, key: impl AsRef<[u8]>) -> io::Result<Option<Item>> {
        match self {
            Connection::Tcp(s) => Ok(retrieval_cmd(s, b"gats", Some(exptime), &[key.as_ref()])
                .await?
                .pop()),
            Connection::Unix(s) => Ok(retrieval_cmd(s, b"gats", Some(exptime), &[key.as_ref()])
                .await?
                .pop()),
            Connection::Udp(_s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// # let mut conn = Connection::default().await?;
    /// assert!(conn.set(b"k8", 0, 0, false, b"v8").await?);
    /// let result = conn.get_multi(&[b"k8"]).await?;
    /// assert_eq!(result[0].key, "k8");
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn get_multi(&mut self, keys: &[impl AsRef<[u8]>]) -> io::Result<Vec<Item>> {
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
            Connection::Unix(s) => {
                retrieval_cmd(
                    s,
                    b"get",
                    None,
                    &keys.iter().map(|x| x.as_ref()).collect::<Vec<&[u8]>>(),
                )
                .await
            }
            Connection::Udp(_s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// # let mut conn = Connection::default().await?;
    /// assert!(conn.set(b"k7", 0, 0, false, b"v7").await?);
    /// let result = conn.gets_multi(&[b"k7"]).await?;
    /// assert_eq!(result[0].key, "k7");
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn gets_multi(&mut self, keys: &[impl AsRef<[u8]>]) -> io::Result<Vec<Item>> {
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
            Connection::Unix(s) => {
                retrieval_cmd(
                    s,
                    b"gets",
                    None,
                    &keys.iter().map(|x| x.as_ref()).collect::<Vec<&[u8]>>(),
                )
                .await
            }
            Connection::Udp(_s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// # let mut conn = Connection::default().await?;
    /// assert!(conn.set(b"k6", 0, 0, false, b"v6").await?);
    /// let result = conn.gat_multi(0, &[b"k6"]).await?;
    /// assert_eq!(result[0].key, "k6");
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn gat_multi(
        &mut self,
        exptime: i64,
        keys: &[impl AsRef<[u8]>],
    ) -> io::Result<Vec<Item>> {
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
            Connection::Unix(s) => {
                retrieval_cmd(
                    s,
                    b"gat",
                    Some(exptime),
                    &keys.iter().map(|x| x.as_ref()).collect::<Vec<&[u8]>>(),
                )
                .await
            }
            Connection::Udp(_s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// assert!(conn.set(b"k5", 0, 0, false, b"v5").await?);
    /// let result = conn.gats_multi(0, &[b"k5"]).await?;
    /// assert_eq!(result[0].key, "k5");
    /// #     Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn gats_multi(
        &mut self,
        exptime: i64,
        keys: &[impl AsRef<[u8]>],
    ) -> io::Result<Vec<Item>> {
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
            Connection::Unix(s) => {
                retrieval_cmd(
                    s,
                    b"gats",
                    Some(exptime),
                    &keys.iter().map(|x| x.as_ref()).collect::<Vec<&[u8]>>(),
                )
                .await
            }
            Connection::Udp(_s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// let result = conn.stats(None).await?;
    /// assert!(result.len() > 0);
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn stats(&mut self, arg: Option<StatsArg>) -> io::Result<HashMap<String, String>> {
        match self {
            Connection::Tcp(s) => stats_cmd(s, arg).await,
            Connection::Unix(s) => stats_cmd(s, arg).await,
            Connection::Udp(_s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::Connection;
    /// use mcmc_rs::SlabsAutomoveArg;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.slabs_automove(SlabsAutomoveArg::Zero).await?;
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn slabs_automove(&mut self, arg: SlabsAutomoveArg) -> io::Result<()> {
        match self {
            Connection::Tcp(s) => slabs_automove_cmd(s, arg).await,
            Connection::Unix(s) => slabs_automove_cmd(s, arg).await,
            Connection::Udp(_s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::{Connection, LruCrawlerArg};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// let result = conn.lru_crawler(LruCrawlerArg::Enable).await;
    /// assert!(result.is_err());
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn lru_crawler(&mut self, arg: LruCrawlerArg) -> io::Result<()> {
        match self {
            Connection::Tcp(s) => lru_crawler_cmd(s, arg).await,
            Connection::Unix(s) => lru_crawler_cmd(s, arg).await,
            Connection::Udp(_s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.lru_crawler_sleep(1_000_000).await?;
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn lru_crawler_sleep(&mut self, microseconds: usize) -> io::Result<()> {
        match self {
            Connection::Tcp(s) => lru_crawler_sleep_cmd(s, microseconds).await,
            Connection::Unix(s) => lru_crawler_sleep_cmd(s, microseconds).await,
            Connection::Udp(_s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.lru_crawler_tocrawl(0).await?;
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn lru_crawler_tocrawl(&mut self, arg: u32) -> io::Result<()> {
        match self {
            Connection::Tcp(s) => lru_crawler_tocrawl_cmd(s, arg).await,
            Connection::Unix(s) => lru_crawler_tocrawl_cmd(s, arg).await,
            Connection::Udp(_s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::{Connection, LruCrawlerCrawlArg};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.lru_crawler_crawl(LruCrawlerCrawlArg::All).await?;
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn lru_crawler_crawl(&mut self, arg: LruCrawlerCrawlArg<'_>) -> io::Result<()> {
        match self {
            Connection::Tcp(s) => lru_crawler_crawl_cmd(s, arg).await,
            Connection::Unix(s) => lru_crawler_crawl_cmd(s, arg).await,
            Connection::Udp(_s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// let result = conn.slabs_reassign(1, 2).await;
    /// assert!(result.is_err());
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn slabs_reassign(
        &mut self,
        source_class: usize,
        dest_class: usize,
    ) -> io::Result<()> {
        match self {
            Connection::Tcp(s) => slabs_reassign_cmd(s, source_class, dest_class).await,
            Connection::Unix(s) => slabs_reassign_cmd(s, source_class, dest_class).await,
            Connection::Udp(_s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::{Connection, LruCrawlerMetadumpArg};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// let result = conn.lru_crawler_metadump(LruCrawlerMetadumpArg::Classids(&[2])).await?;
    /// assert!(result.is_empty());
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn lru_crawler_metadump(
        &mut self,
        arg: LruCrawlerMetadumpArg<'_>,
    ) -> io::Result<Vec<String>> {
        match self {
            Connection::Tcp(s) => lru_crawler_metadump_cmd(s, arg).await,
            Connection::Unix(s) => lru_crawler_metadump_cmd(s, arg).await,
            Connection::Udp(_s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::{Connection, LruCrawlerMgdumpArg};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::unix_connect("/tmp/memcached.sock").await?;
    /// let result = conn.lru_crawler_mgdump(LruCrawlerMgdumpArg::Classids(&[3])).await?;
    /// assert!(result.is_empty());
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn lru_crawler_mgdump(
        &mut self,
        arg: LruCrawlerMgdumpArg<'_>,
    ) -> io::Result<Vec<String>> {
        match self {
            Connection::Tcp(s) => lru_crawler_mgdump_cmd(s, arg).await,
            Connection::Unix(s) => lru_crawler_mgdump_cmd(s, arg).await,
            Connection::Udp(_s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// assert!(conn.mn().await.is_ok());
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn mn(&mut self) -> io::Result<()> {
        match self {
            Connection::Tcp(s) => mn_cmd(s).await,
            Connection::Unix(s) => mn_cmd(s).await,
            Connection::Udp(_s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// assert!(conn.set(b"k6", 0, 0, false, b"v6").await?);
    /// assert!(conn.me(b"k6").await?.is_some());
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn me(&mut self, key: impl AsRef<[u8]>) -> io::Result<Option<String>> {
        match self {
            Connection::Tcp(s) => me_cmd(s, key.as_ref()).await,
            Connection::Unix(s) => me_cmd(s, key.as_ref()).await,
            Connection::Udp(_s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::{Connection, WatchArg};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// assert!(conn.watch(&[WatchArg::Fetchers, WatchArg::Mutations]).await.is_ok());
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn watch(mut self, arg: &[WatchArg]) -> io::Result<WatchStream> {
        match &mut self {
            Connection::Tcp(s) => watch_cmd(s, arg).await?,
            Connection::Unix(s) => watch_cmd(s, arg).await?,
            Connection::Udp(_s) => todo!(),
        };
        Ok(WatchStream(self))
    }

    pub fn pipeline(&mut self) -> Pipeline {
        Pipeline::new(self)
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::{Connection, MgFlag, MgItem};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// let result = conn.mg(b"44OG44K544OI", &[
    ///     MgFlag::Base64Key,
    ///     MgFlag::ReturnCas,
    ///     MgFlag::ReturnFlags,
    ///     MgFlag::ReturnHit,
    ///     MgFlag::ReturnKey,
    ///     MgFlag::ReturnLastAccess,
    ///     MgFlag::Opaque("opaque".to_string()),
    ///     MgFlag::ReturnSize,
    ///     MgFlag::ReturnTtl,
    ///     MgFlag::UnBump,
    ///     MgFlag::ReturnValue,
    ///     MgFlag::NewCas(0),
    ///     MgFlag::Autovivify(-1),
    ///     MgFlag::RecacheTtl(-1),
    ///     MgFlag::UpdateTtl(-1),
    /// ]).await?;
    /// assert_eq!(result, MgItem {
    ///     success: true,
    ///     base64_key: false,
    ///     cas: Some(0),
    ///     flags: Some(0),
    ///     hit: Some(0),
    ///     key: Some("テスト".to_string()),
    ///     last_access_ttl: Some(0),
    ///     opaque: Some("opaque".to_string()),
    ///     size: Some(0),
    ///     ttl: Some(-1),
    ///     data_block: Some(vec![]),
    ///     already_win: false,
    ///     won_recache: true,
    ///     stale: false,
    /// });
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn mg(&mut self, key: impl AsRef<[u8]>, flags: &[MgFlag]) -> io::Result<MgItem> {
        match self {
            Connection::Tcp(s) => mg_cmd(s, key.as_ref(), flags).await,
            Connection::Unix(s) => mg_cmd(s, key.as_ref(), flags).await,
            Connection::Udp(_s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::{Connection, MsItem, MsFlag, MsMode};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// let result = conn.ms(
    ///     b"44OG44K544OI",
    ///     &[
    ///     MsFlag::Base64Key,
    ///     MsFlag::ReturnCas,
    ///     MsFlag::CompareCas(0),
    ///     MsFlag::NewCas(0),
    ///     MsFlag::SetFlags(0),
    ///     MsFlag::Invalidate,
    ///     MsFlag::ReturnKey,
    ///     MsFlag::Opaque("opaque".to_string()),
    ///     MsFlag::ReturnSize,
    ///     MsFlag::Ttl(-1),
    ///     MsFlag::Mode(MsMode::Set),
    ///     MsFlag::Autovivify(0)
    ///     ],
    ///     b"hi").await?;
    /// assert_eq!(result, MsItem {
    ///     success: false,
    ///     cas: Some(0),
    ///     key: Some("44OG44K544OI".to_string()),
    ///     opaque: Some("opaque".to_string()),
    ///     size: Some(2),
    ///     base64_key: true
    /// });
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn ms(
        &mut self,
        key: impl AsRef<[u8]>,
        flags: &[MsFlag],
        data_block: impl AsRef<[u8]>,
    ) -> io::Result<MsItem> {
        match self {
            Connection::Tcp(s) => ms_cmd(s, key.as_ref(), flags, data_block.as_ref()).await,
            Connection::Unix(s) => ms_cmd(s, key.as_ref(), flags, data_block.as_ref()).await,
            Connection::Udp(_s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::{Connection, MdItem, MdFlag};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// let result = conn.md(
    ///     b"44OG44K544OI",
    ///     &[
    ///     MdFlag::Base64Key,
    ///     MdFlag::CompareCas(0),
    ///     MdFlag::NewCas(0),
    ///     MdFlag::Invalidate,
    ///     MdFlag::ReturnKey,
    ///     MdFlag::Opaque("opaque".to_string()),
    ///     MdFlag::UpdateTtl(-1),
    ///     MdFlag::LeaveKey,
    ///     ]).await?;
    /// assert_eq!(result, MdItem {
    ///     success: false,
    ///     key: Some("44OG44K544OI".to_string()),
    ///     opaque: Some("opaque".to_string()),
    ///     base64_key: true
    /// });
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn md(&mut self, key: impl AsRef<[u8]>, flags: &[MdFlag]) -> io::Result<MdItem> {
        match self {
            Connection::Tcp(s) => md_cmd(s, key.as_ref(), flags).await,
            Connection::Unix(s) => md_cmd(s, key.as_ref(), flags).await,
            Connection::Udp(_s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::{Connection, MaItem, MaFlag, MaMode};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// let result = conn.ma(
    ///     b"aGk=",
    ///     &[
    ///     MaFlag::Base64Key,
    ///     MaFlag::CompareCas(0),
    ///     MaFlag::NewCas(0),
    ///     MaFlag::AutoCreate(0),
    ///     MaFlag::InitValue(0),
    ///     MaFlag::DeltaApply(0),
    ///     MaFlag::UpdateTtl(0),
    ///     MaFlag::Mode(MaMode::Incr),
    ///     MaFlag::Opaque("opaque".to_string()),
    ///     MaFlag::ReturnTtl,
    ///     MaFlag::ReturnCas,
    ///     MaFlag::ReturnValue,
    ///     MaFlag::ReturnKey,
    ///     ]).await?;
    /// assert_eq!(result, MaItem {
    ///     success: true,
    ///     opaque: Some("opaque".to_string()),
    ///     ttl: Some(-1),
    ///     cas: Some(0),
    ///     number: Some(0),
    ///     key: Some("aGk=".to_string()),
    ///     base64_key: true
    /// });
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn ma(&mut self, key: impl AsRef<[u8]>, flags: &[MaFlag]) -> io::Result<MaItem> {
        match self {
            Connection::Tcp(s) => ma_cmd(s, key.as_ref(), flags).await,
            Connection::Unix(s) => ma_cmd(s, key.as_ref(), flags).await,
            Connection::Udp(_s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::{Connection, LruArg, LruMode};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// assert!(conn.lru(LruArg::Mode(LruMode::Flat)).await.is_ok());
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn lru(&mut self, arg: LruArg) -> io::Result<()> {
        match self {
            Connection::Tcp(s) => lru_cmd(s, arg).await,
            Connection::Unix(s) => lru_cmd(s, arg).await,
            Connection::Udp(_s) => todo!(),
        }
    }
}

pub struct WatchStream(Connection);
impl WatchStream {
    /// # Example
    ///
    /// ```
    /// use mcmc_rs::{Connection, WatchArg};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// let mut w = conn.watch(&[WatchArg::Fetchers]).await?;
    /// let mut conn = Connection::default().await?;
    /// conn.get(b"key").await?;
    /// let result = w.message().await?;
    /// assert!(result.is_some());
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn message(&mut self) -> io::Result<Option<String>> {
        let mut line = String::new();
        let n = match &mut self.0 {
            Connection::Tcp(s) => s.read_line(&mut line).await?,
            Connection::Unix(s) => s.read_line(&mut line).await?,
            Connection::Udp(_s) => todo!(),
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
    /// use mcmc_rs::{Connection, ClientCrc32};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut client = ClientCrc32::new(
    ///     vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached.sock").await?,
    ///     ]
    /// );
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    ///```
    pub fn new(conns: Vec<Connection>) -> Self {
        Self(conns)
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::{Connection, ClientCrc32};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut client = ClientCrc32::new(
    ///     vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached.sock").await?,
    ///     ]
    /// );
    ///
    /// assert!(client.set(b"k7", 0, 0, false, b"v7").await?);
    /// assert_eq!(client.get(b"k7").await?.unwrap().key, "k7");
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn get(&mut self, key: impl AsRef<[u8]>) -> io::Result<Option<Item>> {
        let size = self.0.len();
        self.0[crc32fast::hash(key.as_ref()) as usize % size]
            .get(key.as_ref())
            .await
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::{Connection, ClientCrc32};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut client = ClientCrc32::new(
    ///     vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached.sock").await?,
    ///     ]
    /// );
    ///
    /// assert!(client.set(b"k8", 0, 0, false, b"v8").await?);
    /// assert_eq!(client.gets(b"k8").await?.unwrap().key, "k8");
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn gets(&mut self, key: impl AsRef<[u8]>) -> io::Result<Option<Item>> {
        let size = self.0.len();
        self.0[crc32fast::hash(key.as_ref()) as usize % size]
            .gets(key.as_ref())
            .await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, Item};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// # let mut conn = Connection::default().await?;
    /// assert!(conn.set(b"k9", 0, 0, false, b"v9").await?);
    /// let result = conn.gat(0, b"k9").await?;
    /// assert_eq!(result.unwrap().key, "k9");
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn gat(&mut self, exptime: i64, key: impl AsRef<[u8]>) -> io::Result<Option<Item>> {
        let size = self.0.len();
        self.0[crc32fast::hash(key.as_ref()) as usize % size]
            .gat(exptime, key.as_ref())
            .await
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, Item};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// # let mut conn = Connection::default().await?;
    /// assert!(conn.set(b"k10", 0, 0, false, b"v10").await?);
    /// let result = conn.gats(0, b"k10").await?;
    /// assert_eq!(result.unwrap().key, "k10");
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn gats(&mut self, exptime: i64, key: impl AsRef<[u8]>) -> io::Result<Option<Item>> {
        let size = self.0.len();
        self.0[crc32fast::hash(key.as_ref()) as usize % size]
            .gats(exptime, key.as_ref())
            .await
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::{Connection, ClientCrc32};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut client = ClientCrc32::new(
    ///     vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached.sock").await?,
    ///     ]
    /// );
    ///
    /// assert!(client.set(b"key", 0, -1, true, b"value").await?);
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn set(
        &mut self,
        key: impl AsRef<[u8]>,
        flags: u32,
        exptime: i64,
        noreply: bool,
        data_block: impl AsRef<[u8]>,
    ) -> io::Result<bool> {
        let size = self.0.len();
        self.0[crc32fast::hash(key.as_ref()) as usize % size]
            .set(key.as_ref(), flags, exptime, noreply, data_block.as_ref())
            .await
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::{Connection, ClientCrc32};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut client = ClientCrc32::new(
    ///     vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached.sock").await?,
    ///     ]
    /// );
    ///
    /// assert!(client.add(b"key", 0, -1, true, b"value").await?);
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn add(
        &mut self,
        key: impl AsRef<[u8]>,
        flags: u32,
        exptime: i64,
        noreply: bool,
        data_block: impl AsRef<[u8]>,
    ) -> io::Result<bool> {
        let size = self.0.len();
        self.0[crc32fast::hash(key.as_ref()) as usize % size]
            .add(key.as_ref(), flags, exptime, noreply, data_block.as_ref())
            .await
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::{Connection, ClientCrc32};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut client = ClientCrc32::new(
    ///     vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached.sock").await?,
    ///     ]
    /// );
    ///
    /// assert!(client.replace(b"key", 0, -1, true, b"value").await?);
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn replace(
        &mut self,
        key: impl AsRef<[u8]>,
        flags: u32,
        exptime: i64,
        noreply: bool,
        data_block: impl AsRef<[u8]>,
    ) -> io::Result<bool> {
        let size = self.0.len();
        self.0[crc32fast::hash(key.as_ref()) as usize % size]
            .replace(key.as_ref(), flags, exptime, noreply, data_block.as_ref())
            .await
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::{Connection, ClientCrc32};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut client = ClientCrc32::new(
    ///     vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached.sock").await?,
    ///     ]
    /// );
    ///
    /// assert!(client.append(b"key", 0, -1, true, b"value").await?);
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn append(
        &mut self,
        key: impl AsRef<[u8]>,
        flags: u32,
        exptime: i64,
        noreply: bool,
        data_block: impl AsRef<[u8]>,
    ) -> io::Result<bool> {
        let size = self.0.len();
        self.0[crc32fast::hash(key.as_ref()) as usize % size]
            .append(key.as_ref(), flags, exptime, noreply, data_block.as_ref())
            .await
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::{Connection, ClientCrc32};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut client = ClientCrc32::new(
    ///     vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached.sock").await?,
    ///     ]
    /// );
    ///
    /// assert!(client.prepend(b"key", 0, -1, true, b"value").await?);
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn prepend(
        &mut self,
        key: impl AsRef<[u8]>,
        flags: u32,
        exptime: i64,
        noreply: bool,
        data_block: impl AsRef<[u8]>,
    ) -> io::Result<bool> {
        let size = self.0.len();
        self.0[crc32fast::hash(key.as_ref()) as usize % size]
            .prepend(key.as_ref(), flags, exptime, noreply, data_block.as_ref())
            .await
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::{Connection, ClientCrc32};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut client = ClientCrc32::new(
    ///     vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached.sock").await?,
    ///     ]
    /// );
    ///
    /// assert!(client.cas(b"key", 0, -1, 0, true, b"value").await?);
    /// # Ok::<(), io::Error>(())
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
    ) -> io::Result<bool> {
        let size = self.0.len();
        self.0[crc32fast::hash(key.as_ref()) as usize % size]
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
    /// use mcmc_rs::{Connection, ClientCrc32};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut client = ClientCrc32::new(
    ///     vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached.sock").await?,
    ///     ]
    /// );
    ///
    /// assert!(client.delete(b"key", true).await?);
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn delete(&mut self, key: impl AsRef<[u8]>, noreply: bool) -> io::Result<bool> {
        let size = self.0.len();
        self.0[crc32fast::hash(key.as_ref()) as usize % size]
            .delete(key.as_ref(), noreply)
            .await
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::{Connection, ClientCrc32};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut client = ClientCrc32::new(
    ///     vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached.sock").await?,
    ///     ]
    /// );
    ///
    /// assert!(client.incr(b"key", 1, true).await?.is_none());
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn incr(
        &mut self,
        key: impl AsRef<[u8]>,
        value: u64,
        noreply: bool,
    ) -> io::Result<Option<u64>> {
        let size = self.0.len();
        self.0[crc32fast::hash(key.as_ref()) as usize % size]
            .incr(key.as_ref(), value, noreply)
            .await
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::{Connection, ClientCrc32};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut client = ClientCrc32::new(
    ///     vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached.sock").await?,
    ///     ]
    /// );
    ///
    /// assert!(client.decr(b"key", 1, true).await?.is_none());
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn decr(
        &mut self,
        key: impl AsRef<[u8]>,
        value: u64,
        noreply: bool,
    ) -> io::Result<Option<u64>> {
        let size = self.0.len();
        self.0[crc32fast::hash(key.as_ref()) as usize % size]
            .decr(key.as_ref(), value, noreply)
            .await
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::{Connection, ClientCrc32};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut client = ClientCrc32::new(
    ///     vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached.sock").await?,
    ///     ]
    /// );
    ///
    /// assert!(client.touch(b"key", -1, true).await?);
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn touch(
        &mut self,
        key: impl AsRef<[u8]>,
        exptime: i64,
        noreply: bool,
    ) -> io::Result<bool> {
        let size = self.0.len();
        self.0[crc32fast::hash(key.as_ref()) as usize % size]
            .touch(key.as_ref(), exptime, noreply)
            .await
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::{Connection, ClientCrc32};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut client = ClientCrc32::new(
    ///     vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached.sock").await?,
    ///     ]
    /// );
    /// assert!(client.set(b"k11", 0, 0, false, b"v11").await?);
    /// assert!(client.me(b"k11").await?.is_some());
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn me(&mut self, key: impl AsRef<[u8]>) -> io::Result<Option<String>> {
        let size = self.0.len();
        self.0[crc32fast::hash(key.as_ref()) as usize % size]
            .me(key.as_ref())
            .await
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::{ClientCrc32, Connection, MgFlag, MgItem};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut client = ClientCrc32::new(
    ///     vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached.sock").await?,
    ///     ]
    /// );
    /// let result = client.mg(b"44OG44K544OI", &[
    ///     MgFlag::Base64Key,
    ///     MgFlag::ReturnCas,
    ///     MgFlag::ReturnFlags,
    ///     MgFlag::ReturnHit,
    ///     MgFlag::ReturnKey,
    ///     MgFlag::ReturnLastAccess,
    ///     MgFlag::Opaque("opaque".to_string()),
    ///     MgFlag::ReturnSize,
    ///     MgFlag::ReturnTtl,
    ///     MgFlag::UnBump,
    ///     MgFlag::ReturnValue,
    ///     MgFlag::NewCas(0),
    ///     MgFlag::Autovivify(-1),
    ///     MgFlag::RecacheTtl(-1),
    ///     MgFlag::UpdateTtl(-1),
    /// ]).await?;
    /// assert_eq!(result, MgItem {
    ///     success: true,
    ///     base64_key: false,
    ///     cas: Some(0),
    ///     flags: Some(0),
    ///     hit: Some(0),
    ///     key: Some("テスト".to_string()),
    ///     last_access_ttl: Some(0),
    ///     opaque: Some("opaque".to_string()),
    ///     size: Some(0),
    ///     ttl: Some(-1),
    ///     data_block: Some(vec![]),
    ///     already_win: false,
    ///     won_recache: true,
    ///     stale: false,
    /// });
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn mg(&mut self, key: impl AsRef<[u8]>, flags: &[MgFlag]) -> io::Result<MgItem> {
        let size = self.0.len();
        self.0[crc32fast::hash(key.as_ref()) as usize % size]
            .mg(key.as_ref(), flags)
            .await
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::{ClientCrc32, Connection, MsItem, MsFlag, MsMode};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut client = ClientCrc32::new(
    ///     vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached.sock").await?,
    ///     ]
    /// );
    /// let result = client.ms(
    ///     b"44OG44K544OI",
    ///     &[
    ///     MsFlag::Base64Key,
    ///     MsFlag::ReturnCas,
    ///     MsFlag::CompareCas(0),
    ///     MsFlag::NewCas(0),
    ///     MsFlag::SetFlags(0),
    ///     MsFlag::Invalidate,
    ///     MsFlag::ReturnKey,
    ///     MsFlag::Opaque("opaque".to_string()),
    ///     MsFlag::ReturnSize,
    ///     MsFlag::Ttl(-1),
    ///     MsFlag::Mode(MsMode::Set),
    ///     MsFlag::Autovivify(0)
    ///     ],
    ///     b"hi").await?;
    /// assert_eq!(result, MsItem {
    ///     success: false,
    ///     cas: Some(0),
    ///     key: Some("44OG44K544OI".to_string()),
    ///     opaque: Some("opaque".to_string()),
    ///     size: Some(2),
    ///     base64_key: true
    /// });
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn ms(
        &mut self,
        key: impl AsRef<[u8]>,
        flags: &[MsFlag],
        data_block: impl AsRef<[u8]>,
    ) -> io::Result<MsItem> {
        let size = self.0.len();
        self.0[crc32fast::hash(key.as_ref()) as usize % size]
            .ms(key.as_ref(), flags, data_block.as_ref())
            .await
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::{ClientCrc32, Connection, MdItem, MdFlag};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut client = ClientCrc32::new(
    ///     vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached.sock").await?,
    ///     ]
    /// );
    /// let result = client.md(
    ///     b"44OG44K544OI",
    ///     &[
    ///     MdFlag::Base64Key,
    ///     MdFlag::CompareCas(0),
    ///     MdFlag::NewCas(0),
    ///     MdFlag::Invalidate,
    ///     MdFlag::ReturnKey,
    ///     MdFlag::Opaque("opaque".to_string()),
    ///     MdFlag::UpdateTtl(-1),
    ///     MdFlag::LeaveKey,
    ///     ]).await?;
    /// assert_eq!(result, MdItem {
    ///     success: false,
    ///     key: Some("44OG44K544OI".to_string()),
    ///     opaque: Some("opaque".to_string()),
    ///     base64_key: true
    /// });
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn md(&mut self, key: impl AsRef<[u8]>, flags: &[MdFlag]) -> io::Result<MdItem> {
        let size = self.0.len();
        self.0[crc32fast::hash(key.as_ref()) as usize % size]
            .md(key.as_ref(), flags)
            .await
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::{ClientCrc32, Connection, MaItem, MaFlag, MaMode};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut client = ClientCrc32::new(
    ///     vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached.sock").await?,
    ///     ]
    /// );
    /// let result = client.ma(
    ///     b"aGk=",
    ///     &[
    ///     MaFlag::Base64Key,
    ///     MaFlag::CompareCas(0),
    ///     MaFlag::NewCas(0),
    ///     MaFlag::AutoCreate(0),
    ///     MaFlag::InitValue(0),
    ///     MaFlag::DeltaApply(0),
    ///     MaFlag::UpdateTtl(0),
    ///     MaFlag::Mode(MaMode::Incr),
    ///     MaFlag::Opaque("opaque".to_string()),
    ///     MaFlag::ReturnTtl,
    ///     MaFlag::ReturnCas,
    ///     MaFlag::ReturnValue,
    ///     MaFlag::ReturnKey,
    ///     ]).await?;
    /// assert_eq!(result, MaItem {
    ///     success: true,
    ///     opaque: Some("opaque".to_string()),
    ///     ttl: Some(-1),
    ///     cas: Some(0),
    ///     number: Some(0),
    ///     key: Some("aGk=".to_string()),
    ///     base64_key: true
    /// });
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn ma(&mut self, key: impl AsRef<[u8]>, flags: &[MaFlag]) -> io::Result<MaItem> {
        let size = self.0.len();
        self.0[crc32fast::hash(key.as_ref()) as usize % size]
            .ma(key.as_ref(), flags)
            .await
    }
}

pub struct Pipeline<'a>(&'a mut Connection, Vec<Vec<u8>>);
impl<'a> Pipeline<'a> {
    /// # Example
    ///
    /// ```
    /// use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.pipeline();
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    fn new(conn: &'a mut Connection) -> Self {
        Self(conn, Vec::new())
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// assert_eq!(conn.pipeline().execute().await?, []);
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn execute(self) -> io::Result<Vec<PipelineResponse>> {
        if self.1.is_empty() {
            return Ok(Vec::new());
        };
        match self.0 {
            Connection::Tcp(s) => execute_cmd(s, &self.1).await,
            Connection::Unix(s) => execute_cmd(s, &self.1).await,
            Connection::Udp(_s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.pipeline().version();
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn version(mut self) -> Self {
        self.1.push(build_version_cmd().to_vec());
        self
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.pipeline().quit();
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn quit(mut self) -> Self {
        self.1.push(build_quit_cmd().to_vec());
        self
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.pipeline().shutdown(false);
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn shutdown(mut self, graceful: bool) -> Self {
        self.1.push(build_shutdown_cmd(graceful).to_vec());
        self
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.pipeline().cache_memlimit(1, false);
    /// # Ok::<(), io::Error>(())
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
    /// use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.pipeline().flush_all(None, false);
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn flush_all(mut self, exptime: Option<i64>, noreply: bool) -> Self {
        self.1.push(build_flush_all_cmd(exptime, noreply).to_vec());
        self
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.pipeline().set(b"key", 0, 0, false, b"value");
    /// # Ok::<(), io::Error>(())
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
    /// use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.pipeline().add(b"key", 0, 0, false, b"value");
    /// # Ok::<(), io::Error>(())
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
    /// use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.pipeline().replace(b"key", 0, 0, false, b"value");
    /// # Ok::<(), io::Error>(())
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
    /// use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.pipeline().append(b"key", 0, 0, false, b"value");
    /// # Ok::<(), io::Error>(())
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
    /// use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.pipeline().prepend(b"key", 0, 0, false, b"value");
    /// # Ok::<(), io::Error>(())
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
    /// use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.pipeline().cas(b"key", 0, 0, 0, false, b"value");
    /// # Ok::<(), io::Error>(())
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
    /// use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.pipeline().auth(b"username", b"password");
    /// # Ok::<(), io::Error>(())
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
    /// use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.pipeline().delete(b"key", false);
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn delete(mut self, key: impl AsRef<[u8]>, noreply: bool) -> Self {
        self.1.push(build_delete_cmd(key.as_ref(), noreply));
        self
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.pipeline().incr(b"key", 1, false);
    /// # Ok::<(), io::Error>(())
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
    /// use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.pipeline().decr(b"key", 1, false);
    /// # Ok::<(), io::Error>(())
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
    /// use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.pipeline().touch(b"key", 1, false);
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn touch(mut self, key: impl AsRef<[u8]>, exptime: i64, noreply: bool) -> Self {
        self.1.push(build_touch_cmd(key.as_ref(), exptime, noreply));
        self
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.pipeline().get(b"key");
    /// # Ok::<(), io::Error>(())
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
    /// use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.pipeline().gets(b"key");
    /// # Ok::<(), io::Error>(())
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
    /// use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.pipeline().gat(0, b"key");
    /// # Ok::<(), io::Error>(())
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
    /// use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.pipeline().gats(0, b"key");
    /// # Ok::<(), io::Error>(())
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
    /// use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.pipeline().get_multi(&[b"key".as_slice(), b"key2".as_slice()]);
    /// # Ok::<(), io::Error>(())
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
    /// use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.pipeline().gets_multi(&[b"key".as_slice(), b"key2".as_slice()]);
    /// # Ok::<(), io::Error>(())
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
    /// use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.pipeline().gat_multi(0, &[b"key".as_slice(), b"key2".as_slice()]);
    /// # Ok::<(), io::Error>(())
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
    /// use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.pipeline().gats_multi(0, &[b"key".as_slice(), b"key2".as_slice()]);
    /// # Ok::<(), io::Error>(())
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
    /// use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.pipeline().stats(None);
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn stats(mut self, arg: Option<StatsArg>) -> Self {
        self.1.push(build_stats_cmd(arg));
        self
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::{Connection, SlabsAutomoveArg};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.pipeline().slabs_automove(SlabsAutomoveArg::Zero);
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn slabs_automove(mut self, arg: SlabsAutomoveArg) -> Self {
        self.1.push(build_slabs_automove_cmd(arg));
        self
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::{Connection, LruCrawlerArg};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.pipeline().lru_crawler(LruCrawlerArg::Enable);
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn lru_crawler(mut self, arg: LruCrawlerArg) -> Self {
        self.1.push(build_lru_crawler_cmd(arg).to_vec());
        self
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.pipeline().lru_crawler_sleep(0);
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn lru_crawler_sleep(mut self, microseconds: usize) -> Self {
        self.1.push(build_lru_clawler_sleep_cmd(microseconds));
        self
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.pipeline().lru_crawler_tocrawl(0);
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn lru_crawler_tocrawl(mut self, arg: u32) -> Self {
        self.1.push(build_lru_crawler_tocrawl_cmd(arg));
        self
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::{Connection, LruCrawlerCrawlArg};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.pipeline().lru_crawler_crawl(LruCrawlerCrawlArg::All);
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn lru_crawler_crawl(mut self, arg: LruCrawlerCrawlArg<'_>) -> Self {
        self.1.push(build_lru_clawler_crawl_cmd(arg));
        self
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.pipeline().slabs_reassign(1, 2);
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn slabs_reassign(mut self, source_class: usize, dest_class: usize) -> Self {
        self.1
            .push(build_slabs_reassign_cmd(source_class, dest_class));
        self
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::{Connection, LruCrawlerMetadumpArg};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.pipeline().lru_crawler_metadump(LruCrawlerMetadumpArg::All);
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn lru_crawler_metadump(mut self, arg: LruCrawlerMetadumpArg<'_>) -> Self {
        self.1.push(build_lru_clawler_metadump_cmd(arg));
        self
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::{Connection, LruCrawlerMgdumpArg};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.pipeline().lru_crawler_mgdump(LruCrawlerMgdumpArg::All);
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn lru_crawler_mgdump(mut self, arg: LruCrawlerMgdumpArg<'_>) -> Self {
        self.1.push(build_lru_clawler_mgdump_cmd(arg));
        self
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.pipeline().mn();
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn mn(mut self) -> Self {
        self.1.push(build_mn_cmd().to_vec());
        self
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.pipeline().me(b"key");
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn me(mut self, key: impl AsRef<[u8]>) -> Self {
        self.1.push(build_me_cmd(key.as_ref()));
        self
    }

    /// # Example
    ///
    /// ```
    /// use mcmc_rs::{Connection, MgFlag};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.pipeline().mg(b"key", &[MgFlag::Base64Key]);
    /// # Ok::<(), io::Error>(())
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
    /// use mcmc_rs::{Connection, MsFlag};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.pipeline().ms(b"key", &[MsFlag::Base64Key], b"value");
    /// # Ok::<(), io::Error>(())
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
    /// use mcmc_rs::{Connection, MdFlag};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.pipeline().md(b"key", &[MdFlag::ReturnKey]);
    /// # Ok::<(), io::Error>(())
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
    /// use mcmc_rs::{Connection, MaFlag};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.pipeline().ma(b"key", &[MaFlag::Base64Key]);
    /// # Ok::<(), io::Error>(())
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
    /// use mcmc_rs::{Connection, LruArg, LruMode};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.pipeline().lru(LruArg::Mode(LruMode::Flat));
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub fn lru(mut self, arg: LruArg) -> Self {
        self.1.push(build_lru_cmd(arg));
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use smol::{block_on, io::Cursor};

    #[test]
    fn test_version() {
        block_on(async {
            let mut c = Cursor::new(b"version\r\nVERSION 1.2.3\r\n".to_vec());
            assert_eq!("1.2.3", version_cmd(&mut c).await.unwrap());

            let mut c = Cursor::new(b"version\r\nERROR\r\n".to_vec());
            assert!(version_cmd(&mut c).await.is_err())
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
            assert!(cache_memlimit_cmd(&mut c, 1, false).await.is_err());
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
            assert!(flush_all_cmd(&mut c, None, false).await.is_err());
        })
    }

    #[test]
    fn test_storage() {
        block_on(async {
            let mut c = Cursor::new(b"cas key 0 0 0 0\r\nvalue\r\nSTORED\r\n".to_vec());
            assert!(
                storage_cmd(&mut c, b"cas", b"key", 0, 0, Some(0), false, b"value")
                    .await
                    .unwrap()
            );

            let mut c = Cursor::new(b"append key 0 0 0 noreply\r\nvalue\r\n".to_vec());
            assert!(
                storage_cmd(&mut c, b"append", b"key", 0, 0, None, true, b"value")
                    .await
                    .unwrap()
            );

            let mut c = Cursor::new(b"prepend key 0 0 0\r\nvalue\r\nNOT_STORED\r\n".to_vec());
            assert!(
                !storage_cmd(&mut c, b"prepend", b"key", 0, 0, None, false, b"value")
                    .await
                    .unwrap()
            );

            let mut c = Cursor::new(b"add key 0 0 0\r\nvalue\r\nERROR\r\n".to_vec());
            assert!(
                storage_cmd(&mut c, b"add", b"key", 0, 0, None, false, b"value")
                    .await
                    .is_err()
            )
        })
    }

    #[test]
    fn test_delete() {
        block_on(async {
            let mut c = Cursor::new(b"delete key\r\nDELETED\r\n".to_vec());
            assert!(delete_cmd(&mut c, b"key", false).await.unwrap());

            let mut c = Cursor::new(b"delete key\r\nNOT_FOUND\r\n".to_vec());
            assert!(!delete_cmd(&mut c, b"key", false).await.unwrap());

            let mut c = Cursor::new(b"delete key noreply\r\n".to_vec());
            assert!(delete_cmd(&mut c, b"key", true).await.unwrap());

            let mut c = Cursor::new(b"delete key\r\nERROR\r\n".to_vec());
            assert!(delete_cmd(&mut c, b"key", false).await.is_err());
        })
    }

    #[test]
    fn test_auth() {
        block_on(async {
            let mut c = Cursor::new(b"set _ _ _ 3\r\na b\r\nSTORED\r\n".to_vec());
            assert!(auth_cmd(&mut c, b"a", b"b").await.is_ok());

            let mut c = Cursor::new(b"set _ _ _ 3\r\na b\r\nERROR\r\n".to_vec());
            assert!(auth_cmd(&mut c, b"a", b"b").await.is_err());
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
            assert!(
                incr_decr_cmd(&mut c, b"incr", b"key", 1, false)
                    .await
                    .is_err()
            );
        })
    }

    #[test]
    fn test_touch() {
        block_on(async {
            let mut c = Cursor::new(b"touch key 0\r\nTOUCHED\r\n".to_vec());
            assert!(touch_cmd(&mut c, b"key", 0, false).await.unwrap());

            let mut c = Cursor::new(b"touch key 0\r\nNOT_FOUND\r\n".to_vec());
            assert!(!touch_cmd(&mut c, b"key", 0, false).await.unwrap());

            let mut c = Cursor::new(b"touch key 0 noreply\r\n".to_vec());
            assert!(touch_cmd(&mut c, b"key", 0, true).await.unwrap());

            let mut c = Cursor::new(b"touch key 0\r\nERROR\r\n".to_vec());
            assert!(touch_cmd(&mut c, b"key", 0, false).await.is_err())
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
            assert!(
                retrieval_cmd(&mut c, b"get", None, &[b"key"])
                    .await
                    .is_err()
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
            assert!(stats_cmd(&mut c, Some(StatsArg::Settings)).await.is_err());

            let mut c = Cursor::new(b"stats items\r\nERROR\r\n".to_vec());
            assert!(stats_cmd(&mut c, Some(StatsArg::Items)).await.is_err());

            let mut c = Cursor::new(b"stats sizes\r\nERROR\r\n".to_vec());
            assert!(stats_cmd(&mut c, Some(StatsArg::Sizes)).await.is_err());

            let mut c = Cursor::new(b"stats slabs\r\nERROR\r\n".to_vec());
            assert!(stats_cmd(&mut c, Some(StatsArg::Slabs)).await.is_err());

            let mut c = Cursor::new(b"stats conns\r\nERROR\r\n".to_vec());
            assert!(stats_cmd(&mut c, Some(StatsArg::Conns)).await.is_err())
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
            assert!(
                slabs_automove_cmd(&mut c, SlabsAutomoveArg::One)
                    .await
                    .is_err()
            );

            let mut c = Cursor::new(b"slabs automove 2\r\nERROR\r\n".to_vec());
            assert!(
                slabs_automove_cmd(&mut c, SlabsAutomoveArg::Two)
                    .await
                    .is_err()
            )
        })
    }

    #[test]
    fn test_lru_crawler() {
        block_on(async {
            let mut c = Cursor::new(b"lru_crawler enable\r\nOK\r\n".to_vec());
            assert!(lru_crawler_cmd(&mut c, LruCrawlerArg::Enable).await.is_ok());

            let mut c = Cursor::new(b"lru_crawler disable\r\nERROR\r\n".to_vec());
            assert!(
                lru_crawler_cmd(&mut c, LruCrawlerArg::Disable)
                    .await
                    .is_err()
            )
        })
    }

    #[test]
    fn test_lru_crawler_sleep() {
        block_on(async {
            let mut c = Cursor::new(b"lru_crawler sleep 1000000\r\nOK\r\n".to_vec());
            assert!(lru_crawler_sleep_cmd(&mut c, 1_000_000).await.is_ok());

            let mut c = Cursor::new(b"lru_crawler sleep 0\r\nERROR\r\n".to_vec());
            assert!(lru_crawler_sleep_cmd(&mut c, 0).await.is_err())
        })
    }

    #[test]
    fn test_lru_crawler_tocrawl() {
        block_on(async {
            let mut c = Cursor::new(b"lru_crawler tocrawl 0\r\nOK\r\n".to_vec());
            assert!(lru_crawler_tocrawl_cmd(&mut c, 0).await.is_ok());

            let mut c = Cursor::new(b"lru_crawler tocrawl 0\r\nERROR\r\n".to_vec());
            assert!(lru_crawler_tocrawl_cmd(&mut c, 0).await.is_err())
        })
    }

    #[test]
    fn test_lru_crawler_crawl() {
        block_on(async {
            let mut c = Cursor::new(b"lru_crawler crawl 1,2,3\r\nOK\r\n".to_vec());
            assert!(
                lru_crawler_crawl_cmd(&mut c, LruCrawlerCrawlArg::Classids(&[1, 2, 3]))
                    .await
                    .is_ok()
            );

            let mut c = Cursor::new(b"lru_crawler crawl all\r\nERROR\r\n".to_vec());
            assert!(
                lru_crawler_crawl_cmd(&mut c, LruCrawlerCrawlArg::All)
                    .await
                    .is_err()
            )
        })
    }

    #[test]
    fn test_slabs_reassign() {
        block_on(async {
            let mut c = Cursor::new(b"slabs reassign 1 10\r\nOK\r\n".to_vec());
            assert!(slabs_reassign_cmd(&mut c, 1, 10).await.is_ok());

            let mut c = Cursor::new(b"slabs reassign 1 10\r\nERROR\r\n".to_vec());
            assert!(slabs_reassign_cmd(&mut c, 1, 10).await.is_err())
        })
    }

    #[test]
    fn test_lru_crawler_metadump() {
        block_on(async {
            let mut c = Cursor::new(b"lru_crawler metadump all\r\nkey=key exp=-1 la=1745299782 cas=2 fetch=no cls=1 size=63 flags=0\r\nkey=key2 exp=-1 la=1745299782 cas=2 fetch=no cls=1 size=63 flags=0\r\nEND\r\n".to_vec());
            assert_eq!(
                lru_crawler_metadump_cmd(&mut c, LruCrawlerMetadumpArg::All)
                    .await
                    .unwrap(),
                [
                    "key=key exp=-1 la=1745299782 cas=2 fetch=no cls=1 size=63 flags=0",
                    "key=key2 exp=-1 la=1745299782 cas=2 fetch=no cls=1 size=63 flags=0"
                ]
            );

            let mut c = Cursor::new(b"lru_crawler metadump 1,2,3\r\nERROR\r\n".to_vec());
            assert!(
                lru_crawler_metadump_cmd(&mut c, LruCrawlerMetadumpArg::Classids(&[1, 2, 3]))
                    .await
                    .is_err()
            );

            let mut c = Cursor::new(b"lru_crawler metadump hash\r\nERROR\r\n".to_vec());
            assert!(
                lru_crawler_metadump_cmd(&mut c, LruCrawlerMetadumpArg::Hash)
                    .await
                    .is_err()
            )
        })
    }

    #[test]
    fn test_lru_crawler_mgdump() {
        block_on(async {
            let mut c =
                Cursor::new(b"lru_crawler mgdump 3\r\nmg key\r\nmg key2\r\nEN\r\n".to_vec());
            assert_eq!(
                lru_crawler_mgdump_cmd(&mut c, LruCrawlerMgdumpArg::Classids(&[3]))
                    .await
                    .unwrap(),
                ["key", "key2"]
            );

            let mut c = Cursor::new(b"lru_crawler mgdump all\r\nERROR\r\n".to_vec());
            assert!(
                lru_crawler_mgdump_cmd(&mut c, LruCrawlerMgdumpArg::All)
                    .await
                    .is_err()
            );

            let mut c = Cursor::new(b"lru_crawler mgdump hash\r\nERROR\r\n".to_vec());
            assert!(
                lru_crawler_mgdump_cmd(&mut c, LruCrawlerMgdumpArg::Hash)
                    .await
                    .is_err()
            )
        })
    }

    #[test]
    fn test_mn() {
        block_on(async {
            let mut c = Cursor::new(b"mn\r\nMN\r\n".to_vec());
            assert!(mn_cmd(&mut c).await.is_ok());

            let mut c = Cursor::new(b"mn\r\nERROR\r\n".to_vec());
            assert!(mn_cmd(&mut c).await.is_err())
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
            assert!(me_cmd(&mut c, b"key").await.is_err());
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
                b"key=key exp=-1 la=1745299782 cas=2 fetch=no cls=1 size=63 flags=0\r\nkey=key2 exp=-1 la=1745299782 cas=2 fetch=no cls=1 size=63 flags=0\r\nEND\r\n".to_vec(),
                b"mg key\r\nmg key2\r\nEN\r\n".to_vec(),
                b"MN\r\n".to_vec(),
                b"ME key exp=-1 la=3 cas=2 fetch=no cls=1 size=63\r\n".to_vec(),
                b"VA 1 b c0 f0 h0 k44OG44K544OI l0 Oopaque s0 t0 W X Z\r\nA\r\n".to_vec(),
                b"HD b c0 k44OG44K544OI Oopaque s0\r\n".to_vec(),
                b"HD k44OG44K544OI Oopaque b\r\n".to_vec(),
                b"VA 2 Oopaque t0 c0 k44OG44K544OI b\r\n10\r\n".to_vec(),
                b"OK\r\n".to_vec(),
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
                    PipelineResponse::Bool(true),
                    PipelineResponse::Bool(true),
                    PipelineResponse::Bool(true),
                    PipelineResponse::Bool(true),
                    PipelineResponse::Unit(()),
                    PipelineResponse::Value(Some(2)),
                    PipelineResponse::Value(None),
                    PipelineResponse::Bool(true),
                    PipelineResponse::Bool(true),
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
                    PipelineResponse::Unit(()),
                    PipelineResponse::Unit(()),
                    PipelineResponse::VecString(vec![
                        "key=key exp=-1 la=1745299782 cas=2 fetch=no cls=1 size=63 flags=0"
                            .to_string(),
                        "key=key2 exp=-1 la=1745299782 cas=2 fetch=no cls=1 size=63 flags=0"
                            .to_string()
                    ]),
                    PipelineResponse::VecString(vec!["key".to_string(), "key2".to_string()]),
                    PipelineResponse::Unit(()),
                    PipelineResponse::OptionString(Some(
                        "key exp=-1 la=3 cas=2 fetch=no cls=1 size=63".to_string()
                    )),
                    PipelineResponse::MetaGet(MgItem {
                        success: true,
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
                        success: true,
                        cas: Some(0),
                        key: Some("44OG44K544OI".to_string()),
                        opaque: Some("opaque".to_string()),
                        size: Some(0),
                        base64_key: true
                    }),
                    PipelineResponse::MetaDelete(MdItem {
                        success: true,
                        key: Some("44OG44K544OI".to_string()),
                        opaque: Some("opaque".to_string()),
                        base64_key: true
                    }),
                    PipelineResponse::MetaArithmetic(MaItem {
                        success: true,
                        opaque: Some("opaque".to_string()),
                        ttl: Some(0),
                        cas: Some(0),
                        number: Some(10),
                        key: Some("44OG44K544OI".to_string()),
                        base64_key: true
                    }),
                    PipelineResponse::Unit(()),
                ]
            );

            let cmds = [b"version\r\n".to_vec(), b"quit\r\n".to_vec()];
            let rps = [b"ERROR\r\n".to_vec(), b"OK\r\n".to_vec()];
            let mut c = Cursor::new([cmds.concat(), rps.concat()].concat().to_vec());
            assert!(execute_cmd(&mut c, &cmds).await.is_err());
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
            assert!(
                watch_cmd(&mut c, &[WatchArg::Fetchers, WatchArg::Mutations])
                    .await
                    .is_err()
            );
        })
    }

    #[test]
    fn test_mg() {
        block_on(async {
            let mut c = Cursor::new(b"mg key b\r\nEN b\r\n".to_vec());
            assert_eq!(
                mg_cmd(&mut c, b"key", &[MgFlag::Base64Key]).await.unwrap(),
                MgItem {
                    success: false,
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

            let mut c = Cursor::new(b"mg 44OG44K544OI b c f h k l Oopaque s t u E0 N0 R0 T0\r\nHD b c0 f0 h0 k44OG44K544OI l0 Oopaque s0 t0 W X Z\r\n".to_vec());
            assert_eq!(
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
                        MgFlag::NewCas(0),
                        MgFlag::Autovivify(0),
                        MgFlag::RecacheTtl(0),
                        MgFlag::UpdateTtl(0),
                    ]
                )
                .await
                .unwrap(),
                MgItem {
                    success: true,
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

            let mut c = Cursor::new(b"mg 44OG44K544OI b c f h k l Oopaque s t u E0 N0 R0 T0 v\r\nVA 1 b c0 f0 h0 k44OG44K544OI l0 Oopaque s0 t0 W X Z\r\nA\r\n".to_vec());
            assert_eq!(
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
                .await
                .unwrap(),
                MgItem {
                    success: true,
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
            assert!(
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
                .await
                .is_err(),
            );
        })
    }

    #[test]
    fn test_ms() {
        block_on(async {
            let mut c = Cursor::new(
                b"ms 44OG44K544OI 2 b c C0 E0 F0 I k Oopaque s T0 MP N0\r\nhi\r\nNF\r\n".to_vec(),
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
                        MsFlag::Autovivify(0)
                    ],
                    b"hi"
                )
                .await
                .unwrap(),
                MsItem {
                    success: false,
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
                    success: false,
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
                    success: false,
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
            assert!(
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
                .await
                .is_err()
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
                    success: true,
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
            let mut c = Cursor::new(b"md 44OG44K544OI b C0 E0 I k Oopaque T0 x\r\nNF\r\n".to_vec());
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
                    success: false,
                    key: None,
                    opaque: None,
                    base64_key: false,
                }
            );

            let mut c = Cursor::new(b"md 44OG44K544OI\r\nEX\r\n".to_vec());
            assert_eq!(
                md_cmd(&mut c, b"44OG44K544OI", &[]).await.unwrap(),
                MdItem {
                    success: false,
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
                    success: true,
                    key: Some("44OG44K544OI".to_string()),
                    opaque: Some("opaque".to_string()),
                    base64_key: true
                }
            );

            let mut c =
                Cursor::new(b"md 44OG44K544OI b C0 E0 I k Oopaque T0 x\r\nERROR\r\n".to_vec());
            assert!(
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
                .is_err(),
            )
        })
    }

    #[test]
    fn test_ma() {
        block_on(async {
            let mut c = Cursor::new(
                b"ma 44OG44K544OI b C0 E0 N0 J0 D0 T0 M+ Oopaque t c v k\r\nNF\r\n".to_vec(),
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
                    success: false,
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
                    success: false,
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
                    success: false,
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
                    success: true,
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
                    success: true,
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
            assert!(
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
                .await
                .is_err()
            )
        })
    }

    #[test]
    fn test_lru() {
        block_on(async {
            let mut c = Cursor::new(b"lru mode flat\r\nERROR\r\n".to_vec());
            assert!(lru_cmd(&mut c, LruArg::Mode(LruMode::Flat)).await.is_err());

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
}
