use std::collections::HashMap;

use crc32fast;
use smol::io::{self, BufReader};
use smol::net::{TcpStream, UdpSocket, unix::UnixStream};
use smol::prelude::*;

pub enum StatsArg {
    Empty,
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

#[derive(Debug, PartialEq)]
pub struct Item {
    pub key: String,
    pub flags: u32,
    pub cas_unique: Option<u64>,
    pub data_block: Vec<u8>,
}

async fn version_cmd<S>(s: &mut S) -> io::Result<String>
where
    S: AsyncBufRead + AsyncWrite + Unpin,
{
    s.write_all(b"version\r\n").await?;
    s.flush().await?;
    let mut line = String::new();
    let n = s.read_line(&mut line).await?;
    if line.starts_with("VERSION") {
        Ok(line[8..n - 2].to_string())
    } else {
        Err(io::Error::other(line))
    }
}

async fn quit_cmd<S>(s: &mut S) -> io::Result<()>
where
    S: AsyncBufRead + AsyncWrite + Unpin,
{
    s.write_all(b"quit\r\n").await?;
    s.flush().await?;
    Ok(())
}

async fn shutdown_cmd<S>(s: &mut S, graceful: bool) -> io::Result<()>
where
    S: AsyncBufRead + AsyncWrite + Unpin,
{
    let cmd: &[u8] = if graceful {
        b"shutdown graceful\r\n"
    } else {
        b"shutdown\r\n"
    };
    s.write_all(cmd).await?;
    s.flush().await?;
    Ok(())
}

async fn cache_memlimit_cmd<S>(s: &mut S, limit: usize, noreply: bool) -> io::Result<()>
where
    S: AsyncBufRead + AsyncWrite + Unpin,
{
    let n: &[u8] = if noreply { b" noreply" } else { b"" };
    let cmd = [b"cache_memlimit ", limit.to_string().as_bytes(), n, b"\r\n"].concat();
    s.write_all(&cmd).await?;
    s.flush().await?;
    if noreply {
        return Ok(());
    };
    let mut line = String::new();
    s.read_line(&mut line).await?;
    if line == "OK\r\n" {
        Ok(())
    } else {
        Err(io::Error::other(line))
    }
}

async fn flush_all_cmd<S>(s: &mut S, exptime: Option<i64>, noreply: bool) -> io::Result<()>
where
    S: AsyncBufRead + AsyncWrite + Unpin,
{
    let d = match exptime {
        Some(x) => format!(" {x}"),
        None => "".to_string(),
    };
    let n: &[u8] = if noreply { b" noreply" } else { b"" };
    let cmd = [b"flush_all", d.as_bytes(), n, b"\r\n"].concat();
    s.write_all(&cmd).await?;
    s.flush().await?;
    if noreply {
        return Ok(());
    };
    let mut line = String::new();
    s.read_line(&mut line).await?;
    if line == "OK\r\n" {
        Ok(())
    } else {
        Err(io::Error::other(line))
    }
}

async fn storage_cmd<S>(
    s: &mut S,
    command_name: &[u8],
    key: &[u8],
    flags: u32,
    exptime: i64,
    cas_unique: Option<u64>,
    noreply: bool,
    data_block: &[u8],
) -> io::Result<bool>
where
    S: AsyncBufRead + AsyncWrite + Unpin,
{
    let n: &[u8] = if noreply { b" noreply" } else { b"" };
    let cas = match cas_unique {
        Some(x) => format!(" {x}"),
        None => "".to_string(),
    };
    let cmd = [
        command_name,
        b" ",
        key,
        b" ",
        flags.to_string().as_bytes(),
        b" ",
        exptime.to_string().as_bytes(),
        b" ",
        data_block.len().to_string().as_bytes(),
        cas.as_bytes(),
        n,
        b"\r\n",
        data_block,
        b"\r\n",
    ]
    .concat();
    s.write_all(&cmd).await?;
    s.flush().await?;
    if noreply {
        return Ok(true);
    };
    let mut line = String::new();
    s.read_line(&mut line).await?;
    match line.as_str() {
        "STORED\r\n" => Ok(true),
        "NOT_STORED\r\n" | "EXISTS\r\n" | "NOT_FOUND\r\n" => Ok(false),
        _ => Err(io::Error::other(line)),
    }
}

async fn delete_cmd<S>(s: &mut S, key: &[u8], noreply: bool) -> io::Result<bool>
where
    S: AsyncBufRead + AsyncWrite + Unpin,
{
    let n: &[u8] = if noreply { b" noreply" } else { b"" };
    let cmd = [b"delete ", key, n, b"\r\n"].concat();
    s.write_all(&cmd).await?;
    s.flush().await?;
    if noreply {
        return Ok(true);
    };
    let mut line = String::new();
    s.read_line(&mut line).await?;
    match line.as_str() {
        "DELETED\r\n" => Ok(true),
        "NOT_FOUND\r\n" => Ok(false),
        _ => Err(io::Error::other(line)),
    }
}

async fn auth_cmd<S>(s: &mut S, username: &[u8], password: &[u8]) -> io::Result<()>
where
    S: AsyncBufRead + AsyncWrite + Unpin,
{
    let cmd = [
        b"set _ _ _ ",
        (username.len() + password.len() + 1).to_string().as_bytes(),
        b"\r\n",
        username,
        b" ",
        password,
        b"\r\n",
    ]
    .concat();
    s.write_all(&cmd).await?;
    s.flush().await?;
    let mut line = String::new();
    s.read_line(&mut line).await?;
    if line == "STORED\r\n" {
        Ok(())
    } else {
        Err(io::Error::other(line))
    }
}

async fn incr_decr_cmd<S>(
    s: &mut S,
    command_name: &[u8],
    key: &[u8],
    value: u64,
    noreply: bool,
) -> io::Result<Option<u64>>
where
    S: AsyncBufRead + AsyncWrite + Unpin,
{
    let n: &[u8] = if noreply { b" noreply" } else { b"" };
    let cmd = [
        command_name,
        b" ",
        key,
        b" ",
        value.to_string().as_bytes(),
        n,
        b"\r\n",
    ]
    .concat();
    s.write_all(&cmd).await?;
    s.flush().await?;
    if noreply {
        return Ok(None);
    };
    let mut line = String::new();
    s.read_line(&mut line).await?;
    match line.trim_end().parse::<u64>() {
        Ok(v) => Ok(Some(v)),
        Err(_) => Err(io::Error::other(line)),
    }
}

async fn touch_cmd<S>(s: &mut S, key: &[u8], exptime: i64, noreply: bool) -> io::Result<bool>
where
    S: AsyncBufRead + AsyncWrite + Unpin,
{
    let n: &[u8] = if noreply { b" noreply" } else { b"" };
    let cmd = [
        b"touch ",
        key,
        b" ",
        exptime.to_string().as_bytes(),
        n,
        b"\r\n",
    ]
    .concat();
    s.write_all(&cmd).await?;
    s.flush().await?;
    if noreply {
        return Ok(true);
    };
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

async fn retrieval_cmd<S>(
    s: &mut S,
    command_name: &[u8],
    exptime: Option<i64>,
    keys: &[&[u8]],
) -> io::Result<Vec<Item>>
where
    S: AsyncBufRead + AsyncWrite + Unpin,
{
    let t = match exptime {
        Some(x) => format!("{x} "),
        None => "".to_string(),
    };
    let cmd = [
        command_name,
        b" ",
        t.as_bytes(),
        keys.join(b" ".as_slice()).as_slice(),
        b"\r\n",
    ]
    .concat();
    s.write_all(&cmd).await?;
    s.flush().await?;
    let mut items = Vec::new();
    let mut line = String::new();
    s.read_line(&mut line).await?;
    while line.starts_with("VALUE") {
        let mut split = line.split(' ');
        split.next();
        let (key, flags, bytes, cas_unique) = (
            split.next().unwrap().to_string(),
            split.next().unwrap().parse::<u32>().unwrap(),
            split.next().unwrap().trim_end().parse::<usize>().unwrap(),
            split.next().map(|x| x.trim_end().parse::<u64>().unwrap()),
        );
        let mut data_block = vec![0; bytes];
        s.read_exact(&mut data_block).await?;
        s.read_line(&mut String::new()).await?;
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

async fn stats_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    arg: StatsArg,
) -> io::Result<HashMap<String, String>> {
    let a: &[u8] = match arg {
        StatsArg::Empty => b"",
        StatsArg::Settings => b" settings",
        StatsArg::Items => b" items",
        StatsArg::Sizes => b" sizes",
        StatsArg::Slabs => b" slabs",
        StatsArg::Conns => b" conns",
    };
    let cmd = [b"stats", a, b"\r\n"].concat();
    s.write_all(&cmd).await?;
    s.flush().await?;
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
        };
    }
    Ok(items)
}

async fn slabs_automove_cmd<S>(s: &mut S, arg: SlabsAutomoveArg) -> io::Result<()>
where
    S: AsyncBufRead + AsyncWrite + Unpin,
{
    let a: &[u8] = match arg {
        SlabsAutomoveArg::Zero => b"0",
        SlabsAutomoveArg::One => b"1",
        SlabsAutomoveArg::Two => b"2",
    };
    let cmd = [b"slabs automove ", a, b"\r\n"].concat();
    s.write_all(&cmd).await?;
    s.flush().await?;
    let mut line = String::new();
    s.read_line(&mut line).await?;
    if line == "OK\r\n" {
        Ok(())
    } else {
        Err(io::Error::other(line))
    }
}

async fn lru_crawler_cmd<S>(s: &mut S, arg: LruCrawlerArg) -> io::Result<()>
where
    S: AsyncBufRead + AsyncWrite + Unpin,
{
    let cmd: &[u8] = match arg {
        LruCrawlerArg::Enable => b"lru_crawler enable\r\n",
        LruCrawlerArg::Disable => b"lru_crawler disable\r\n",
    };
    s.write_all(cmd).await?;
    s.flush().await?;
    let mut line = String::new();
    s.read_line(&mut line).await?;
    if line == "OK\r\n" {
        Ok(())
    } else {
        Err(io::Error::other(line))
    }
}

async fn lru_crawler_sleep_cmd<S>(s: &mut S, microseconds: usize) -> io::Result<()>
where
    S: AsyncBufRead + AsyncWrite + Unpin,
{
    let cmd = [
        b"lru_crawler sleep ",
        microseconds.to_string().as_bytes(),
        b"\r\n",
    ]
    .concat();
    s.write_all(&cmd).await?;
    s.flush().await?;
    let mut line = String::new();
    s.read_line(&mut line).await?;
    if line == "OK\r\n" {
        Ok(())
    } else {
        Err(io::Error::other(line))
    }
}

async fn lru_crawler_tocrawl_cmd<S>(s: &mut S, arg: u32) -> io::Result<()>
where
    S: AsyncBufRead + AsyncWrite + Unpin,
{
    let cmd = [b"lru_crawler tocrawl ", arg.to_string().as_bytes(), b"\r\n"].concat();
    s.write_all(&cmd).await?;
    s.flush().await?;
    let mut line = String::new();
    s.read_line(&mut line).await?;
    if line == "OK\r\n" {
        Ok(())
    } else {
        Err(io::Error::other(line))
    }
}

async fn lru_crawler_crawl_cmd<S>(s: &mut S, arg: LruCrawlerCrawlArg<'_>) -> io::Result<()>
where
    S: AsyncBufRead + AsyncWrite + Unpin,
{
    let a = match arg {
        LruCrawlerCrawlArg::Classids(ids) => ids
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<_>>()
            .join(","),
        LruCrawlerCrawlArg::All => "all".to_string(),
    };
    let cmd = [b"lru_crawler crawl ", a.as_bytes(), b"\r\n"].concat();
    s.write_all(&cmd).await?;
    s.flush().await?;
    let mut line = String::new();
    s.read_line(&mut line).await?;
    if line == "OK\r\n" {
        Ok(())
    } else {
        Err(io::Error::other(line))
    }
}

async fn slabs_reassign_cmd<S>(s: &mut S, source_class: usize, dest_class: usize) -> io::Result<()>
where
    S: AsyncBufRead + AsyncWrite + Unpin,
{
    let cmd = [
        b"slabs reassign ",
        source_class.to_string().as_bytes(),
        b" ",
        dest_class.to_string().as_bytes(),
        b"\r\n",
    ]
    .concat();
    s.write_all(&cmd).await?;
    s.flush().await?;
    let mut line = String::new();
    s.read_line(&mut line).await?;
    if line == "OK\r\n" {
        Ok(())
    } else {
        Err(io::Error::other(line))
    }
}

async fn lru_crawler_metadump_cmd<S>(
    s: &mut S,
    arg: LruCrawlerMetadumpArg<'_>,
) -> io::Result<Vec<String>>
where
    S: AsyncBufRead + AsyncWrite + Unpin,
{
    let a = match arg {
        LruCrawlerMetadumpArg::Classids(ids) => ids
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<_>>()
            .join(","),
        LruCrawlerMetadumpArg::All => "all".to_string(),
        LruCrawlerMetadumpArg::Hash => "hash".to_string(),
    };
    let cmd = [b"lru_crawler metadump ", a.as_bytes(), b"\r\n"].concat();
    s.write_all(&cmd).await?;
    s.flush().await?;
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

async fn lru_crawler_mgdump_cmd<S>(
    s: &mut S,
    arg: LruCrawlerMgdumpArg<'_>,
) -> io::Result<Vec<String>>
where
    S: AsyncBufRead + AsyncWrite + Unpin,
{
    let a = match arg {
        LruCrawlerMgdumpArg::Classids(ids) => ids
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<_>>()
            .join(","),
        LruCrawlerMgdumpArg::All => "all".to_string(),
        LruCrawlerMgdumpArg::Hash => "hash".to_string(),
    };
    let cmd = [b"lru_crawler mgdump ", a.as_bytes(), b"\r\n"].concat();
    s.write_all(&cmd).await?;
    s.flush().await?;
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

async fn mn_cmd<S>(s: &mut S) -> io::Result<()>
where
    S: AsyncBufRead + AsyncWrite + Unpin,
{
    s.write_all(b"mn\r\n").await?;
    s.flush().await?;
    let mut line = String::new();
    s.read_line(&mut line).await?;
    if line == "MN\r\n" {
        Ok(())
    } else {
        Err(io::Error::other(line))
    }
}

async fn me_cmd<S>(s: &mut S, key: &[u8]) -> io::Result<Option<String>>
where
    S: AsyncBufRead + AsyncWrite + Unpin,
{
    let cmd = [b"me ", key, b"\r\n"].concat();
    s.write_all(&cmd).await?;
    let mut line = String::new();
    s.read_line(&mut line).await?;
    if line == "EN\r\n" {
        Ok(None)
    } else if line.starts_with("ME") {
        Ok(Some(line[4 + key.len()..line.len() - 2].to_string()))
    } else {
        Err(io::Error::other(line))
    }
}

pub enum Connection {
    Tcp(BufReader<TcpStream>),
    Unix(BufReader<UnixStream>),
    Udp(UdpSocket),
}
impl Connection {
    /// # Example
    ///
    /// ```rust
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
    /// ```rust
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
    /// ```rust
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
    /// ```rust
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
            Connection::Udp(s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```rust
    /// # use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// conn.quit().await?;
    /// #     Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn quit(&mut self) -> io::Result<()> {
        match self {
            Connection::Tcp(s) => quit_cmd(s).await,
            Connection::Unix(s) => quit_cmd(s).await,
            Connection::Udp(s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```rust
    /// # use mcmc_rs::Connection;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::tcp_connect("127.0.0.1:11213").await?;
    /// conn.shutdown(true).await?;
    /// #     Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn shutdown(&mut self, graceful: bool) -> io::Result<()> {
        match self {
            Connection::Tcp(s) => shutdown_cmd(s, graceful).await,
            Connection::Unix(s) => shutdown_cmd(s, graceful).await,
            Connection::Udp(s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```rust
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
            Connection::Udp(s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```rust
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
            Connection::Udp(s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```rust
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
            Connection::Udp(s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```rust
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
            Connection::Udp(s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```rust
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
            Connection::Udp(s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```rust
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
            Connection::Udp(s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```rust
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
            Connection::Udp(s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```rust
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
            Connection::Udp(s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```rust
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
            Connection::Udp(s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```rust
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
            Connection::Udp(s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```rust
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
            Connection::Udp(s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```rust
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
            Connection::Udp(s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```rust
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
            Connection::Udp(s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```rust
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
            Connection::Udp(s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```rust
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
            Connection::Udp(s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```rust
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
            Connection::Udp(s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```rust
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
            Connection::Udp(s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```rust
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
            Connection::Udp(s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```rust
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
            Connection::Udp(s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```rust
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
            Connection::Udp(s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```rust
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
            Connection::Udp(s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```rust
    /// use mcmc_rs::Connection;
    /// use mcmc_rs::StatsArg;
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// let result = conn.stats(StatsArg::Empty).await?;
    /// assert!(result.len() > 0);
    /// # Ok::<(), io::Error>(())
    /// # }).unwrap()
    /// ```
    pub async fn stats(&mut self, arg: StatsArg) -> io::Result<HashMap<String, String>> {
        match self {
            Connection::Tcp(s) => stats_cmd(s, arg).await,
            Connection::Unix(s) => stats_cmd(s, arg).await,
            Connection::Udp(s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```rust
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
            Connection::Udp(s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```rust
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
            Connection::Udp(s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```rust
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
            Connection::Udp(s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```rust
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
            Connection::Udp(s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```rust
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
            Connection::Udp(s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```rust
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
            Connection::Udp(s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```rust
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
            Connection::Udp(s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```rust
    /// use mcmc_rs::{Connection, LruCrawlerMgdumpArg};
    /// # use smol::{io, block_on};
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
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
            Connection::Udp(s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```rust
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
            Connection::Udp(s) => todo!(),
        }
    }

    /// # Example
    ///
    /// ```rust
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
            Connection::Udp(s) => todo!(),
        }
    }
}

pub struct ClientCrc32(Vec<Connection>);
impl ClientCrc32 {
    pub fn new(conns: Vec<Connection>) -> Self {
        Self(conns)
    }

    /// # Example
    ///
    /// ```rust
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
    /// ```rust
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
            let mut c = Cursor::new(b"set key 0 0 0 0\r\nvalue\r\nSTORED\r\n".to_vec());
            assert!(
                storage_cmd(&mut c, b"set", b"key", 0, 0, Some(0), false, b"value")
                    .await
                    .unwrap()
            );

            let mut c = Cursor::new(b"set key 0 0 0 noreply\r\nvalue\r\n".to_vec());
            assert!(
                storage_cmd(&mut c, b"set", b"key", 0, 0, None, true, b"value")
                    .await
                    .unwrap()
            );

            let mut c = Cursor::new(b"set key 0 0 0\r\nvalue\r\nNOT_STORED\r\n".to_vec());
            assert!(
                !storage_cmd(&mut c, b"set", b"key", 0, 0, None, false, b"value")
                    .await
                    .unwrap()
            );

            let mut c = Cursor::new(b"set key 0 0 0\r\nvalue\r\nERROR\r\n".to_vec());
            assert!(
                storage_cmd(&mut c, b"set", b"key", 0, 0, None, false, b"value")
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
            let mut c = Cursor::new(b"set _ _ _ 2\r\na b\r\nSTORED\r\n".to_vec());
            assert!(auth_cmd(&mut c, b"a", b"b").await.is_ok());

            let mut c = Cursor::new(b"set _ _ _ 2\r\na b\r\nERROR\r\n".to_vec());
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
            assert_eq!(
                incr_decr_cmd(&mut c, b"incr", b"key", 1, true)
                    .await
                    .unwrap(),
                None,
            );

            let mut c = Cursor::new(b"incr key 1\r\nNOT_FOUND\r\n".to_vec());
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
            let mut c = Cursor::new(b"touch 0 key\r\nTOUCHED\r\n".to_vec());
            assert!(touch_cmd(&mut c, b"key", 0, false).await.unwrap());

            let mut c = Cursor::new(b"touch 0 key\r\nNOT_FOUND\r\n".to_vec());
            assert!(!touch_cmd(&mut c, b"key", 0, false).await.unwrap());

            let mut c = Cursor::new(b"touch 0 key noreply\r\n".to_vec());
            assert!(touch_cmd(&mut c, b"key", 0, true).await.unwrap());

            let mut c = Cursor::new(b"touch 0 key\r\nERROR\r\n".to_vec());
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
                stats_cmd(&mut c, StatsArg::Empty).await.unwrap(),
                HashMap::from([
                    ("version".to_string(), "1.2.3".to_string()),
                    ("threads".to_string(), "4".to_string()),
                ])
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
            assert!(
                slabs_automove_cmd(&mut c, SlabsAutomoveArg::One)
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
                "exp=-1 la=3 cas=2 fetch=no cls=1 size=63"
            );

            let mut c = Cursor::new(b"me key\r\nERROR\r\n".to_vec());
            assert!(me_cmd(&mut c, b"key").await.is_err());
        })
    }
}
