//! Async client for [Memcached](https://memcached.org/).
//!
//! `mcmc-rs` implements the Memcached text protocol and supports TCP, Unix
//! domain sockets, UDP and TLS. Both the [`smol`](https://docs.rs/smol) and
//! [`tokio`](https://docs.rs/tokio) runtimes are supported; the runtime is
//! selected at compile time with Cargo features.
//!
//! # Feature flags
//!
//! The crate has **no default features**. At minimum, one async runtime must be
//! enabled:
//!
//! | Feature    | Description                                              |
//! | ---------- | -------------------------------------------------------- |
//! | `smol-rt`  | Uses `async-net` and `futures-lite` (the smol ecosystem). |
//! | `tokio-rt` | Uses Tokio networking and I/O traits.                     |
//!
//! Exactly one runtime must be selected. Enabling both is not supported.
//!
//! ## TLS
//!
//! TLS is optional. Each TLS feature enables the runtime it belongs to, so
//! listing the TLS feature alone is enough:
//!
//! | Feature                | Runtime | Backend                              |
//! | ---------------------- | ------- | ------------------------------------ |
//! | `smol-rustls-tls`      | smol    | rustls via `futures-rustls`          |
//! | `tokio-rustls-tls`     | tokio   | rustls via `tokio-rustls`            |
//! | `smol-native-tls-tls`  | smol    | native-tls via `async-native-tls`    |
//! | `tokio-native-tls-tls` | tokio   | native-tls via `tokio-native-tls`    |
//!
//! Use at most one TLS backend. `Connection::tls_connect` is available only
//! when one of these features is enabled.
//!
//! ### rustls certificate roots
//!
//! The rustls backend starts with an empty root store. To verify public
//! certificates, enable one or both of:
//!
//! - `webpki-roots-tls` — use the bundled Mozilla root certificates;
//! - `rustls-native-certs-tls` — use the operating system trust store.
//!
//! [`TlsConfig`] selects which roots are trusted:
//! [`TlsConfig::with_custom_ca_only`] uses only the given PEM file,
//! [`TlsConfig::with_additional_ca`] adds it to the system roots, and
//! [`TlsConfig::new`] uses the system roots alone. The native-tls backend uses
//! the platform trust store automatically and does not need these features.
//!
//! ### rustls CryptoProvider
//!
//! `mcmc-rs` does not select or install a rustls
//! [`CryptoProvider`](https://docs.rs/rustls/latest/rustls/crypto/struct.CryptoProvider.html).
//! With a rustls backend, choose a provider in your application. The TLS
//! helper crates may enable `aws-lc-rs` through their default features, but you
//! should not rely on that. The simplest explicit option is to add the `rustls`
//! dependency with its default features, which enables the `aws-lc-rs`
//! provider:
//!
//! ```toml
//! rustls = "0.23"
//! ```
//!
//! Alternatively, install the provider explicitly before creating the first
//! TLS connection:
//!
//! ```ignore
//! rustls::crypto::aws_lc_rs::default_provider()
//!     .install_default()
//!     .expect("failed to install rustls CryptoProvider");
//! ```
//!
//! To use *ring* instead, enable that provider on the `rustls` dependency and
//! install it:
//!
//! ```toml
//! rustls = { version = "0.23", default-features = false, features = ["ring"] }
//! ```
//!
//! ```ignore
//! rustls::crypto::ring::default_provider()
//!     .install_default()
//!     .expect("failed to install rustls CryptoProvider");
//! ```
//!
//! Selecting the provider explicitly also prepares the code for rustls 0.24,
//! where this becomes mandatory. The native-tls backends do not use a rustls
//! `CryptoProvider`.
//!
//! ### Insecure TLS
//!
//! The `insecure-tls` feature enables `TlsConfig::danger_accept_all()`, which
//! disables certificate and hostname verification. It is intended for local
//! testing only. Without the feature, the `ServerVerification::DangerAcceptAll`
//! variant is not available.
//!
//! # Choosing features
//!
//! ```toml
//! # smol + rustls with bundled roots
//! mcmc-rs = { version = "0.17.0", features = ["smol-rustls-tls", "webpki-roots-tls"] }
//!
//! # tokio + native-tls
//! mcmc-rs = { version = "0.17.0", features = ["tokio-native-tls-tls"] }
//!
//! # smol without TLS
//! mcmc-rs = { version = "0.17.0", features = ["smol-rt"] }
//! ```
//!
//! # Connections
//!
//! [`Connection`] is an enum over the supported transports:
//!
//! - `Connection::default` connects to `127.0.0.1:11211` over TCP;
//! - `Connection::tcp_connect` connects to a TCP address;
//! - `Connection::unix_connect` connects to a Unix domain socket (Unix only);
//! - `Connection::udp_connect` creates a UDP connection;
//! - `Connection::tls_connect` creates a TLS connection (requires a TLS
//!   feature).
//!
//! TCP and TLS connections enable `TCP_NODELAY` automatically to avoid Nagle's
//! algorithm delaying small protocol messages.
//!
//! # Basic usage
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
//!
//! # Tokio usage
//!
//! The same API is used with Tokio when `tokio-rt` (or a Tokio TLS feature) is
//! enabled:
//!
//! ```no_run
//! use mcmc_rs::Connection;
//!
//! fn main() -> mcmc_rs::Result<()> {
//!     let runtime = tokio::runtime::Runtime::new()?;
//!     runtime.block_on(async {
//!         let mut conn = Connection::default().await?;
//!         conn.set(b"key", 0, 0, false, b"value").await?;
//!         Ok(())
//!     })
//! }
//! ```
//!
//! # TLS usage
//!
//! TLS requires one of the TLS features listed above:
//!
//! ```ignore
//! use mcmc_rs::{Connection, TlsConfig};
//!
//! let mut conn = Connection::tls_connect(
//!     "localhost",
//!     11216,
//!     TlsConfig::with_additional_ca("ca.crt")
//!         .with_client_identity("client.crt", "client.key"),
//! )
//! .await?;
//! ```
//!
//! # Pipelines, watchers and clusters
//!
//! - [`Pipeline`] batches several commands and executes them in one round trip;
//! - [`WatchStream`] exposes Memcached watch events;
//! - [`ClientModN`] distributes keys across connections with modulo hashing;
//! - [`ClientHashRing`] uses a consistent hash ring with weighted virtual nodes;
//! - [`ClientRendezvous`] uses rendezvous (highest random weight) hashing.
//!
//! By default the sharded clients hash keys with `rapidhash` and a fixed seed.
//! Each of them also accepts a custom seed or any [`std::hash::BuildHasher`],
//! and [`ClientHashRing`] supports per-node weights. A client may be
//! constructed without connections; routing a key through it then returns
//! [`MemcachedError::ClientConfig`].
//!
//! All sharded clients implement [`ServerSelector`] and provide the multi-key
//! retrieval methods `get_multi`, `gets_multi`, `gat_multi` and `gats_multi`.
//! They group keys by their target server, query each server concurrently and
//! merge the results. The returned order is not guaranteed to match the input
//! order, and an empty key slice returns an empty vector without sending any
//! request. [`Connection`] exposes the same multi-key methods for a single
//! server; they are not supported over UDP and panic if called on a UDP
//! connection.
//!
//! # Key and flag validation
//!
//! Command builders validate their input before writing to the socket. A key
//! must be non-empty, at most 250 bytes long, and must not contain whitespace
//! or control characters. Meta-flag tokens must not contain whitespace or
//! control characters. Invalid input produces [`MemcachedError::InvalidInput`]
//! and nothing is sent to the server.
//!
//! [`Connection`] methods return this error directly. [`Pipeline`] builder
//! methods return `Self`, so validation errors are reported by
//! [`Pipeline::execute`].
//!
//! # Errors
//!
//! Fallible operations return [`Result`], which is an alias for
//! `std::result::Result<T, MemcachedError>`. See [`MemcachedError`] for the
//! list of error kinds. [`MemcachedError::InvalidInput`] reports keys or flags
//! rejected by client-side validation.

use std::collections::HashMap;
use std::hash::BuildHasher;
use std::io::Write;

use hashring::HashRing;
use hrw::Rendezvous;
use rapidhash::quality::SeedableState;

#[cfg(not(any(feature = "smol-rt", feature = "tokio-rt")))]
compile_error!("enable exactly one of the features - \"smol-rt\" or \"tokio-rt\"");

// Runtime abstraction. The rest of the crate uses the names re-exported here
// and does not depend on the concrete runtime. Exactly one runtime is expected;
// if both are enabled, smol takes priority so that `--all-features` builds and
// documentation can still be produced.
#[cfg(feature = "smol-rt")]
mod rt {
    #[cfg(unix)]
    pub use async_net::unix::UnixStream;
    pub use async_net::{TcpStream, UdpSocket};
    pub use futures_lite::io::{BufReader, Cursor};
    pub use futures_lite::prelude::*;
}
#[cfg(all(feature = "tokio-rt", not(feature = "smol-rt")))]
mod rt {
    pub use std::io::Cursor;
    pub use tokio::io::{
        AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader,
    };
    #[cfg(unix)]
    pub use tokio::net::UnixStream;
    pub use tokio::net::{TcpStream, UdpSocket};
}
#[cfg(any(feature = "smol-rt", feature = "tokio-rt"))]
use rt::*;

// TLS backend abstraction. The public API is the same for both backends; only
// the connector and stream types are selected here. If more than one backend
// is enabled, rustls takes priority over native-tls.
#[cfg(feature = "smol-rustls-tls")]
mod rustls_tls {
    pub use futures_rustls::TlsConnector;
    pub use futures_rustls::client::TlsStream;
    pub use futures_rustls::rustls;
    pub use futures_rustls::rustls::pki_types::pem::PemObject;
}
#[cfg(all(feature = "tokio-rustls-tls", not(feature = "smol-rustls-tls")))]
mod rustls_tls {
    pub use tokio_rustls::TlsConnector;
    pub use tokio_rustls::client::TlsStream;
    pub use tokio_rustls::rustls;
    pub use tokio_rustls::rustls::pki_types::pem::PemObject;
}
#[cfg(any(feature = "smol-rustls-tls", feature = "tokio-rustls-tls"))]
use rustls_tls::*;

#[cfg(all(
    feature = "smol-native-tls-tls",
    not(any(feature = "smol-rustls-tls", feature = "tokio-rustls-tls"))
))]
mod native_tls_tls {
    pub use async_native_tls::TlsConnector;
    pub use async_native_tls::TlsStream;
}
#[cfg(all(feature = "tokio-native-tls-tls", not(feature = "smol-native-tls-tls")))]
mod native_tls_tls {
    pub use tokio_native_tls::TlsConnector;
    pub use tokio_native_tls::TlsStream;
}
#[cfg(all(
    any(feature = "smol-native-tls-tls", feature = "tokio-native-tls-tls"),
    not(any(feature = "smol-rustls-tls", feature = "tokio-rustls-tls"))
))]
use native_tls_tls::*;

// Custom rustls verifier used for `ServerVerification::DangerAcceptAll` with
// the `insecure-tls` feature. It deliberately accepts every certificate.
#[cfg(any(feature = "smol-rustls-tls", feature = "tokio-rustls-tls"))]
mod danger {
    use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
    use rustls::crypto::{CryptoProvider, verify_tls12_signature, verify_tls13_signature};
    use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
    use rustls::{DigitallySignedStruct, Error, SignatureScheme};

    #[derive(Debug)]
    pub(super) struct NoCertificateVerification(CryptoProvider);

    impl NoCertificateVerification {
        pub(super) fn new(provider: CryptoProvider) -> Self {
            Self(provider)
        }
    }

    impl ServerCertVerifier for NoCertificateVerification {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp: &[u8],
            _now: UnixTime,
        ) -> Result<ServerCertVerified, Error> {
            Ok(ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, Error> {
            verify_tls12_signature(
                message,
                cert,
                dss,
                &self.0.signature_verification_algorithms,
            )
        }

        fn verify_tls13_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, Error> {
            verify_tls13_signature(
                message,
                cert,
                dss,
                &self.0.signature_verification_algorithms,
            )
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            self.0.signature_verification_algorithms.supported_schemes()
        }
    }
}

use thiserror::Error;

#[cfg(any(feature = "smol-rustls-tls", feature = "tokio-rustls-tls"))]
#[derive(Error, Debug)]
pub enum RustlsTlsError {
    #[error("invalid CA certificate: {0}")]
    InvalidCa(#[from] rustls::pki_types::pem::Error),

    #[error("TLS error: {0}")]
    Tls(#[from] rustls::Error),

    #[error("invalid server name: {0}")]
    InvalidServerName(#[from] rustls::pki_types::InvalidDnsNameError),

    #[cfg(feature = "rustls-native-certs-tls")]
    #[error("failed to load platform certificates: {0}")]
    PlatformCerts(#[from] rustls_native_certs::Error),
}

#[derive(Error, Debug)]
pub enum ProtocolError {
    #[error("Client sent a nonexistent command name")]
    UnknownCommand,

    #[error("The input doesn't conform to the protocol. Client error: {0}")]
    Client(String),

    #[error("Internal server error: {0}")]
    Server(String),
}

#[derive(Error, Debug)]
pub enum MemcachedError {
    #[cfg(any(feature = "smol-native-tls-tls", feature = "tokio-native-tls-tls"))]
    #[error("NativeTls TLS error: {0}")]
    NativeTlsError(#[from] native_tls::Error),

    #[cfg(any(feature = "smol-rustls-tls", feature = "tokio-rustls-tls"))]
    #[error("Rustls TLS error: {0}")]
    RustlsTls(#[from] RustlsTlsError),

    #[error("Protocol error: {0}")]
    Protocol(#[from] ProtocolError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Unexpected error: {0}")]
    Unexpected(String),

    #[error("Wrong configuration: {0}")]
    ClientConfig(String),

    /// A key or meta-flag token rejected by client-side validation.
    #[error("Invalid input: {0}")]
    InvalidInput(String),
}

pub type Result<T> = std::result::Result<T, MemcachedError>;

fn check_error(line: String) -> MemcachedError {
    if line == "ERROR\r\n" {
        return MemcachedError::Protocol(ProtocolError::UnknownCommand);
    };
    if let Some(line) = line.strip_prefix("CLIENT_ERROR ") {
        return MemcachedError::Protocol(ProtocolError::Client(line.trim().to_owned()));
    }
    if let Some(line) = line.strip_prefix("SERVER_ERROR ") {
        return MemcachedError::Protocol(ProtocolError::Server(line.trim().to_owned()));
    };
    MemcachedError::Unexpected(line)
}

fn validate_key(key: &[u8]) -> Result<()> {
    if key.is_empty() {
        return Err(MemcachedError::InvalidInput(
            "key must not be empty".to_owned(),
        ));
    }
    if key.len() > 250 {
        return Err(MemcachedError::InvalidInput(
            "key length must not exceed 250 bytes".to_owned(),
        ));
    }
    if key.iter().any(|&b| b <= 0x20 || b == 0x7f) {
        return Err(MemcachedError::InvalidInput(
            "key must not contain whitespace or control characters".to_owned(),
        ));
    }
    Ok(())
}

fn validate_flag(token: &[u8]) -> Result<()> {
    if token.is_empty() {
        return Err(MemcachedError::InvalidInput(
            "flag token must not be empty".to_owned(),
        ));
    }
    if token.iter().any(|&b| b <= 0x20 || b == 0x7f) {
        return Err(MemcachedError::InvalidInput(
            "flag token must not contain whitespace or control characters".to_owned(),
        ));
    }
    Ok(())
}

const MAX_ITEM_SIZE: usize = 128 * 1024 * 1024; // 128 MiB

fn parse_err<T: std::fmt::Display>(msg: T) -> MemcachedError {
    MemcachedError::Unexpected(msg.to_string())
}

#[derive(Debug, PartialEq)]
pub enum StorageResult {
    Stored,
    NotStored,
    Exists,
    NotFound,
    NoReply,
}

#[derive(Debug, PartialEq)]
pub enum DeleteResult {
    Deleted,
    NotFound,
    NoReply,
}

#[derive(Debug, PartialEq)]
pub enum TouchResult {
    Touched,
    NotFound,
    NoReply,
}

#[derive(Debug, PartialEq)]
pub enum IncrDecrResult {
    Value(u64),
    NotFound,
    NoReply,
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
    Proxy,
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
    IncrDecrResult(IncrDecrResult),
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

pub enum PipelineExpect {
    RetrievalOne,
    RetrievalMulti,
    Storage { noreply: bool },
    Delete { noreply: bool },
    IncrDecr { noreply: bool },
    Touch { noreply: bool },
    Version,
    Ok { noreply: bool },
    Stats,
    LruCrawler,
    SlabsReassign,
    LruCrawlerMetaDump,
    LruCrawlerMgDump,
    Mn,
    Me,
    MetaGet,
    MetaSet,
    MetaDelete,
    MetaArithmetic,
    Auth,
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

/// Root certificates used to verify a TLS server.
#[derive(Default)]
pub enum TrustStore<'a> {
    /// Trust only the system trust store (or the bundled roots for rustls).
    #[default]
    SystemOnly,
    /// Trust only the certificates in the given PEM file.
    CustomOnly(&'a str),
    /// Trust the system trust store and the certificates in the given PEM file.
    SystemAndCustom(&'a str),
}

/// Server certificate verification policy.
pub enum ServerVerification<'a> {
    /// Verify the server certificate using the given trust store.
    Verify(TrustStore<'a>),
    /// Accept any certificate and hostname.
    ///
    /// Available only with the `insecure-tls` feature. Intended for local
    /// testing only.
    #[cfg(feature = "insecure-tls")]
    DangerAcceptAll,
}

impl Default for ServerVerification<'_> {
    fn default() -> Self {
        Self::Verify(TrustStore::SystemOnly)
    }
}

/// Client certificate chain and private key in PEM format.
pub struct ClientIdentity<'a> {
    /// Path to the PEM-encoded client certificate chain.
    pub cert_chain_path: &'a str,
    /// Path to the PEM-encoded private key.
    pub private_key_path: &'a str,
}

/// TLS settings for [`Connection::tls_connect`].
///
/// [`TlsConfig::new`] verifies the server against the system trust store and
/// does not present a client certificate. Use the builder-style methods to
/// customize verification and mutual TLS.
#[derive(Default)]
pub struct TlsConfig<'a> {
    /// How the server certificate is verified.
    pub verification: ServerVerification<'a>,
    /// Optional client certificate chain and private key.
    pub identity: Option<ClientIdentity<'a>>,
}
impl<'a> TlsConfig<'a> {
    /// Creates a configuration that verifies the server against the system
    /// trust store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a configuration that trusts only the certificates in `ca_path`.
    pub fn with_custom_ca_only(ca_path: &'a str) -> Self {
        Self {
            verification: ServerVerification::Verify(TrustStore::CustomOnly(ca_path)),
            identity: None,
        }
    }

    /// Creates a configuration that trusts the system roots and the
    /// certificates in `ca_path`.
    pub fn with_additional_ca(ca_path: &'a str) -> Self {
        Self {
            verification: ServerVerification::Verify(TrustStore::SystemAndCustom(ca_path)),
            identity: None,
        }
    }

    /// Creates a configuration that accepts any server certificate and
    /// hostname.
    ///
    /// Available only with the `insecure-tls` feature. Intended for local
    /// testing only.
    #[cfg(feature = "insecure-tls")]
    pub fn danger_accept_all() -> Self {
        Self {
            verification: ServerVerification::DangerAcceptAll,
            identity: None,
        }
    }

    /// Presents a client certificate chain and private key for mutual TLS.
    ///
    /// Both files must be PEM-encoded.
    pub fn with_client_identity(
        mut self,
        cert_chain_path: &'a str,
        private_key_path: &'a str,
    ) -> Self {
        self.identity = Some(ClientIdentity {
            cert_chain_path,
            private_key_path,
        });
        self
    }
}

async fn parse_storage_rp<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    noreply: bool,
) -> Result<StorageResult> {
    if noreply {
        return Ok(StorageResult::NoReply);
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
            split.next().ok_or_else(|| parse_err(&line))?.to_string(),
            split
                .next()
                .ok_or_else(|| parse_err(&line))?
                .parse()
                .map_err(parse_err)?,
            split
                .next()
                .ok_or_else(|| parse_err(&line))?
                .trim()
                .parse()
                .map_err(parse_err)?,
            split
                .next()
                .map(|x| x.trim().parse().map_err(parse_err))
                .transpose()?,
        );
        if bytes > MAX_ITEM_SIZE {
            return Err(parse_err("item size too large"));
        }
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
    s.read_line(&mut line).await?;
    if let Some(line) = line.strip_prefix("VERSION ") {
        Ok(line.trim().to_owned())
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
        return Ok(DeleteResult::NoReply);
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
) -> Result<IncrDecrResult> {
    if noreply {
        return Ok(IncrDecrResult::NoReply);
    }
    let mut line = String::new();
    s.read_line(&mut line).await?;
    if line == "NOT_FOUND\r\n" {
        return Ok(IncrDecrResult::NotFound);
    }
    match line.trim().parse() {
        Ok(v) => Ok(IncrDecrResult::Value(v)),
        Err(_) => Err(check_error(line)),
    }
}

async fn parse_touch_rp<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    noreply: bool,
) -> Result<TouchResult> {
    if noreply {
        return Ok(TouchResult::NoReply);
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
    let mut line = String::new();
    while s.read_line(&mut line).await? > 0 && line.starts_with("STAT") {
        let mut split = line.split(' ');
        split.next();
        let (k, v) = (
            split.next().ok_or_else(|| parse_err(&line))?.to_string(),
            split
                .next()
                .ok_or_else(|| parse_err(&line))?
                .trim()
                .to_string(),
        );
        items.insert(k, v);
        line.clear();
    }
    match line.as_str() {
        "END\r\n" => Ok(items),
        _ => Err(check_error(line)),
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
    if let Some(line) = line.strip_prefix("BUSY ") {
        return Ok(LruCrawlerResult::Busy(line.trim().to_owned()));
    }
    if let Some(line) = line.strip_prefix("BADCLASS ") {
        return Ok(LruCrawlerResult::BadClass(line.trim().to_owned()));
    }
    Err(check_error(line))
}

async fn parse_lru_crawler_metadump_rp<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
) -> Result<LruCrawlerDumpResult> {
    let mut line = String::new();
    let mut items = Vec::new();
    s.read_line(&mut line).await?;
    if let Some(line) = line.strip_prefix("BUSY ") {
        return Ok(LruCrawlerDumpResult::Busy(line.trim().to_owned()));
    }
    if let Some(line) = line.strip_prefix("BADCLASS ") {
        return Ok(LruCrawlerDumpResult::BadClass(line.trim().to_owned()));
    }
    if line == "OK\r\n" {
        while {
            line.clear();
            s.read_line(&mut line).await? > 0
        } && line.starts_with("key=")
        {
            items.push(line.trim().to_string())
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
    if let Some(line) = line.strip_prefix("BUSY ") {
        return Ok(LruCrawlerDumpResult::Busy(line.trim().to_owned()));
    }
    if let Some(line) = line.strip_prefix("BADCLASS ") {
        return Ok(LruCrawlerDumpResult::BadClass(line.trim().to_owned()));
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
    s.read_line(&mut line).await?;
    match line.as_str() {
        "EN\r\n" => Ok(None),
        _ if let Some(line) = line.strip_prefix("ME ") => Ok(Some(line.trim().to_owned())),
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
    let mut split = line.trim().split(' ');
    let data_len = if line.starts_with("VA") {
        status = MgStatus::WithBody;
        split.next();
        Some(
            split
                .next()
                .ok_or_else(|| parse_err(&line))?
                .parse()
                .map_err(parse_err)?,
        )
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
        let Some((prefix, f)) = flag.split_at_checked(1) else {
            return Err(parse_err(&line));
        };
        match prefix {
            "b" => base64_key = true,
            "c" => cas = Some(f.parse().map_err(parse_err)?),
            "f" => flags = Some(f.parse().map_err(parse_err)?),
            "h" => hit = Some(f.parse().map_err(parse_err)?),
            "k" => key = Some(f.to_string()),
            "l" => last_access_ttl = Some(f.parse().map_err(parse_err)?),
            "O" => opaque = Some(f.to_string()),
            "s" => size = Some(f.parse().map_err(parse_err)?),
            "t" => ttl = Some(f.parse().map_err(parse_err)?),
            "W" => won_recache = true,
            "X" => stale = true,
            "Z" => already_win = true,
            other => return Err(parse_err(format!("unexpected mg flag: {other}"))),
        }
    }
    if let Some(bytes) = data_len {
        if bytes > MAX_ITEM_SIZE {
            return Err(parse_err("item size too large"));
        }
        let mut buf = vec![0; bytes + 2];
        s.read_exact(&mut buf).await?;
        buf.truncate(bytes);
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
    let mut split = line.trim().split(' ');
    split.next();
    for flag in split {
        let Some((prefix, f)) = flag.split_at_checked(1) else {
            return Err(parse_err(&line));
        };
        match prefix {
            "c" => cas = Some(f.parse().map_err(parse_err)?),
            "k" => key = Some(f.to_string()),
            "O" => opaque = Some(f.to_string()),
            "s" => size = Some(f.parse().map_err(parse_err)?),
            "b" => base64_key = true,
            other => return Err(parse_err(format!("unexpected ms flag: {other}"))),
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
    let mut split = line.trim().split(' ');
    split.next();
    for flag in split {
        let Some((prefix, f)) = flag.split_at_checked(1) else {
            return Err(parse_err(&line));
        };
        match prefix {
            "k" => key = Some(f.to_string()),
            "O" => opaque = Some(f.to_string()),
            "b" => base64_key = true,
            other => return Err(parse_err(format!("unexpected md flag: {other}"))),
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
    let mut split = line.trim().split(' ');
    let data_len = if line.starts_with("VA") {
        split.next();
        status = MaStatus::WithBody;
        Some(
            split
                .next()
                .ok_or_else(|| parse_err(&line))?
                .parse()
                .map_err(parse_err)?,
        )
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
        let Some((prefix, f)) = flag.split_at_checked(1) else {
            return Err(parse_err(&line));
        };
        match prefix {
            "O" => opaque = Some(f.to_string()),
            "t" => ttl = Some(f.parse().map_err(parse_err)?),
            "c" => cas = Some(f.parse().map_err(parse_err)?),
            "k" => key = Some(f.to_string()),
            "b" => base64_key = true,
            other => return Err(parse_err(format!("unexpected ma flag: {other}"))),
        }
    }
    if let Some(bytes) = data_len {
        if bytes > MAX_ITEM_SIZE {
            return Err(parse_err("item size too large"));
        }
        let mut buf = vec![0; bytes + 2];
        s.read_exact(&mut buf).await?;
        buf.truncate(bytes);
        number = Some(String::from_utf8_lossy(&buf).parse().map_err(parse_err)?);
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
    if let Some(line) = line.strip_prefix("BUSY ") {
        return Ok(SlabsReassignResult::Busy(line.trim().to_owned()));
    }
    if let Some(line) = line.strip_prefix("BADCLASS ") {
        return Ok(SlabsReassignResult::BadClass(line.trim().to_owned()));
    }
    if let Some(line) = line.strip_prefix("NOSPARE ") {
        return Ok(SlabsReassignResult::NoSpare(line.trim().to_owned()));
    }
    if let Some(line) = line.strip_prefix("NOTFULL ") {
        return Ok(SlabsReassignResult::NotFull(line.trim().to_owned()));
    }
    if let Some(line) = line.strip_prefix("UNSAFE ") {
        return Ok(SlabsReassignResult::UnSafe(line.trim().to_owned()));
    }
    if let Some(line) = line.strip_prefix("SAME ") {
        return Ok(SlabsReassignResult::Same(line.trim().to_owned()));
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
) -> Result<Vec<u8>> {
    validate_key(key)?;
    let mut w = Vec::from(command_name);
    w.push(b' ');
    w.extend(key);
    w.push(b' ');
    write!(&mut w, "{flags} {exptime} {}", data_block.len())?;
    if let Some(x) = cas_unique {
        write!(&mut w, " {x}")?
    }
    if noreply {
        w.extend(b" noreply")
    }
    w.extend(b"\r\n");
    w.extend(data_block);
    w.extend(b"\r\n");
    Ok(w)
}

fn build_retrieval_cmd(
    command_name: &[u8],
    exptime: Option<i64>,
    keys: &[&[u8]],
) -> Result<Vec<u8>> {
    for &k in keys {
        validate_key(k)?
    }
    let mut w = Vec::from(command_name);
    if let Some(x) = exptime {
        write!(&mut w, " {x}")?
    }
    keys.iter().for_each(|&x| {
        w.push(b' ');
        w.extend(x)
    });
    w.extend(b"\r\n");
    Ok(w)
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

fn build_delete_cmd(key: &[u8], noreply: bool) -> Result<Vec<u8>> {
    validate_key(key)?;
    let mut w = Vec::from(b"delete ");
    w.extend(key);
    if noreply {
        w.extend(b" noreply")
    }
    w.extend(b"\r\n");
    Ok(w)
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

fn build_incr_decr_cmd(
    command_name: &[u8],
    key: &[u8],
    value: u64,
    noreply: bool,
) -> Result<Vec<u8>> {
    validate_key(key)?;
    let mut w = Vec::from(command_name);
    w.push(b' ');
    w.extend(key);
    write!(
        &mut w,
        " {value}{}\r\n",
        if noreply { " noreply" } else { "" }
    )?;
    Ok(w)
}

fn build_touch_cmd(key: &[u8], exptime: i64, noreply: bool) -> Result<Vec<u8>> {
    validate_key(key)?;
    let mut w = Vec::from(b"touch ");
    w.extend(key);
    write!(
        &mut w,
        " {exptime}{}\r\n",
        if noreply { " noreply" } else { "" }
    )?;
    Ok(w)
}

fn build_stats_cmd(arg: Option<StatsArg>) -> &'static [u8] {
    match arg {
        Some(a) => match a {
            StatsArg::Settings => b"stats settings\r\n",
            StatsArg::Items => b"stats items\r\n",
            StatsArg::Sizes => b"stats sizes\r\n",
            StatsArg::Slabs => b"stats slabs\r\n",
            StatsArg::Conns => b"stats conns\r\n",
            StatsArg::Proxy => b"stats proxy\r\n",
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

fn build_lru_crawler_sleep_cmd(microseconds: usize) -> Vec<u8> {
    format!("lru_crawler sleep {microseconds}\r\n").into_bytes()
}

fn build_lru_crawler_tocrawl_cmd(arg: u32) -> Vec<u8> {
    format!("lru_crawler tocrawl {arg}\r\n").into_bytes()
}

fn build_lru_crawler_crawl_cmd(arg: LruCrawlerCrawlArg) -> Vec<u8> {
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

fn build_lru_crawler_metadump_cmd(arg: LruCrawlerMetadumpArg) -> Vec<u8> {
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

fn build_lru_crawler_mgdump_cmd(arg: LruCrawlerMgdumpArg) -> Vec<u8> {
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

fn build_me_cmd(key: &[u8]) -> Result<Vec<u8>> {
    validate_key(key)?;
    let mut w = Vec::from(b"me ");
    w.extend(key);
    w.extend(b"\r\n");
    Ok(w)
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
) -> Result<Vec<u8>> {
    validate_key(key)?;
    let mut w = Vec::from(command_name);
    w.push(b' ');
    w.extend(key);
    if let Some(x) = data_block {
        write!(&mut w, " {}", x.len())?;
        w.extend(flags);
        w.extend(b"\r\n");
        w.extend(x);
        w.extend(b"\r\n");
    } else {
        w.extend(flags);
        w.extend(b"\r\n");
    }
    Ok(w)
}

fn build_ms_flags(flags: &[MsFlag]) -> Result<Vec<u8>> {
    let mut w = Vec::new();
    for x in flags {
        match x {
            MsFlag::Base64Key => w.extend(b" b"),
            MsFlag::ReturnCas => w.extend(b" c"),
            MsFlag::CompareCas(token) => write!(&mut w, " C{token}")?,
            MsFlag::NewCas(token) => write!(&mut w, " E{token}")?,
            MsFlag::SetFlags(token) => write!(&mut w, " F{token}")?,
            MsFlag::Invalidate => w.extend(b" I"),
            MsFlag::ReturnKey => w.extend(b" k"),
            MsFlag::Opaque(token) => {
                validate_flag(token.as_bytes())?;
                write!(&mut w, " O{token}")?;
            }
            MsFlag::ReturnSize => w.extend(b" s"),
            MsFlag::Ttl(token) => write!(&mut w, " T{token}")?,
            MsFlag::Mode(token) => match token {
                MsMode::Add => w.extend(b" ME"),
                MsMode::Append => w.extend(b" MA"),
                MsMode::Prepend => w.extend(b" MP"),
                MsMode::Replace => w.extend(b" MR"),
                MsMode::Set => w.extend(b" MS"),
            },
            MsFlag::Autovivify(token) => write!(&mut w, " N{token}")?,
            MsFlag::ProxyHint(token) => {
                validate_flag(token.as_bytes())?;
                write!(&mut w, " P{token}")?;
            }
            MsFlag::LocationHint(token) => {
                validate_flag(token.as_bytes())?;
                write!(&mut w, " L{token}")?;
            }
        }
    }
    Ok(w)
}

fn build_mg_flags(flags: &[MgFlag]) -> Result<Vec<u8>> {
    let mut w = Vec::new();
    for x in flags {
        match x {
            MgFlag::Base64Key => w.extend(b" b"),
            MgFlag::ReturnCas => w.extend(b" c"),
            MgFlag::CheckCas(token) => write!(&mut w, " C{token}")?,
            MgFlag::ReturnFlags => w.extend(b" f"),
            MgFlag::ReturnHit => w.extend(b" h"),
            MgFlag::ReturnKey => w.extend(b" k"),
            MgFlag::ReturnLastAccess => w.extend(b" l"),
            MgFlag::Opaque(token) => {
                validate_flag(token.as_bytes())?;
                write!(&mut w, " O{token}")?;
            }
            MgFlag::ReturnSize => w.extend(b" s"),
            MgFlag::ReturnTtl => w.extend(b" t"),
            MgFlag::UnBump => w.extend(b" u"),
            MgFlag::ReturnValue => w.extend(b" v"),
            MgFlag::NewCas(token) => write!(&mut w, " E{token}")?,
            MgFlag::Autovivify(token) => write!(&mut w, " N{token}")?,
            MgFlag::RecacheTtl(token) => write!(&mut w, " R{token}")?,
            MgFlag::UpdateTtl(token) => write!(&mut w, " T{token}")?,
            MgFlag::ProxyHint(token) => {
                validate_flag(token.as_bytes())?;
                write!(&mut w, " P{token}")?;
            }
            MgFlag::LocationHint(token) => {
                validate_flag(token.as_bytes())?;
                write!(&mut w, " L{token}")?;
            }
        }
    }
    Ok(w)
}

fn build_md_flags(flags: &[MdFlag]) -> Result<Vec<u8>> {
    let mut w = Vec::new();
    for x in flags {
        match x {
            MdFlag::Base64Key => w.extend(b" b"),
            MdFlag::CompareCas(token) => write!(&mut w, " C{token}")?,
            MdFlag::NewCas(token) => write!(&mut w, " E{token}")?,
            MdFlag::Invalidate => w.extend(b" I"),
            MdFlag::ReturnKey => w.extend(b" k"),
            MdFlag::Opaque(token) => {
                validate_flag(token.as_bytes())?;
                write!(&mut w, " O{token}")?;
            }
            MdFlag::UpdateTtl(token) => write!(&mut w, " T{token}")?,
            MdFlag::LeaveKey => w.extend(b" x"),
            MdFlag::ProxyHint(token) => {
                validate_flag(token.as_bytes())?;
                write!(&mut w, " P{token}")?;
            }
            MdFlag::LocationHint(token) => {
                validate_flag(token.as_bytes())?;
                write!(&mut w, " L{token}")?;
            }
        }
    }
    Ok(w)
}

fn build_ma_flags(flags: &[MaFlag]) -> Result<Vec<u8>> {
    let mut w = Vec::new();
    for x in flags {
        match x {
            MaFlag::Base64Key => w.extend(b" b"),
            MaFlag::CompareCas(token) => write!(&mut w, " C{token}")?,
            MaFlag::NewCas(token) => write!(&mut w, " E{token}")?,
            MaFlag::AutoCreate(token) => write!(&mut w, " N{token}")?,
            MaFlag::InitValue(token) => write!(&mut w, " J{token}")?,
            MaFlag::DeltaApply(token) => write!(&mut w, " D{token}")?,
            MaFlag::UpdateTtl(token) => write!(&mut w, " T{token}")?,
            MaFlag::Mode(token) => match token {
                MaMode::Incr => w.extend(b" M+"),
                MaMode::Decr => w.extend(b" M-"),
            },
            MaFlag::Opaque(token) => {
                validate_flag(token.as_bytes())?;
                write!(&mut w, " O{token}")?;
            }
            MaFlag::ReturnTtl => w.extend(b" t"),
            MaFlag::ReturnCas => w.extend(b" c"),
            MaFlag::ReturnValue => w.extend(b" v"),
            MaFlag::ReturnKey => w.extend(b" k"),
            MaFlag::ProxyHint(token) => {
                validate_flag(token.as_bytes())?;
                write!(&mut w, " P{token}")?;
            }
            MaFlag::LocationHint(token) => {
                validate_flag(token.as_bytes())?;
                write!(&mut w, " L{token}")?;
            }
        }
    }
    Ok(w)
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

fn build_refresh_certs_cmd() -> &'static [u8] {
    b"refresh_certs\r\n"
}

async fn udp_send_cmd(s: &UdpSocket, r: &mut u16, cmd: &[u8]) -> Result<()> {
    *r = r.wrapping_add(1);
    let mut msg = Vec::with_capacity(8 + cmd.len());
    msg.extend(r.to_be_bytes());
    msg.extend([0, 0, 0, 1, 0, 0]);
    msg.extend(cmd);
    s.send(&msg).await?;
    Ok(())
}

async fn udp_recv_rp(s: &UdpSocket, r: &u16) -> Result<Vec<u8>> {
    let mut chunks = Vec::new();
    let mut received = 0;
    let mut buf = [0; 1400];
    loop {
        let n = s.recv(&mut buf).await?;
        if n < 8 {
            return Err(MemcachedError::Unexpected("Invalid UDP header".to_string()));
        }
        let request_id = u16::from_be_bytes([buf[0], buf[1]]);
        if *r != request_id {
            continue;
        }
        let sequence_number = u16::from_be_bytes([buf[2], buf[3]]) as usize;
        let total_number_datagrams = u16::from_be_bytes([buf[4], buf[5]]) as usize;
        if total_number_datagrams == 0
            || total_number_datagrams > 1024
            || sequence_number >= total_number_datagrams
        {
            return Err(MemcachedError::Unexpected(
                "Invalid UDP datagram sequence".to_owned(),
            ));
        }
        if chunks.is_empty() {
            chunks.resize(total_number_datagrams, None);
        } else if chunks.len() != total_number_datagrams {
            return Err(MemcachedError::Unexpected(
                "Inconsistent UDP datagram count".to_owned(),
            ));
        }
        if chunks[sequence_number].is_none() {
            chunks[sequence_number] = Some(buf[8..n].to_vec());
            received += 1;
            if received == total_number_datagrams {
                break;
            }
        }
    }
    Ok(chunks.into_iter().flatten().flatten().collect())
}

async fn version_cmd_udp(s: &UdpSocket, r: &mut u16) -> Result<String> {
    udp_send_cmd(s, r, build_version_cmd()).await?;
    parse_version_rp(&mut Cursor::new(udp_recv_rp(s, r).await?)).await
}

pub async fn version_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> Result<String> {
    s.write_all(build_version_cmd()).await?;
    parse_version_rp(s).await
}

async fn quit_cmd_udp(s: &UdpSocket, r: &mut u16) -> Result<()> {
    udp_send_cmd(s, r, build_quit_cmd()).await
}

pub async fn quit_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> Result<()> {
    Ok(s.write_all(build_quit_cmd()).await?)
}

async fn shutdown_cmd_udp(s: &UdpSocket, r: &mut u16, graceful: bool) -> Result<()> {
    udp_send_cmd(s, r, build_shutdown_cmd(graceful)).await
}

pub async fn shutdown_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    graceful: bool,
) -> Result<()> {
    Ok(s.write_all(build_shutdown_cmd(graceful)).await?)
}

async fn cache_memlimit_cmd_udp(
    s: &UdpSocket,
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

pub async fn cache_memlimit_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    limit: usize,
    noreply: bool,
) -> Result<()> {
    s.write_all(&build_cache_memlimit_cmd(limit, noreply))
        .await?;
    parse_ok_rp(s, noreply).await
}

async fn flush_all_cmd_udp(
    s: &UdpSocket,
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

pub async fn flush_all_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    exptime: Option<i64>,
    noreply: bool,
) -> Result<()> {
    s.write_all(&build_flush_all_cmd(exptime, noreply)).await?;
    parse_ok_rp(s, noreply).await
}

async fn storage_cmd_udp(
    s: &UdpSocket,
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
        )?,
    )
    .await?;
    if noreply {
        Ok(StorageResult::NoReply)
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
    )?)
    .await?;
    parse_storage_rp(s, noreply).await
}

async fn delete_cmd_udp(
    s: &UdpSocket,
    r: &mut u16,
    key: &[u8],
    noreply: bool,
) -> Result<DeleteResult> {
    udp_send_cmd(s, r, &build_delete_cmd(key, noreply)?).await?;
    if noreply {
        Ok(DeleteResult::NoReply)
    } else {
        parse_delete_rp(&mut Cursor::new(udp_recv_rp(s, r).await?), noreply).await
    }
}

pub async fn delete_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    key: &[u8],
    noreply: bool,
) -> Result<DeleteResult> {
    s.write_all(&build_delete_cmd(key, noreply)?).await?;
    parse_delete_rp(s, noreply).await
}

pub async fn auth_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    username: &[u8],
    password: &[u8],
) -> Result<()> {
    s.write_all(&build_auth_cmd(username, password)).await?;
    parse_auth_rp(s).await
}

async fn incr_decr_cmd_udp(
    s: &UdpSocket,
    r: &mut u16,
    command_name: &[u8],
    key: &[u8],
    value: u64,
    noreply: bool,
) -> Result<IncrDecrResult> {
    udp_send_cmd(
        s,
        r,
        &build_incr_decr_cmd(command_name, key, value, noreply)?,
    )
    .await?;
    if noreply {
        Ok(IncrDecrResult::NoReply)
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
) -> Result<IncrDecrResult> {
    s.write_all(&build_incr_decr_cmd(command_name, key, value, noreply)?)
        .await?;
    parse_incr_decr_rp(s, noreply).await
}

async fn touch_cmd_udp(
    s: &UdpSocket,
    r: &mut u16,
    key: &[u8],
    exptime: i64,
    noreply: bool,
) -> Result<TouchResult> {
    udp_send_cmd(s, r, &build_touch_cmd(key, exptime, noreply)?).await?;
    if noreply {
        Ok(TouchResult::NoReply)
    } else {
        parse_touch_rp(&mut Cursor::new(udp_recv_rp(s, r).await?), noreply).await
    }
}

pub async fn touch_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    key: &[u8],
    exptime: i64,
    noreply: bool,
) -> Result<TouchResult> {
    s.write_all(&build_touch_cmd(key, exptime, noreply)?)
        .await?;
    parse_touch_rp(s, noreply).await
}

async fn retrieval_cmd_udp(
    s: &UdpSocket,
    r: &mut u16,
    command_name: &[u8],
    exptime: Option<i64>,
    keys: &[&[u8]],
) -> Result<Vec<Item>> {
    udp_send_cmd(s, r, &build_retrieval_cmd(command_name, exptime, keys)?).await?;
    parse_retrieval_rp(&mut Cursor::new(udp_recv_rp(s, r).await?)).await
}

pub async fn retrieval_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    command_name: &[u8],
    exptime: Option<i64>,
    keys: &[&[u8]],
) -> Result<Vec<Item>> {
    s.write_all(&build_retrieval_cmd(command_name, exptime, keys)?)
        .await?;
    parse_retrieval_rp(s).await
}

async fn stats_cmd_udp(
    s: &UdpSocket,
    r: &mut u16,
    arg: Option<StatsArg>,
) -> Result<HashMap<String, String>> {
    udp_send_cmd(s, r, build_stats_cmd(arg)).await?;
    parse_stats_rp(&mut Cursor::new(udp_recv_rp(s, r).await?)).await
}

pub async fn stats_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    arg: Option<StatsArg>,
) -> Result<HashMap<String, String>> {
    s.write_all(build_stats_cmd(arg)).await?;
    parse_stats_rp(s).await
}

async fn slabs_automove_cmd_udp(s: &UdpSocket, r: &mut u16, arg: SlabsAutomoveArg) -> Result<()> {
    udp_send_cmd(s, r, build_slabs_automove_cmd(arg)).await?;
    parse_ok_rp(&mut Cursor::new(udp_recv_rp(s, r).await?), false).await
}

pub async fn slabs_automove_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    arg: SlabsAutomoveArg,
) -> Result<()> {
    s.write_all(build_slabs_automove_cmd(arg)).await?;
    parse_ok_rp(s, false).await
}

async fn lru_crawler_cmd_udp(s: &UdpSocket, r: &mut u16, arg: LruCrawlerArg) -> Result<()> {
    udp_send_cmd(s, r, build_lru_crawler_cmd(arg)).await?;
    parse_ok_rp(&mut Cursor::new(udp_recv_rp(s, r).await?), false).await
}

pub async fn lru_crawler_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    arg: LruCrawlerArg,
) -> Result<()> {
    s.write_all(build_lru_crawler_cmd(arg)).await?;
    parse_ok_rp(s, false).await
}

async fn lru_crawler_sleep_cmd_udp(s: &UdpSocket, r: &mut u16, microseconds: usize) -> Result<()> {
    udp_send_cmd(s, r, &build_lru_crawler_sleep_cmd(microseconds)).await?;
    parse_ok_rp(&mut Cursor::new(udp_recv_rp(s, r).await?), false).await
}

pub async fn lru_crawler_sleep_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    microseconds: usize,
) -> Result<()> {
    s.write_all(&build_lru_crawler_sleep_cmd(microseconds))
        .await?;
    parse_ok_rp(s, false).await
}

async fn lru_crawler_tocrawl_cmd_udp(s: &UdpSocket, r: &mut u16, arg: u32) -> Result<()> {
    udp_send_cmd(s, r, &build_lru_crawler_tocrawl_cmd(arg)).await?;
    parse_ok_rp(&mut Cursor::new(udp_recv_rp(s, r).await?), false).await
}

pub async fn lru_crawler_tocrawl_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    arg: u32,
) -> Result<()> {
    s.write_all(&build_lru_crawler_tocrawl_cmd(arg)).await?;
    parse_ok_rp(s, false).await
}

async fn lru_crawler_crawl_cmd_udp(
    s: &UdpSocket,
    r: &mut u16,
    arg: LruCrawlerCrawlArg<'_>,
) -> Result<LruCrawlerResult> {
    udp_send_cmd(s, r, &build_lru_crawler_crawl_cmd(arg)).await?;
    parse_lru_crawler_rp(&mut Cursor::new(udp_recv_rp(s, r).await?)).await
}

pub async fn lru_crawler_crawl_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    arg: LruCrawlerCrawlArg<'_>,
) -> Result<LruCrawlerResult> {
    s.write_all(&build_lru_crawler_crawl_cmd(arg)).await?;
    parse_lru_crawler_rp(s).await
}

async fn slabs_reassign_cmd_udp(
    s: &UdpSocket,
    r: &mut u16,
    source_class: isize,
    dest_class: isize,
) -> Result<SlabsReassignResult> {
    udp_send_cmd(s, r, &build_slabs_reassign_cmd(source_class, dest_class)).await?;
    parse_slabs_reassign_rp(&mut Cursor::new(udp_recv_rp(s, r).await?)).await
}

pub async fn slabs_reassign_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    source_class: isize,
    dest_class: isize,
) -> Result<SlabsReassignResult> {
    s.write_all(&build_slabs_reassign_cmd(source_class, dest_class))
        .await?;
    parse_slabs_reassign_rp(s).await
}

pub async fn lru_crawler_metadump_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    arg: LruCrawlerMetadumpArg<'_>,
) -> Result<LruCrawlerDumpResult> {
    s.write_all(&build_lru_crawler_metadump_cmd(arg)).await?;
    parse_lru_crawler_metadump_rp(s).await
}

pub async fn lru_crawler_mgdump_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    arg: LruCrawlerMgdumpArg<'_>,
) -> Result<LruCrawlerDumpResult> {
    s.write_all(&build_lru_crawler_mgdump_cmd(arg)).await?;
    parse_lru_crawler_mgdump_rp(s).await
}

async fn mn_cmd_udp(s: &UdpSocket, r: &mut u16) -> Result<()> {
    udp_send_cmd(s, r, build_mn_cmd()).await?;
    parse_mn_rp(&mut Cursor::new(udp_recv_rp(s, r).await?)).await
}

pub async fn mn_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> Result<()> {
    s.write_all(build_mn_cmd()).await?;
    parse_mn_rp(s).await
}

async fn me_cmd_udp(s: &UdpSocket, r: &mut u16, key: &[u8]) -> Result<Option<String>> {
    udp_send_cmd(s, r, &build_me_cmd(key)?).await?;
    parse_me_rp(&mut Cursor::new(udp_recv_rp(s, r).await?)).await
}

pub async fn me_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    key: &[u8],
) -> Result<Option<String>> {
    s.write_all(&build_me_cmd(key)?).await?;
    parse_me_rp(s).await
}

pub async fn execute_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    cmds: &[(Vec<u8>, PipelineExpect)],
) -> Result<Vec<Result<PipelineResponse>>> {
    let capacity = cmds.iter().map(|x| x.0.len()).sum();
    let mut buf = Vec::with_capacity(capacity);
    cmds.iter().for_each(|x| buf.extend(&x.0));
    s.write_all(&buf).await?;
    let mut result = Vec::with_capacity(cmds.len());
    for x in cmds {
        match x.1 {
            PipelineExpect::RetrievalOne => result.push(
                parse_retrieval_rp(s)
                    .await
                    .map(|mut x| PipelineResponse::OptionItem(x.pop())),
            ),
            PipelineExpect::RetrievalMulti => {
                result.push(parse_retrieval_rp(s).await.map(PipelineResponse::VecItem))
            }
            PipelineExpect::Auth => result.push(parse_auth_rp(s).await.map(PipelineResponse::Unit)),
            PipelineExpect::Storage { noreply } => result.push(
                parse_storage_rp(s, noreply)
                    .await
                    .map(PipelineResponse::StorageResult),
            ),
            PipelineExpect::Version => {
                result.push(parse_version_rp(s).await.map(PipelineResponse::String))
            }
            PipelineExpect::Delete { noreply } => result.push(
                parse_delete_rp(s, noreply)
                    .await
                    .map(PipelineResponse::DeleteResult),
            ),
            PipelineExpect::IncrDecr { noreply } => result.push(
                parse_incr_decr_rp(s, noreply)
                    .await
                    .map(PipelineResponse::IncrDecrResult),
            ),
            PipelineExpect::Touch { noreply } => result.push(
                parse_touch_rp(s, noreply)
                    .await
                    .map(PipelineResponse::TouchResult),
            ),
            PipelineExpect::Ok { noreply } => {
                result.push(parse_ok_rp(s, noreply).await.map(PipelineResponse::Unit))
            }
            PipelineExpect::LruCrawler => result.push(
                parse_lru_crawler_rp(s)
                    .await
                    .map(PipelineResponse::LruCrawlerResult),
            ),
            PipelineExpect::SlabsReassign => result.push(
                parse_slabs_reassign_rp(s)
                    .await
                    .map(PipelineResponse::SlabsReassignResult),
            ),
            PipelineExpect::Mn => result.push(parse_mn_rp(s).await.map(PipelineResponse::Unit)),
            PipelineExpect::Stats => {
                result.push(parse_stats_rp(s).await.map(PipelineResponse::HashMap))
            }
            PipelineExpect::LruCrawlerMetaDump => result.push(
                parse_lru_crawler_metadump_rp(s)
                    .await
                    .map(PipelineResponse::LruCrawlerDumpResult),
            ),
            PipelineExpect::LruCrawlerMgDump => result.push(
                parse_lru_crawler_mgdump_rp(s)
                    .await
                    .map(PipelineResponse::LruCrawlerDumpResult),
            ),
            PipelineExpect::MetaGet => {
                result.push(parse_mg_rp(s).await.map(PipelineResponse::MetaGet))
            }
            PipelineExpect::MetaSet => {
                result.push(parse_ms_rp(s).await.map(PipelineResponse::MetaSet))
            }
            PipelineExpect::MetaDelete => {
                result.push(parse_md_rp(s).await.map(PipelineResponse::MetaDelete))
            }
            PipelineExpect::MetaArithmetic => {
                result.push(parse_ma_rp(s).await.map(PipelineResponse::MetaArithmetic))
            }
            PipelineExpect::Me => {
                result.push(parse_me_rp(s).await.map(PipelineResponse::OptionString))
            }
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
    s: &UdpSocket,
    r: &mut u16,
    key: &[u8],
    flags: &[MsFlag],
    data_block: &[u8],
) -> Result<MsItem> {
    udp_send_cmd(
        s,
        r,
        &build_mc_cmd(b"ms", key, &build_ms_flags(flags)?, Some(data_block))?,
    )
    .await?;
    parse_ms_rp(&mut Cursor::new(udp_recv_rp(s, r).await?)).await
}

pub async fn ms_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    key: &[u8],
    flags: &[MsFlag],
    data_block: &[u8],
) -> Result<MsItem> {
    s.write_all(&build_mc_cmd(
        b"ms",
        key,
        &build_ms_flags(flags)?,
        Some(data_block),
    )?)
    .await?;
    parse_ms_rp(s).await
}

async fn mg_cmd_udp(s: &UdpSocket, r: &mut u16, key: &[u8], flags: &[MgFlag]) -> Result<MgItem> {
    udp_send_cmd(
        s,
        r,
        &build_mc_cmd(b"mg", key, &build_mg_flags(flags)?, None)?,
    )
    .await?;
    parse_mg_rp(&mut Cursor::new(udp_recv_rp(s, r).await?)).await
}

pub async fn mg_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    key: &[u8],
    flags: &[MgFlag],
) -> Result<MgItem> {
    s.write_all(&build_mc_cmd(b"mg", key, &build_mg_flags(flags)?, None)?)
        .await?;
    parse_mg_rp(s).await
}

async fn md_cmd_udp(s: &UdpSocket, r: &mut u16, key: &[u8], flags: &[MdFlag]) -> Result<MdItem> {
    udp_send_cmd(
        s,
        r,
        &build_mc_cmd(b"md", key, &build_md_flags(flags)?, None)?,
    )
    .await?;
    parse_md_rp(&mut Cursor::new(udp_recv_rp(s, r).await?)).await
}

pub async fn md_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    key: &[u8],
    flags: &[MdFlag],
) -> Result<MdItem> {
    s.write_all(&build_mc_cmd(b"md", key, &build_md_flags(flags)?, None)?)
        .await?;
    parse_md_rp(s).await
}

async fn ma_cmd_udp(s: &UdpSocket, r: &mut u16, key: &[u8], flags: &[MaFlag]) -> Result<MaItem> {
    udp_send_cmd(
        s,
        r,
        &build_mc_cmd(b"ma", key, &build_ma_flags(flags)?, None)?,
    )
    .await?;
    parse_ma_rp(&mut Cursor::new(udp_recv_rp(s, r).await?)).await
}

pub async fn ma_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    key: &[u8],
    flags: &[MaFlag],
) -> Result<MaItem> {
    s.write_all(&build_mc_cmd(b"ma", key, &build_ma_flags(flags)?, None)?)
        .await?;
    parse_ma_rp(s).await
}

async fn lru_cmd_udp(s: &UdpSocket, r: &mut u16, arg: LruArg) -> Result<()> {
    udp_send_cmd(s, r, &build_lru_cmd(arg)).await?;
    parse_ok_rp(&mut Cursor::new(udp_recv_rp(s, r).await?), false).await
}

pub async fn lru_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S, arg: LruArg) -> Result<()> {
    s.write_all(&build_lru_cmd(arg)).await?;
    parse_ok_rp(s, false).await
}

async fn verbosity_cmd_udp(
    s: &UdpSocket,
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

pub async fn verbosity_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(
    s: &mut S,
    arg: VerbosityArg,
    noreply: bool,
) -> Result<()> {
    s.write_all(&build_verbosity_cmd(arg, noreply)).await?;
    parse_ok_rp(s, noreply).await
}

async fn refresh_certs_cmd_udp(s: &UdpSocket, r: &mut u16) -> Result<()> {
    udp_send_cmd(s, r, build_refresh_certs_cmd()).await?;
    parse_ok_rp(&mut Cursor::new(udp_recv_rp(s, r).await?), false).await
}

pub async fn refresh_certs_cmd<S: AsyncBufRead + AsyncWrite + Unpin>(s: &mut S) -> Result<()> {
    s.write_all(build_refresh_certs_cmd()).await?;
    parse_ok_rp(s, false).await
}

#[cfg(any(feature = "smol-rustls-tls", feature = "tokio-rustls-tls"))]
async fn get_tls_stream_from_rustls(
    hostname: &str,
    port: u16,
    tls_config: TlsConfig<'_>,
) -> Result<TlsStream<TcpStream>> {
    use std::sync::Arc;

    let builder = rustls::ClientConfig::builder();
    let builder = match tls_config.verification {
        ServerVerification::Verify(store) => {
            let mut root_store = rustls::RootCertStore::empty();
            if matches!(
                store,
                TrustStore::SystemOnly | TrustStore::SystemAndCustom(_)
            ) {
                #[cfg(feature = "webpki-roots-tls")]
                root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
                #[cfg(feature = "rustls-native-certs-tls")]
                {
                    let result = rustls_native_certs::load_native_certs();
                    if result.certs.is_empty()
                        && let Some(err) = result.errors.into_iter().next()
                    {
                        return Err(RustlsTlsError::from(err).into());
                    }
                    for cert in result.certs {
                        root_store.add(cert).map_err(RustlsTlsError::from)?;
                    }
                }
            }
            if let TrustStore::CustomOnly(path) | TrustStore::SystemAndCustom(path) = store {
                for cert_der in rustls::pki_types::CertificateDer::pem_file_iter(path)
                    .map_err(RustlsTlsError::from)?
                {
                    root_store
                        .add(cert_der.map_err(RustlsTlsError::from)?)
                        .map_err(RustlsTlsError::from)?;
                }
            }
            builder.with_root_certificates(root_store)
        }
        #[cfg(feature = "insecure-tls")]
        ServerVerification::DangerAcceptAll => {
            let provider = rustls::crypto::CryptoProvider::get_default().ok_or(
                MemcachedError::ClientConfig("CryptoProvider not found".to_owned()),
            )?;
            builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(danger::NoCertificateVerification::new(
                    (**provider).clone(),
                )))
        }
    };
    let config = if let Some(c) = tls_config.identity {
        let mut cert_chain = Vec::new();
        for c in rustls::pki_types::CertificateDer::pem_file_iter(c.cert_chain_path)
            .map_err(RustlsTlsError::from)?
        {
            cert_chain.push(c.map_err(RustlsTlsError::from)?);
        }
        let key_der = rustls::pki_types::PrivateKeyDer::from_pem_file(c.private_key_path)
            .map_err(RustlsTlsError::from)?;
        builder
            .with_client_auth_cert(cert_chain, key_der)
            .map_err(RustlsTlsError::from)?
    } else {
        builder.with_no_client_auth()
    };
    let domain = hostname
        .to_owned()
        .try_into()
        .map_err(RustlsTlsError::from)?;
    let connector = TlsConnector::from(Arc::new(config));
    let tcp_stream = TcpStream::connect((hostname, port)).await?;
    tcp_stream.set_nodelay(true)?;
    Ok(connector.connect(domain, tcp_stream).await?)
}

#[cfg(all(
    any(feature = "smol-native-tls-tls", feature = "tokio-native-tls-tls"),
    not(any(feature = "smol-rustls-tls", feature = "tokio-rustls-tls"))
))]
async fn get_tls_stream_from_native_tls(
    hostname: &str,
    port: u16,
    tls_config: TlsConfig<'_>,
) -> Result<TlsStream<TcpStream>> {
    use std::fs;
    let use_sni = hostname.parse::<std::net::IpAddr>().is_err();
    let mut builder = native_tls::TlsConnector::builder();
    builder.use_sni(use_sni);
    match tls_config.verification {
        ServerVerification::Verify(s) => {
            if matches!(s, TrustStore::CustomOnly(_)) {
                builder.disable_built_in_roots(true);
            }
            if let TrustStore::CustomOnly(i) | TrustStore::SystemAndCustom(i) = s {
                for cert in native_tls::Certificate::stack_from_pem(&fs::read(i)?)? {
                    builder.add_root_certificate(cert);
                }
            }
        }
        #[cfg(feature = "insecure-tls")]
        ServerVerification::DangerAcceptAll => {
            builder
                .danger_accept_invalid_certs(true)
                .danger_accept_invalid_hostnames(true);
        }
    }
    if let Some(c) = tls_config.identity {
        builder.identity(native_tls::Identity::from_pkcs8(
            &fs::read(c.cert_chain_path)?,
            &fs::read(c.private_key_path)?,
        )?);
    }
    #[cfg(feature = "smol-native-tls-tls")]
    let connector = TlsConnector::from(builder);
    #[cfg(all(feature = "tokio-native-tls-tls", not(feature = "smol-native-tls-tls")))]
    let connector = TlsConnector::from(builder.build()?);
    let tcp_stream = TcpStream::connect((hostname, port)).await?;
    tcp_stream.set_nodelay(true)?;
    Ok(connector.connect(hostname, tcp_stream).await?)
}

pub enum Connection {
    Tcp(BufReader<TcpStream>),
    #[cfg(unix)]
    Unix(BufReader<UnixStream>),
    Udp(UdpSocket, u16),
    #[cfg(any(
        feature = "smol-rustls-tls",
        feature = "tokio-rustls-tls",
        feature = "smol-native-tls-tls",
        feature = "tokio-native-tls-tls"
    ))]
    Tls(Box<BufReader<TlsStream<TcpStream>>>),
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
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub async fn default() -> Result<Self> {
        let stream = TcpStream::connect("127.0.0.1:11211").await?;
        stream.set_nodelay(true)?;
        Ok(Connection::Tcp(BufReader::new(stream)))
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
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub async fn tcp_connect(addr: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(Connection::Tcp(BufReader::new(stream)))
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
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
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
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub async fn udp_connect(bind_addr: &str, connect_addr: &str) -> Result<Self> {
        let s = UdpSocket::bind(bind_addr).await?;
        s.connect(connect_addr).await?;
        Ok(Connection::Udp(s, fastrand::u16(..)))
    }

    /// Connects to a TLS-enabled Memcached server.
    ///
    /// `tls_config` controls certificate verification and optional mutual TLS.
    /// [`TlsConfig::new`] verifies the server against the system trust store;
    /// use [`TlsConfig::with_custom_ca_only`] or
    /// [`TlsConfig::with_additional_ca`] to trust a private CA, and
    /// [`TlsConfig::with_client_identity`] to present a client certificate.
    ///
    /// # rustls CryptoProvider
    ///
    /// With a rustls backend, [`mcmc-rs`](crate) does not select or install a
    /// `rustls::crypto::CryptoProvider`. Enable a provider feature on the
    /// `rustls` dependency or call `CryptoProvider::install_default` before
    /// the first TLS connection. If no provider is available, rustls panics on
    /// the first TLS connection. This becomes mandatory in rustls 0.24. The
    /// native-tls backends do not use a rustls `CryptoProvider`.
    ///
    /// # Example
    ///
    /// The example exercises every supported verification mode, both with and
    /// without a client identity, against the test servers. Port 11218 requires
    /// SASL authentication but no client certificate, while port 11216 requires
    /// a client certificate. `TlsConfig::new()` only trusts the system roots, so
    /// it cannot verify the private test CA.
    ///
    /// ```
    /// # use mcmc_rs::{Connection, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let ca = "sets/ca.crt";
    /// let cert = "sets/client.crt";
    /// let key = "sets/client.key";
    ///
    /// let mut cases = vec![
    ///     // Trust only the test CA.
    ///     (11218, TlsConfig::with_custom_ca_only(ca), true),
    ///     (
    ///         11216,
    ///         TlsConfig::with_custom_ca_only(ca).with_client_identity(cert, key),
    ///         false,
    ///     ),
    ///     // Trust the system roots and the test CA.
    ///     (11218, TlsConfig::with_additional_ca(ca), true),
    ///     (
    ///         11216,
    ///         TlsConfig::with_additional_ca(ca).with_client_identity(cert, key),
    ///         false,
    ///     ),
    /// ];
    ///
    /// // `danger_accept_all` is available only with the `insecure-tls` feature.
    /// cases.extend([
    ///     (11218, TlsConfig::danger_accept_all(), true),
    ///     (
    ///         11216,
    ///         TlsConfig::danger_accept_all().with_client_identity(cert, key),
    ///         false,
    ///     ),
    /// ]);
    ///
    /// for (port, config, needs_auth) in cases {
    ///     let mut conn = Connection::tls_connect("localhost", port, config).await?;
    ///     if needs_auth {
    ///         conn.auth(b"a", b"a").await?;
    ///     }
    ///     assert!(!conn.version().await?.is_empty());
    /// }
    ///
    /// // The private test CA is not in the system trust store.
    /// assert!(
    ///     Connection::tls_connect("localhost", 11218, TlsConfig::new())
    ///         .await
    ///         .is_err()
    /// );
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    #[cfg(any(
        feature = "smol-rustls-tls",
        feature = "tokio-rustls-tls",
        feature = "smol-native-tls-tls",
        feature = "tokio-native-tls-tls"
    ))]
    pub async fn tls_connect(hostname: &str, port: u16, tls_config: TlsConfig<'_>) -> Result<Self> {
        #[cfg(any(feature = "smol-rustls-tls", feature = "tokio-rustls-tls"))]
        let tls_stream = { get_tls_stream_from_rustls(hostname, port, tls_config).await? };
        #[cfg(all(
            any(feature = "smol-native-tls-tls", feature = "tokio-native-tls-tls"),
            not(any(feature = "smol-rustls-tls", feature = "tokio-rustls-tls"))
        ))]
        let tls_stream = { get_tls_stream_from_native_tls(hostname, port, tls_config).await? };
        Ok(Connection::Tls(Box::new(BufReader::new(tls_stream))))
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect(
    ///         "localhost",
    ///         11216,
    ///         TlsConfig::with_additional_ca("sets/ca.crt")
    ///             .with_client_identity("sets/client.crt", "sets/client.key"),
    ///     )
    ///     .await?,
    /// ] {
    ///     let version = c.version().await?;
    ///     assert!(!version.is_empty());
    ///     assert!(version.split('.').all(|part| part.parse::<u64>().is_ok()));
    /// }
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub async fn version(&mut self) -> Result<String> {
        match self {
            Connection::Tcp(s) => version_cmd(s).await,
            #[cfg(unix)]
            Connection::Unix(s) => version_cmd(s).await,
            Connection::Udp(s, r) => version_cmd_udp(s, r).await,
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
            Connection::Tls(s) => version_cmd(s).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect(
    ///         "localhost",
    ///         11216,
    ///         TlsConfig::with_additional_ca("sets/ca.crt")
    ///             .with_client_identity("sets/client.crt", "sets/client.key"),
    ///     )
    ///     .await?,
    /// ] {
    ///     assert!(c.quit().await.is_ok());
    /// }
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub async fn quit(mut self) -> Result<()> {
        match &mut self {
            Connection::Tcp(s) => quit_cmd(s).await,
            #[cfg(unix)]
            Connection::Unix(s) => quit_cmd(s).await,
            Connection::Udp(s, r) => quit_cmd_udp(s, r).await,
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
            Connection::Tls(s) => quit_cmd(s).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::tcp_connect("127.0.0.1:11213").await?,
    ///     Connection::unix_connect("/tmp/memcached1.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11215").await?,
    ///     Connection::tls_connect(
    ///         "localhost",
    ///         11217,
    ///         TlsConfig::with_additional_ca("sets/ca.crt"),
    ///     )
    ///     .await?,
    /// ] {
    ///     assert_eq!(c.shutdown(true).await?, ());
    /// }
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub async fn shutdown(mut self, graceful: bool) -> Result<()> {
        match &mut self {
            Connection::Tcp(s) => shutdown_cmd(s, graceful).await,
            #[cfg(unix)]
            Connection::Unix(s) => shutdown_cmd(s, graceful).await,
            Connection::Udp(s, r) => shutdown_cmd_udp(s, r, graceful).await,
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
            Connection::Tls(s) => shutdown_cmd(s, graceful).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect(
    ///         "localhost",
    ///         11216,
    ///         TlsConfig::with_additional_ca("sets/ca.crt")
    ///             .with_client_identity("sets/client.crt", "sets/client.key"),
    ///     )
    ///     .await?,
    /// ] {
    ///     assert_eq!(c.cache_memlimit(64, false).await?, ());
    /// }
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub async fn cache_memlimit(&mut self, limit: usize, noreply: bool) -> Result<()> {
        match self {
            Connection::Tcp(s) => cache_memlimit_cmd(s, limit, noreply).await,
            #[cfg(unix)]
            Connection::Unix(s) => cache_memlimit_cmd(s, limit, noreply).await,
            Connection::Udp(s, r) => cache_memlimit_cmd_udp(s, r, limit, noreply).await,
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
            Connection::Tls(s) => cache_memlimit_cmd(s, limit, noreply).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, StorageResult, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect(
    ///         "localhost",
    ///         11216,
    ///         TlsConfig::with_additional_ca("sets/ca.crt")
    ///             .with_client_identity("sets/client.crt", "sets/client.key"),
    ///     )
    ///     .await?,
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
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub async fn flush_all(&mut self, exptime: Option<i64>, noreply: bool) -> Result<()> {
        match self {
            Connection::Tcp(s) => flush_all_cmd(s, exptime, noreply).await,
            #[cfg(unix)]
            Connection::Unix(s) => flush_all_cmd(s, exptime, noreply).await,
            Connection::Udp(s, r) => flush_all_cmd_udp(s, r, exptime, noreply).await,
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
            Connection::Tls(s) => flush_all_cmd(s, exptime, noreply).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, Item, StorageResult, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect(
    ///         "localhost",
    ///         11216,
    ///         TlsConfig::with_additional_ca("sets/ca.crt")
    ///             .with_client_identity("sets/client.crt", "sets/client.key"),
    ///     )
    ///     .await?,
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
    ///     assert_eq!(
    ///         c.set(b"doc_conn_set_noreply", 7, 0, true, b"value").await?,
    ///         StorageResult::NoReply
    ///     );
    /// }
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
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
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
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
    /// # use mcmc_rs::{Connection, StorageResult, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect(
    ///         "localhost",
    ///         11216,
    ///         TlsConfig::with_additional_ca("sets/ca.crt")
    ///             .with_client_identity("sets/client.crt", "sets/client.key"),
    ///     )
    ///     .await?,
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
    ///     assert_eq!(
    ///         c.add(b"doc_conn_add_noreply", 0, 0, true, b"value").await?,
    ///         StorageResult::NoReply
    ///     );
    /// }
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
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
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
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
    /// # use mcmc_rs::{Connection, StorageResult, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect(
    ///         "localhost",
    ///         11216,
    ///         TlsConfig::with_additional_ca("sets/ca.crt")
    ///             .with_client_identity("sets/client.crt", "sets/client.key"),
    ///     )
    ///     .await?,
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
    ///     assert_eq!(
    ///         c.replace(b"doc_conn_replace", 0, 0, true, b"noreply")
    ///             .await?,
    ///         StorageResult::NoReply
    ///     );
    /// }
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
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
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
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
    /// # use mcmc_rs::{Connection, StorageResult, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect(
    ///         "localhost",
    ///         11216,
    ///         TlsConfig::with_additional_ca("sets/ca.crt")
    ///             .with_client_identity("sets/client.crt", "sets/client.key"),
    ///     )
    ///     .await?,
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
    ///     assert_eq!(
    ///         c.append(b"doc_conn_append", 0, 0, true, b"-noreply")
    ///             .await?,
    ///         StorageResult::NoReply
    ///     );
    /// }
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
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
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
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
    /// # use mcmc_rs::{Connection, StorageResult, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect(
    ///         "localhost",
    ///         11216,
    ///         TlsConfig::with_additional_ca("sets/ca.crt")
    ///             .with_client_identity("sets/client.crt", "sets/client.key"),
    ///     )
    ///     .await?,
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
    ///     assert_eq!(
    ///         c.prepend(b"doc_conn_prepend", 0, 0, true, b"noreply-")
    ///             .await?,
    ///         StorageResult::NoReply
    ///     );
    /// }
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
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
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
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
    /// # use mcmc_rs::{Connection, StorageResult, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect(
    ///         "localhost",
    ///         11216,
    ///         TlsConfig::with_additional_ca("sets/ca.crt")
    ///             .with_client_identity("sets/client.crt", "sets/client.key"),
    ///     )
    ///     .await?,
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
    ///     let cas = c.gets(b"doc_conn_cas").await?.unwrap().cas_unique.unwrap();
    ///     assert_eq!(
    ///         c.cas(b"doc_conn_cas", 0, 0, cas, true, b"noreply").await?,
    ///         StorageResult::NoReply
    ///     );
    /// }
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
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
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
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
    /// # use mcmc_rs::{Connection, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::tcp_connect("127.0.0.1:11212").await?,
    ///     Connection::unix_connect("/tmp/memcached2.sock").await?,
    ///     Connection::tls_connect(
    ///         "localhost",
    ///         11218,
    ///         TlsConfig::with_additional_ca("sets/ca.crt"),
    ///     )
    ///     .await?,
    /// ] {
    ///     c.auth(b"a", b"a").await?;
    ///     assert!(!c.version().await?.is_empty());
    /// }
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
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
                panic!("Cannot enable UDP while using binary SASL authentication.")
            }
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
            Connection::Tls(s) => auth_cmd(s, username.as_ref(), password.as_ref()).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, DeleteResult, StorageResult, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect(
    ///         "localhost",
    ///         11216,
    ///         TlsConfig::with_additional_ca("sets/ca.crt")
    ///             .with_client_identity("sets/client.crt", "sets/client.key"),
    ///     )
    ///     .await?,
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
    ///     assert_eq!(
    ///         c.delete(b"doc_conn_delete", true).await?,
    ///         DeleteResult::NoReply
    ///     );
    /// }
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub async fn delete(&mut self, key: impl AsRef<[u8]>, noreply: bool) -> Result<DeleteResult> {
        match self {
            Connection::Tcp(s) => delete_cmd(s, key.as_ref(), noreply).await,
            #[cfg(unix)]
            Connection::Unix(s) => delete_cmd(s, key.as_ref(), noreply).await,
            Connection::Udp(s, r) => delete_cmd_udp(s, r, key.as_ref(), noreply).await,
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
            Connection::Tls(s) => delete_cmd(s, key.as_ref(), noreply).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, IncrDecrResult, StorageResult, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect(
    ///         "localhost",
    ///         11216,
    ///         TlsConfig::with_additional_ca("sets/ca.crt")
    ///             .with_client_identity("sets/client.crt", "sets/client.key"),
    ///     )
    ///     .await?,
    /// ] {
    ///     let _ = c.delete(b"doc_conn_incr", false).await?;
    ///     assert_eq!(
    ///         c.incr(b"doc_conn_incr", 1, false).await?,
    ///         IncrDecrResult::NotFound
    ///     );
    ///     assert_eq!(
    ///         c.set(b"doc_conn_incr", 0, 0, false, b"10").await?,
    ///         StorageResult::Stored
    ///     );
    ///     assert_eq!(
    ///         c.incr(b"doc_conn_incr", 5, false).await?,
    ///         IncrDecrResult::Value(15)
    ///     );
    ///     assert_eq!(
    ///         c.incr(b"doc_conn_incr", 1, true).await?,
    ///         IncrDecrResult::NoReply
    ///     );
    /// }
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub async fn incr(
        &mut self,
        key: impl AsRef<[u8]>,
        value: u64,
        noreply: bool,
    ) -> Result<IncrDecrResult> {
        match self {
            Connection::Tcp(s) => incr_decr_cmd(s, b"incr", key.as_ref(), value, noreply).await,
            #[cfg(unix)]
            Connection::Unix(s) => incr_decr_cmd(s, b"incr", key.as_ref(), value, noreply).await,
            Connection::Udp(s, r) => {
                incr_decr_cmd_udp(s, r, b"incr", key.as_ref(), value, noreply).await
            }
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
            Connection::Tls(s) => incr_decr_cmd(s, b"incr", key.as_ref(), value, noreply).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, IncrDecrResult, StorageResult, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect(
    ///         "localhost",
    ///         11216,
    ///         TlsConfig::with_additional_ca("sets/ca.crt")
    ///             .with_client_identity("sets/client.crt", "sets/client.key"),
    ///     )
    ///     .await?,
    /// ] {
    ///     let _ = c.delete(b"doc_conn_decr", false).await?;
    ///     assert_eq!(
    ///         c.decr(b"doc_conn_decr", 1, false).await?,
    ///         IncrDecrResult::NotFound
    ///     );
    ///     assert_eq!(
    ///         c.set(b"doc_conn_decr", 0, 0, false, b"10").await?,
    ///         StorageResult::Stored
    ///     );
    ///     assert_eq!(
    ///         c.decr(b"doc_conn_decr", 5, false).await?,
    ///         IncrDecrResult::Value(5)
    ///     );
    ///     assert_eq!(
    ///         c.decr(b"doc_conn_decr", 1, true).await?,
    ///         IncrDecrResult::NoReply
    ///     );
    /// }
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub async fn decr(
        &mut self,
        key: impl AsRef<[u8]>,
        value: u64,
        noreply: bool,
    ) -> Result<IncrDecrResult> {
        match self {
            Connection::Tcp(s) => incr_decr_cmd(s, b"decr", key.as_ref(), value, noreply).await,
            #[cfg(unix)]
            Connection::Unix(s) => incr_decr_cmd(s, b"decr", key.as_ref(), value, noreply).await,
            Connection::Udp(s, r) => {
                incr_decr_cmd_udp(s, r, b"decr", key.as_ref(), value, noreply).await
            }
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
            Connection::Tls(s) => incr_decr_cmd(s, b"decr", key.as_ref(), value, noreply).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, StorageResult, TlsConfig, TouchResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect(
    ///         "localhost",
    ///         11216,
    ///         TlsConfig::with_additional_ca("sets/ca.crt")
    ///             .with_client_identity("sets/client.crt", "sets/client.key"),
    ///     )
    ///     .await?,
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
    ///     assert_eq!(
    ///         c.touch(b"doc_conn_touch", 60, true).await?,
    ///         TouchResult::NoReply
    ///     );
    /// }
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
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
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
            Connection::Tls(s) => touch_cmd(s, key.as_ref(), exptime, noreply).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, Item, StorageResult, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect(
    ///         "localhost",
    ///         11216,
    ///         TlsConfig::with_additional_ca("sets/ca.crt")
    ///             .with_client_identity("sets/client.crt", "sets/client.key"),
    ///     )
    ///     .await?,
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
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
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
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
            Connection::Tls(s) => Ok(retrieval_cmd(s, b"get", None, &[key.as_ref()]).await?.pop()),
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, StorageResult, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect(
    ///         "localhost",
    ///         11216,
    ///         TlsConfig::with_additional_ca("sets/ca.crt")
    ///             .with_client_identity("sets/client.crt", "sets/client.key"),
    ///     )
    ///     .await?,
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
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
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
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
            Connection::Tls(s) => Ok(retrieval_cmd(s, b"gets", None, &[key.as_ref()])
                .await?
                .pop()),
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, StorageResult, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect(
    ///         "localhost",
    ///         11216,
    ///         TlsConfig::with_additional_ca("sets/ca.crt")
    ///             .with_client_identity("sets/client.crt", "sets/client.key"),
    ///     )
    ///     .await?,
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
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
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
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
            Connection::Tls(s) => Ok(retrieval_cmd(s, b"gat", Some(exptime), &[key.as_ref()])
                .await?
                .pop()),
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, StorageResult, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect(
    ///         "localhost",
    ///         11216,
    ///         TlsConfig::with_additional_ca("sets/ca.crt")
    ///             .with_client_identity("sets/client.crt", "sets/client.key"),
    ///     )
    ///     .await?,
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
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
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
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
            Connection::Tls(s) => Ok(retrieval_cmd(s, b"gats", Some(exptime), &[key.as_ref()])
                .await?
                .pop()),
        }
    }

    /// Retrieves several keys in a single `get` command.
    ///
    /// This is the multi-key form of [`Connection::get`]. It is not supported
    /// over UDP and panics if called on a UDP connection.
    ///
    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, Item, StorageResult, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::tls_connect(
    ///         "localhost",
    ///         11216,
    ///         TlsConfig::with_additional_ca("sets/ca.crt")
    ///             .with_client_identity("sets/client.crt", "sets/client.key"),
    ///     )
    ///     .await?,
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
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
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
            Connection::Udp(_s, _r) => panic!("Udp not work with multicommands"),
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
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

    /// Retrieves several keys with their CAS tokens in a single `gets` command.
    ///
    /// This is the multi-key form of [`Connection::gets`]. It is not supported
    /// over UDP and panics if called on a UDP connection.
    ///
    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, StorageResult, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::tls_connect(
    ///         "localhost",
    ///         11216,
    ///         TlsConfig::with_additional_ca("sets/ca.crt")
    ///             .with_client_identity("sets/client.crt", "sets/client.key"),
    ///     )
    ///     .await?,
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
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
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
            Connection::Udp(_s, _r) => panic!("Udp not work with multicommands"),
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
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

    /// Retrieves several keys and updates their expiration time in a single
    /// `gat` command.
    ///
    /// This is the multi-key form of [`Connection::gat`]. It is not supported
    /// over UDP and panics if called on a UDP connection.
    ///
    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, Item, StorageResult, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::tls_connect(
    ///         "localhost",
    ///         11216,
    ///         TlsConfig::with_additional_ca("sets/ca.crt")
    ///             .with_client_identity("sets/client.crt", "sets/client.key"),
    ///     )
    ///     .await?,
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
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
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
            Connection::Udp(_s, _r) => panic!("Udp not work with multicommands"),
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
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

    /// Retrieves several keys with their CAS tokens and updates their
    /// expiration time in a single `gats` command.
    ///
    /// This is the multi-key form of [`Connection::gats`]. It is not supported
    /// over UDP and panics if called on a UDP connection.
    ///
    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, StorageResult, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::tls_connect(
    ///         "localhost",
    ///         11216,
    ///         TlsConfig::with_additional_ca("sets/ca.crt")
    ///             .with_client_identity("sets/client.crt", "sets/client.key"),
    ///     )
    ///     .await?,
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
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
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
            Connection::Udp(_s, _r) => panic!("Udp not work with multicommands"),
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
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
    /// # use mcmc_rs::{Connection, StatsArg, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect(
    ///         "localhost",
    ///         11216,
    ///         TlsConfig::with_additional_ca("sets/ca.crt")
    ///             .with_client_identity("sets/client.crt", "sets/client.key"),
    ///     )
    ///     .await?,
    /// ] {
    ///     let result = c.stats(None).await?;
    ///     assert!(result.contains_key("pid"));
    ///     assert!(result.contains_key("uptime"));
    ///     assert!(result["pid"].parse::<u32>().is_ok());
    ///     assert!(result["uptime"].parse::<u64>().is_ok());
    ///     let version = c.version().await?;
    ///     assert_eq!(result.get("version"), Some(&version));
    /// }
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub async fn stats(&mut self, arg: Option<StatsArg>) -> Result<HashMap<String, String>> {
        match self {
            Connection::Tcp(s) => stats_cmd(s, arg).await,
            #[cfg(unix)]
            Connection::Unix(s) => stats_cmd(s, arg).await,
            Connection::Udp(s, r) => stats_cmd_udp(s, r, arg).await,
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
            Connection::Tls(s) => stats_cmd(s, arg).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, SlabsAutomoveArg, StatsArg, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect(
    ///         "localhost",
    ///         11216,
    ///         TlsConfig::with_additional_ca("sets/ca.crt")
    ///             .with_client_identity("sets/client.crt", "sets/client.key"),
    ///     )
    ///     .await?,
    /// ] {
    ///     assert!(c.slabs_automove(SlabsAutomoveArg::Zero).await.is_ok());
    ///     let settings = c.stats(Some(StatsArg::Settings)).await?;
    ///     assert_eq!(settings.get("slab_automove").map(String::as_str), Some("0"));
    /// }
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub async fn slabs_automove(&mut self, arg: SlabsAutomoveArg) -> Result<()> {
        match self {
            Connection::Tcp(s) => slabs_automove_cmd(s, arg).await,
            #[cfg(unix)]
            Connection::Unix(s) => slabs_automove_cmd(s, arg).await,
            Connection::Udp(s, r) => slabs_automove_cmd_udp(s, r, arg).await,
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
            Connection::Tls(s) => slabs_automove_cmd(s, arg).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, LruCrawlerArg, StatsArg, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect(
    ///         "localhost",
    ///         11216,
    ///         TlsConfig::with_additional_ca("sets/ca.crt")
    ///             .with_client_identity("sets/client.crt", "sets/client.key"),
    ///     )
    ///     .await?,
    /// ] {
    ///     let _ = c.lru_crawler(LruCrawlerArg::Enable).await;
    ///     let settings = c.stats(Some(StatsArg::Settings)).await?;
    ///     assert_eq!(settings.get("lru_crawler").map(String::as_str), Some("yes"));
    /// }
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub async fn lru_crawler(&mut self, arg: LruCrawlerArg) -> Result<()> {
        match self {
            Connection::Tcp(s) => lru_crawler_cmd(s, arg).await,
            #[cfg(unix)]
            Connection::Unix(s) => lru_crawler_cmd(s, arg).await,
            Connection::Udp(s, r) => lru_crawler_cmd_udp(s, r, arg).await,
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
            Connection::Tls(s) => lru_crawler_cmd(s, arg).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, LruCrawlerArg, StatsArg, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect(
    ///         "localhost",
    ///         11216,
    ///         TlsConfig::with_additional_ca("sets/ca.crt")
    ///             .with_client_identity("sets/client.crt", "sets/client.key"),
    ///     )
    ///     .await?,
    /// ] {
    ///     let _ = c.lru_crawler(LruCrawlerArg::Enable).await;
    ///     assert!(c.lru_crawler_sleep(1_000_000).await.is_ok());
    ///     let settings = c.stats(Some(StatsArg::Settings)).await?;
    ///     assert_eq!(
    ///         settings.get("lru_crawler_sleep").map(String::as_str),
    ///         Some("1000000")
    ///     );
    /// }
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub async fn lru_crawler_sleep(&mut self, microseconds: usize) -> Result<()> {
        match self {
            Connection::Tcp(s) => lru_crawler_sleep_cmd(s, microseconds).await,
            #[cfg(unix)]
            Connection::Unix(s) => lru_crawler_sleep_cmd(s, microseconds).await,
            Connection::Udp(s, r) => lru_crawler_sleep_cmd_udp(s, r, microseconds).await,
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
            Connection::Tls(s) => lru_crawler_sleep_cmd(s, microseconds).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, LruCrawlerArg, StatsArg, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect(
    ///         "localhost",
    ///         11216,
    ///         TlsConfig::with_additional_ca("sets/ca.crt")
    ///             .with_client_identity("sets/client.crt", "sets/client.key"),
    ///     )
    ///     .await?,
    /// ] {
    ///     let _ = c.lru_crawler(LruCrawlerArg::Enable).await;
    ///     assert!(c.lru_crawler_tocrawl(1).await.is_ok());
    ///     let settings = c.stats(Some(StatsArg::Settings)).await?;
    ///     assert_eq!(
    ///         settings.get("lru_crawler_tocrawl").map(String::as_str),
    ///         Some("1")
    ///     );
    /// }
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub async fn lru_crawler_tocrawl(&mut self, arg: u32) -> Result<()> {
        match self {
            Connection::Tcp(s) => lru_crawler_tocrawl_cmd(s, arg).await,
            #[cfg(unix)]
            Connection::Unix(s) => lru_crawler_tocrawl_cmd(s, arg).await,
            Connection::Udp(s, r) => lru_crawler_tocrawl_cmd_udp(s, r, arg).await,
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
            Connection::Tls(s) => lru_crawler_tocrawl_cmd(s, arg).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, LruCrawlerArg, LruCrawlerCrawlArg, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect(
    ///         "localhost",
    ///         11216,
    ///         TlsConfig::with_additional_ca("sets/ca.crt")
    ///             .with_client_identity("sets/client.crt", "sets/client.key"),
    ///     )
    ///     .await?,
    /// ] {
    ///     c.lru_crawler_crawl(LruCrawlerCrawlArg::All).await?;
    /// }
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
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
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
            Connection::Tls(s) => lru_crawler_crawl_cmd(s, arg).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect(
    ///         "localhost",
    ///         11216,
    ///         TlsConfig::with_additional_ca("sets/ca.crt")
    ///             .with_client_identity("sets/client.crt", "sets/client.key"),
    ///     )
    ///     .await?,
    /// ] {
    ///     c.slabs_reassign(1, 2).await?;
    /// }
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
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
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
            Connection::Tls(s) => slabs_reassign_cmd(s, source_class, dest_class).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, LruCrawlerArg, LruCrawlerMetadumpArg, StorageResult, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::tls_connect(
    ///         "localhost",
    ///         11216,
    ///         TlsConfig::with_additional_ca("sets/ca.crt")
    ///             .with_client_identity("sets/client.crt", "sets/client.key"),
    ///     )
    ///     .await?,
    /// ] {
    ///     assert_eq!(
    ///         c.set(b"doc_conn_metadump", 0, 0, false, b"value").await?,
    ///         StorageResult::Stored
    ///     );
    ///     c.lru_crawler_metadump(LruCrawlerMetadumpArg::All).await?;
    /// }
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
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
            Connection::Udp(_s, _r) => panic!("this command not work with udp connection!"),
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
            Connection::Tls(s) => lru_crawler_metadump_cmd(s, arg).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, LruCrawlerArg, LruCrawlerMgdumpArg, StorageResult, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::tls_connect(
    ///         "localhost",
    ///         11216,
    ///         TlsConfig::with_additional_ca("sets/ca.crt")
    ///             .with_client_identity("sets/client.crt", "sets/client.key"),
    ///     )
    ///     .await?,
    /// ] {
    ///     assert_eq!(
    ///         c.set(b"doc_conn_mgdump", 0, 0, false, b"value").await?,
    ///         StorageResult::Stored
    ///     );
    ///     c.lru_crawler_mgdump(LruCrawlerMgdumpArg::All).await?;
    /// }
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
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
            Connection::Udp(_s, _r) => panic!("this command not work with udp connection!"),
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
            Connection::Tls(s) => lru_crawler_mgdump_cmd(s, arg).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect(
    ///         "localhost",
    ///         11216,
    ///         TlsConfig::with_additional_ca("sets/ca.crt")
    ///             .with_client_identity("sets/client.crt", "sets/client.key"),
    ///     )
    ///     .await?,
    /// ] {
    ///     assert!(c.mn().await.is_ok());
    /// }
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub async fn mn(&mut self) -> Result<()> {
        match self {
            Connection::Tcp(s) => mn_cmd(s).await,
            #[cfg(unix)]
            Connection::Unix(s) => mn_cmd(s).await,
            Connection::Udp(s, r) => mn_cmd_udp(s, r).await,
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
            Connection::Tls(s) => mn_cmd(s).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, StorageResult, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect(
    ///         "localhost",
    ///         11216,
    ///         TlsConfig::with_additional_ca("sets/ca.crt")
    ///             .with_client_identity("sets/client.crt", "sets/client.key"),
    ///     )
    ///     .await?,
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
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub async fn me(&mut self, key: impl AsRef<[u8]>) -> Result<Option<String>> {
        match self {
            Connection::Tcp(s) => me_cmd(s, key.as_ref()).await,
            #[cfg(unix)]
            Connection::Unix(s) => me_cmd(s, key.as_ref()).await,
            Connection::Udp(s, r) => me_cmd_udp(s, r, key.as_ref()).await,
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
            Connection::Tls(s) => me_cmd(s, key.as_ref()).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, StorageResult, WatchArg, TlsConfig};
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
    ///         Connection::tls_connect(
    ///             "localhost",
    ///             11216,
    ///             TlsConfig::with_additional_ca("sets/ca.crt")
    ///                 .with_client_identity("sets/client.crt", "sets/client.key"),
    ///         )
    ///         .await?,
    ///         Connection::tls_connect(
    ///             "localhost",
    ///             11216,
    ///             TlsConfig::with_additional_ca("sets/ca.crt")
    ///                 .with_client_identity("sets/client.crt", "sets/client.key"),
    ///         )
    ///         .await?,
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
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub async fn watch(mut self, arg: &[WatchArg]) -> Result<WatchStream> {
        match &mut self {
            Connection::Tcp(s) => watch_cmd(s, arg).await?,
            #[cfg(unix)]
            Connection::Unix(s) => watch_cmd(s, arg).await?,
            Connection::Udp(_s, _r) => panic!("this command not work with udp!"),
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
            Connection::Tls(s) => watch_cmd(s, arg).await?,
        };
        Ok(WatchStream(self))
    }

    /// Starts a new command pipeline.
    ///
    /// Commands are buffered and sent together by [`Pipeline::execute`].
    /// Builder methods validate keys and flags immediately but return `Self`,
    /// so validation errors are reported by `execute`.
    ///
    /// # Example
    ///
    /// ```
    /// # use std::assert_matches;
    /// # use mcmc_rs::{Connection, PipelineResponse};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::default().await?;
    /// let result = conn
    ///     .pipeline()
    ///     .get("key1")
    ///     .get("key2")
    ///     .get("key3")
    ///     .version()
    ///     .execute()
    ///     .await?;
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
    /// assert_eq!(result.len(), 4);
    /// assert_eq!(result[0], PipelineResponse::OptionItem(None));
    /// assert_eq!(result[1], PipelineResponse::OptionItem(None));
    /// assert_eq!(result[2], PipelineResponse::OptionItem(None));
    /// assert_matches!(result[3], PipelineResponse::String(_));
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn pipeline(&mut self) -> Pipeline<'_> {
        Pipeline(self, Ok(Vec::new()))
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, MgFlag, MgStatus, StorageResult, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect(
    ///         "localhost",
    ///         11216,
    ///         TlsConfig::with_additional_ca("sets/ca.crt")
    ///             .with_client_identity("sets/client.crt", "sets/client.key"),
    ///     )
    ///     .await?,
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
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub async fn mg(&mut self, key: impl AsRef<[u8]>, flags: &[MgFlag]) -> Result<MgItem> {
        match self {
            Connection::Tcp(s) => mg_cmd(s, key.as_ref(), flags).await,
            #[cfg(unix)]
            Connection::Unix(s) => mg_cmd(s, key.as_ref(), flags).await,
            Connection::Udp(s, r) => mg_cmd_udp(s, r, key.as_ref(), flags).await,
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
            Connection::Tls(s) => mg_cmd(s, key.as_ref(), flags).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, MsFlag, MsMode, StorageResult, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect(
    ///         "localhost",
    ///         11216,
    ///         TlsConfig::with_additional_ca("sets/ca.crt")
    ///             .with_client_identity("sets/client.crt", "sets/client.key"),
    ///     )
    ///     .await?,
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
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
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
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
            Connection::Tls(s) => ms_cmd(s, key.as_ref(), flags, data_block.as_ref()).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, MdFlag, MdStatus, StorageResult, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect(
    ///         "localhost",
    ///         11216,
    ///         TlsConfig::with_additional_ca("sets/ca.crt")
    ///             .with_client_identity("sets/client.crt", "sets/client.key"),
    ///     )
    ///     .await?,
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
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub async fn md(&mut self, key: impl AsRef<[u8]>, flags: &[MdFlag]) -> Result<MdItem> {
        match self {
            Connection::Tcp(s) => md_cmd(s, key.as_ref(), flags).await,
            #[cfg(unix)]
            Connection::Unix(s) => md_cmd(s, key.as_ref(), flags).await,
            Connection::Udp(s, r) => md_cmd_udp(s, r, key.as_ref(), flags).await,
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
            Connection::Tls(s) => md_cmd(s, key.as_ref(), flags).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, MaFlag, MaMode, MaStatus, StorageResult, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect(
    ///         "localhost",
    ///         11216,
    ///         TlsConfig::with_additional_ca("sets/ca.crt")
    ///             .with_client_identity("sets/client.crt", "sets/client.key"),
    ///     )
    ///     .await?,
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
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub async fn ma(&mut self, key: impl AsRef<[u8]>, flags: &[MaFlag]) -> Result<MaItem> {
        match self {
            Connection::Tcp(s) => ma_cmd(s, key.as_ref(), flags).await,
            #[cfg(unix)]
            Connection::Unix(s) => ma_cmd(s, key.as_ref(), flags).await,
            Connection::Udp(s, r) => ma_cmd_udp(s, r, key.as_ref(), flags).await,
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
            Connection::Tls(s) => ma_cmd(s, key.as_ref(), flags).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, LruArg, LruMode, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect(
    ///         "localhost",
    ///         11216,
    ///         TlsConfig::with_additional_ca("sets/ca.crt")
    ///             .with_client_identity("sets/client.crt", "sets/client.key"),
    ///     )
    ///     .await?,
    /// ] {
    ///     c.lru(LruArg::Mode(LruMode::Flat)).await?;
    /// }
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub async fn lru(&mut self, arg: LruArg) -> Result<()> {
        match self {
            Connection::Tcp(s) => lru_cmd(s, arg).await,
            #[cfg(unix)]
            Connection::Unix(s) => lru_cmd(s, arg).await,
            Connection::Udp(s, r) => lru_cmd_udp(s, r, arg).await,
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
            Connection::Tls(s) => lru_cmd(s, arg).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, VerbosityArg, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11214").await?,
    ///     Connection::tls_connect(
    ///         "localhost",
    ///         11216,
    ///         TlsConfig::with_additional_ca("sets/ca.crt")
    ///             .with_client_identity("sets/client.crt", "sets/client.key"),
    ///     )
    ///     .await?,
    /// ] {
    ///     assert!(c.verbosity(VerbosityArg::Off, false).await.is_ok());
    ///     assert!(c.verbosity(VerbosityArg::Off, true).await.is_ok());
    /// }
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub async fn verbosity(&mut self, arg: VerbosityArg, noreply: bool) -> Result<()> {
        match self {
            Connection::Tcp(s) => verbosity_cmd(s, arg, noreply).await,
            #[cfg(unix)]
            Connection::Unix(s) => verbosity_cmd(s, arg, noreply).await,
            Connection::Udp(s, r) => verbosity_cmd_udp(s, r, arg, noreply).await,
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
            Connection::Tls(s) => verbosity_cmd(s, arg, noreply).await,
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::tcp_connect("127.0.0.1:11219").await?,
    ///     Connection::unix_connect("/tmp/memcached3.sock").await?,
    ///     Connection::udp_connect("127.0.0.1:0", "127.0.0.1:11220").await?,
    ///     Connection::tls_connect(
    ///         "localhost",
    ///         11216,
    ///         TlsConfig::with_additional_ca("sets/ca.crt")
    ///             .with_client_identity("sets/client.crt", "sets/client.key"),
    ///     )
    ///     .await?,
    /// ] {
    ///     assert!(c.refresh_certs().await.is_ok());
    /// }
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub async fn refresh_certs(&mut self) -> Result<()> {
        match self {
            Connection::Tcp(s) => refresh_certs_cmd(s).await,
            #[cfg(unix)]
            Connection::Unix(s) => refresh_certs_cmd(s).await,
            Connection::Udp(s, r) => refresh_certs_cmd_udp(s, r).await,
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
            Connection::Tls(s) => refresh_certs_cmd(s).await,
        }
    }
}

pub struct WatchStream(Connection);
impl WatchStream {
    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, StorageResult, WatchArg, TlsConfig};
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
    ///         Connection::tls_connect(
    ///             "localhost",
    ///             11216,
    ///             TlsConfig::with_additional_ca("sets/ca.crt")
    ///                 .with_client_identity("sets/client.crt", "sets/client.key"),
    ///         )
    ///         .await?,
    ///         Connection::tls_connect(
    ///             "localhost",
    ///             11216,
    ///             TlsConfig::with_additional_ca("sets/ca.crt")
    ///                 .with_client_identity("sets/client.crt", "sets/client.key"),
    ///         )
    ///         .await?,
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
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub async fn message(&mut self) -> Result<Option<String>> {
        let mut line = String::new();
        let n = match &mut self.0 {
            Connection::Tcp(s) => s.read_line(&mut line).await?,
            #[cfg(unix)]
            Connection::Unix(s) => s.read_line(&mut line).await?,
            Connection::Udp(_s, _r) => panic!("this command not work with udp connection"),
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
            Connection::Tls(s) => s.read_line(&mut line).await?,
        };
        if n == 0 {
            Ok(None)
        } else {
            Ok(Some(line.trim().to_string()))
        }
    }
}

async fn try_join_all<F, T>(futs: Vec<F>) -> Result<Vec<T>>
where
    F: Future<Output = Result<T>>,
{
    let mut futs: Vec<_> = futs.into_iter().map(Box::pin).collect();
    let mut results: Vec<Option<T>> = (0..futs.len()).map(|_| None).collect();
    std::future::poll_fn(|cx| {
        let mut all_done = true;
        for (i, fut) in futs.iter_mut().enumerate() {
            if results[i].is_none() {
                match fut.as_mut().poll(cx) {
                    std::task::Poll::Ready(Ok(val)) => results[i] = Some(val),
                    std::task::Poll::Ready(Err(err)) => return std::task::Poll::Ready(Err(err)),
                    std::task::Poll::Pending => all_done = false,
                }
            }
        }
        if all_done {
            let out = std::mem::take(&mut results).into_iter().flatten().collect();
            std::task::Poll::Ready(Ok(out))
        } else {
            std::task::Poll::Pending
        }
    })
    .await
}

fn no_servers_err() -> MemcachedError {
    MemcachedError::ClientConfig("no connections".to_owned())
}

/// Routes keys to server connections.
///
/// The crate provides [`ClientModN`], [`ClientHashRing`] and
/// [`ClientRendezvous`]. Implement this trait to build a custom routing
/// strategy; both [`ServerSelector::select_server`] and
/// [`ServerSelector::select_servers`] are required so that single-key and
/// multi-key operations use the same mapping.
pub trait ServerSelector: Send + Sync {
    /// Returns the connection responsible for `key`.
    fn select_server(&mut self, key: &[u8]) -> Result<&mut Connection>;

    /// Groups `keys` by their target connection.
    ///
    /// Each returned pair contains a connection and the keys routed to it.
    /// Connections without matching keys may be omitted. Multi-key methods use
    /// this to issue one command per server.
    fn select_servers<'a>(
        &mut self,
        keys: &'a [&'a [u8]],
    ) -> Result<Vec<(&mut Connection, Vec<&'a [u8]>)>>;

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientModN, Connection, ServerSelector, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientModN::new(vec![Connection::default().await?]);
    /// assert_eq!(
    ///     client.set(b"doc_selector_get", 0, 0, false, b"value").await?,
    ///     StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client.get(b"doc_selector_get").await?.unwrap().data_block,
    ///     b"value"
    /// );
    /// assert_eq!(client.get(b"doc_selector_get_missing").await?, None);
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    fn get(
        &mut self,
        key: impl AsRef<[u8]> + Send,
    ) -> impl Future<Output = Result<Option<Item>>> + Send {
        async move { self.select_server(key.as_ref())?.get(key).await }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientModN, Connection, ServerSelector, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientModN::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// assert_eq!(
    ///     client.set(b"doc_modn_gets", 0, 0, false, b"value").await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// let item = client.gets(b"doc_modn_gets").await?.unwrap();
    /// assert_eq!(item.key, "doc_modn_gets");
    /// assert_eq!(item.data_block, b"value");
    /// assert!(item.cas_unique.is_some());
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    fn gets(
        &mut self,
        key: impl AsRef<[u8]> + Send,
    ) -> impl Future<Output = Result<Option<Item>>> + Send {
        async move { self.select_server(key.as_ref())?.gets(key).await }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientModN, Connection, ServerSelector, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientModN::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// assert_eq!(
    ///     client.set(b"doc_modn_gat", 7, 0, false, b"value").await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// let item = client.gat(60, b"doc_modn_gat").await?.unwrap();
    /// assert_eq!(item.key, "doc_modn_gat");
    /// assert_eq!(item.flags, 7);
    /// assert_eq!(item.data_block, b"value");
    /// assert_eq!(item.cas_unique, None);
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    fn gat(
        &mut self,
        exptime: i64,
        key: impl AsRef<[u8]> + Send,
    ) -> impl Future<Output = Result<Option<Item>>> + Send {
        async move { self.select_server(key.as_ref())?.gat(exptime, key).await }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientModN, Connection, ServerSelector, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientModN::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// assert_eq!(
    ///     client.set(b"doc_modn_gats", 7, 0, false, b"value").await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// let item = client.gats(60, b"doc_modn_gats").await?.unwrap();
    /// assert_eq!(item.key, "doc_modn_gats");
    /// assert_eq!(item.flags, 7);
    /// assert_eq!(item.data_block, b"value");
    /// assert!(item.cas_unique.is_some());
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    fn gats(
        &mut self,
        exptime: i64,
        key: impl AsRef<[u8]> + Send,
    ) -> impl Future<Output = Result<Option<Item>>> + Send {
        async move { self.select_server(key.as_ref())?.gats(exptime, key).await }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientModN, Connection, ServerSelector, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientModN::new(vec![Connection::default().await?]);
    /// assert_eq!(
    ///     client.set(b"doc_selector_set", 7, 0, false, b"value").await?,
    ///     StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client.set(b"doc_selector_set_noreply", 7, 0, true, b"value").await?,
    ///     StorageResult::NoReply
    /// );
    /// let item = client.get(b"doc_selector_set").await?.unwrap();
    /// assert_eq!(item.flags, 7);
    /// assert_eq!(item.data_block, b"value");
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    fn set(
        &mut self,
        key: impl AsRef<[u8]> + Send,
        flags: u32,
        exptime: i64,
        noreply: bool,
        data_block: impl AsRef<[u8]> + Send,
    ) -> impl Future<Output = Result<StorageResult>> + Send {
        async move {
            self.select_server(key.as_ref())?
                .set(key, flags, exptime, noreply, data_block)
                .await
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientModN, Connection, ServerSelector, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientModN::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// let _ = client.delete(b"doc_modn_add", false).await?;
    /// assert_eq!(
    ///     client.add(b"doc_modn_add", 0, 0, false, b"first").await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client.add(b"doc_modn_add", 0, 0, false, b"second").await?,
    ///     mcmc_rs::StorageResult::NotStored
    /// );
    /// assert_eq!(
    ///     client.get(b"doc_modn_add").await?.unwrap().data_block,
    ///     b"first"
    /// );
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    fn add(
        &mut self,
        key: impl AsRef<[u8]> + Send,
        flags: u32,
        exptime: i64,
        noreply: bool,
        data_block: impl AsRef<[u8]> + Send,
    ) -> impl Future<Output = Result<StorageResult>> + Send {
        async move {
            self.select_server(key.as_ref())?
                .add(key, flags, exptime, noreply, data_block)
                .await
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientModN, Connection, ServerSelector, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientModN::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// let _ = client.delete(b"doc_modn_replace", false).await?;
    /// assert_eq!(
    ///     client
    ///         .replace(b"doc_modn_replace", 0, 0, false, b"value")
    ///         .await?,
    ///     mcmc_rs::StorageResult::NotStored
    /// );
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_modn_replace", 0, 0, false, b"first")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client
    ///         .replace(b"doc_modn_replace", 0, 0, false, b"second")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client.get(b"doc_modn_replace").await?.unwrap().data_block,
    ///     b"second"
    /// );
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    fn replace(
        &mut self,
        key: impl AsRef<[u8]> + Send,
        flags: u32,
        exptime: i64,
        noreply: bool,
        data_block: impl AsRef<[u8]> + Send,
    ) -> impl Future<Output = Result<StorageResult>> + Send {
        async move {
            self.select_server(key.as_ref())?
                .replace(key, flags, exptime, noreply, data_block)
                .await
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientModN, Connection, ServerSelector, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientModN::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// let _ = client.delete(b"doc_modn_append", false).await?;
    /// assert_eq!(
    ///     client
    ///         .append(b"doc_modn_append", 0, 0, false, b"-tail")
    ///         .await?,
    ///     mcmc_rs::StorageResult::NotStored
    /// );
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_modn_append", 0, 0, false, b"base")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client
    ///         .append(b"doc_modn_append", 0, 0, false, b"-tail")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client.get(b"doc_modn_append").await?.unwrap().data_block,
    ///     b"base-tail"
    /// );
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    fn append(
        &mut self,
        key: impl AsRef<[u8]> + Send,
        flags: u32,
        exptime: i64,
        noreply: bool,
        data_block: impl AsRef<[u8]> + Send,
    ) -> impl Future<Output = Result<StorageResult>> + Send {
        async move {
            self.select_server(key.as_ref())?
                .append(key, flags, exptime, noreply, data_block)
                .await
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientModN, Connection, ServerSelector, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientModN::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// let _ = client.delete(b"doc_modn_prepend", false).await?;
    /// assert_eq!(
    ///     client
    ///         .prepend(b"doc_modn_prepend", 0, 0, false, b"head-")
    ///         .await?,
    ///     mcmc_rs::StorageResult::NotStored
    /// );
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_modn_prepend", 0, 0, false, b"base")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client
    ///         .prepend(b"doc_modn_prepend", 0, 0, false, b"head-")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client.get(b"doc_modn_prepend").await?.unwrap().data_block,
    ///     b"head-base"
    /// );
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    fn prepend(
        &mut self,
        key: impl AsRef<[u8]> + Send,
        flags: u32,
        exptime: i64,
        noreply: bool,
        data_block: impl AsRef<[u8]> + Send,
    ) -> impl Future<Output = Result<StorageResult>> + Send {
        async move {
            self.select_server(key.as_ref())?
                .prepend(key, flags, exptime, noreply, data_block)
                .await
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientModN, Connection, ServerSelector, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientModN::new(vec![Connection::default().await?]);
    /// client.set(b"doc_selector_cas", 0, 0, false, b"base").await?;
    /// let cas = client
    ///     .gets(b"doc_selector_cas")
    ///     .await?
    ///     .unwrap()
    ///     .cas_unique
    ///     .unwrap();
    /// assert_eq!(
    ///     client
    ///         .cas(b"doc_selector_cas", 0, 0, cas, false, b"new")
    ///         .await?,
    ///     StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client
    ///         .cas(b"doc_selector_cas", 0, 0, cas, false, b"stale")
    ///         .await?,
    ///     StorageResult::Exists
    /// );
    /// assert_eq!(
    ///     client.get(b"doc_selector_cas").await?.unwrap().data_block,
    ///     b"new"
    /// );
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    fn cas(
        &mut self,
        key: impl AsRef<[u8]> + Send,
        flags: u32,
        exptime: i64,
        cas_unique: u64,
        noreply: bool,
        data_block: impl AsRef<[u8]> + Send,
    ) -> impl Future<Output = Result<StorageResult>> + Send {
        async move {
            self.select_server(key.as_ref())?
                .cas(key, flags, exptime, cas_unique, noreply, data_block)
                .await
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientModN, Connection, ServerSelector, DeleteResult, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientModN::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_modn_delete", 0, 0, false, b"value")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client.delete(b"doc_modn_delete", false).await?,
    ///     mcmc_rs::DeleteResult::Deleted
    /// );
    /// assert_eq!(
    ///     client.delete(b"doc_modn_delete", false).await?,
    ///     mcmc_rs::DeleteResult::NotFound
    /// );
    /// assert_eq!(
    ///     client.delete(b"doc_modn_delete", true).await?,
    ///     mcmc_rs::DeleteResult::NoReply
    /// );
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    fn delete(
        &mut self,
        key: impl AsRef<[u8]> + Send,
        noreply: bool,
    ) -> impl Future<Output = Result<DeleteResult>> + Send {
        async move { self.select_server(key.as_ref())?.delete(key, noreply).await }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientModN, Connection, ServerSelector, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientModN::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// let _ = client.delete(b"doc_modn_incr", false).await?;
    /// assert_eq!(
    ///     client.incr(b"doc_modn_incr", 1, false).await?,
    ///     mcmc_rs::IncrDecrResult::NotFound
    /// );
    /// assert_eq!(
    ///     client.set(b"doc_modn_incr", 0, 0, false, b"10").await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client.incr(b"doc_modn_incr", 5, false).await?,
    ///     mcmc_rs::IncrDecrResult::Value(15)
    /// );
    /// assert_eq!(
    ///     client.incr(b"doc_modn_incr", 1, true).await?,
    ///     mcmc_rs::IncrDecrResult::NoReply
    /// );
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    fn incr(
        &mut self,
        key: impl AsRef<[u8]> + Send,
        value: u64,
        noreply: bool,
    ) -> impl Future<Output = Result<IncrDecrResult>> + Send {
        async move {
            self.select_server(key.as_ref())?
                .incr(key, value, noreply)
                .await
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientModN, Connection, ServerSelector, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientModN::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// let _ = client.delete(b"doc_modn_decr", false).await?;
    /// assert_eq!(
    ///     client.decr(b"doc_modn_decr", 1, false).await?,
    ///     mcmc_rs::IncrDecrResult::NotFound
    /// );
    /// assert_eq!(
    ///     client.set(b"doc_modn_decr", 0, 0, false, b"10").await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client.decr(b"doc_modn_decr", 5, false).await?,
    ///     mcmc_rs::IncrDecrResult::Value(5)
    /// );
    /// assert_eq!(
    ///     client.decr(b"doc_modn_decr", 1, true).await?,
    ///     mcmc_rs::IncrDecrResult::NoReply
    /// );
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    fn decr(
        &mut self,
        key: impl AsRef<[u8]> + Send,
        value: u64,
        noreply: bool,
    ) -> impl Future<Output = Result<IncrDecrResult>> + Send {
        async move {
            self.select_server(key.as_ref())?
                .decr(key, value, noreply)
                .await
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientModN, Connection, ServerSelector, TouchResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientModN::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// assert_eq!(
    ///     client.touch(b"doc_modn_touch_missing", 60, false).await?,
    ///     mcmc_rs::TouchResult::NotFound
    /// );
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_modn_touch", 0, 0, false, b"value")
    ///         .await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client.touch(b"doc_modn_touch", 60, false).await?,
    ///     mcmc_rs::TouchResult::Touched
    /// );
    /// assert_eq!(
    ///     client.touch(b"doc_modn_touch", 60, true).await?,
    ///     mcmc_rs::TouchResult::NoReply
    /// );
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    fn touch(
        &mut self,
        key: impl AsRef<[u8]> + Send,
        exptime: i64,
        noreply: bool,
    ) -> impl Future<Output = Result<TouchResult>> + Send {
        async move {
            self.select_server(key.as_ref())?
                .touch(key, exptime, noreply)
                .await
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientModN, Connection, ServerSelector, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientModN::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// assert_eq!(
    ///     client.set(b"doc_modn_me", 0, 0, false, b"value").await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// let info = client.me(b"doc_modn_me").await?.unwrap();
    /// assert!(info.starts_with("doc_modn_me "));
    /// assert!(info.contains("size="));
    /// assert_eq!(client.me(b"doc_modn_me_missing").await?, None);
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    fn me(
        &mut self,
        key: impl AsRef<[u8]> + Send,
    ) -> impl Future<Output = Result<Option<String>>> + Send {
        async move { self.select_server(key.as_ref())?.me(key).await }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientModN, Connection, MgFlag, MgStatus, ServerSelector, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientModN::new(vec![Connection::default().await?]);
    /// client.set(b"doc_selector_mg", 7, 0, false, b"hello").await?;
    /// let item = client
    ///     .mg(
    ///         b"doc_selector_mg",
    ///         &[MgFlag::ReturnKey, MgFlag::ReturnFlags, MgFlag::ReturnValue],
    ///     )
    ///     .await?;
    /// assert_eq!(item.status, MgStatus::WithBody);
    /// assert_eq!(item.key.as_deref(), Some("doc_selector_mg"));
    /// assert_eq!(item.flags, Some(7));
    /// assert_eq!(item.data_block.as_deref(), Some(&b"hello"[..]));
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    fn mg(
        &mut self,
        key: impl AsRef<[u8]> + Send,
        flags: &[MgFlag],
    ) -> impl Future<Output = Result<MgItem>> + Send {
        async move { self.select_server(key.as_ref())?.mg(key, flags).await }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientModN, Connection, ServerSelector, MsFlag, MsMode, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientModN::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// let _ = client.delete(b"doc_modn_ms", false).await?;
    /// let item = client
    ///     .ms(
    ///         b"doc_modn_ms",
    ///         &[MsFlag::ReturnKey, MsFlag::ReturnSize, MsFlag::ReturnCas],
    ///         b"hello",
    ///     )
    ///     .await?;
    /// assert_eq!(item.status, mcmc_rs::StorageResult::Stored);
    /// assert_eq!(item.key.as_deref(), Some("doc_modn_ms"));
    /// assert_eq!(item.size, Some(5));
    /// assert!(item.cas.is_some());
    /// let item = client
    ///     .ms(
    ///         b"doc_modn_ms",
    ///         &[MsFlag::Mode(MsMode::Add), MsFlag::ReturnKey],
    ///         b"again",
    ///     )
    ///     .await?;
    /// assert_eq!(item.status, mcmc_rs::StorageResult::NotStored);
    /// assert_eq!(
    ///     client.get(b"doc_modn_ms").await?.unwrap().data_block,
    ///     b"hello"
    /// );
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    fn ms(
        &mut self,
        key: impl AsRef<[u8]> + Send,
        flags: &[MsFlag],
        data_block: impl AsRef<[u8]> + Send,
    ) -> impl Future<Output = Result<MsItem>> + Send {
        async move {
            self.select_server(key.as_ref())?
                .ms(key, flags, data_block)
                .await
        }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientModN, Connection, ServerSelector, MdFlag, MdStatus, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientModN::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// assert_eq!(
    ///     client.set(b"doc_modn_md", 0, 0, false, b"value").await?,
    ///     mcmc_rs::StorageResult::Stored
    /// );
    /// let item = client.md(b"doc_modn_md", &[MdFlag::ReturnKey]).await?;
    /// assert_eq!(item.status, mcmc_rs::MdStatus::Deleted);
    /// assert_eq!(item.key.as_deref(), Some("doc_modn_md"));
    /// assert_eq!(client.get(b"doc_modn_md").await?, None);
    /// let item = client.md(b"doc_modn_md", &[MdFlag::ReturnKey]).await?;
    /// assert_eq!(item.status, mcmc_rs::MdStatus::NotFound);
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    fn md(
        &mut self,
        key: impl AsRef<[u8]> + Send,
        flags: &[MdFlag],
    ) -> impl Future<Output = Result<MdItem>> + Send {
        async move { self.select_server(key.as_ref())?.md(key, flags).await }
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientModN, Connection, ServerSelector, MaFlag, MaMode, MaStatus, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientModN::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// let _ = client.delete(b"doc_modn_ma", false).await?;
    /// let item = client
    ///     .ma(
    ///         b"doc_modn_ma",
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
    /// assert_eq!(item.key.as_deref(), Some("doc_modn_ma"));
    /// let item = client
    ///     .ma(
    ///         b"doc_modn_ma",
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
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    fn ma(
        &mut self,
        key: impl AsRef<[u8]> + Send,
        flags: &[MaFlag],
    ) -> impl Future<Output = Result<MaItem>> + Send {
        async move { self.select_server(key.as_ref())?.ma(key, flags).await }
    }

    /// Retrieves several keys, grouping them by server.
    ///
    /// One `get` command is sent to each server that owns at least one of the
    /// keys, and the results are merged. The returned order is not guaranteed
    /// to match the input order. An empty key slice returns an empty vector.
    ///
    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientHashRing, Connection, ServerSelector, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientHashRing::new([
    ///     ("node1", Connection::default().await?),
    ///     ("node2", Connection::unix_connect("/tmp/memcached0.sock").await?),
    /// ]);
    /// client.set(b"doc_selector_get_multi_a", 0, 0, false, b"a").await?;
    /// client.set(b"doc_selector_get_multi_b", 0, 0, false, b"b").await?;
    /// let mut items = client
    ///     .get_multi(&[b"doc_selector_get_multi_a", b"doc_selector_get_multi_b"])
    ///     .await?;
    /// items.sort_by(|a, b| a.key.cmp(&b.key));
    /// assert_eq!(items.len(), 2);
    /// assert_eq!(items[0].data_block, b"a");
    /// assert_eq!(items[1].data_block, b"b");
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    fn get_multi(
        &mut self,
        keys: &[impl AsRef<[u8]> + Sync],
    ) -> impl Future<Output = Result<Vec<Item>>> + Send {
        async move {
            if keys.is_empty() {
                return Ok(Vec::new());
            }
            let key_refs: Vec<&[u8]> = keys.iter().map(AsRef::as_ref).collect();
            let groups = self.select_servers(&key_refs)?;
            let futs = groups
                .into_iter()
                .map(|(conn, shard_keys)| async move { conn.get_multi(&shard_keys).await })
                .collect();
            let chunks = try_join_all(futs).await?;
            Ok(chunks.into_iter().flatten().collect())
        }
    }

    /// Retrieves several keys together with their CAS tokens, grouping them by
    /// server.
    ///
    /// This is the multi-key form of [`Connection::gets`]. The returned order
    /// is not guaranteed to match the input order, and an empty key slice
    /// returns an empty vector.
    ///
    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientHashRing, Connection, ServerSelector, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientHashRing::new([
    ///     ("node1", Connection::default().await?),
    ///     ("node2", Connection::unix_connect("/tmp/memcached0.sock").await?),
    /// ]);
    /// client.set(b"doc_selector_gets_multi_a", 0, 0, false, b"a").await?;
    /// client.set(b"doc_selector_gets_multi_b", 0, 0, false, b"b").await?;
    /// let mut items = client
    ///     .gets_multi(&[b"doc_selector_gets_multi_a", b"doc_selector_gets_multi_b"])
    ///     .await?;
    /// items.sort_by(|a, b| a.key.cmp(&b.key));
    /// assert_eq!(items.len(), 2);
    /// assert_eq!(items[0].data_block, b"a");
    /// assert_eq!(items[1].data_block, b"b");
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    fn gets_multi(
        &mut self,
        keys: &[impl AsRef<[u8]> + Sync],
    ) -> impl Future<Output = Result<Vec<Item>>> + Send {
        async move {
            if keys.is_empty() {
                return Ok(Vec::new());
            }
            let key_refs: Vec<&[u8]> = keys.iter().map(AsRef::as_ref).collect();
            let groups = self.select_servers(&key_refs)?;
            let futs = groups
                .into_iter()
                .map(|(conn, shard_keys)| async move { conn.gets_multi(&shard_keys).await })
                .collect();
            let chunks = try_join_all(futs).await?;
            Ok(chunks.into_iter().flatten().collect())
        }
    }

    /// Retrieves several keys and updates their expiration time, grouping them
    /// by server.
    ///
    /// This is the multi-key form of [`Connection::gat`]. The returned order is
    /// not guaranteed to match the input order, and an empty key slice returns
    /// an empty vector.
    ///
    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientHashRing, Connection, ServerSelector, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientHashRing::new([
    ///     ("node1", Connection::default().await?),
    ///     ("node2", Connection::unix_connect("/tmp/memcached0.sock").await?),
    /// ]);
    /// client.set(b"doc_selector_gat_multi_a", 0, 0, false, b"a").await?;
    /// client.set(b"doc_selector_gat_multi_b", 0, 0, false, b"b").await?;
    /// let mut items = client
    ///     .gat_multi(0, &[b"doc_selector_gat_multi_a", b"doc_selector_gat_multi_b"])
    ///     .await?;
    /// items.sort_by(|a, b| a.key.cmp(&b.key));
    /// assert_eq!(items.len(), 2);
    /// assert_eq!(items[0].data_block, b"a");
    /// assert_eq!(items[1].data_block, b"b");
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    fn gat_multi(
        &mut self,
        exptime: i64,
        keys: &[impl AsRef<[u8]> + Sync],
    ) -> impl Future<Output = Result<Vec<Item>>> + Send {
        async move {
            if keys.is_empty() {
                return Ok(Vec::new());
            }
            let key_refs: Vec<&[u8]> = keys.iter().map(AsRef::as_ref).collect();
            let groups = self.select_servers(&key_refs)?;
            let futs = groups
                .into_iter()
                .map(|(conn, shard_keys)| async move { conn.gat_multi(exptime, &shard_keys).await })
                .collect();
            let chunks = try_join_all(futs).await?;
            Ok(chunks.into_iter().flatten().collect())
        }
    }

    /// Retrieves several keys with their CAS tokens and updates their
    /// expiration time, grouping them by server.
    ///
    /// This is the multi-key form of [`Connection::gats`]. The returned order
    /// is not guaranteed to match the input order, and an empty key slice
    /// returns an empty vector.
    ///
    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientHashRing, Connection, ServerSelector, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientHashRing::new([
    ///     ("node1", Connection::default().await?),
    ///     ("node2", Connection::unix_connect("/tmp/memcached0.sock").await?),
    /// ]);
    /// client.set(b"doc_selector_gats_multi_a", 0, 0, false, b"a").await?;
    /// client.set(b"doc_selector_gats_multi_b", 0, 0, false, b"b").await?;
    /// let mut items = client
    ///     .gats_multi(0, &[b"doc_selector_gats_multi_a", b"doc_selector_gats_multi_b"])
    ///     .await?;
    /// items.sort_by(|a, b| a.key.cmp(&b.key));
    /// assert_eq!(items.len(), 2);
    /// assert_eq!(items[0].data_block, b"a");
    /// assert_eq!(items[1].data_block, b"b");
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    fn gats_multi(
        &mut self,
        exptime: i64,
        keys: &[impl AsRef<[u8]> + Sync],
    ) -> impl Future<Output = Result<Vec<Item>>> + Send {
        async move {
            if keys.is_empty() {
                return Ok(Vec::new());
            }
            let key_refs: Vec<&[u8]> = keys.iter().map(AsRef::as_ref).collect();
            let groups = self.select_servers(&key_refs)?;
            let futs = groups
                .into_iter()
                .map(
                    |(conn, shard_keys)| async move { conn.gats_multi(exptime, &shard_keys).await },
                )
                .collect();
            let chunks = try_join_all(futs).await?;
            Ok(chunks.into_iter().flatten().collect())
        }
    }
}

/// Distributes keys across connections with modulo hashing.
///
/// The target connection is `hasher.hash_one(key) % N`, where the hasher is
/// `rapidhash` with a fixed seed by default or any [`std::hash::BuildHasher`]
/// supplied by the caller. Because the bucket is the remainder of the hash,
/// adding or removing a connection remaps most keys unless `N` stays the same.
///
/// Use [`ClientModN::new`], [`ClientModN::with_seed`] or
/// [`ClientModN::with_hasher`] to build one.
pub struct ClientModN<S = SeedableState<'static>> {
    conns: Vec<Connection>,
    hasher: S,
}

impl ClientModN {
    /// Creates a modulo client using `rapidhash` with a fixed default seed.
    ///
    /// An empty connection list is allowed. Routing a key through an empty
    /// client returns [`MemcachedError::ClientConfig`].
    ///
    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientModN, Connection, ServerSelector, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientModN::new(vec![
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    /// ]);
    /// assert_eq!(client.len(), 2);
    /// assert_eq!(
    ///     client.set(b"doc_modn_new", 0, 0, false, b"value").await?,
    ///     StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client.get(b"doc_modn_new").await?.unwrap().data_block,
    ///     b"value"
    /// );
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn new(conns: Vec<Connection>) -> Self {
        Self::with_hasher(conns, SeedableState::fixed())
    }

    /// Creates a modulo client using `rapidhash` with a custom `seed`.
    ///
    /// Sharing the seed across application replicas keeps key routing identical
    /// between them. This is useful when cache keys are derived from untrusted
    /// input.
    ///
    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientModN, Connection, ServerSelector, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientModN::with_seed(
    ///     vec![
    ///         Connection::default().await?,
    ///         Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     ],
    ///     42,
    /// );
    /// assert_eq!(
    ///     client.set(b"doc_modn_with_seed", 0, 0, false, b"value").await?,
    ///     StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client.get(b"doc_modn_with_seed").await?.unwrap().data_block,
    ///     b"value"
    /// );
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn with_seed(conns: Vec<Connection>, seed: u64) -> Self {
        Self::with_hasher(conns, SeedableState::new(seed))
    }
}

impl<S: BuildHasher> ClientModN<S> {
    /// Creates a modulo client with a custom [`BuildHasher`].
    ///
    /// # Example
    ///
    /// ```
    /// # use std::collections::hash_map::DefaultHasher;
    /// # use std::hash::BuildHasherDefault;
    /// # use mcmc_rs::{ClientModN, Connection, ServerSelector, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let hasher = BuildHasherDefault::<DefaultHasher>::default();
    /// let mut client = ClientModN::with_hasher(
    ///     vec![
    ///         Connection::default().await?,
    ///         Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     ],
    ///     hasher,
    /// );
    /// assert_eq!(
    ///     client.set(b"doc_modn_with_hasher", 0, 0, false, b"value").await?,
    ///     StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client
    ///         .get(b"doc_modn_with_hasher")
    ///         .await?
    ///         .unwrap()
    ///         .data_block,
    ///     b"value"
    /// );
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn with_hasher(conns: Vec<Connection>, hasher: S) -> Self {
        Self { conns, hasher }
    }

    pub fn len(&self) -> usize {
        self.conns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.conns.is_empty()
    }
}

impl<S: BuildHasher + Send + Sync> ServerSelector for ClientModN<S> {
    fn select_server(&mut self, key: &[u8]) -> Result<&mut Connection> {
        if self.conns.is_empty() {
            return Err(no_servers_err());
        }
        let index = (self.hasher.hash_one(key) as usize) % self.conns.len();
        Ok(&mut self.conns[index])
    }

    fn select_servers<'a>(
        &mut self,
        keys: &'a [&'a [u8]],
    ) -> Result<Vec<(&mut Connection, Vec<&'a [u8]>)>> {
        if self.conns.is_empty() {
            return Err(no_servers_err());
        }
        let mut grouped: HashMap<usize, Vec<&'a [u8]>> = HashMap::new();
        for &key in keys {
            let index = (self.hasher.hash_one(key) as usize) % self.conns.len();
            grouped.entry(index).or_default().push(key);
        }
        Ok(self
            .conns
            .iter_mut()
            .enumerate()
            .filter_map(|(i, conn)| grouped.remove(&i).map(|shard_keys| (conn, shard_keys)))
            .collect())
    }
}

const DEFAULT_WEIGHT: usize = 160;

#[derive(Hash, PartialEq, Eq, Clone)]
struct VNode(String, usize);

/// Distributes keys across connections with a consistent hash ring.
///
/// Every server is represented by `weight` virtual nodes on the ring. The
/// default weight is 160, so adding or removing a server remaps
/// only the keys that belong to that server. Keys are hashed with `rapidhash`
/// and a fixed seed by default; a custom seed or any [`std::hash::BuildHasher`]
/// can be supplied instead.
///
/// Use [`ClientHashRing::new`], [`ClientHashRing::with_seed`],
/// [`ClientHashRing::with_weights`], [`ClientHashRing::with_hasher`] or
/// [`ClientHashRing::with_weights_and_hasher`] to build one.
pub struct ClientHashRing<S = SeedableState<'static>> {
    conns: HashMap<String, Connection>,
    weights: HashMap<String, usize>,
    ring: HashRing<VNode, S>,
}

impl Default for ClientHashRing {
    /// Creates an empty hash ring.
    ///
    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::ClientHashRing;
    /// let client = ClientHashRing::default();
    /// assert!(client.is_empty());
    /// assert_eq!(client.len(), 0);
    /// ```
    fn default() -> Self {
        Self::new(std::iter::empty::<(String, Connection)>())
    }
}

impl ClientHashRing {
    /// Creates a consistent hash ring using `rapidhash` with a fixed default
    /// seed and the default weight for every server.
    ///
    /// An empty node list is allowed. Routing a key through an empty ring
    /// returns [`MemcachedError::ClientConfig`].
    ///
    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientHashRing, Connection, ServerSelector, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientHashRing::new([
    ///     ("node1", Connection::default().await?),
    ///     ("node2", Connection::unix_connect("/tmp/memcached0.sock").await?),
    /// ]);
    /// assert_eq!(client.len(), 2);
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_hash_ring_new", 0, 0, false, b"value")
    ///         .await?,
    ///     StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client.get(b"doc_hash_ring_new").await?.unwrap().data_block,
    ///     b"value"
    /// );
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn new<I, K>(nodes: I) -> Self
    where
        I: IntoIterator<Item = (K, Connection)>,
        K: Into<String>,
    {
        Self::with_hasher(nodes, SeedableState::fixed())
    }

    /// Creates a consistent hash ring using `rapidhash` with a custom `seed`
    /// and the default weight for every server.
    ///
    /// Sharing the seed across application replicas keeps key routing identical
    /// between them.
    ///
    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientHashRing, Connection, ServerSelector, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientHashRing::with_seed(
    ///     [
    ///         ("node1", Connection::default().await?),
    ///         ("node2", Connection::unix_connect("/tmp/memcached0.sock").await?),
    ///     ],
    ///     42,
    /// );
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_hash_ring_with_seed", 0, 0, false, b"value")
    ///         .await?,
    ///     StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client
    ///         .get(b"doc_hash_ring_with_seed")
    ///         .await?
    ///         .unwrap()
    ///         .data_block,
    ///     b"value"
    /// );
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn with_seed<I, K>(nodes: I, seed: u64) -> Self
    where
        I: IntoIterator<Item = (K, Connection)>,
        K: Into<String>,
    {
        Self::with_hasher(nodes, SeedableState::new(seed))
    }

    /// Creates a consistent hash ring with explicit per-server weights using
    /// `rapidhash` and a fixed default seed.
    ///
    /// A larger weight places more virtual nodes on the ring and therefore
    /// receives a larger share of the keys. Weights must be greater than zero.
    ///
    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientHashRing, Connection};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let client = ClientHashRing::with_weights([
    ///     ("node1", Connection::default().await?, 100),
    ///     ("node2", Connection::unix_connect("/tmp/memcached0.sock").await?, 200),
    /// ]);
    /// assert_eq!(client.len(), 2);
    /// let node = client.server_for_key(b"doc_hash_ring_with_weights").unwrap();
    /// assert!(node == "node1" || node == "node2");
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn with_weights<I, K>(nodes: I) -> Self
    where
        I: IntoIterator<Item = (K, Connection, usize)>,
        K: Into<String>,
    {
        Self::with_weights_and_hasher(nodes, SeedableState::fixed())
    }
}

impl<S: BuildHasher> ClientHashRing<S> {
    /// Creates a consistent hash ring with a custom [`BuildHasher`] and the
    /// default weight for every server.
    ///
    /// # Example
    ///
    /// ```
    /// # use std::collections::hash_map::DefaultHasher;
    /// # use std::hash::BuildHasherDefault;
    /// # use mcmc_rs::{ClientHashRing, Connection, ServerSelector, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let hasher = BuildHasherDefault::<DefaultHasher>::default();
    /// let mut client = ClientHashRing::with_hasher(
    ///     [
    ///         ("node1", Connection::default().await?),
    ///         ("node2", Connection::unix_connect("/tmp/memcached0.sock").await?),
    ///     ],
    ///     hasher,
    /// );
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_hash_ring_with_hasher", 0, 0, false, b"value")
    ///         .await?,
    ///     StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client
    ///         .get(b"doc_hash_ring_with_hasher")
    ///         .await?
    ///         .unwrap()
    ///         .data_block,
    ///     b"value"
    /// );
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn with_hasher<I, K>(nodes: I, hasher: S) -> Self
    where
        I: IntoIterator<Item = (K, Connection)>,
        K: Into<String>,
    {
        Self::with_weights_and_hasher(
            nodes.into_iter().map(|(k, c)| (k, c, DEFAULT_WEIGHT)),
            hasher,
        )
    }

    /// Creates a consistent hash ring with explicit per-server weights and a
    /// custom [`BuildHasher`].
    ///
    /// Weights must be greater than zero.
    ///
    /// # Example
    ///
    /// ```
    /// # use std::collections::hash_map::DefaultHasher;
    /// # use std::hash::BuildHasherDefault;
    /// # use mcmc_rs::{ClientHashRing, Connection};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let hasher = BuildHasherDefault::<DefaultHasher>::default();
    /// let client = ClientHashRing::with_weights_and_hasher(
    ///     [
    ///         ("node1", Connection::default().await?, 100),
    ///         ("node2", Connection::unix_connect("/tmp/memcached0.sock").await?, 200),
    ///     ],
    ///     hasher,
    /// );
    /// assert_eq!(client.len(), 2);
    /// assert!(client.server_for_key(b"doc_hash_ring_weights_hasher").is_some());
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn with_weights_and_hasher<I, K>(nodes: I, hasher: S) -> Self
    where
        I: IntoIterator<Item = (K, Connection, usize)>,
        K: Into<String>,
    {
        let mut this = Self {
            conns: HashMap::new(),
            weights: HashMap::new(),
            ring: HashRing::with_hasher(hasher),
        };
        for (name, conn, weight) in nodes {
            this.add_server_with_weight(name, conn, weight);
        }
        this
    }

    pub fn add_server(&mut self, name: impl Into<String>, conn: Connection) -> Option<Connection> {
        let name = name.into();
        let weight = self.weights.get(&name).copied().unwrap_or(DEFAULT_WEIGHT);
        self.add_server_with_weight(name, conn, weight)
    }

    pub fn add_server_with_weight(
        &mut self,
        name: impl Into<String>,
        conn: Connection,
        weight: usize,
    ) -> Option<Connection> {
        assert!(weight > 0, "weight must be greater than 0");
        let name = name.into();
        let prev = self.conns.insert(name.clone(), conn);
        match self.weights.insert(name.clone(), weight) {
            Some(old_weight) if old_weight != weight => {
                for i in 0..old_weight {
                    self.ring.remove(&VNode(name.clone(), i));
                }
                for i in 0..weight {
                    self.ring.add(VNode(name.clone(), i));
                }
            }
            None => {
                for i in 0..weight {
                    self.ring.add(VNode(name.clone(), i));
                }
            }
            Some(_) => {}
        }
        prev
    }

    pub fn remove_server(&mut self, name: &str) -> Option<Connection> {
        let conn = self.conns.remove(name)?;
        if let Some(weight) = self.weights.remove(name) {
            for i in 0..weight {
                self.ring.remove(&VNode(name.to_owned(), i));
            }
        }
        Some(conn)
    }

    pub fn len(&self) -> usize {
        self.conns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.conns.is_empty()
    }

    pub fn contains_server(&self, name: &str) -> bool {
        self.conns.contains_key(name)
    }

    pub fn servers(&self) -> impl Iterator<Item = &str> {
        self.conns.keys().map(String::as_str)
    }

    pub fn server_for_key(&self, key: &[u8]) -> Option<&str> {
        self.ring.get(&key).map(|vnode| vnode.0.as_str())
    }
}

impl<S: BuildHasher + Send + Sync> ServerSelector for ClientHashRing<S> {
    fn select_server(&mut self, key: &[u8]) -> Result<&mut Connection> {
        let node = self.ring.get(&key).ok_or_else(no_servers_err)?;
        self.conns.get_mut(&node.0).ok_or_else(no_servers_err)
    }

    fn select_servers<'a>(
        &mut self,
        keys: &'a [&'a [u8]],
    ) -> Result<Vec<(&mut Connection, Vec<&'a [u8]>)>> {
        if self.conns.is_empty() {
            return Err(no_servers_err());
        }
        let mut grouped: HashMap<&str, Vec<&'a [u8]>> = HashMap::new();
        for &key in keys {
            let node = self.ring.get(&key).ok_or_else(no_servers_err)?;
            grouped.entry(node.0.as_str()).or_default().push(key);
        }
        Ok(self
            .conns
            .iter_mut()
            .filter_map(|(name, conn)| {
                grouped
                    .remove(name.as_str())
                    .map(|shard_keys| (conn, shard_keys))
            })
            .collect())
    }
}

/// Distributes keys across connections with rendezvous (highest random
/// weight) hashing.
///
/// For each key every server is scored independently and the highest score
/// wins, so adding or removing a server remaps only the keys owned by that
/// server. Keys are hashed with `rapidhash` and a fixed seed by default; a
/// custom seed or any [`std::hash::BuildHasher`] can be supplied instead.
///
/// Use [`ClientRendezvous::new`], [`ClientRendezvous::with_seed`] or
/// [`ClientRendezvous::with_hasher`] to build one.
pub struct ClientRendezvous<S = SeedableState<'static>> {
    conns: HashMap<String, Connection>,
    hrw: Rendezvous<String, S>,
}

impl Default for ClientRendezvous {
    /// Creates an empty rendezvous client.
    ///
    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::ClientRendezvous;
    /// let client = ClientRendezvous::default();
    /// assert!(client.is_empty());
    /// assert_eq!(client.len(), 0);
    /// ```
    fn default() -> Self {
        Self::new(std::iter::empty::<(String, Connection)>())
    }
}

impl ClientRendezvous {
    /// Creates a rendezvous (HRW) client using `rapidhash` with a fixed default
    /// seed.
    ///
    /// An empty node list is allowed. Routing a key through an empty client
    /// returns [`MemcachedError::ClientConfig`].
    ///
    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientRendezvous, Connection, ServerSelector, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientRendezvous::new([
    ///     ("node1", Connection::default().await?),
    ///     ("node2", Connection::unix_connect("/tmp/memcached0.sock").await?),
    /// ]);
    /// assert_eq!(client.len(), 2);
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_rendezvous_new", 0, 0, false, b"value")
    ///         .await?,
    ///     StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client.get(b"doc_rendezvous_new").await?.unwrap().data_block,
    ///     b"value"
    /// );
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn new<I, K>(nodes: I) -> Self
    where
        I: IntoIterator<Item = (K, Connection)>,
        K: Into<String>,
    {
        Self::with_hasher(nodes, SeedableState::fixed())
    }

    /// Creates a rendezvous (HRW) client using `rapidhash` with a custom
    /// `seed`.
    ///
    /// Sharing the seed across application replicas keeps key routing identical
    /// between them.
    ///
    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{ClientRendezvous, Connection, ServerSelector, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut client = ClientRendezvous::with_seed(
    ///     [
    ///         ("node1", Connection::default().await?),
    ///         ("node2", Connection::unix_connect("/tmp/memcached0.sock").await?),
    ///     ],
    ///     42,
    /// );
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_rendezvous_with_seed", 0, 0, false, b"value")
    ///         .await?,
    ///     StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client
    ///         .get(b"doc_rendezvous_with_seed")
    ///         .await?
    ///         .unwrap()
    ///         .data_block,
    ///     b"value"
    /// );
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn with_seed<I, K>(nodes: I, seed: u64) -> Self
    where
        I: IntoIterator<Item = (K, Connection)>,
        K: Into<String>,
    {
        Self::with_hasher(nodes, SeedableState::new(seed))
    }
}

impl<S: BuildHasher + Clone> ClientRendezvous<S> {
    /// Creates a rendezvous (HRW) client with a custom [`BuildHasher`].
    ///
    /// # Example
    ///
    /// ```
    /// # use std::collections::hash_map::DefaultHasher;
    /// # use std::hash::BuildHasherDefault;
    /// # use mcmc_rs::{ClientRendezvous, Connection, ServerSelector, StorageResult};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let hasher = BuildHasherDefault::<DefaultHasher>::default();
    /// let mut client = ClientRendezvous::with_hasher(
    ///     [
    ///         ("node1", Connection::default().await?),
    ///         ("node2", Connection::unix_connect("/tmp/memcached0.sock").await?),
    ///     ],
    ///     hasher,
    /// );
    /// assert_eq!(
    ///     client
    ///         .set(b"doc_rendezvous_with_hasher", 0, 0, false, b"value")
    ///         .await?,
    ///     StorageResult::Stored
    /// );
    /// assert_eq!(
    ///     client
    ///         .get(b"doc_rendezvous_with_hasher")
    ///         .await?
    ///         .unwrap()
    ///         .data_block,
    ///     b"value"
    /// );
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn with_hasher<I, K>(nodes: I, hasher: S) -> Self
    where
        I: IntoIterator<Item = (K, Connection)>,
        K: Into<String>,
    {
        let mut conns = HashMap::new();
        let mut names = Vec::new();
        for (name, conn) in nodes {
            let name = name.into();
            if conns.insert(name.clone(), conn).is_none() {
                names.push(name);
            }
        }
        let hrw = Rendezvous::from_nodes_and_hasher(names, hasher);
        Self { conns, hrw }
    }

    pub fn add_server(&mut self, name: impl Into<String>, conn: Connection) -> Option<Connection> {
        let name = name.into();
        let prev = self.conns.insert(name.clone(), conn);
        if prev.is_none() {
            self.hrw.add_node(name);
        }
        prev
    }

    pub fn remove_server(&mut self, name: &str) -> Option<Connection> {
        let conn = self.conns.remove(name)?;
        self.hrw.remove_node(&name.to_owned());
        Some(conn)
    }

    pub fn len(&self) -> usize {
        self.conns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.conns.is_empty()
    }

    pub fn contains_server(&self, name: &str) -> bool {
        self.conns.contains_key(name)
    }

    pub fn servers(&self) -> impl Iterator<Item = &str> {
        self.conns.keys().map(String::as_str)
    }

    pub fn server_for_key(&self, key: &[u8]) -> Option<&str> {
        self.hrw.pick_top(&key).map(String::as_str)
    }
}

impl<S: BuildHasher + Clone + Send + Sync> ServerSelector for ClientRendezvous<S> {
    fn select_server(&mut self, key: &[u8]) -> Result<&mut Connection> {
        let node = self.hrw.pick_top(&key).ok_or_else(no_servers_err)?;
        self.conns.get_mut(node).ok_or_else(no_servers_err)
    }

    fn select_servers<'a>(
        &mut self,
        keys: &'a [&'a [u8]],
    ) -> Result<Vec<(&mut Connection, Vec<&'a [u8]>)>> {
        if self.conns.is_empty() {
            return Err(no_servers_err());
        }
        let mut grouped: HashMap<&str, Vec<&'a [u8]>> = HashMap::new();
        for &key in keys {
            let node = self.hrw.pick_top(&key).ok_or_else(no_servers_err)?;
            grouped.entry(node.as_str()).or_default().push(key);
        }
        Ok(self
            .conns
            .iter_mut()
            .filter_map(|(name, conn)| {
                grouped
                    .remove(name.as_str())
                    .map(|shard_keys| (conn, shard_keys))
            })
            .collect())
    }
}

/// A batch of commands executed in one round trip.
///
/// Builder methods return `Self` and validate their input immediately. The
/// first validation error is stored in the pipeline and returned by
/// [`Pipeline::execute`].
pub struct Pipeline<'a>(&'a mut Connection, Result<Vec<(Vec<u8>, PipelineExpect)>>);
impl<'a> Pipeline<'a> {
    fn push_cmd(&mut self, cmd: Result<Vec<u8>>, expect: PipelineExpect) {
        if let Ok(cmds) = &mut self.1 {
            match cmd {
                Ok(bytes) => cmds.push((bytes, expect)),
                Err(err) => self.1 = Err(err),
            }
        }
    }

    /// Sends all queued commands and returns one result per command.
    ///
    /// If any builder method rejected its input (for example, an empty key),
    /// the stored [`MemcachedError::InvalidInput`] is returned and no commands
    /// are sent. An empty pipeline returns an empty vector.
    ///
    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, Item, PipelineResponse, StorageResult, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// for mut c in [
    ///     Connection::default().await?,
    ///     Connection::unix_connect("/tmp/memcached0.sock").await?,
    ///     Connection::tls_connect(
    ///         "localhost",
    ///         11216,
    ///         TlsConfig::with_additional_ca("sets/ca.crt")
    ///             .with_client_identity("sets/client.crt", "sets/client.key"),
    ///     )
    ///     .await?,
    /// ] {
    ///     let result = c
    ///         .pipeline()
    ///         .set(b"doc_pipeline_execute", 0, 0, false, b"value")
    ///         .get(b"doc_pipeline_execute")
    ///         .execute()
    ///         .await?;
    ///     let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
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
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub async fn execute(self) -> Result<Vec<Result<PipelineResponse>>> {
        let cmds = self.1?;
        if cmds.is_empty() {
            return Ok(Vec::new());
        };
        match self.0 {
            Connection::Tcp(s) => execute_cmd(s, &cmds).await,
            #[cfg(unix)]
            Connection::Unix(s) => execute_cmd(s, &cmds).await,
            Connection::Udp(_s, _r) => panic!("pipeline not work with udp!"),
            #[cfg(any(
                feature = "smol-rustls-tls",
                feature = "tokio-rustls-tls",
                feature = "smol-native-tls-tls",
                feature = "tokio-native-tls-tls"
            ))]
            Connection::Tls(s) => execute_cmd(s, &cmds).await,
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
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
    /// match &result[0] {
    ///     PipelineResponse::String(version) => assert!(!version.is_empty()),
    ///     other => panic!("unexpected response: {other:?}"),
    /// }
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn version(mut self) -> Self {
        self.push_cmd(Ok(build_version_cmd().to_vec()), PipelineExpect::Version);
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
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
    /// assert_eq!(result, [PipelineResponse::Unit(())]);
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn cache_memlimit(mut self, limit: usize, noreply: bool) -> Self {
        self.push_cmd(
            Ok(build_cache_memlimit_cmd(limit, noreply).to_vec()),
            PipelineExpect::Ok { noreply },
        );
        self
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, PipelineResponse, StorageResult, TlsConfig};
    /// # use smol::block_on;
    /// #
    /// # block_on(async {
    /// let mut conn = Connection::tls_connect(
    ///     "localhost",
    ///     11216,
    ///     TlsConfig::with_additional_ca("sets/ca.crt")
    ///         .with_client_identity("sets/client.crt", "sets/client.key"),
    /// )
    /// .await?;
    /// assert_eq!(
    ///     conn.set(b"doc_pipeline_flush_all", 0, 0, false, b"value")
    ///         .await?,
    ///     StorageResult::Stored
    /// );
    /// let result = conn.pipeline().flush_all(Some(999), false).execute().await?;
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
    /// assert_eq!(result, [PipelineResponse::Unit(())]);
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn flush_all(mut self, exptime: Option<i64>, noreply: bool) -> Self {
        self.push_cmd(
            Ok(build_flush_all_cmd(exptime, noreply).to_vec()),
            PipelineExpect::Ok { noreply },
        );
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
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
    /// assert_eq!(
    ///     result,
    ///     [PipelineResponse::StorageResult(StorageResult::Stored)]
    /// );
    /// let result = conn
    ///     .pipeline()
    ///     .set(b"doc_pipeline_set_noreply", 0, 0, true, b"value")
    ///     .execute()
    ///     .await?;
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
    /// assert_eq!(
    ///     result,
    ///     [PipelineResponse::StorageResult(StorageResult::NoReply)]
    /// );
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
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
        self.push_cmd(
            build_storage_cmd(
                b"set",
                key.as_ref(),
                flags,
                exptime,
                None,
                noreply,
                data_block.as_ref(),
            ),
            PipelineExpect::Storage { noreply },
        );
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
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
    /// assert_eq!(
    ///     result,
    ///     [
    ///         PipelineResponse::StorageResult(StorageResult::Stored),
    ///         PipelineResponse::StorageResult(StorageResult::NotStored),
    ///     ]
    /// );
    /// let result = conn
    ///     .pipeline()
    ///     .add(b"doc_pipeline_add_noreply", 0, 0, true, b"value")
    ///     .execute()
    ///     .await?;
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
    /// assert_eq!(
    ///     result,
    ///     [PipelineResponse::StorageResult(StorageResult::NoReply)]
    /// );
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
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
        self.push_cmd(
            build_storage_cmd(
                b"add",
                key.as_ref(),
                flags,
                exptime,
                None,
                noreply,
                data_block.as_ref(),
            ),
            PipelineExpect::Storage { noreply },
        );
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
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
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
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
    /// assert_eq!(
    ///     result,
    ///     [PipelineResponse::StorageResult(StorageResult::Stored)]
    /// );
    /// let result = conn
    ///     .pipeline()
    ///     .replace(b"doc_pipeline_replace", 0, 0, true, b"noreply")
    ///     .execute()
    ///     .await?;
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
    /// assert_eq!(
    ///     result,
    ///     [PipelineResponse::StorageResult(StorageResult::NoReply)]
    /// );
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
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
        self.push_cmd(
            build_storage_cmd(
                b"replace",
                key.as_ref(),
                flags,
                exptime,
                None,
                noreply,
                data_block.as_ref(),
            ),
            PipelineExpect::Storage { noreply },
        );
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
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
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
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
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
    /// let result = conn
    ///     .pipeline()
    ///     .append(b"doc_pipeline_append", 0, 0, true, b"-noreply")
    ///     .execute()
    ///     .await?;
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
    /// assert_eq!(
    ///     result,
    ///     [PipelineResponse::StorageResult(StorageResult::NoReply)]
    /// );
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
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
        self.push_cmd(
            build_storage_cmd(
                b"append",
                key.as_ref(),
                flags,
                exptime,
                None,
                noreply,
                data_block.as_ref(),
            ),
            PipelineExpect::Storage { noreply },
        );
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
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
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
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
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
    /// let result = conn
    ///     .pipeline()
    ///     .prepend(b"doc_pipeline_prepend", 0, 0, true, b"noreply-")
    ///     .execute()
    ///     .await?;
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
    /// assert_eq!(
    ///     result,
    ///     [PipelineResponse::StorageResult(StorageResult::NoReply)]
    /// );
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
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
        self.push_cmd(
            build_storage_cmd(
                b"prepend",
                key.as_ref(),
                flags,
                exptime,
                None,
                noreply,
                data_block.as_ref(),
            ),
            PipelineExpect::Storage { noreply },
        );
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
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
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
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
    /// assert_eq!(
    ///     result,
    ///     [PipelineResponse::StorageResult(StorageResult::Stored)]
    /// );
    /// let cas = conn
    ///     .gets(b"doc_pipeline_cas")
    ///     .await?
    ///     .unwrap()
    ///     .cas_unique
    ///     .unwrap();
    /// let result = conn
    ///     .pipeline()
    ///     .cas(b"doc_pipeline_cas", 0, 0, cas, true, b"noreply")
    ///     .execute()
    ///     .await?;
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
    /// assert_eq!(
    ///     result,
    ///     [PipelineResponse::StorageResult(StorageResult::NoReply)]
    /// );
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
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
        self.push_cmd(
            build_storage_cmd(
                b"cas",
                key.as_ref(),
                flags,
                exptime,
                Some(cas_unique),
                noreply,
                data_block.as_ref(),
            ),
            PipelineExpect::Storage { noreply },
        );
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
    /// let result = result
    ///     .into_iter()
    ///     .collect::<mcmc_rs::Result<Vec<_>>>()?;
    /// assert_eq!(result[0], PipelineResponse::Unit(()));
    /// assert_matches!(&result[1], PipelineResponse::String(version) if !version.is_empty());
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn auth(mut self, username: impl AsRef<[u8]>, password: impl AsRef<[u8]>) -> Self {
        self.push_cmd(
            Ok(build_auth_cmd(username.as_ref(), password.as_ref())),
            PipelineExpect::Auth,
        );
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
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
    /// assert_eq!(
    ///     result,
    ///     [
    ///         PipelineResponse::DeleteResult(DeleteResult::Deleted),
    ///         PipelineResponse::DeleteResult(DeleteResult::NotFound),
    ///     ]
    /// );
    /// let result = conn
    ///     .pipeline()
    ///     .delete(b"doc_pipeline_delete", true)
    ///     .execute()
    ///     .await?;
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
    /// assert_eq!(
    ///     result,
    ///     [PipelineResponse::DeleteResult(DeleteResult::NoReply)]
    /// );
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn delete(mut self, key: impl AsRef<[u8]>, noreply: bool) -> Self {
        self.push_cmd(
            build_delete_cmd(key.as_ref(), noreply),
            PipelineExpect::Delete { noreply },
        );
        self
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, IncrDecrResult, PipelineResponse, StorageResult};
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
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
    /// assert_eq!(
    ///     result,
    ///     [PipelineResponse::IncrDecrResult(IncrDecrResult::NotFound)]
    /// );
    /// conn.set(b"doc_pipeline_incr", 0, 0, false, b"10").await?;
    /// let result = conn
    ///     .pipeline()
    ///     .incr(b"doc_pipeline_incr", 5, false)
    ///     .execute()
    ///     .await?;
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
    /// assert_eq!(
    ///     result,
    ///     [PipelineResponse::IncrDecrResult(IncrDecrResult::Value(15))]
    /// );
    /// let result = conn
    ///     .pipeline()
    ///     .incr(b"doc_pipeline_incr", 1, true)
    ///     .execute()
    ///     .await?;
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
    /// assert_eq!(
    ///     result,
    ///     [PipelineResponse::IncrDecrResult(IncrDecrResult::NoReply)]
    /// );
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn incr(mut self, key: impl AsRef<[u8]>, value: u64, noreply: bool) -> Self {
        self.push_cmd(
            build_incr_decr_cmd(b"incr", key.as_ref(), value, noreply),
            PipelineExpect::IncrDecr { noreply },
        );
        self
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, IncrDecrResult, PipelineResponse, StorageResult};
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
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
    /// assert_eq!(
    ///     result,
    ///     [PipelineResponse::IncrDecrResult(IncrDecrResult::NotFound)]
    /// );
    /// conn.set(b"doc_pipeline_decr", 0, 0, false, b"10").await?;
    /// let result = conn
    ///     .pipeline()
    ///     .decr(b"doc_pipeline_decr", 5, false)
    ///     .execute()
    ///     .await?;
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
    /// assert_eq!(
    ///     result,
    ///     [PipelineResponse::IncrDecrResult(IncrDecrResult::Value(5))]
    /// );
    /// let result = conn
    ///     .pipeline()
    ///     .decr(b"doc_pipeline_decr", 1, true)
    ///     .execute()
    ///     .await?;
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
    /// assert_eq!(
    ///     result,
    ///     [PipelineResponse::IncrDecrResult(IncrDecrResult::NoReply)]
    /// );
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn decr(mut self, key: impl AsRef<[u8]>, value: u64, noreply: bool) -> Self {
        self.push_cmd(
            build_incr_decr_cmd(b"decr", key.as_ref(), value, noreply),
            PipelineExpect::IncrDecr { noreply },
        );
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
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
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
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
    /// assert_eq!(
    ///     result,
    ///     [PipelineResponse::TouchResult(TouchResult::Touched)]
    /// );
    /// let result = conn
    ///     .pipeline()
    ///     .touch(b"doc_pipeline_touch", 60, true)
    ///     .execute()
    ///     .await?;
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
    /// assert_eq!(
    ///     result,
    ///     [PipelineResponse::TouchResult(TouchResult::NoReply)]
    /// );
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn touch(mut self, key: impl AsRef<[u8]>, exptime: i64, noreply: bool) -> Self {
        self.push_cmd(
            build_touch_cmd(key.as_ref(), exptime, noreply),
            PipelineExpect::Touch { noreply },
        );
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
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
    /// assert_eq!(result, [PipelineResponse::OptionItem(None)]);
    /// conn.set(b"doc_pipeline_get", 0, 0, false, b"value").await?;
    /// let result = conn.pipeline().get(b"doc_pipeline_get").execute().await?;
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
    /// assert_eq!(
    ///     result,
    ///     [PipelineResponse::OptionItem(Some(Item {
    ///         key: "doc_pipeline_get".to_string(),
    ///         flags: 0,
    ///         cas_unique: None,
    ///         data_block: b"value".to_vec()
    ///     }))]
    /// );
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn get(mut self, key: impl AsRef<[u8]>) -> Self {
        self.push_cmd(
            build_retrieval_cmd(b"get", None, &[key.as_ref()]),
            PipelineExpect::RetrievalOne,
        );
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
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
    /// match &result[0] {
    ///     PipelineResponse::OptionItem(Some(item)) => {
    ///         assert_eq!(item.key, "doc_pipeline_gets");
    ///         assert_eq!(item.data_block, b"value");
    ///         assert!(item.cas_unique.is_some());
    ///     }
    ///     other => panic!("unexpected response: {other:?}"),
    /// }
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn gets(mut self, key: impl AsRef<[u8]>) -> Self {
        self.push_cmd(
            build_retrieval_cmd(b"gets", None, &[key.as_ref()]),
            PipelineExpect::RetrievalOne,
        );
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
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
    /// match &result[0] {
    ///     PipelineResponse::OptionItem(Some(item)) => {
    ///         assert_eq!(item.key, "doc_pipeline_gat");
    ///         assert_eq!(item.flags, 7);
    ///         assert_eq!(item.data_block, b"value");
    ///         assert_eq!(item.cas_unique, None);
    ///     }
    ///     other => panic!("unexpected response: {other:?}"),
    /// }
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn gat(mut self, exptime: i64, key: impl AsRef<[u8]>) -> Self {
        self.push_cmd(
            build_retrieval_cmd(b"gat", Some(exptime), &[key.as_ref()]),
            PipelineExpect::RetrievalOne,
        );
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
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
    /// match &result[0] {
    ///     PipelineResponse::OptionItem(Some(item)) => {
    ///         assert_eq!(item.key, "doc_pipeline_gats");
    ///         assert_eq!(item.flags, 7);
    ///         assert_eq!(item.data_block, b"value");
    ///         assert!(item.cas_unique.is_some());
    ///     }
    ///     other => panic!("unexpected response: {other:?}"),
    /// }
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn gats(mut self, exptime: i64, key: impl AsRef<[u8]>) -> Self {
        self.push_cmd(
            build_retrieval_cmd(b"gats", Some(exptime), &[key.as_ref()]),
            PipelineExpect::RetrievalOne,
        );
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
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
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
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
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
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn get_multi(mut self, keys: &[impl AsRef<[u8]>]) -> Self {
        self.push_cmd(
            build_retrieval_cmd(
                b"get",
                None,
                &keys.iter().map(|x| x.as_ref()).collect::<Vec<&[u8]>>(),
            ),
            PipelineExpect::RetrievalMulti,
        );
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
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
    /// match &result[0] {
    ///     PipelineResponse::VecItem(items) => {
    ///         assert_eq!(items.len(), 2);
    ///         assert_eq!(items[0].key, "doc_pipeline_gets_multi_a");
    ///         assert_eq!(items[1].key, "doc_pipeline_gets_multi_b");
    ///         assert!(items.iter().all(|item| item.cas_unique.is_some()));
    ///     }
    ///     other => panic!("unexpected response: {other:?}"),
    /// }
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn gets_multi(mut self, keys: &[impl AsRef<[u8]>]) -> Self {
        self.push_cmd(
            build_retrieval_cmd(
                b"gets",
                None,
                &keys.iter().map(|x| x.as_ref()).collect::<Vec<&[u8]>>(),
            ),
            PipelineExpect::RetrievalMulti,
        );
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
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
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
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn gat_multi(mut self, exptime: i64, keys: &[impl AsRef<[u8]>]) -> Self {
        self.push_cmd(
            build_retrieval_cmd(
                b"gat",
                Some(exptime),
                &keys.iter().map(|x| x.as_ref()).collect::<Vec<&[u8]>>(),
            ),
            PipelineExpect::RetrievalMulti,
        );
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
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
    /// match &result[0] {
    ///     PipelineResponse::VecItem(items) => {
    ///         assert_eq!(items.len(), 2);
    ///         assert_eq!(items[0].key, "doc_pipeline_gats_multi_a");
    ///         assert_eq!(items[1].key, "doc_pipeline_gats_multi_b");
    ///         assert!(items.iter().all(|item| item.cas_unique.is_some()));
    ///     }
    ///     other => panic!("unexpected response: {other:?}"),
    /// }
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn gats_multi(mut self, exptime: i64, keys: &[impl AsRef<[u8]>]) -> Self {
        self.push_cmd(
            build_retrieval_cmd(
                b"gats",
                Some(exptime),
                &keys.iter().map(|x| x.as_ref()).collect::<Vec<&[u8]>>(),
            ),
            PipelineExpect::RetrievalMulti,
        );
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
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
    /// match &result[0] {
    ///     PipelineResponse::HashMap(stats) => {
    ///         assert!(stats.contains_key("pid"));
    ///         assert!(stats.contains_key("uptime"));
    ///         assert!(stats["uptime"].parse::<u64>().is_ok());
    ///     }
    ///     other => panic!("unexpected response: {other:?}"),
    /// }
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn stats(mut self, arg: Option<StatsArg>) -> Self {
        self.push_cmd(Ok(build_stats_cmd(arg).to_vec()), PipelineExpect::Stats);
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
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
    /// assert_eq!(result, [PipelineResponse::Unit(())]);
    /// let settings = conn.stats(Some(StatsArg::Settings)).await?;
    /// assert_eq!(settings.get("slab_automove").map(String::as_str), Some("0"));
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn slabs_automove(mut self, arg: SlabsAutomoveArg) -> Self {
        self.push_cmd(
            Ok(build_slabs_automove_cmd(arg).to_vec()),
            PipelineExpect::Ok { noreply: false },
        );
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
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn lru_crawler(mut self, arg: LruCrawlerArg) -> Self {
        self.push_cmd(
            Ok(build_lru_crawler_cmd(arg).to_vec()),
            PipelineExpect::Ok { noreply: false },
        );
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
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
    /// assert_eq!(result, [PipelineResponse::Unit(())]);
    /// let settings = conn.stats(Some(StatsArg::Settings)).await?;
    /// assert_eq!(
    ///     settings.get("lru_crawler_sleep").map(String::as_str),
    ///     Some("1000000")
    /// );
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn lru_crawler_sleep(mut self, microseconds: usize) -> Self {
        self.push_cmd(
            Ok(build_lru_crawler_sleep_cmd(microseconds)),
            PipelineExpect::Ok { noreply: false },
        );
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
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
    /// assert_eq!(result, [PipelineResponse::Unit(())]);
    /// let settings = conn.stats(Some(StatsArg::Settings)).await?;
    /// assert_eq!(
    ///     settings.get("lru_crawler_tocrawl").map(String::as_str),
    ///     Some("1")
    /// );
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn lru_crawler_tocrawl(mut self, arg: u32) -> Self {
        self.push_cmd(
            Ok(build_lru_crawler_tocrawl_cmd(arg)),
            PipelineExpect::Ok { noreply: false },
        );
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
    /// conn.pipeline()
    ///     .lru_crawler_crawl(LruCrawlerCrawlArg::All)
    ///     .execute()
    ///     .await?;
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn lru_crawler_crawl(mut self, arg: LruCrawlerCrawlArg<'_>) -> Self {
        self.push_cmd(
            Ok(build_lru_crawler_crawl_cmd(arg)),
            PipelineExpect::LruCrawler,
        );
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
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn slabs_reassign(mut self, source_class: isize, dest_class: isize) -> Self {
        self.push_cmd(
            Ok(build_slabs_reassign_cmd(source_class, dest_class)),
            PipelineExpect::SlabsReassign,
        );
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
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn lru_crawler_metadump(mut self, arg: LruCrawlerMetadumpArg<'_>) -> Self {
        self.push_cmd(
            Ok(build_lru_crawler_metadump_cmd(arg)),
            PipelineExpect::LruCrawlerMetaDump,
        );
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
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn lru_crawler_mgdump(mut self, arg: LruCrawlerMgdumpArg<'_>) -> Self {
        self.push_cmd(
            Ok(build_lru_crawler_mgdump_cmd(arg)),
            PipelineExpect::LruCrawlerMgDump,
        );
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
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
    /// assert_eq!(result, [PipelineResponse::Unit(())]);
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn mn(mut self) -> Self {
        self.push_cmd(Ok(build_mn_cmd().to_vec()), PipelineExpect::Mn);
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
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
    /// match &result[0] {
    ///     PipelineResponse::OptionString(Some(info)) => assert!(info.starts_with("doc_pipeline_me ")),
    ///     other => panic!("unexpected response: {other:?}"),
    /// }
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn me(mut self, key: impl AsRef<[u8]>) -> Self {
        self.push_cmd(build_me_cmd(key.as_ref()), PipelineExpect::Me);
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
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
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
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn mg(mut self, key: impl AsRef<[u8]>, flags: &[MgFlag]) -> Self {
        self.push_cmd(
            build_mg_flags(flags).and_then(|flags| build_mc_cmd(b"mg", key.as_ref(), &flags, None)),
            PipelineExpect::MetaGet,
        );
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
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
    /// match &result[0] {
    ///     PipelineResponse::MetaSet(item) => {
    ///         assert_eq!(item.status, StorageResult::Stored);
    ///         assert_eq!(item.key, Some("doc_pipeline_ms".to_string()));
    ///         assert_eq!(item.size, Some(5));
    ///     }
    ///     other => panic!("unexpected response: {other:?}"),
    /// }
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn ms(
        mut self,
        key: impl AsRef<[u8]>,
        flags: &[MsFlag],
        data_block: impl AsRef<[u8]>,
    ) -> Self {
        self.push_cmd(
            build_ms_flags(flags).and_then(|flags| {
                build_mc_cmd(b"ms", key.as_ref(), &flags, Some(data_block.as_ref()))
            }),
            PipelineExpect::MetaSet,
        );
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
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
    /// match &result[0] {
    ///     PipelineResponse::MetaDelete(item) => {
    ///         assert_eq!(item.status, MdStatus::Deleted);
    ///         assert_eq!(item.key, Some("doc_pipeline_md".to_string()));
    ///     }
    ///     other => panic!("unexpected response: {other:?}"),
    /// }
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn md(mut self, key: impl AsRef<[u8]>, flags: &[MdFlag]) -> Self {
        self.push_cmd(
            build_md_flags(flags).and_then(|flags| build_mc_cmd(b"md", key.as_ref(), &flags, None)),
            PipelineExpect::MetaDelete,
        );
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
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
    /// match &result[0] {
    ///     PipelineResponse::MetaArithmetic(item) => {
    ///         assert_eq!(item.status, MaStatus::WithBody);
    ///         assert_eq!(item.number, Some(5));
    ///         assert_eq!(item.key, Some("doc_pipeline_ma".to_string()));
    ///     }
    ///     other => panic!("unexpected response: {other:?}"),
    /// }
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn ma(mut self, key: impl AsRef<[u8]>, flags: &[MaFlag]) -> Self {
        self.push_cmd(
            build_ma_flags(flags).and_then(|flags| build_mc_cmd(b"ma", key.as_ref(), &flags, None)),
            PipelineExpect::MetaArithmetic,
        );
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
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn lru(mut self, arg: LruArg) -> Self {
        self.push_cmd(
            Ok(build_lru_cmd(arg)),
            PipelineExpect::Ok { noreply: false },
        );
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
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
    /// assert_eq!(result, [PipelineResponse::Unit(())]);
    /// #     Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn verbosity(mut self, arg: VerbosityArg, noreply: bool) -> Self {
        self.push_cmd(
            Ok(build_verbosity_cmd(arg, noreply)),
            PipelineExpect::Ok { noreply },
        );
        self
    }

    /// # Example
    ///
    /// ```
    /// # use mcmc_rs::{Connection, PipelineResponse};
    /// # use smol::block_on;
    /// # block_on(async {
    /// let mut conn = Connection::tcp_connect("127.0.0.1:11219").await?;
    /// let result = conn.pipeline().refresh_certs().execute().await?;
    /// let result = result.into_iter().collect::<mcmc_rs::Result<Vec<_>>>()?;
    /// assert_eq!(result, [PipelineResponse::Unit(())]);
    /// # Ok::<(), mcmc_rs::MemcachedError>(())
    /// # }).unwrap()
    /// ```
    pub fn refresh_certs(mut self) -> Self {
        self.push_cmd(
            Ok(build_refresh_certs_cmd().to_vec()),
            PipelineExpect::Ok { noreply: false },
        );
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
            assert_matches!(
                version_cmd(&mut c).await,
                Err(MemcachedError::Protocol(ProtocolError::UnknownCommand))
            );

            let mut c = Cursor::new(b"version\r\nCLIENT_ERROR client msg error\r\n".to_vec());
            assert_matches!(
                version_cmd(&mut c).await,
                Err(MemcachedError::Protocol(ProtocolError::Client(e)))
                    if e == "client msg error"
            );

            let mut c = Cursor::new(b"version\r\nSERVER_ERROR server msg error\r\n".to_vec());
            assert_matches!(
                version_cmd(&mut c).await,
                Err(MemcachedError::Protocol(ProtocolError::Server(e)))
                    if e == "server msg error"
            );

            let mut c = Cursor::new(b"version\r\nUNEXPECTED_ERROR msg error\r\n".to_vec());
            assert_matches!(
                version_cmd(&mut c).await,
                Err(MemcachedError::Unexpected(e)) if e == "UNEXPECTED_ERROR msg error\r\n"
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
                Err(MemcachedError::Protocol(ProtocolError::UnknownCommand))
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
            assert_matches!(
                flush_all_cmd(&mut c, None, false).await,
                Err(MemcachedError::Protocol(ProtocolError::UnknownCommand))
            );
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
                StorageResult::NoReply
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
                Err(MemcachedError::Protocol(ProtocolError::UnknownCommand))
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
                DeleteResult::NoReply
            );

            let mut c = Cursor::new(b"delete key\r\nERROR\r\n".to_vec());
            assert_matches!(
                delete_cmd(&mut c, b"key", false).await,
                Err(MemcachedError::Protocol(ProtocolError::UnknownCommand))
            );
        })
    }

    #[test]
    fn test_auth() {
        block_on(async {
            let mut c = Cursor::new(b"set _ _ _ 3\r\na b\r\nSTORED\r\n".to_vec());
            assert!(auth_cmd(&mut c, b"a", b"b").await.is_ok());

            let mut c = Cursor::new(b"set _ _ _ 3\r\na b\r\nERROR\r\n".to_vec());
            assert_matches!(
                auth_cmd(&mut c, b"a", b"b").await,
                Err(MemcachedError::Protocol(ProtocolError::UnknownCommand))
            );
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
                IncrDecrResult::Value(2)
            );

            let mut c = Cursor::new(b"incr key 1 noreply\r\n".to_vec());
            assert_eq!(
                incr_decr_cmd(&mut c, b"incr", b"key", 1, true)
                    .await
                    .unwrap(),
                IncrDecrResult::NoReply,
            );

            let mut c = Cursor::new(b"decr key 1\r\n2\r\n".to_vec());
            assert_eq!(
                incr_decr_cmd(&mut c, b"decr", b"key", 1, false)
                    .await
                    .unwrap(),
                IncrDecrResult::Value(2)
            );

            let mut c = Cursor::new(b"decr key 1 noreply\r\n".to_vec());
            assert_eq!(
                incr_decr_cmd(&mut c, b"decr", b"key", 1, true)
                    .await
                    .unwrap(),
                IncrDecrResult::NoReply,
            );

            let mut c = Cursor::new(b"incr key 1\r\nNOT_FOUND\r\n".to_vec());
            assert_eq!(
                incr_decr_cmd(&mut c, b"incr", b"key", 1, false)
                    .await
                    .unwrap(),
                IncrDecrResult::NotFound
            );

            let mut c = Cursor::new(b"incr key 1\r\nERROR\r\n".to_vec());
            assert_matches!(
                incr_decr_cmd(&mut c, b"incr", b"key", 1, false).await,
                Err(MemcachedError::Protocol(ProtocolError::UnknownCommand))
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
                TouchResult::NoReply
            );

            let mut c = Cursor::new(b"touch key 0\r\nERROR\r\n".to_vec());
            assert_matches!(
                touch_cmd(&mut c, b"key", 0, false).await,
                Err(MemcachedError::Protocol(ProtocolError::UnknownCommand))
            )
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
                Err(MemcachedError::Protocol(ProtocolError::UnknownCommand))
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
                Err(MemcachedError::Protocol(ProtocolError::UnknownCommand))
            );

            let mut c = Cursor::new(b"stats items\r\nERROR\r\n".to_vec());
            assert_matches!(
                stats_cmd(&mut c, Some(StatsArg::Items)).await,
                Err(MemcachedError::Protocol(ProtocolError::UnknownCommand))
            );

            let mut c = Cursor::new(b"stats sizes\r\nERROR\r\n".to_vec());
            assert_matches!(
                stats_cmd(&mut c, Some(StatsArg::Sizes)).await,
                Err(MemcachedError::Protocol(ProtocolError::UnknownCommand))
            );

            let mut c = Cursor::new(b"stats slabs\r\nERROR\r\n".to_vec());
            assert_matches!(
                stats_cmd(&mut c, Some(StatsArg::Slabs)).await,
                Err(MemcachedError::Protocol(ProtocolError::UnknownCommand))
            );

            let mut c = Cursor::new(b"stats conns\r\nERROR\r\n".to_vec());
            assert_matches!(
                stats_cmd(&mut c, Some(StatsArg::Conns)).await,
                Err(MemcachedError::Protocol(ProtocolError::UnknownCommand))
            );

            let mut c = Cursor::new(b"stats proxy\r\nERROR\r\n".to_vec());
            assert_matches!(
                stats_cmd(&mut c, Some(StatsArg::Proxy)).await,
                Err(MemcachedError::Protocol(ProtocolError::UnknownCommand))
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
                Err(MemcachedError::Protocol(ProtocolError::UnknownCommand))
            );

            let mut c = Cursor::new(b"slabs automove 2\r\nERROR\r\n".to_vec());
            assert_matches!(
                slabs_automove_cmd(&mut c, SlabsAutomoveArg::Two).await,
                Err(MemcachedError::Protocol(ProtocolError::UnknownCommand))
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
                Err(MemcachedError::Protocol(ProtocolError::UnknownCommand))
            )
        })
    }

    #[test]
    fn test_lru_crawler_sleep() {
        block_on(async {
            let mut c = Cursor::new(b"lru_crawler sleep 1000000\r\nOK\r\n".to_vec());
            assert!(lru_crawler_sleep_cmd(&mut c, 1_000_000).await.is_ok());

            let mut c = Cursor::new(b"lru_crawler sleep 0\r\nERROR\r\n".to_vec());
            assert_matches!(
                lru_crawler_sleep_cmd(&mut c, 0).await,
                Err(MemcachedError::Protocol(ProtocolError::UnknownCommand))
            )
        })
    }

    #[test]
    fn test_lru_crawler_tocrawl() {
        block_on(async {
            let mut c = Cursor::new(b"lru_crawler tocrawl 0\r\nOK\r\n".to_vec());
            assert!(lru_crawler_tocrawl_cmd(&mut c, 0).await.is_ok());

            let mut c = Cursor::new(b"lru_crawler tocrawl 0\r\nERROR\r\n".to_vec());
            assert_matches!(
                lru_crawler_tocrawl_cmd(&mut c, 0).await,
                Err(MemcachedError::Protocol(ProtocolError::UnknownCommand))
            )
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
                Err(MemcachedError::Protocol(ProtocolError::UnknownCommand))
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
            assert_matches!(
                slabs_reassign_cmd(&mut c, 1, 10).await,
                Err(MemcachedError::Protocol(ProtocolError::UnknownCommand))
            )
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
                Err(MemcachedError::Protocol(ProtocolError::UnknownCommand))
            );

            let mut c = Cursor::new(b"lru_crawler metadump hash\r\nERROR\r\n".to_vec());
            assert_matches!(
                lru_crawler_metadump_cmd(&mut c, LruCrawlerMetadumpArg::Hash).await,
                Err(MemcachedError::Protocol(ProtocolError::UnknownCommand))
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
                Err(MemcachedError::Protocol(ProtocolError::UnknownCommand))
            )
        })
    }

    #[test]
    fn test_mn() {
        block_on(async {
            let mut c = Cursor::new(b"mn\r\nMN\r\n".to_vec());
            assert!(mn_cmd(&mut c).await.is_ok());

            let mut c = Cursor::new(b"mn\r\nERROR\r\n".to_vec());
            assert_matches!(
                mn_cmd(&mut c).await,
                Err(MemcachedError::Protocol(ProtocolError::UnknownCommand))
            )
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
            assert_matches!(
                me_cmd(&mut c, b"key").await,
                Err(MemcachedError::Protocol(ProtocolError::UnknownCommand))
            );
        })
    }

    #[test]
    fn test_pipeline() {
        block_on(async {
            let cmds = [
                (b"version\r\n".to_vec(), PipelineExpect::Version),
                // b"quit\r\n".to_vec(),
                // b"shutdown\r\n".to_vec(),
                (
                    b"cache_memlimit 1\r\n".to_vec(),
                    PipelineExpect::Ok { noreply: false },
                ),
                (
                    b"cache_memlimit 1 noreply\r\n".to_vec(),
                    PipelineExpect::Ok { noreply: true },
                ),
                (
                    b"flush_all\r\n".to_vec(),
                    PipelineExpect::Ok { noreply: false },
                ),
                (
                    b"flush_all 1 noreply\r\n".to_vec(),
                    PipelineExpect::Ok { noreply: true },
                ),
                (
                    b"cas key 0 0 5 0\r\nvalue\r\n".to_vec(),
                    PipelineExpect::Storage { noreply: false },
                ),
                (
                    b"append key 0 0 5 noreply\r\nvalue\r\n".to_vec(),
                    PipelineExpect::Storage { noreply: true },
                ),
                (
                    b"delete key\r\n".to_vec(),
                    PipelineExpect::Delete { noreply: false },
                ),
                (
                    b"delete key noreply\r\n".to_vec(),
                    PipelineExpect::Delete { noreply: true },
                ),
                (b"set _ _ _ 3\r\na b\r\n".to_vec(), PipelineExpect::Auth),
                (
                    b"incr key 1\r\n".to_vec(),
                    PipelineExpect::IncrDecr { noreply: false },
                ),
                (
                    b"incr key 1 noreply\r\n".to_vec(),
                    PipelineExpect::IncrDecr { noreply: true },
                ),
                (
                    b"touch key 0\r\n".to_vec(),
                    PipelineExpect::Touch { noreply: false },
                ),
                (
                    b"touch key 0 noreply\r\n".to_vec(),
                    PipelineExpect::Touch { noreply: true },
                ),
                (b"gets key\r\n".to_vec(), PipelineExpect::RetrievalOne),
                (b"get key key2\r\n".to_vec(), PipelineExpect::RetrievalMulti),
                (
                    b"gat 0 key key2\r\n".to_vec(),
                    PipelineExpect::RetrievalMulti,
                ),
                (b"gats 0 key\r\n".to_vec(), PipelineExpect::RetrievalOne),
                (b"stats\r\n".to_vec(), PipelineExpect::Stats),
                (
                    b"slabs automove 0\r\n".to_vec(),
                    PipelineExpect::Ok { noreply: false },
                ),
                (
                    b"lru_crawler enable\r\n".to_vec(),
                    PipelineExpect::Ok { noreply: false },
                ),
                (
                    b"lru_crawler disable\r\n".to_vec(),
                    PipelineExpect::Ok { noreply: false },
                ),
                (
                    b"lru_crawler sleep 1000000\r\n".to_vec(),
                    PipelineExpect::Ok { noreply: false },
                ),
                (
                    b"lru_crawler tocrawl 0\r\n".to_vec(),
                    PipelineExpect::Ok { noreply: false },
                ),
                (
                    b"lru_crawler crawl 1,2,3\r\n".to_vec(),
                    PipelineExpect::LruCrawler,
                ),
                (
                    b"slabs reassign 1 10\r\n".to_vec(),
                    PipelineExpect::SlabsReassign,
                ),
                (
                    b"lru_crawler metadump all\r\n".to_vec(),
                    PipelineExpect::LruCrawlerMetaDump,
                ),
                (
                    b"lru_crawler mgdump 3\r\n".to_vec(),
                    PipelineExpect::LruCrawlerMgDump,
                ),
                (b"mn\r\n".to_vec(), PipelineExpect::Mn),
                (b"me key\r\n".to_vec(), PipelineExpect::Me),
                (
                    b"mg 44OG44K544OI b c f h k l Oopaque s t u E0 N0 R0 T0 v\r\n".to_vec(),
                    PipelineExpect::MetaGet,
                ),
                (
                    b"ms 44OG44K544OI 2 b c C0 E0 F0 I k Oopaque s T0 MS N0\r\nhi\r\n".to_vec(),
                    PipelineExpect::MetaSet,
                ),
                (
                    b"md 44OG44K544OI b C0 E0 I k Oopaque T0 x\r\n".to_vec(),
                    PipelineExpect::MetaDelete,
                ),
                (
                    b"ma 44OG44K544OI b C0 E0 N0 J0 D0 T0 M+ Oopaque t c v k\r\n".to_vec(),
                    PipelineExpect::MetaArithmetic,
                ),
                (
                    b"lru mode flat\r\n".to_vec(),
                    PipelineExpect::Ok { noreply: false },
                ),
                (
                    b"verbosity 0\r\n".to_vec(),
                    PipelineExpect::Ok { noreply: false },
                ),
                (
                    b"refresh_certs\r\n".to_vec(),
                    PipelineExpect::Ok { noreply: false },
                ),
            ];
            let rps = b"\
                VERSION 1.2.3\r\n\
                OK\r\n\
                OK\r\n\
                STORED\r\n\
                DELETED\r\n\
                STORED\r\n\
                2\r\n\
                TOUCHED\r\n\
                END\r\n\
                END\r\n\
                VALUE key 0 1 0\r\na\r\nVALUE key2 0 1 0\r\na\r\nEND\r\n\
                VALUE key 0 1 0\r\na\r\nEND\r\n\
                STAT version 1.2.3\r\nSTAT threads 4\r\nEND\r\n\
                OK\r\n\
                OK\r\n\
                OK\r\n\
                OK\r\n\
                OK\r\n\
                OK\r\n\
                OK\r\n\
                OK\r\nkey=key exp=-1 la=1745299782 cas=2 fetch=no cls=1 size=63 flags=0\r\nkey=key2 exp=-1 la=1745299782 cas=2 fetch=no cls=1 size=63 flags=0\r\nEND\r\n\
                OK\r\nmg key\r\nmg key2\r\nEN\r\n\
                MN\r\n\
                ME key exp=-1 la=3 cas=2 fetch=no cls=1 size=63\r\n\
                VA 1 b c0 f0 h0 k44OG44K544OI l0 Oopaque s0 t0 W X Z\r\nA\r\n\
                HD b c0 k44OG44K544OI Oopaque s0\r\n\
                HD k44OG44K544OI Oopaque b\r\n\
                VA 2 Oopaque t0 c0 k44OG44K544OI b\r\n10\r\n\
                OK\r\n\
                OK\r\n\
                OK\r\n\
                ";
            let mut c = Cursor::new(
                [
                    cmds.iter().flat_map(|x| &x.0).copied().collect::<Vec<_>>(),
                    rps.to_vec(),
                ]
                .concat(),
            );
            assert_eq!(
                execute_cmd(&mut c, &cmds)
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|x| x.unwrap())
                    .collect::<Vec<_>>(),
                [
                    PipelineResponse::String("1.2.3".to_string()),
                    PipelineResponse::Unit(()),
                    PipelineResponse::Unit(()),
                    PipelineResponse::Unit(()),
                    PipelineResponse::Unit(()),
                    PipelineResponse::StorageResult(StorageResult::Stored),
                    PipelineResponse::StorageResult(StorageResult::NoReply),
                    PipelineResponse::DeleteResult(DeleteResult::Deleted),
                    PipelineResponse::DeleteResult(DeleteResult::NoReply),
                    PipelineResponse::Unit(()),
                    PipelineResponse::IncrDecrResult(IncrDecrResult::Value(2)),
                    PipelineResponse::IncrDecrResult(IncrDecrResult::NoReply),
                    PipelineResponse::TouchResult(TouchResult::Touched),
                    PipelineResponse::TouchResult(TouchResult::NoReply),
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
                    PipelineResponse::Unit(()),
                    PipelineResponse::Unit(()),
                ]
            );

            let cmds = [
                (b"version\r\n".to_vec(), PipelineExpect::Version),
                (b"version\r\n".to_vec(), PipelineExpect::Version),
                (b"version\r\n".to_vec(), PipelineExpect::Version),
            ];
            let rps = b"\
                VERSION 1.2.3\r\n\
                ERROR\r\n\
                VERSION 1.2.3\r\n\
                ";
            let mut c = Cursor::new(
                [
                    cmds.iter().flat_map(|x| &x.0).copied().collect::<Vec<_>>(),
                    rps.to_vec(),
                ]
                .concat(),
            );
            assert_matches!(
                execute_cmd(&mut c, &cmds).await.unwrap().as_slice(),
                [
                    Ok(PipelineResponse::String(_)),
                    Err(MemcachedError::Protocol(ProtocolError::UnknownCommand)),
                    Ok(PipelineResponse::String(_)),
                ]
            );
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
                Err(MemcachedError::Protocol(ProtocolError::UnknownCommand))
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
                Err(MemcachedError::Protocol(ProtocolError::UnknownCommand))
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
                Err(MemcachedError::Protocol(ProtocolError::UnknownCommand))
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
                Err(MemcachedError::Protocol(ProtocolError::UnknownCommand))
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
                Err(MemcachedError::Protocol(ProtocolError::UnknownCommand))
            )
        })
    }

    #[test]
    fn test_lru() {
        block_on(async {
            let mut c = Cursor::new(b"lru mode flat\r\nERROR\r\n".to_vec());
            assert_matches!(
                lru_cmd(&mut c, LruArg::Mode(LruMode::Flat)).await,
                Err(MemcachedError::Protocol(ProtocolError::UnknownCommand))
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
                Err(MemcachedError::Protocol(ProtocolError::UnknownCommand))
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

    #[test]
    fn test_refresh_certs() {
        block_on(async {
            let mut c = Cursor::new(b"refresh_certs\r\nERROR\r\n".to_vec());
            assert_matches!(
                refresh_certs_cmd(&mut c).await,
                Err(MemcachedError::Protocol(ProtocolError::UnknownCommand))
            );

            let mut c = Cursor::new(b"refresh_certs\r\nOK\r\n".to_vec());
            assert!(refresh_certs_cmd(&mut c).await.is_ok());
        })
    }

    #[test]
    fn test_validate_key() {
        assert!(validate_key(b"valid_key:123").is_ok());
        assert!(validate_key("ключ_utf8".as_bytes()).is_ok());
        assert!(validate_key(&[b'a'; 250]).is_ok());

        assert_matches!(validate_key(b""), Err(MemcachedError::InvalidInput(_)));
        assert_matches!(
            validate_key(&[b'a'; 251]),
            Err(MemcachedError::InvalidInput(_))
        );
        assert_matches!(
            validate_key(b"bad key"),
            Err(MemcachedError::InvalidInput(_))
        );
        assert_matches!(
            validate_key(b"bad\r\nkey"),
            Err(MemcachedError::InvalidInput(_))
        );
        assert_matches!(
            validate_key(b"bad\0key"),
            Err(MemcachedError::InvalidInput(_))
        );
        assert_matches!(
            validate_key(b"bad\x7fkey"),
            Err(MemcachedError::InvalidInput(_))
        );
    }

    #[test]
    fn test_invalid_key_does_not_write_to_stream() {
        block_on(async {
            let mut c = Cursor::new(Vec::new());
            assert_matches!(
                storage_cmd(&mut c, b"set", b"bad key", 0, 0, None, false, b"value").await,
                Err(MemcachedError::InvalidInput(_))
            );
            assert!(c.get_ref().is_empty());

            let mut c = Cursor::new(Vec::new());
            assert_matches!(
                retrieval_cmd(&mut c, b"get", None, &[b"ok_key", b"bad\nkey"]).await,
                Err(MemcachedError::InvalidInput(_))
            );
            assert!(c.get_ref().is_empty());
        });
    }
}
