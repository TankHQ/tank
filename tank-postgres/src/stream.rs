#[cfg(feature = "tls")]
use anyhow::anyhow;
#[cfg(feature = "tls")]
use core::mem::MaybeUninit;
use core::num::NonZeroUsize;
#[cfg(feature = "tls")]
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
#[cfg(feature = "tls")]
use tokio_native_tls::{TlsConnector, TlsStream};
use wtx::{
    collections::MaybeUninitSlice,
    net::{Stream, StreamCommon, StreamReader, StreamWriter},
};

/// A connection stream that is either a plain TCP socket or a TLS-wrapped one.
///
/// TLS is handled by `native-tls` (which supports client certificates / mTLS,
/// unlike `wtx`'s built-in TLS client), and the resulting encrypted byte stream
/// is presented to `wtx` as a plaintext transport.
pub(crate) enum ConnectionStream {
    Plain(TcpStream),
    #[cfg(feature = "tls")]
    Tls(Box<TlsStream<TcpStream>>),
}

impl ConnectionStream {
    /// Opens a plain TCP connection.
    pub(crate) async fn connect_plain(host: &str, port: u16) -> anyhow::Result<Self> {
        let stream = TcpStream::connect((host, port)).await?;
        stream.set_nodelay(true)?;
        Ok(Self::Plain(stream))
    }

    /// Opens a TCP connection and upgrades it to TLS using `connector`.
    ///
    /// PostgreSQL requires an `SSLRequest` round-trip before the TLS handshake:
    /// the client sends an 8-byte `SSLRequest` message and the server answers
    /// with a single byte (`S` to accept, `N` to refuse).
    #[cfg(feature = "tls")]
    pub(crate) async fn connect_tls(
        host: &str,
        port: u16,
        connector: TlsConnector,
        domain: &str,
    ) -> anyhow::Result<Self> {
        let mut stream = TcpStream::connect((host, port)).await?;
        stream.set_nodelay(true)?;
        // SSLRequest: length (8) then the magic code 80877103.
        AsyncWriteExt::write_all(&mut stream, &8u32.to_be_bytes()).await?;
        AsyncWriteExt::write_all(&mut stream, &80877103u32.to_be_bytes()).await?;
        AsyncWriteExt::flush(&mut stream).await?;
        let mut response = [0u8; 1];
        stream.read_exact(&mut response).await?;
        if response[0] != b'S' {
            return Err(anyhow!(
                "The Postgres server refused the TLS upgrade (response byte {})",
                response[0]
            ));
        }
        let tls = connector.connect(domain, stream).await?;
        Ok(Self::Tls(Box::new(tls)))
    }

    /// Builds a `tokio::io::ReadBuf` over a wtx uninitialized byte slice, preserving
    /// the already-initialized prefix.
    #[cfg(feature = "tls")]
    fn read_buf<'a>(slice: &'a mut MaybeUninitSlice<'_, u8>) -> ReadBuf<'a> {
        let initialized = slice.all().len() - slice.uninitialized_mut().len();
        // SAFETY: `all_mut` requires that initialized elements stay initialized; we
        // only write through `ReadBuf`, which advances the initialized length.
        let all: &mut [MaybeUninit<u8>] = unsafe { slice.all_mut() };
        let mut read_buf = ReadBuf::uninit(all);
        // SAFETY: the first `initialized` bytes were already initialized by wtx.
        unsafe { read_buf.assume_init(initialized) };
        read_buf
    }
}

impl StreamCommon for ConnectionStream {}

impl StreamReader for ConnectionStream {
    #[inline]
    async fn read(&mut self, bytes: MaybeUninitSlice<'_, u8>) -> wtx::Result<Option<NonZeroUsize>> {
        match self {
            Self::Plain(stream) => StreamReader::read(stream, bytes).await,
            #[cfg(feature = "tls")]
            Self::Tls(stream) => {
                let mut bytes = bytes;
                let mut read_buf = Self::read_buf(&mut bytes);
                AsyncReadExt::read_buf(stream.as_mut(), &mut read_buf).await?;
                Ok(NonZeroUsize::new(read_buf.filled().len()))
            }
        }
    }
}

impl StreamWriter for ConnectionStream {
    #[inline]
    async fn write_all(&mut self, bytes: &[u8]) -> wtx::Result<()> {
        match self {
            Self::Plain(stream) => StreamWriter::write_all(stream, bytes).await,
            #[cfg(feature = "tls")]
            Self::Tls(stream) => {
                AsyncWriteExt::write_all(stream.as_mut(), bytes).await?;
                Ok(())
            }
        }
    }

    #[inline]
    async fn write_all_vectored(&mut self, bytes: &[&[u8]]) -> wtx::Result<()> {
        match self {
            Self::Plain(stream) => StreamWriter::write_all_vectored(stream, bytes).await,
            #[cfg(feature = "tls")]
            Self::Tls(_) => {
                for chunk in bytes {
                    self.write_all(chunk).await?;
                }
                Ok(())
            }
        }
    }
}

impl Stream for ConnectionStream {
    type BridgeOwned = ();
    type ReadHalfOwned = ();
    type WriteHalfOwned = ();

    fn into_split(
        self,
    ) -> wtx::Result<(Self::BridgeOwned, Self::ReadHalfOwned, Self::WriteHalfOwned)> {
        Ok(((), (), ()))
    }
}
