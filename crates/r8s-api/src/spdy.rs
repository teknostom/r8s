//! SPDY/3.1 transport for `pods/exec`, as client-go's SPDY executor speaks
//! it (the e2e framework, and kubectl when WebSockets fail): a POST with
//! `Upgrade: SPDY/3.1`, then one SPDY stream per channel, each opened by the
//! client with a `streamType` header. Implements the subset that client uses:
//! no flow control, no settings, no server-initiated streams.

use std::sync::Arc;
use std::time::Duration;

use flate2::{Compress, Compression, Decompress, FlushCompress, FlushDecompress};
use r8s_runtime::{ExecConfig, ExecRuntime, ExecStreams};
use rustc_hash::FxHashMap;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;

/// Stream protocol versions this server accepts, most preferred first.
pub const SUPPORTED_PROTOCOLS: [&str; 4] = [
    "v4.channel.k8s.io",
    "v3.channel.k8s.io",
    "v2.channel.k8s.io",
    "channel.k8s.io",
];

const VERSION: u16 = 3;
const CONTROL_BIT: u32 = 0x8000_0000;
const STREAM_ID_MASK: u32 = 0x7fff_ffff;
const TYPE_SYN_STREAM: u16 = 1;
const TYPE_SYN_REPLY: u16 = 2;
const TYPE_RST_STREAM: u16 = 3;
const TYPE_PING: u16 = 6;
const TYPE_GOAWAY: u16 = 7;
const TYPE_HEADERS: u16 = 8;
const FLAG_FIN: u8 = 0x01;
/// Largest DATA frame we emit; the protocol allows 2^24-1.
const DATA_CHUNK: usize = 64 * 1024;
const STREAM_CREATION_TIMEOUT: Duration = Duration::from_secs(30);
const RST_CANCEL: u32 = 5;

/// SPDY/3 header compression dictionary (draft 3, verbatim).
const SPDY_DICTIONARY: &[u8] = b"\
\x00\x00\x00\x07options\x00\x00\x00\x04head\x00\x00\x00\x04post\x00\x00\x00\x03put\x00\
\x00\x00\x06delete\x00\x00\x00\x05trace\x00\x00\x00\x06accept\x00\x00\x00\x0eaccept-char\
set\x00\x00\x00\x0faccept-encoding\x00\x00\x00\x0faccept-language\x00\x00\x00\x0daccept-\
ranges\x00\x00\x00\x03age\x00\x00\x00\x05allow\x00\x00\x00\x0dauthorization\x00\x00\x00\
\x0dcache-control\x00\x00\x00\x0aconnection\x00\x00\x00\x0ccontent-base\x00\x00\x00\x10c\
ontent-encoding\x00\x00\x00\x10content-language\x00\x00\x00\x0econtent-length\x00\x00\
\x00\x10content-location\x00\x00\x00\x0bcontent-md5\x00\x00\x00\x0dcontent-range\x00\x00\
\x00\x0ccontent-type\x00\x00\x00\x04date\x00\x00\x00\x04etag\x00\x00\x00\x06expect\x00\
\x00\x00\x07expires\x00\x00\x00\x04from\x00\x00\x00\x04host\x00\x00\x00\x08if-match\x00\
\x00\x00\x11if-modified-since\x00\x00\x00\x0dif-none-match\x00\x00\x00\x08if-range\x00\
\x00\x00\x13if-unmodified-since\x00\x00\x00\x0dlast-modified\x00\x00\x00\x08location\x00\
\x00\x00\x0cmax-forwards\x00\x00\x00\x06pragma\x00\x00\x00\x12proxy-authenticate\x00\x00\
\x00\x13proxy-authorization\x00\x00\x00\x05range\x00\x00\x00\x07referer\x00\x00\x00\x0br\
etry-after\x00\x00\x00\x06server\x00\x00\x00\x02te\x00\x00\x00\x07trailer\x00\x00\x00\
\x11transfer-encoding\x00\x00\x00\x07upgrade\x00\x00\x00\x0auser-agent\x00\x00\x00\x04va\
ry\x00\x00\x00\x03via\x00\x00\x00\x07warning\x00\x00\x00\x10www-authenticate\x00\x00\x00\
\x06method\x00\x00\x00\x03get\x00\x00\x00\x06status\x00\x00\x00\x06200 OK\x00\x00\x00\
\x07version\x00\x00\x00\x08HTTP/1.1\x00\x00\x00\x03url\x00\x00\x00\x06public\x00\x00\x00\
\x0aset-cookie\x00\x00\x00\x0akeep-alive\x00\x00\x00\x06origin10010120120220520630030230\
3304305306307402405406407408409410411412413414415416417502504505203 Non-Authoritative In\
formation204 No Content301 Moved Permanently400 Bad Request401 Unauthorized403 Forbidden\
404 Not Found500 Internal Server Error501 Not Implemented503 Service UnavailableJan Feb \
Mar Apr May Jun Jul Aug Sept Oct Nov Dec 00:00:00 Mon, Tue, Wed, Thu, Fri, Sat, Sun, GMT\
chunked,text/html,image/png,image/jpg,image/gif,application/xml,application/xhtml+xml,te\
xt/plain,text/javascript,publicprivatemax-age=gzip,deflate,sdchcharset=utf-8charset=iso-\
8859-1,utf-,*,enq=0.\
";

type Headers = Vec<(String, String)>;

#[derive(Debug)]
enum Frame {
    SynStream {
        stream_id: u32,
        fin: bool,
        headers: Headers,
    },
    RstStream {
        stream_id: u32,
    },
    Ping {
        id: u32,
    },
    GoAway,
    Data {
        stream_id: u32,
        fin: bool,
        data: Vec<u8>,
    },
    /// SYN_REPLY, HEADERS, SETTINGS, WINDOW_UPDATE and unknown types.
    Other,
}

fn io_err<E: std::fmt::Display>(e: E) -> std::io::Error {
    std::io::Error::other(e.to_string())
}

/// Inflate one header block on the connection's persistent zlib stream.
fn inflate(z: &mut Decompress, mut input: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut out = Vec::new();
    let mut buf = vec![0u8; 4096];
    loop {
        let (in_before, out_before) = (z.total_in(), z.total_out());
        let result = z.decompress(input, &mut buf, FlushDecompress::Sync);
        let consumed = (z.total_in() - in_before) as usize;
        let produced = (z.total_out() - out_before) as usize;
        out.extend_from_slice(&buf[..produced]);
        input = &input[consumed..];
        match result {
            Ok(_) => {
                if produced < buf.len() && input.is_empty() {
                    return Ok(out);
                }
                if consumed == 0 && produced == 0 {
                    return Err(io_err("header block stalled"));
                }
            }
            Err(e) if e.needs_dictionary().is_some() => {
                z.set_dictionary(SPDY_DICTIONARY).map_err(io_err)?;
            }
            Err(e) => return Err(io_err(e)),
        }
    }
}

/// Deflate one header block on the connection's persistent zlib stream,
/// sync-flushed so the peer can decode it on its own.
fn deflate(z: &mut Compress, mut input: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut buf = vec![0u8; 4096];
    loop {
        let (in_before, out_before) = (z.total_in(), z.total_out());
        let _ = z.compress(input, &mut buf, FlushCompress::Sync);
        let consumed = (z.total_in() - in_before) as usize;
        let produced = (z.total_out() - out_before) as usize;
        out.extend_from_slice(&buf[..produced]);
        input = &input[consumed..];
        if input.is_empty() && produced < buf.len() {
            return out;
        }
    }
}

fn be32(b: &[u8]) -> u32 {
    u32::from_be_bytes([b[0], b[1], b[2], b[3]])
}

/// Name/value block: count, then length-prefixed name and value pairs;
/// several values of one name are joined by NUL.
fn parse_header_block(plain: &[u8]) -> std::io::Result<Headers> {
    let short = || io_err("truncated header block");
    let mut pos = 0usize;
    let take = |pos: &mut usize, n: usize| -> std::io::Result<&[u8]> {
        let end = pos.checked_add(n).ok_or_else(short)?;
        let s = plain.get(*pos..end).ok_or_else(short)?;
        *pos = end;
        Ok(s)
    };
    let count = be32(take(&mut pos, 4)?);
    let mut headers = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let len = be32(take(&mut pos, 4)?) as usize;
        let name = String::from_utf8_lossy(take(&mut pos, len)?).to_lowercase();
        let len = be32(take(&mut pos, 4)?) as usize;
        let value = take(&mut pos, len)?;
        for v in value.split(|b| *b == 0) {
            headers.push((name.clone(), String::from_utf8_lossy(v).into_owned()));
        }
    }
    Ok(headers)
}

fn encode_header_block(headers: &[(String, String)]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(headers.len() as u32).to_be_bytes());
    for (name, value) in headers {
        let name = name.to_lowercase();
        out.extend_from_slice(&(name.len() as u32).to_be_bytes());
        out.extend_from_slice(name.as_bytes());
        out.extend_from_slice(&(value.len() as u32).to_be_bytes());
        out.extend_from_slice(value.as_bytes());
    }
    out
}

struct FrameReader<R> {
    inner: R,
    inflater: Decompress,
}

impl<R: AsyncRead + Unpin> FrameReader<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            inflater: Decompress::new(true),
        }
    }

    /// The next frame, or `None` at a clean end of stream.
    async fn read_frame(&mut self) -> std::io::Result<Option<Frame>> {
        let mut head = [0u8; 8];
        match self.inner.read_exact(&mut head).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }
        let first = be32(&head[..4]);
        let flags = head[4];
        let len = u32::from_be_bytes([0, head[5], head[6], head[7]]) as usize;
        let mut payload = vec![0u8; len];
        self.inner.read_exact(&mut payload).await?;
        if first & CONTROL_BIT == 0 {
            return Ok(Some(Frame::Data {
                stream_id: first & STREAM_ID_MASK,
                fin: flags & FLAG_FIN != 0,
                data: payload,
            }));
        }
        let frame_type = (first & 0xffff) as u16;
        let short = || io_err("truncated control frame");
        let frame = match frame_type {
            TYPE_SYN_STREAM => {
                let block = payload.get(10..).ok_or_else(short)?;
                let plain = inflate(&mut self.inflater, block)?;
                Frame::SynStream {
                    stream_id: be32(&payload[..4]) & STREAM_ID_MASK,
                    fin: flags & FLAG_FIN != 0,
                    headers: parse_header_block(&plain)?,
                }
            }
            // Both carry a header block that keeps the zlib stream in step.
            TYPE_SYN_REPLY | TYPE_HEADERS => {
                let block = payload.get(4..).ok_or_else(short)?;
                inflate(&mut self.inflater, block)?;
                Frame::Other
            }
            TYPE_RST_STREAM => Frame::RstStream {
                stream_id: be32(payload.get(..4).ok_or_else(short)?) & STREAM_ID_MASK,
            },
            TYPE_PING => Frame::Ping {
                id: be32(payload.get(..4).ok_or_else(short)?),
            },
            TYPE_GOAWAY => Frame::GoAway,
            _ => Frame::Other,
        };
        Ok(Some(frame))
    }
}

struct FrameWriter<W> {
    inner: W,
    deflater: Compress,
}

impl<W: AsyncWrite + Unpin> FrameWriter<W> {
    fn new(inner: W) -> Self {
        let mut deflater = Compress::new(Compression::default(), true);
        // The dictionary is fixed by the protocol; a failure here is a bug.
        deflater
            .set_dictionary(SPDY_DICTIONARY)
            .expect("SPDY dictionary accepted");
        Self { inner, deflater }
    }

    async fn control(&mut self, frame_type: u16, flags: u8, payload: &[u8]) -> std::io::Result<()> {
        let mut out = Vec::with_capacity(8 + payload.len());
        out.extend_from_slice(
            &(CONTROL_BIT | (u32::from(VERSION) << 16) | u32::from(frame_type)).to_be_bytes(),
        );
        out.extend_from_slice(&((u32::from(flags) << 24) | payload.len() as u32).to_be_bytes());
        out.extend_from_slice(payload);
        self.inner.write_all(&out).await
    }

    async fn syn_reply(&mut self, stream_id: u32) -> std::io::Result<()> {
        let block = deflate(&mut self.deflater, &encode_header_block(&[]));
        let mut payload = stream_id.to_be_bytes().to_vec();
        payload.extend_from_slice(&block);
        self.control(TYPE_SYN_REPLY, 0, &payload).await
    }

    async fn rst_stream(&mut self, stream_id: u32, status: u32) -> std::io::Result<()> {
        let mut payload = stream_id.to_be_bytes().to_vec();
        payload.extend_from_slice(&status.to_be_bytes());
        self.control(TYPE_RST_STREAM, 0, &payload).await
    }

    async fn ping(&mut self, id: u32) -> std::io::Result<()> {
        self.control(TYPE_PING, 0, &id.to_be_bytes()).await
    }

    async fn goaway(&mut self, last_stream_id: u32) -> std::io::Result<()> {
        let mut payload = last_stream_id.to_be_bytes().to_vec();
        payload.extend_from_slice(&0u32.to_be_bytes());
        self.control(TYPE_GOAWAY, 0, &payload).await
    }

    async fn data(&mut self, stream_id: u32, data: &[u8], fin: bool) -> std::io::Result<()> {
        let mut chunks = data.chunks(DATA_CHUNK).peekable();
        if chunks.peek().is_none() {
            return self.data_frame(stream_id, &[], fin).await;
        }
        while let Some(chunk) = chunks.next() {
            let last = chunks.peek().is_none();
            self.data_frame(stream_id, chunk, fin && last).await?;
        }
        Ok(())
    }

    async fn data_frame(&mut self, stream_id: u32, chunk: &[u8], fin: bool) -> std::io::Result<()> {
        let mut out = Vec::with_capacity(8 + chunk.len());
        out.extend_from_slice(&(stream_id & STREAM_ID_MASK).to_be_bytes());
        let flags = if fin { FLAG_FIN } else { 0 };
        out.extend_from_slice(&((u32::from(flags) << 24) | chunk.len() as u32).to_be_bytes());
        out.extend_from_slice(chunk);
        self.inner.write_all(&out).await
    }
}

/// What the client asked for in the exec query.
#[derive(Debug, Clone, Copy)]
pub struct StreamOpts {
    pub stdin: bool,
    pub stdout: bool,
    pub stderr: bool,
    pub tty: bool,
}

/// Stream protocol negotiated from `X-Stream-Protocol-Version`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    V1,
    V2,
    V3,
    V4,
}

impl Protocol {
    pub fn from_name(name: &str) -> Protocol {
        match name {
            "v4.channel.k8s.io" => Protocol::V4,
            "v3.channel.k8s.io" => Protocol::V3,
            "v2.channel.k8s.io" => Protocol::V2,
            _ => Protocol::V1,
        }
    }

    fn supports_resize(self) -> bool {
        matches!(self, Protocol::V3 | Protocol::V4)
    }
}

/// Pick the first client protocol the server speaks, like upstream's
/// `negotiateProtocol`. An empty client list falls back to v1; `None` means
/// the client offered only protocols the server lacks.
pub fn negotiate(client_protocols: &[String]) -> Option<&'static str> {
    if client_protocols.is_empty() {
        return Some(SUPPORTED_PROTOCOLS[3]);
    }
    client_protocols
        .iter()
        .find_map(|p| SUPPORTED_PROTOCOLS.iter().find(|s| *s == p).copied())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Kind {
    Error,
    Stdin,
    Stdout,
    Stderr,
    Resize,
}

impl Kind {
    fn parse(name: &str) -> Option<Kind> {
        Some(match name {
            "error" => Kind::Error,
            "stdin" => Kind::Stdin,
            "stdout" => Kind::Stdout,
            "stderr" => Kind::Stderr,
            "resize" => Kind::Resize,
            _ => return None,
        })
    }
}

enum Out {
    SynReply(u32),
    Data {
        stream_id: u32,
        data: Vec<u8>,
        fin: bool,
    },
    Rst(u32),
    Ping(u32),
    /// GOAWAY, then the write side is shut down.
    Finish,
}

/// Run the writer side: encode queued frames until `Finish` or the client
/// goes away.
async fn write_frames<W: AsyncWrite + Unpin>(
    mut writer: FrameWriter<W>,
    mut rx: mpsc::Receiver<Out>,
) {
    let mut last_stream = 0;
    while let Some(out) = rx.recv().await {
        let result = match out {
            Out::SynReply(id) => {
                last_stream = last_stream.max(id);
                writer.syn_reply(id).await
            }
            Out::Data {
                stream_id,
                data,
                fin,
            } => writer.data(stream_id, &data, fin).await,
            Out::Rst(id) => writer.rst_stream(id, RST_CANCEL).await,
            Out::Ping(id) => writer.ping(id).await,
            Out::Finish => {
                let _ = writer.goaway(last_stream).await;
                let _ = writer.inner.shutdown().await;
                return;
            }
        };
        if let Err(e) = result {
            tracing::debug!("spdy exec: write failed: {e}");
            return;
        }
    }
}

#[derive(Deserialize)]
struct TerminalSize {
    #[serde(rename = "Width")]
    width: u16,
    #[serde(rename = "Height")]
    height: u16,
}
use serde::Deserialize;

/// Terminal sizes in a resize-stream chunk, `(cols, rows)`.
fn parse_resizes(data: &[u8]) -> Vec<(u16, u16)> {
    serde_json::Deserializer::from_slice(data)
        .into_iter::<TerminalSize>()
        .flatten()
        .map(|s| (s.width, s.height))
        .collect()
}

/// Status the error stream carries when the command ends.
pub fn exit_status(code: i32) -> serde_json::Value {
    if code == 0 {
        serde_json::json!({"kind": "Status", "apiVersion": "v1", "metadata": {}, "status": "Success"})
    } else {
        serde_json::json!({
            "kind": "Status",
            "apiVersion": "v1",
            "metadata": {},
            "status": "Failure",
            "reason": "NonZeroExitCode",
            "message": format!("command terminated with non-zero exit code: command terminated with exit code {code}"),
            "details": {"causes": [{"reason": "ExitCode", "message": format!("{code}")}]},
        })
    }
}

/// The error stream payload for `status` under `protocol`: v4 carries the
/// Status object, earlier versions only a failure's message.
fn status_payload(protocol: Protocol, status: &serde_json::Value) -> Vec<u8> {
    if protocol == Protocol::V4 {
        return serde_json::to_vec(status).unwrap_or_default();
    }
    if status["status"] == "Success" {
        return Vec::new();
    }
    status["message"]
        .as_str()
        .unwrap_or("error")
        .as_bytes()
        .to_vec()
}

/// Serve one upgraded connection: accept the client's streams, run the
/// command, pump data both ways, and report the exit status.
pub async fn serve_exec<IO>(
    io: IO,
    protocol: Protocol,
    opts: StreamOpts,
    runtime: Arc<dyn ExecRuntime>,
    config: ExecConfig,
) where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (rd, wr) = tokio::io::split(io);
    let mut reader = FrameReader::new(rd);
    let (out_tx, out_rx) = mpsc::channel::<Out>(64);
    let writer_task = tokio::spawn(write_frames(FrameWriter::new(wr), out_rx));

    let stderr = opts.stderr && !opts.tty;
    let expected = 1
        + usize::from(opts.stdin)
        + usize::from(opts.stdout)
        + usize::from(stderr)
        + usize::from(opts.tty && protocol.supports_resize());

    let mut streams: FxHashMap<Kind, u32> = FxHashMap::default();
    let mut by_id: FxHashMap<u32, Kind> = FxHashMap::default();
    let mut early_stdin: Vec<u8> = Vec::new();
    let mut stdin_closed = false;

    // Phase 1: the client opens its streams; each gets a SYN_REPLY.
    let accept = async {
        while streams.len() < expected {
            let frame = match reader.read_frame().await {
                Ok(Some(f)) => f,
                Ok(None) => return Err(io_err("connection closed before streams were created")),
                Err(e) => return Err(e),
            };
            match frame {
                Frame::SynStream {
                    stream_id,
                    fin,
                    headers,
                } => {
                    let kind = headers
                        .iter()
                        .find(|(n, _)| n == "streamtype")
                        .and_then(|(_, v)| Kind::parse(v));
                    match kind {
                        Some(kind) if !streams.contains_key(&kind) => {
                            streams.insert(kind, stream_id);
                            by_id.insert(stream_id, kind);
                            if kind == Kind::Stdin && fin {
                                stdin_closed = true;
                            }
                            let _ = out_tx.send(Out::SynReply(stream_id)).await;
                        }
                        _ => {
                            tracing::warn!("spdy exec: unexpected stream {stream_id} {headers:?}");
                            let _ = out_tx.send(Out::Rst(stream_id)).await;
                        }
                    }
                }
                Frame::Data {
                    stream_id,
                    fin,
                    data,
                } if by_id.get(&stream_id) == Some(&Kind::Stdin) => {
                    early_stdin.extend_from_slice(&data);
                    stdin_closed |= fin;
                }
                Frame::Ping { id } => {
                    let _ = out_tx.send(Out::Ping(id)).await;
                }
                Frame::GoAway | Frame::RstStream { .. } => {
                    return Err(io_err("client went away during stream creation"));
                }
                Frame::Data { .. } | Frame::Other => {}
            }
        }
        Ok(())
    };
    match tokio::time::timeout(STREAM_CREATION_TIMEOUT, accept).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            tracing::warn!("spdy exec: {e}");
            let _ = out_tx.send(Out::Finish).await;
            let _ = writer_task.await;
            return;
        }
        Err(_) => {
            tracing::warn!("spdy exec: timed out waiting for client to create streams");
            let _ = out_tx.send(Out::Finish).await;
            let _ = writer_task.await;
            return;
        }
    }

    let error_id = streams.get(&Kind::Error).copied();
    let stdout_id = streams.get(&Kind::Stdout).copied();
    let stderr_id = streams.get(&Kind::Stderr).copied();
    let stdin_id = streams.get(&Kind::Stdin).copied();
    let resize_id = streams.get(&Kind::Resize).copied();

    // Everything the command produces, then its status, then the end.
    let finish = {
        let out_tx = out_tx.clone();
        move |status: serde_json::Value| async move {
            if let Some(id) = error_id {
                let payload = status_payload(protocol, &status);
                let _ = out_tx
                    .send(Out::Data {
                        stream_id: id,
                        data: payload,
                        fin: true,
                    })
                    .await;
            }
            for id in [stdout_id, stderr_id].into_iter().flatten() {
                let _ = out_tx
                    .send(Out::Data {
                        stream_id: id,
                        data: Vec::new(),
                        fin: true,
                    })
                    .await;
            }
            let _ = out_tx.send(Out::Finish).await;
        }
    };

    let mut exec_streams: ExecStreams = match runtime.exec(config).await {
        Ok(s) => s,
        Err(e) => {
            finish(serde_json::json!({
                "kind": "Status", "apiVersion": "v1", "metadata": {},
                "status": "Failure",
                "message": format!("error executing command in container: {e}"),
                "reason": "InternalError", "code": 500,
            }))
            .await;
            let _ = writer_task.await;
            return;
        }
    };

    // Output forwarders.
    let mut stdout_rx = std::mem::replace(&mut exec_streams.stdout_rx, mpsc::channel(1).1);
    let stdout_task = if let Some(id) = stdout_id {
        let out_tx = out_tx.clone();
        tokio::spawn(async move {
            while let Some(chunk) = stdout_rx.recv().await {
                if out_tx
                    .send(Out::Data {
                        stream_id: id,
                        data: chunk,
                        fin: false,
                    })
                    .await
                    .is_err()
                {
                    break;
                }
            }
        })
    } else {
        tokio::spawn(async move { while stdout_rx.recv().await.is_some() {} })
    };
    let stderr_task = exec_streams.stderr_rx.take().map(|mut stderr_rx| {
        if let Some(id) = stderr_id {
            let out_tx = out_tx.clone();
            tokio::spawn(async move {
                while let Some(chunk) = stderr_rx.recv().await {
                    if out_tx
                        .send(Out::Data {
                            stream_id: id,
                            data: chunk,
                            fin: false,
                        })
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            })
        } else {
            tokio::spawn(async move { while stderr_rx.recv().await.is_some() {} })
        }
    });

    // Exit: the status goes on the error stream after the output, which the
    // forwarders have sent once the process closed its pipes.
    let exit_rx = exec_streams.exit_rx;
    let exit_task = tokio::spawn(async move {
        let code = exit_rx.await.unwrap_or(-1);
        let _ = stdout_task.await;
        if let Some(task) = stderr_task {
            let _ = task.await;
        }
        finish(exit_status(code)).await;
    });

    // Input side, on this task.
    let mut stdin_tx = Some(exec_streams.stdin_tx);
    if !early_stdin.is_empty()
        && let Some(tx) = &stdin_tx
    {
        let _ = tx.send(std::mem::take(&mut early_stdin)).await;
    }
    if stdin_closed {
        stdin_tx = None;
    }
    let resize_tx = exec_streams.resize_tx;
    loop {
        let frame = match reader.read_frame().await {
            Ok(Some(f)) => f,
            Ok(None) => break,
            Err(e) => {
                tracing::debug!("spdy exec: read failed: {e}");
                break;
            }
        };
        match frame {
            Frame::Data {
                stream_id,
                fin,
                data,
            } => {
                if Some(stream_id) == stdin_id {
                    if !data.is_empty()
                        && let Some(tx) = &stdin_tx
                        && tx.send(data).await.is_err()
                    {
                        stdin_tx = None;
                    }
                    if fin {
                        stdin_tx = None;
                    }
                } else if Some(stream_id) == resize_id {
                    for size in parse_resizes(&data) {
                        let _ = resize_tx.send(size).await;
                    }
                }
            }
            Frame::RstStream { stream_id } if Some(stream_id) == stdin_id => {
                stdin_tx = None;
            }
            Frame::Ping { id } => {
                let _ = out_tx.send(Out::Ping(id)).await;
            }
            Frame::GoAway => break,
            Frame::SynStream { stream_id, .. } => {
                let _ = out_tx.send(Out::Rst(stream_id)).await;
            }
            Frame::RstStream { .. } | Frame::Other => {}
        }
    }
    drop(stdin_tx);
    drop(resize_tx);
    drop(out_tx);
    exit_task.abort();
    let _ = writer_task.await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dictionary_is_the_spdy3_one() {
        assert_eq!(SPDY_DICTIONARY.len(), 1423);
        assert_eq!(
            SPDY_DICTIONARY.iter().map(|&b| u64::from(b)).sum::<u64>(),
            108466
        );
        assert!(SPDY_DICTIONARY.ends_with(b"enq=0."));
    }

    /// A client-side SYN_STREAM, compressed on its own zlib stream.
    fn syn_stream(
        z: &mut Compress,
        stream_id: u32,
        headers: &[(&str, &str)],
        fin: bool,
    ) -> Vec<u8> {
        let headers: Headers = headers
            .iter()
            .map(|(n, v)| (n.to_string(), v.to_string()))
            .collect();
        let block = deflate(z, &encode_header_block(&headers));
        let mut payload = stream_id.to_be_bytes().to_vec();
        payload.extend_from_slice(&0u32.to_be_bytes());
        payload.extend_from_slice(&[0, 0]);
        payload.extend_from_slice(&block);
        let mut out = (CONTROL_BIT | (u32::from(VERSION) << 16) | u32::from(TYPE_SYN_STREAM))
            .to_be_bytes()
            .to_vec();
        let flags = if fin { FLAG_FIN } else { 0 };
        out.extend_from_slice(&((u32::from(flags) << 24) | payload.len() as u32).to_be_bytes());
        out.extend_from_slice(&payload);
        out
    }

    #[tokio::test]
    async fn reads_client_frames_across_one_zlib_stream() {
        let mut z = Compress::new(Compression::default(), true);
        z.set_dictionary(SPDY_DICTIONARY).unwrap();
        let mut bytes = syn_stream(&mut z, 1, &[("streamType", "error")], false);
        bytes.extend(syn_stream(&mut z, 3, &[("streamType", "stdout")], true));
        // DATA on stream 1 with FIN.
        bytes.extend_from_slice(&1u32.to_be_bytes());
        bytes.extend_from_slice(&((u32::from(FLAG_FIN) << 24) | 3).to_be_bytes());
        bytes.extend_from_slice(b"abc");
        // PING 5.
        bytes.extend_from_slice(
            &(CONTROL_BIT | (u32::from(VERSION) << 16) | u32::from(TYPE_PING)).to_be_bytes(),
        );
        bytes.extend_from_slice(&4u32.to_be_bytes());
        bytes.extend_from_slice(&5u32.to_be_bytes());

        let mut reader = FrameReader::new(std::io::Cursor::new(bytes));
        match reader.read_frame().await.unwrap().unwrap() {
            Frame::SynStream {
                stream_id,
                fin,
                headers,
            } => {
                assert_eq!((stream_id, fin), (1, false));
                assert_eq!(
                    headers,
                    vec![("streamtype".to_string(), "error".to_string())]
                );
            }
            other => panic!("{other:?}"),
        }
        match reader.read_frame().await.unwrap().unwrap() {
            Frame::SynStream {
                stream_id,
                fin,
                headers,
            } => {
                assert_eq!((stream_id, fin), (3, true));
                assert_eq!(headers[0].1, "stdout");
            }
            other => panic!("{other:?}"),
        }
        match reader.read_frame().await.unwrap().unwrap() {
            Frame::Data {
                stream_id,
                fin,
                data,
            } => assert_eq!((stream_id, fin, data.as_slice()), (1, true, &b"abc"[..])),
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            reader.read_frame().await.unwrap(),
            Some(Frame::Ping { id: 5 })
        ));
        assert!(reader.read_frame().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn writes_frames_the_client_decoder_accepts() {
        let mut buf = Vec::new();
        {
            let mut w = FrameWriter::new(&mut buf);
            w.syn_reply(1).await.unwrap();
            w.data(1, b"hello", true).await.unwrap();
            w.goaway(1).await.unwrap();
        }
        // SYN_REPLY: control header, stream id, then a block that inflates to
        // an empty header list.
        assert_eq!(&buf[..4], &(CONTROL_BIT | 0x0003_0000 | 2).to_be_bytes());
        let len = u32::from_be_bytes([0, buf[5], buf[6], buf[7]]) as usize;
        assert_eq!(&buf[8..12], &1u32.to_be_bytes());
        let mut z = Decompress::new(true);
        let plain = inflate(&mut z, &buf[12..8 + len]).unwrap();
        assert_eq!(
            parse_header_block(&plain).unwrap(),
            Vec::<(String, String)>::new()
        );
        // DATA frame follows.
        let data = &buf[8 + len..];
        assert_eq!(&data[..4], &1u32.to_be_bytes());
        assert_eq!(data[4], FLAG_FIN);
        assert_eq!(&data[8..13], b"hello");
        // GOAWAY last.
        assert_eq!(
            &data[13..17],
            &(CONTROL_BIT | 0x0003_0000 | 7).to_be_bytes()
        );
    }

    #[test]
    fn negotiation_prefers_the_clients_order_within_what_we_speak() {
        let offered = |s: &[&str]| s.iter().map(|x| x.to_string()).collect::<Vec<_>>();
        assert_eq!(
            negotiate(&offered(&["v4.channel.k8s.io", "v3.channel.k8s.io"])),
            Some("v4.channel.k8s.io")
        );
        assert_eq!(
            negotiate(&offered(&["v9.channel.k8s.io", "v2.channel.k8s.io"])),
            Some("v2.channel.k8s.io")
        );
        assert_eq!(negotiate(&offered(&["v9.channel.k8s.io"])), None);
        assert_eq!(negotiate(&[]), Some("channel.k8s.io"));
    }

    #[test]
    fn status_payload_by_protocol() {
        let ok = exit_status(0);
        assert!(status_payload(Protocol::V3, &ok).is_empty());
        assert!(String::from_utf8_lossy(&status_payload(Protocol::V4, &ok)).contains("Success"));
        let failed = exit_status(3);
        assert_eq!(failed["details"]["causes"][0]["message"], "3");
        assert!(
            String::from_utf8_lossy(&status_payload(Protocol::V2, &failed)).contains("exit code 3")
        );
    }
}
