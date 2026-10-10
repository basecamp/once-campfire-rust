//! The server side of a WebSocket (RFC 6455) for Action Cable connections, with per-message
//! compression (RFC 7692, `permessage-deflate`) when the browser offers it, as browsers do.
//!
//! Written for fan-out to many sockets. A broadcast reaches every subscriber as the same
//! [`Frame`], whose payload is shared rather than copied: it goes to each socket as a few header
//! bytes plus the shared payload in one vectored write, and nothing is kept per socket once it's
//! written (a general-purpose WebSocket copies each frame into a per-socket buffer that then keeps
//! its size). Compression is negotiated without context takeover, so every message is compressed
//! on its own, and a broadcast is compressed once for all of its subscribers.
//!
//! Only what Action Cable needs from a client is supported: text messages, fragmented or not,
//! compressed or not, and control frames. Protocol errors close the connection, with the code
//! Action Cable's websocket-driver would close it with.

use std::io::{self, IoSlice};

use axum::http::{HeaderMap, HeaderValue, header};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use flate2::{Compress, Compression, Decompress, FlushCompress, FlushDecompress, Status};
use sha1::{Digest, Sha1};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// The largest message a client may send (Action Cable commands are a few hundred bytes), where
/// websocket-driver takes up to 64 MiB (`WebSocket::Driver::MAX_LENGTH`).
pub const MAX_MESSAGE: usize = 1 << 20;
/// Smaller frames go out uncompressed even when compression is on: not worth a deflate stream.
const MIN_COMPRESSED: usize = 256;
/// The extension response: every message compressed on its own, in both directions.
const DEFLATE_RESPONSE: &str = "permessage-deflate; server_no_context_takeover; client_no_context_takeover";
/// RFC 6455's key suffix.
const ACCEPT_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
/// A sync-flushed deflate stream ends with these; RFC 7692 leaves them off the wire.
const DEFLATE_TAIL: [u8; 4] = [0, 0, 0xff, 0xff];

const OP_CONTINUATION: u8 = 0x0;
const OP_TEXT: u8 = 0x1;
const OP_BINARY: u8 = 0x2;
const OP_CLOSE: u8 = 0x8;
const OP_PING: u8 = 0x9;
const OP_PONG: u8 = 0xA;

// --- Frames ------------------------------------------------------------------------------------

/// A text frame's payload, shared by every connection that sends it and compressed at most once.
pub use campfire_bus::Frame;

/// The payload for a socket with compression on: `Some(deflated)` when compressing is worth it.
fn deflated(frame: &Frame) -> Option<&[u8]> {
    (frame.len() >= MIN_COMPRESSED).then(|| frame.encoded(|text| deflate(text.as_bytes())))
}

/// Raw deflate at level 6, sync-flushed, without the trailing empty block's 4 bytes.
fn deflate(input: &[u8]) -> Box<[u8]> {
    let mut compress = Compress::new(Compression::default(), false);
    let mut out = Vec::with_capacity(input.len() / 3 + 64);
    loop {
        if out.capacity() - out.len() < 64 {
            out.reserve(out.capacity());
        }
        let consumed = compress.total_in() as usize;
        compress.compress_vec(&input[consumed..], &mut out, FlushCompress::Sync).expect("deflate doesn't fail on valid input");
        if compress.total_in() as usize == input.len() && out.len() < out.capacity() {
            break;
        }
    }
    if out.ends_with(&DEFLATE_TAIL) {
        out.truncate(out.len() - DEFLATE_TAIL.len());
    }
    out.into_boxed_slice()
}

// --- Handshake ---------------------------------------------------------------------------------

/// The server's side of an accepted opening handshake.
#[derive(Debug, PartialEq)]
pub struct Handshake {
    accept: String,
    deflate: bool,
}

impl Handshake {
    /// Checks the client's handshake (version 13 and a key; the caller has checked the method and
    /// `Upgrade`/`Connection` headers) and picks compression if one of its offers suits.
    pub fn accept(headers: &HeaderMap) -> Option<Self> {
        let version = headers.get(header::SEC_WEBSOCKET_VERSION)?;
        let key = headers.get(header::SEC_WEBSOCKET_KEY)?;
        if version != "13" || STANDARD.decode(key.as_bytes()).map_or(true, |decoded| decoded.len() != 16) {
            return None;
        }
        let mut sha = Sha1::new();
        sha.update(key.as_bytes());
        sha.update(ACCEPT_GUID.as_bytes());
        let accept = STANDARD.encode(sha.finalize());
        let deflate = headers
            .get_all(header::SEC_WEBSOCKET_EXTENSIONS)
            .iter()
            .filter_map(|value| value.to_str().ok())
            .flat_map(|value| value.split(','))
            .any(acceptable_deflate_offer);
        Some(Self { accept, deflate })
    }

    /// The 101 response's WebSocket headers (the caller adds `Sec-WebSocket-Protocol`).
    pub fn response_headers(&self, headers: &mut HeaderMap) {
        headers.insert(header::UPGRADE, HeaderValue::from_static("websocket"));
        headers.insert(header::CONNECTION, HeaderValue::from_static("upgrade"));
        headers.insert(header::SEC_WEBSOCKET_ACCEPT, HeaderValue::from_str(&self.accept).expect("base64 is a header value"));
        if self.deflate {
            headers.insert(header::SEC_WEBSOCKET_EXTENSIONS, HeaderValue::from_static(DEFLATE_RESPONSE));
        }
    }

    pub fn deflate(&self) -> bool {
        self.deflate
    }
}

/// A `permessage-deflate` offer we can answer with [`DEFLATE_RESPONSE`]: any parameters but a
/// server window smaller than zlib's 15 bits (browsers don't ask for one).
fn acceptable_deflate_offer(offer: &str) -> bool {
    let mut params = offer.split(';').map(str::trim);
    if params.next() != Some("permessage-deflate") {
        return false;
    }
    params.all(|param| {
        let (name, value) = param.split_once('=').map_or((param, None), |(n, v)| (n.trim(), Some(v.trim().trim_matches('"'))));
        match name {
            "server_no_context_takeover" | "client_no_context_takeover" => value.is_none(),
            "client_max_window_bits" => value.is_none_or(|v| v.parse::<u8>().is_ok_and(|bits| (8..=15).contains(&bits))),
            "server_max_window_bits" => value == Some("15"),
            _ => false,
        }
    })
}

// --- Reading -----------------------------------------------------------------------------------

/// What a client sent.
#[derive(Debug, PartialEq)]
pub enum Incoming {
    Text(String),
    Binary,
    Ping(Vec<u8>),
    Pong,
    /// A close frame, with its status code if it had one.
    Close(Option<u16>),
}

/// Why a [`Reader`] stopped.
#[derive(Debug)]
pub enum ReadError {
    /// Reading the socket failed, or the client went away.
    Io(io::Error),
    /// The client broke the protocol: the connection closes with `code`, the one websocket-driver
    /// (under Action Cable) closes with for the same failure.
    Protocol { code: u16 },
}

impl From<io::Error> for ReadError {
    fn from(error: io::Error) -> Self {
        ReadError::Io(error)
    }
}

// websocket-driver's codes for what a client can get wrong (`WebSocket::Driver::Hybi::ERRORS`).

/// Anything not covered below.
pub const PROTOCOL_ERROR: u16 = 1002;
/// An unmasked frame.
pub const UNACCEPTABLE: u16 = 1003;
/// A text message that isn't UTF-8.
pub const ENCODING_ERROR: u16 = 1007;
/// A message over [`MAX_MESSAGE`].
pub const TOO_LARGE: u16 = 1009;

fn fail<T>(code: u16) -> Result<T, ReadError> {
    Err(ReadError::Protocol { code })
}

/// A frame's header.
struct Header {
    fin: bool,
    /// RSV1: the message this frame starts is compressed.
    compressed: bool,
    opcode: u8,
    len: u64,
    mask: [u8; 4],
}

/// A fragmented message so far.
struct Partial {
    opcode: u8,
    compressed: bool,
    data: Vec<u8>,
}

/// Reads client frames. It keeps no buffer between messages: frames are read header first, then
/// exactly their payload, so an idle socket holds nothing.
pub struct Reader<R> {
    io: R,
    deflate: bool,
    /// Kept across the control frames that may arrive between a message's fragments.
    partial: Option<Partial>,
}

impl<R: AsyncRead + Unpin> Reader<R> {
    pub fn new(io: R, deflate: bool) -> Self {
        Self { io, deflate, partial: None }
    }

    /// The next message or control frame, reassembling fragments and decompressing.
    pub async fn next(&mut self) -> Result<Incoming, ReadError> {
        loop {
            let header = self.header().await?;
            let payload = self.payload(&header).await?;
            match header.opcode {
                OP_CLOSE => return close_code(&payload).map(Incoming::Close),
                OP_PING => return Ok(Incoming::Ping(payload)),
                OP_PONG => return Ok(Incoming::Pong),
                OP_TEXT | OP_BINARY => self.partial = Some(Partial { opcode: header.opcode, compressed: header.compressed, data: payload }),
                _ => match &mut self.partial {
                    Some(partial) => partial.data.extend_from_slice(&payload),
                    None => return fail(PROTOCOL_ERROR), // a continuation of nothing
                },
            }
            if header.fin {
                let Partial { opcode, compressed, data } = self.partial.take().expect("a message was started");
                let data = if compressed { inflate(&data)? } else { data };
                return match opcode {
                    OP_TEXT => String::from_utf8(data).map(Incoming::Text).or_else(|_| fail(ENCODING_ERROR)),
                    _ => Ok(Incoming::Binary),
                };
            }
        }
    }

    /// A frame's header, checked in the order websocket-driver checks it (`Hybi#parse_opcode`,
    /// `#parse_length`, `#check_frame_length`), which decides the close code when a frame is wrong
    /// in more than one way.
    async fn header(&mut self) -> Result<Header, ReadError> {
        let mut head = [0u8; 2];
        self.io.read_exact(&mut head).await?;
        let (fin, compressed, opcode) = (head[0] & 0x80 != 0, head[0] & 0x40 != 0, head[0] & 0x0f);
        let control = opcode >= 0x8;
        // RSV1 may only start a message, and only with compression negotiated.
        let reserved = head[0] & 0x30 != 0 || (compressed && !(self.deflate && matches!(opcode, OP_TEXT | OP_BINARY)));
        let known = matches!(opcode, OP_CONTINUATION | OP_TEXT | OP_BINARY | OP_CLOSE | OP_PING | OP_PONG);
        let interrupts_a_message = matches!(opcode, OP_TEXT | OP_BINARY) && self.partial.is_some();
        if reserved || !known || (control && !fin) || interrupts_a_message {
            return fail(PROTOCOL_ERROR);
        }
        if head[1] & 0x80 == 0 {
            return fail(UNACCEPTABLE);
        }
        let len = match head[1] & 0x7f {
            126 => self.io.read_u16().await? as u64,
            127 => self.io.read_u64().await?,
            len => len as u64,
        };
        if control && len > 125 {
            return fail(PROTOCOL_ERROR);
        }
        let so_far = match &self.partial {
            Some(partial) if !control => partial.data.len() as u64,
            _ => 0,
        };
        if so_far.saturating_add(len) > MAX_MESSAGE as u64 {
            return fail(TOO_LARGE);
        }
        let mut mask = [0u8; 4];
        self.io.read_exact(&mut mask).await?;
        Ok(Header { fin, compressed, opcode, len, mask })
    }

    /// A frame's payload, unmasked. It's grown as bytes arrive rather than allocated from the
    /// header's claim, so a client can't make the server hold a megabyte per socket by announcing a
    /// frame it never sends.
    async fn payload(&mut self, header: &Header) -> io::Result<Vec<u8>> {
        let mut payload = Vec::new();
        (&mut self.io).take(header.len).read_to_end(&mut payload).await?;
        if payload.len() as u64 != header.len {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        for (i, byte) in payload.iter_mut().enumerate() {
            *byte ^= header.mask[i % 4];
        }
        Ok(payload)
    }
}

/// A close frame's status code (RFC 6455 §5.5.1, §7.4): no payload, or a code a peer may send
/// followed by a UTF-8 reason.
fn close_code(payload: &[u8]) -> Result<Option<u16>, ReadError> {
    match payload {
        [] => Ok(None),
        [_] => fail(PROTOCOL_ERROR),
        [high, low, reason @ ..] => {
            let code = u16::from_be_bytes([*high, *low]);
            if !matches!(code, 1000..=1003 | 1007..=1014 | 3000..=4999) || std::str::from_utf8(reason).is_err() {
                return fail(PROTOCOL_ERROR);
            }
            Ok(Some(code))
        }
    }
}

/// A compressed message's text. Invalid deflate data is a protocol error: Action Cable doesn't
/// negotiate compression, so there's no websocket-driver code to match.
fn inflate(data: &[u8]) -> Result<Vec<u8>, ReadError> {
    let mut decompress = Decompress::new(false);
    let input = [data, &DEFLATE_TAIL[..]].concat();
    let mut out = Vec::with_capacity(data.len() * 4 + 64);
    loop {
        if out.capacity() - out.len() < 1024 {
            out.reserve(out.capacity());
        }
        let consumed = decompress.total_in() as usize;
        let Ok(status) = decompress.decompress_vec(&input[consumed..], &mut out, FlushDecompress::Sync) else {
            return fail(PROTOCOL_ERROR);
        };
        if out.len() > MAX_MESSAGE {
            return fail(TOO_LARGE);
        }
        let done = decompress.total_in() as usize == input.len() && out.len() < out.capacity();
        if done || status == Status::StreamEnd {
            return Ok(out);
        }
    }
}

// --- Writing -----------------------------------------------------------------------------------

/// How long a write may wait for a client to read before it fails. A client that stops reading
/// doesn't hold its connection's task forever.
pub const WRITE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Writes server frames: unmasked, each as its header plus its (shared) payload. Every write fails
/// after [`WRITE_TIMEOUT`].
pub struct Writer<W> {
    io: W,
    deflate: bool,
}

impl<W: AsyncWrite + Unpin> Writer<W> {
    pub fn new(io: W, deflate: bool) -> Self {
        Self { io, deflate }
    }

    /// Writes `frames` as text messages, in order, in as few writes as the socket takes.
    pub async fn send<'a>(&mut self, frames: impl IntoIterator<Item = &'a Frame>) -> io::Result<()> {
        let payloads: Vec<(bool, &[u8])> = frames
            .into_iter()
            .map(|frame| match self.deflate.then(|| deflated(frame)).flatten() {
                Some(deflated) => (true, deflated),
                None => (false, frame.as_str().as_bytes()),
            })
            .collect();
        let headers: Vec<([u8; 10], usize)> =
            payloads.iter().map(|(compressed, payload)| header(OP_TEXT, *compressed, payload.len())).collect();
        let mut slices: Vec<IoSlice<'_>> = Vec::with_capacity(payloads.len() * 2);
        for ((header, len), (_, payload)) in headers.iter().zip(&payloads) {
            slices.push(IoSlice::new(&header[..*len]));
            slices.push(IoSlice::new(payload));
        }
        self.write(&mut slices).await
    }

    pub async fn pong(&mut self, payload: &[u8]) -> io::Result<()> {
        self.control(OP_PONG, payload).await
    }

    /// A close frame with `code` and no reason.
    pub async fn close(&mut self, code: u16) -> io::Result<()> {
        self.control(OP_CLOSE, &code.to_be_bytes()).await
    }

    /// The reply to a client's close frame, echoing its code (none if it sent none), which
    /// completes the closing handshake.
    pub async fn close_reply(&mut self, code: Option<u16>) -> io::Result<()> {
        match code {
            Some(code) => self.close(code).await,
            None => self.control(OP_CLOSE, &[]).await,
        }
    }

    async fn control(&mut self, opcode: u8, payload: &[u8]) -> io::Result<()> {
        let (header, len) = header(opcode, false, payload.len());
        let mut slices = [IoSlice::new(&header[..len]), IoSlice::new(payload)];
        self.write(&mut slices).await
    }

    async fn write(&mut self, slices: &mut [IoSlice<'_>]) -> io::Result<()> {
        let io = &mut self.io;
        let write = async move {
            write_all_vectored(io, slices).await?;
            io.flush().await
        };
        tokio::time::timeout(WRITE_TIMEOUT, write).await.unwrap_or_else(|_| Err(io::ErrorKind::TimedOut.into()))
    }
}

/// A final frame's header: FIN, RSV1 when `compressed`, `opcode`, and the payload length.
fn header(opcode: u8, compressed: bool, len: usize) -> ([u8; 10], usize) {
    let mut header = [0u8; 10];
    header[0] = 0x80 | if compressed { 0x40 } else { 0 } | opcode;
    let used = if len < 126 {
        header[1] = len as u8;
        2
    } else if len <= u16::MAX as usize {
        header[1] = 126;
        header[2..4].copy_from_slice(&(len as u16).to_be_bytes());
        4
    } else {
        header[1] = 127;
        header[2..10].copy_from_slice(&(len as u64).to_be_bytes());
        10
    };
    (header, used)
}

async fn write_all_vectored<W: AsyncWrite + Unpin>(io: &mut W, mut slices: &mut [IoSlice<'_>]) -> io::Result<()> {
    while !slices.is_empty() {
        let written = io.write_vectored(slices).await?;
        if written == 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }
        IoSlice::advance_slices(&mut slices, written);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client_frame(opcode: u8, fin: bool, rsv1: bool, payload: &[u8]) -> Vec<u8> {
        let mask = [0x12, 0x34, 0x56, 0x78];
        let mut frame = vec![(if fin { 0x80 } else { 0 }) | (if rsv1 { 0x40 } else { 0 }) | opcode];
        match payload.len() {
            len if len < 126 => frame.push(0x80 | len as u8),
            len if len <= 65535 => {
                frame.push(0x80 | 126);
                frame.extend_from_slice(&(len as u16).to_be_bytes());
            }
            len => {
                frame.push(0x80 | 127);
                frame.extend_from_slice(&(len as u64).to_be_bytes());
            }
        }
        frame.extend_from_slice(&mask);
        frame.extend(payload.iter().enumerate().map(|(i, b)| b ^ mask[i % 4]));
        frame
    }

    fn deflated(text: &str) -> Vec<u8> {
        deflate(text.as_bytes()).into_vec()
    }

    async fn read_all(bytes: Vec<u8>, deflate: bool) -> Vec<Result<Incoming, ReadError>> {
        let mut reader = Reader::new(&bytes[..], deflate);
        let mut out = Vec::new();
        loop {
            match reader.next().await {
                Err(ReadError::Io(error)) if error.kind() == io::ErrorKind::UnexpectedEof => return out,
                result => {
                    let stop = result.is_err();
                    out.push(result);
                    if stop {
                        return out;
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn reads_text_fragments_compressed_messages_and_control_frames() {
        let mut bytes = client_frame(OP_TEXT, true, false, br#"{"command":"subscribe"}"#);
        bytes.extend(client_frame(OP_TEXT, false, false, b"hel"));
        bytes.extend(client_frame(OP_PING, true, false, b"p"));
        bytes.extend(client_frame(OP_CONTINUATION, true, false, "lo ☃".as_bytes()));
        bytes.extend(client_frame(OP_TEXT, true, true, &deflated(&"compressed ".repeat(100))));
        bytes.extend(client_frame(OP_CLOSE, true, false, &[&1001u16.to_be_bytes()[..], "going away".as_bytes()].concat()));
        let read: Vec<Incoming> = read_all(bytes, true).await.into_iter().map(Result::unwrap).collect();
        assert_eq!(
            read,
            vec![
                Incoming::Text(r#"{"command":"subscribe"}"#.into()),
                Incoming::Ping(b"p".to_vec()),
                Incoming::Text("hello ☃".into()),
                Incoming::Text("compressed ".repeat(100)),
                Incoming::Close(Some(1001)),
            ]
        );
    }

    /// Each closes with websocket-driver's code for it.
    #[tokio::test]
    async fn rejects_protocol_errors() {
        let half = vec![b'a'; MAX_MESSAGE / 2 + 1];
        let cases = [
            ("unmasked", vec![0x81, 0x02, b'h', b'i'], false, UNACCEPTABLE),
            ("unmasked, with a reserved opcode", vec![0x83, 0x01, b'x'], false, PROTOCOL_ERROR),
            ("compressed, not negotiated", client_frame(OP_TEXT, true, true, &deflated("hi")), false, PROTOCOL_ERROR),
            (
                "compressed continuation",
                [client_frame(OP_TEXT, false, true, b""), client_frame(OP_CONTINUATION, true, true, b"")].concat(),
                true,
                PROTOCOL_ERROR,
            ),
            ("compressed ping", client_frame(OP_PING, true, true, b"x"), true, PROTOCOL_ERROR),
            ("invalid deflate data", client_frame(OP_TEXT, true, true, &[0xff; 8]), true, PROTOCOL_ERROR),
            ("not UTF-8", client_frame(OP_TEXT, true, false, &[0xff, 0xfe]), false, ENCODING_ERROR),
            ("nothing to continue", client_frame(OP_CONTINUATION, true, false, b"x"), false, PROTOCOL_ERROR),
            (
                "a new message mid-message",
                [client_frame(OP_TEXT, false, false, b"x"), client_frame(OP_TEXT, true, false, b"y")].concat(),
                false,
                PROTOCOL_ERROR,
            ),
            ("fragmented control frame", client_frame(OP_PING, false, false, b"x"), false, PROTOCOL_ERROR),
            ("long control frame", client_frame(OP_PING, true, false, &[0; 126]), false, PROTOCOL_ERROR),
            ("reserved opcode", client_frame(0x3, true, false, b"x"), false, PROTOCOL_ERROR),
            ("too large", client_frame(OP_TEXT, true, false, &vec![b'a'; MAX_MESSAGE + 1]), false, TOO_LARGE),
            (
                "too large in fragments",
                [client_frame(OP_TEXT, false, false, &half), client_frame(OP_CONTINUATION, true, false, &half)].concat(),
                false,
                TOO_LARGE,
            ),
            ("too large once inflated", client_frame(OP_TEXT, true, true, &deflated(&"a".repeat(MAX_MESSAGE + 1))), true, TOO_LARGE),
            ("one-byte close", client_frame(OP_CLOSE, true, false, &[0x03]), false, PROTOCOL_ERROR),
            ("reserved close code", client_frame(OP_CLOSE, true, false, &1005u16.to_be_bytes()), false, PROTOCOL_ERROR),
            ("close reason not UTF-8", client_frame(OP_CLOSE, true, false, &[0x03, 0xe8, 0xff]), false, PROTOCOL_ERROR),
        ];
        for (case, bytes, deflate, code) in cases {
            let read = read_all(bytes, deflate).await;
            assert!(matches!(read.last(), Some(Err(ReadError::Protocol { code: closed })) if *closed == code), "{case}: {read:?}");
        }
    }

    /// A fragment that would make its message too large is refused from its header, before its
    /// payload is read.
    #[tokio::test]
    async fn refuses_a_too_large_fragment_from_its_header() {
        let half = vec![b'a'; MAX_MESSAGE / 2 + 1];
        let mut bytes = [client_frame(OP_TEXT, false, false, &half), client_frame(OP_CONTINUATION, true, false, &half)].concat();
        bytes.truncate(bytes.len() - half.len());
        let read = read_all(bytes, false).await;
        assert!(matches!(read.last(), Some(Err(ReadError::Protocol { code: TOO_LARGE }))), "{read:?}");
    }

    #[tokio::test]
    async fn writes_shared_frames_compressed_only_when_worth_it() {
        let small = Frame::from(r#"{"type":"ping","message":1}"#);
        let big = Frame::from(format!(r#"{{"identifier":"x","message":"{}"}}"#, "<div>message</div>".repeat(200)));
        for deflate in [false, true] {
            let mut out = Vec::new();
            Writer::new(&mut out, deflate).send(&[small.clone(), big.clone()]).await.unwrap();
            // Read them back as a client would: unmasked frames, RSV1 for compressed ones.
            let mut at = 0;
            let mut texts = Vec::new();
            while at < out.len() {
                let (b0, b1) = (out[at], out[at + 1]);
                assert_eq!(b0 & 0x80, 0x80);
                assert_eq!(b1 & 0x80, 0, "server frames are unmasked");
                let (len, start) = match b1 & 0x7f {
                    126 => (u16::from_be_bytes([out[at + 2], out[at + 3]]) as usize, at + 4),
                    127 => (u64::from_be_bytes(out[at + 2..at + 10].try_into().unwrap()) as usize, at + 10),
                    len => (len as usize, at + 2),
                };
                let payload = &out[start..start + len];
                let text = if b0 & 0x40 != 0 {
                    String::from_utf8(inflate(payload).unwrap()).unwrap()
                } else {
                    String::from_utf8(payload.to_vec()).unwrap()
                };
                texts.push((b0 & 0x40 != 0, text));
                at = start + len;
            }
            assert_eq!(texts, vec![(false, small.as_str().to_string()), (deflate, big.as_str().to_string())]);
        }
        let compressed = super::deflated(&big).unwrap();
        assert!(compressed.len() < big.as_str().len() / 10, "{} bytes", compressed.len());
        assert!(std::ptr::eq(compressed, super::deflated(&big.clone()).unwrap()), "compressed once, shared by clones");
    }

    fn handshake(extensions: &[&str]) -> Option<Handshake> {
        let mut headers = HeaderMap::new();
        headers.insert(header::SEC_WEBSOCKET_VERSION, "13".parse().unwrap());
        headers.insert(header::SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==".parse().unwrap());
        for extension in extensions {
            headers.append(header::SEC_WEBSOCKET_EXTENSIONS, extension.parse().unwrap());
        }
        Handshake::accept(&headers)
    }

    #[test]
    fn handshakes_and_negotiates_compression() {
        // RFC 6455's example key.
        assert_eq!(handshake(&[]).unwrap().accept, "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");
        assert!(!handshake(&[]).unwrap().deflate());
        // What Chrome, Firefox and Safari offer.
        assert!(handshake(&["permessage-deflate; client_max_window_bits"]).unwrap().deflate());
        assert!(handshake(&["x-webkit-deflate-frame", "permessage-deflate"]).unwrap().deflate());
        assert!(handshake(&["permessage-deflate; server_no_context_takeover; client_max_window_bits=10"]).unwrap().deflate());
        assert!(!handshake(&["permessage-deflate; server_max_window_bits=10"]).unwrap().deflate());
        assert!(!handshake(&["permessage-deflate; unknown"]).unwrap().deflate());

        let mut headers = HeaderMap::new();
        headers.insert(header::SEC_WEBSOCKET_VERSION, "8".parse().unwrap());
        headers.insert(header::SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==".parse().unwrap());
        assert!(Handshake::accept(&headers).is_none());
    }
}
