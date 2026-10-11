//! `Rack::Deflater` (rack 3.2), which the reference installs around the whole app in `config.ru`:
//! it gzips every response with a body when the client accepts gzip, whatever its size or type,
//! and adds `Accept-Encoding` to `Vary`. A gzipped body has no `Content-Length`, so it goes out
//! chunked (and the front server's compression leaves it alone).

use std::io::Write;
use std::sync::{Arc, LazyLock};

use axum::body::Body;
use axum::extract::Request;
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::Next;
use axum::response::Response;
use bytes::Bytes;
use flate2::write::GzEncoder;
use flate2::{Compression, GzBuilder};
use futures_util::StreamExt;
use http_body_util::BodyExt;

pub mod splice;

use splice::{ENTRY_OVERHEAD, Generations, SHARDS, Shards};

/// A response `ActionDispatch::Static` served (a public file or an asset). Marks responses the
/// middleware below `Static` in the reference (`Rack::Runtime`, `ActionDispatch::RequestId`)
/// never saw.
#[derive(Debug, Clone, Copy)]
pub struct StaticFile;

/// A `Content-Length` the app set itself (`PublicExceptions`, `ShowExceptions#pass_response`),
/// as opposed to one hyper adds after this middleware from the body's size.
#[derive(Debug, Clone, Copy)]
pub struct AppContentLength;

/// The SHA-256 `Rack::ETag` took of a response's whole body: the body's identity, so the gzip of a
/// body that repeats (the sidebar) comes from [`GZIPPED`] instead of being deflated again.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct BodyDigest(pub(crate) [u8; 32]);

/// A bound on the bytes of kept gzip members (a sidebar's is ~6 KB).
const MAX_GZIPPED_BYTES: usize = 16 << 20;

/// The `Rack::Deflater` middleware.
pub async fn deflater(request: Request, next: Next) -> Response {
    let accept_encoding = request.headers().get(header::ACCEPT_ENCODING).and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
    let path = request.uri().path_and_query().map(|p| p.as_str().to_string()).unwrap_or_default();
    let mut response = next.run(request).await;
    if !should_deflate(&response) {
        return response;
    }

    let encoding = select_best_encoding(&["gzip", "identity"], &parse_accept_encoding(&accept_encoding));

    let vary: Vec<String> = response
        .headers()
        .get_all(header::VARY)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(',').map(|t| t.trim().to_string()).collect::<Vec<_>>())
        .collect();
    if !vary.iter().any(|v| v == "*" || v.eq_ignore_ascii_case("accept-encoding")) {
        let mut vary: Vec<String> = if response.headers().contains_key(header::VARY) { vary } else { Vec::new() };
        vary.push("Accept-Encoding".into());
        if let Ok(value) = HeaderValue::from_str(&vary.join(",")) {
            response.headers_mut().insert(header::VARY, value);
        }
    }

    match encoding {
        Some("gzip") => {
            let mtime = response
                .headers()
                .get(header::LAST_MODIFIED)
                .and_then(|v| v.to_str().ok())
                .and_then(crate::clock::parse_httpdate)
                .map(|t| t.as_second() as u32)
                .unwrap_or(0);
            let headers = response.headers_mut();
            headers.insert(header::CONTENT_ENCODING, HeaderValue::from_static("gzip"));
            headers.remove(header::CONTENT_LENGTH);
            let (mut parts, body) = response.into_parts();
            let page_parts = parts.extensions.remove::<Arc<splice::PageParts>>();
            let body = if let Some(page_parts) = page_parts.filter(|page_parts| page_parts.fits(&body)) {
                // The same decoded bytes as `gzip_stream`, from the parts' stored pieces.
                single_chunk(page_parts.gzip(mtime).into())
            } else if let Some(digest) = parts.extensions.remove::<BodyDigest>() {
                gzip_digested(body, digest, mtime).await
            } else {
                gzip_stream(body, mtime)
            };
            Response::from_parts(parts, body)
        }
        Some(_) => response,
        None => {
            let message = format!("An acceptable encoding for the requested resource {path} could not be found.");
            let mut response = Response::new(Body::from(message.clone()));
            *response.status_mut() = StatusCode::NOT_ACCEPTABLE;
            response.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_static("text/plain"));
            response.headers_mut().insert(header::CONTENT_LENGTH, HeaderValue::from(message.len()));
            response
        }
    }
}

/// `should_deflate?` (rack 3.2.6; the reference passes no `:include` or `:if`): not for statuses
/// without a body (1xx, 204, 304), `no-transform`, non-identity `Content-Encoding`, or a
/// `Content-Length: 0` the app set (static files and error pages; an app response's length is
/// otherwise the server's doing, after this middleware, so empty rendered bodies are gzipped).
fn should_deflate(response: &Response) -> bool {
    let status = response.status().as_u16();
    if matches!(status, 100..=199 | 204 | 304) {
        return false;
    }
    let headers = response.headers();
    let get = |name| headers.get(name).and_then(|v: &HeaderValue| v.to_str().ok());
    if get(header::CACHE_CONTROL).is_some_and(|cc| has_word(cc, "no-transform")) {
        return false;
    }
    if get(header::CONTENT_ENCODING).is_some_and(|ce| !has_word(ce, "identity")) {
        return false;
    }
    let extensions = response.extensions();
    let app_set_length = extensions.get::<StaticFile>().is_some() || extensions.get::<AppContentLength>().is_some();
    !(app_set_length && get(header::CONTENT_LENGTH) == Some("0"))
}

/// `/\bword\b/`
fn has_word(haystack: &str, word: &str) -> bool {
    let is_word = |c: char| c.is_ascii_alphanumeric() || c == '_';
    haystack.match_indices(word).any(|(i, _)| {
        let before = haystack[..i].chars().next_back();
        let after = haystack[i + word.len()..].chars().next();
        !before.is_some_and(is_word) && !after.is_some_and(is_word)
    })
}

/// `Rack::Request#accept_encoding` (`parse_http_accept_header`).
fn parse_accept_encoding(header: &str) -> Vec<(String, f64)> {
    header
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(|part| {
            let (attribute, parameters) = match part.split_once(';') {
                Some((a, p)) => (a.trim(), Some(p.trim())),
                None => (part, None),
            };
            // `/\Aq=([\d.]+)/ =~ parameters`, else 1.0.
            let quality = parameters
                .and_then(|p| p.strip_prefix("q="))
                .map(|q| &q[..q.find(|c: char| !c.is_ascii_digit() && c != '.').unwrap_or(q.len())])
                .filter(|digits| !digits.is_empty())
                .map(ruby_compat::to_f)
                .unwrap_or(1.0);
            (attribute.to_string(), quality)
        })
        .collect()
}

/// `Rack::Utils.select_best_encoding`
fn select_best_encoding(available: &[&'static str], accept: &[(String, f64)]) -> Option<&'static str> {
    let accept = &accept[..accept.len().min(16)];
    let mut expanded: Vec<(String, f64, usize)> = Vec::new();
    let mut wildcard_seen = false;
    for (m, q) in accept {
        let preference = available.iter().position(|a| a == m).unwrap_or(available.len());
        if m == "*" {
            if !wildcard_seen {
                for m2 in available.iter().filter(|a| !accept.iter().any(|(m, _)| m == *a)) {
                    expanded.push((m2.to_string(), *q, preference));
                }
                wildcard_seen = true;
            }
        } else {
            expanded.push((m.clone(), *q, preference));
        }
    }
    let mut sorted = expanded.clone();
    sorted.sort_by(|(_, q1, p1), (_, q2, p2)| q2.partial_cmp(q1).unwrap_or(std::cmp::Ordering::Equal).then(p1.cmp(p2)));
    let mut candidates: Vec<String> = sorted.into_iter().map(|(m, _, _)| m).collect();
    if !candidates.iter().any(|c| c == "identity") {
        candidates.push("identity".into());
    }
    for (m, q, _) in &expanded {
        if *q == 0.0 {
            candidates.retain(|c| c != m);
        }
    }
    candidates.iter().find_map(|c| available.iter().copied().find(|a| a == c))
}

/// `GzipStream` with `sync: true`: each body chunk is compressed and flushed as it arrives.
/// `Zlib::GzipWriter` writes the header with the given mtime and the Unix OS code.
fn gzip_stream(body: Body, mtime: u32) -> Body {
    let encoder = gzip_encoder(mtime, level_for(hyper::body::Body::size_hint(&body).exact()));
    let chunks = body.into_data_stream();
    let stream = futures_util::stream::unfold(Some((chunks, encoder)), |state| async move {
        let (mut chunks, mut encoder) = state?;
        loop {
            match chunks.next().await {
                Some(Ok(chunk)) => {
                    if chunk.is_empty() {
                        continue;
                    }
                    let output = compress(&mut encoder, &chunk);
                    return Some((output, Some((chunks, encoder))));
                }
                Some(Err(error)) => return Some((Err(std::io::Error::other(error)), None)),
                None => return Some((encoder.finish().map(Bytes::from), None)),
            }
        }
    });
    Body::from_stream(stream)
}

fn gzip_encoder(mtime: u32, level: Compression) -> GzEncoder<Vec<u8>> {
    GzBuilder::new().mtime(mtime).operating_system(3).write(Vec::new(), level)
}

/// Bodies of a known size up to this are gzipped at level 1.
const SMALL_BODY: u64 = 16 << 10;

/// The gzip level for a body of `len` bytes: level 1 for a small one, `Zlib`'s default (6) for
/// the rest and for bodies of unknown size. On a small body, which is mostly unique (a posted
/// message's turbo stream), level 6's longer match search costs several times level 1's CPU for
/// output that is at most a few hundred bytes smaller.
fn level_for(len: Option<u64>) -> Compression {
    match len {
        Some(len) if len <= SMALL_BODY => Compression::fast(),
        _ => Compression::default(),
    }
}

/// A body `Rack::ETag` digested (always a single buffer), gzipped once while it keeps repeating.
async fn gzip_digested(body: Body, digest: BodyDigest, mtime: u32) -> Body {
    let key = (digest, mtime);
    let cached = GZIPPED.lock_for(&key).get(&key, Bytes::clone);
    if let Some(gzipped) = cached {
        return single_chunk(gzipped);
    }
    let gzipped = match body.collect().await {
        Ok(collected) => gzip_member(&collected.to_bytes(), mtime),
        Err(error) => Err(std::io::Error::other(error)),
    };
    match gzipped {
        Ok(gzipped) => {
            // One huge body mustn't push out everything else.
            if gzipped.len() <= MAX_GZIPPED_BYTES / 4 {
                GZIPPED.lock_for(&key).insert(key, gzipped.clone());
            }
            single_chunk(gzipped)
        }
        Err(error) => Body::from_stream(futures_util::stream::once(async move { Err::<Bytes, _>(error) })),
    }
}

/// What [`gzip_stream`] sends for a single-buffer body, in one piece.
fn gzip_member(body: &[u8], mtime: u32) -> std::io::Result<Bytes> {
    let mut encoder = gzip_encoder(mtime, level_for(Some(body.len() as u64)));
    if !body.is_empty() {
        encoder.write_all(body)?;
        encoder.flush()?;
    }
    let mut member = encoder.finish()?;
    // Kept for as long as it's used, so without the spare capacity growing it left.
    member.shrink_to_fit();
    Ok(member.into())
}

/// Streamed like [`gzip_stream`]'s output, so no `Content-Length` goes with it.
fn single_chunk(gzipped: Bytes) -> Body {
    Body::from_stream(futures_util::stream::once(async move { Ok::<_, std::io::Error>(gzipped) }))
}

fn compress(encoder: &mut GzEncoder<Vec<u8>>, chunk: &[u8]) -> std::io::Result<Bytes> {
    encoder.write_all(chunk)?;
    encoder.flush()?;
    Ok(Bytes::from(std::mem::take(encoder.get_mut())))
}

/// Gzip members by body digest and gzip mtime.
static GZIPPED: LazyLock<Shards<Generations<(BodyDigest, u32), Bytes>>> = LazyLock::new(|| {
    Shards::new(|| Generations::with_budget(MAX_GZIPPED_BYTES / SHARDS, |_, gzipped: &Bytes| gzipped.len() + ENTRY_OVERHEAD))
});

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::Digest;

    fn best(header: &str) -> Option<&'static str> {
        select_best_encoding(&["gzip", "identity"], &parse_accept_encoding(header))
    }

    #[test]
    fn encoding_selection() {
        assert_eq!(best("gzip, deflate, br"), Some("gzip"));
        assert_eq!(best(""), Some("identity"));
        assert_eq!(best("br"), Some("identity"));
        assert_eq!(best("gzip;q=0"), Some("identity"));
        assert_eq!(best("identity;q=0.5, gzip;q=0.1"), Some("identity"));
        assert_eq!(best("*"), Some("gzip"));
        assert_eq!(best("identity;q=0, *;q=0"), None);
        assert_eq!(best("gzip;q=0, identity;q=0"), None);
    }

    #[test]
    fn q_values_read_like_rack() {
        // `Rack::Request#accept_encoding` and `select_best_encoding` in the reference, where
        // `identity;q=` is a 200 and `identity;q=0` a 406.
        assert_eq!(best("identity;q="), Some("identity"));
        assert_eq!(best("identity;q=0"), None);
        assert_eq!(best("gzip;q="), Some("gzip"));
        assert_eq!(best("gzip;q=abc"), Some("gzip"));
        assert_eq!(best("gzip;Q=0.5, identity;q=0.9"), Some("gzip"));
        assert_eq!(best("gzip;q=.5"), Some("gzip"));
        assert_eq!(best("gzip;q=."), Some("identity"));
        assert_eq!(best("gzip;q=0..5, identity;q=0.1"), Some("identity"));
        assert_eq!(best("gzip;q=0.5.1, identity;q=0.6"), Some("identity"));
        assert_eq!(
            parse_accept_encoding("gzip;q=0.5.1, identity;q=, br;q=1."),
            [("gzip".to_string(), 0.5), ("identity".to_string(), 1.0), ("br".to_string(), 1.0)]
        );
    }

    #[test]
    fn words() {
        assert!(has_word("no-transform, public", "no-transform"));
        assert!(!has_word("xno-transform", "no-transform"));
        assert!(has_word("identity", "identity"));
    }

    #[tokio::test]
    async fn gzip_round_trip_and_empty_bodies() {
        use http_body_util::BodyExt;
        use std::io::Read;
        for input in [&b""[..], b"hello world"] {
            let body = gzip_stream(Body::from(Bytes::copy_from_slice(input)), 0);
            let bytes = body.collect().await.unwrap().to_bytes();
            assert!(bytes.len() >= 20);
            let mut out = Vec::new();
            flate2::read::GzDecoder::new(&bytes[..]).read_to_end(&mut out).unwrap();
            assert_eq!(out, input);
        }
    }

    async fn gzipped(body: Body) -> Bytes {
        body.collect().await.unwrap().to_bytes()
    }

    fn digest(body: &[u8]) -> BodyDigest {
        BodyDigest(sha2::Sha256::digest(body).into())
    }

    #[tokio::test]
    async fn digested_bodies_are_gzipped_once() {
        let body = Bytes::from("<a href=\"/rooms/1\">Room</a>".repeat(300));
        let first = gzipped(gzip_digested(Body::from(body.clone()), digest(&body), 0).await).await;
        assert_eq!(first, gzipped(gzip_stream(Body::from(body.clone()), 0)).await, "what gzip_stream sends");
        let key = (digest(&body), 0);
        let kept = GZIPPED.lock_for(&key).get(&key, Bytes::clone).expect("kept");
        let again = gzipped(gzip_digested(Body::from(body.clone()), digest(&body), 0).await).await;
        assert_eq!(again.as_ptr(), kept.as_ptr(), "not gzipped again");

        let other = Bytes::from("<a href=\"/rooms/2\">Room</a>".repeat(300));
        let gzip = gzipped(gzip_digested(Body::from(other.clone()), digest(&other), 0).await).await;
        assert_eq!(gzip, gzipped(gzip_stream(Body::from(other), 0)).await);
    }

    #[tokio::test]
    async fn gzip_round_trip_of_a_page_larger_than_the_window() {
        use std::io::Read;
        // Long repeats and several windows' worth of input, in chunks: the match comparison, the
        // window slide and the CRC all run, in their SIMD versions where zlib-rs has them for this
        // CPU (the scalar ones compute the same bytes).
        let page: Vec<u8> = (0..3000)
            .flat_map(|n| format!("<div id=\"message_{n}\" class=\"message\">{}</div>\n", "hello there ".repeat(n % 13)).into_bytes())
            .collect();
        assert!(page.len() > 4 * 32 * 1024);
        let chunks: Vec<std::io::Result<Bytes>> = page.chunks(40_000).map(|chunk| Ok(Bytes::copy_from_slice(chunk))).collect();
        let body = gzip_stream(Body::from_stream(futures_util::stream::iter(chunks)), 0);
        let bytes = body.collect().await.unwrap().to_bytes();
        let mut out = Vec::new();
        flate2::read::GzDecoder::new(&bytes[..]).read_to_end(&mut out).unwrap();
        assert_eq!(out, page);
    }
}
