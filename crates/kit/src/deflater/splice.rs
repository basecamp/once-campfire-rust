//! gzip and ETags for pages made mostly of cached fragments (a room's messages), without
//! recompressing or rehashing the whole page on every request.
//!
//! A page arrives in parts that cover it end to end: its cached fragments (each with the few
//! bytes of text before it, when that follows another fragment) and the text in between (the
//! layout). The template recorded where each fragment went as it rendered, so the page's bytes
//! are never joined into one buffer, nor searched for its fragments: a plain response sends the
//! parts one after another. Each part is compressed once, against the part before it as a preset
//! dictionary, and kept with the part's CRC-32: deflate back-references can reach anything in the
//! last 32 KB of output, so a piece is valid wherever the same predecessor comes right before it.
//! Pages render the same until what they show changes (there are no per-request CSRF tokens), so
//! from one request to the next a page is a run of stored pieces: gzip costs some copying and
//! combining their CRCs, and the ETag a hash of the parts' digests instead of the whole body.
//! Compressing each part on its own would lose what consecutive messages share and make a room
//! page ~4× larger; chained like this it's within 1% of compressing the page whole.
//!
//! A stored piece is found by its part (a text by its SHA-256, a fragment by the `Arc` the fragment
//! cache hands out) and by the SHA-256 of the part before it: those bytes are all it depends on
//! besides its own, so it's valid after any part with the same ones. The digests are remembered,
//! so pages don't hash their parts on every request: a fragment's with its `Arc`, a text's by the
//! text's bytes.

use std::borrow::{Borrow, Cow};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::convert::Infallible;
use std::hash::{BuildHasher, Hash};
use std::pin::Pin;
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, Weak};
use std::task::{Context, Poll};

use bytes::Bytes;
use flate2::{Compress, Compression, FlushCompress};
use foldhash::fast::RandomState;
use hyper::body::{Body as HttpBody, Frame, SizeHint};
use sha2::{Digest, Sha256};

/// Smaller fragments aren't worth a part of their own; they stay in the text around them.
const MIN_FRAGMENT: usize = 1024;
/// At most this much text between two fragments travels with the second; more is a text part.
const MAX_GLUE: usize = 256;
/// Deflate's window.
const WINDOW: usize = 32 * 1024;
/// A bound on what remembered fragments cost, their pieces included. A message's fragment is ~10 KB
/// and its pieces (one for each predecessor it's seen after, usually one or two) ~1 KB each, so
/// the ~3,300 messages a 32 MB fragment cache holds take ~7 MB, well within a generation.
const MAX_FRAGMENT_BYTES: usize = 32 << 20;
/// Pieces kept per fragment, for the predecessors it's seen with: a message follows the same one in
/// its room and on a page of older messages, and other ones in search results.
const PIECES_PER_FRAGMENT: usize = 4;
/// A bound on the bytes of stored text pieces (a room page's layout is ~10 KB compressed).
const MAX_TEXT_PIECE_BYTES: usize = 16 << 20;
/// Larger pieces aren't stored: one would take a good part of a generation, and rotating
/// generations to fit it would push out the pieces pages keep using.
const MAX_STORED_TEXT_PIECE: usize = MAX_TEXT_PIECE_BYTES / 64;
/// A bound on the bytes of texts remembered with their SHA-256. A room page's texts (the layout
/// before and after its messages) are ~34 KB for each person and room, so a generation holds those
/// of ~240 rooms as people see them: every page in use at a small install. A text that isn't
/// remembered is simply hashed again.
const MAX_TEXT_BYTES: usize = 16 << 20;
/// Larger texts are hashed every time, for the same reason larger pieces aren't stored.
const MAX_STORED_TEXT: usize = MAX_TEXT_BYTES / 64;
/// What each stored entry costs beyond its bytes: the map's slot, its key and the allocation.
pub(super) const ENTRY_OVERHEAD: usize = 128;

type Sha = [u8; 32];

/// A page's body split at its cached fragments, with each part's identity.
#[derive(Debug)]
pub struct PageParts {
    len: usize,
    parts: Vec<Part>,
}

#[derive(Debug)]
enum Part {
    Text {
        text: Bytes,
        sha: Sha,
    },
    Fragment {
        fragment: Arc<String>,
        sha: Sha,
        /// The text since the previous fragment, which goes out just before this one.
        glue: Bytes,
    },
}

/// What comes right before a part, which its piece may refer back into: the SHA-256 of exactly
/// the bytes it may use (see [`Part::as_before`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Before {
    Nothing,
    Fragment(Sha),
    Text(Sha),
}

/// A part compressed: raw deflate ending on a sync flush, and the part's CRC.
#[derive(Clone)]
struct Piece {
    deflated: Bytes,
    crc: Crc,
}

impl Part {
    /// The part's bytes, the glue joined to its fragment (only a part being compressed needs that).
    fn bytes(&self) -> Cow<'_, [u8]> {
        match self {
            Part::Text { text, .. } => Cow::Borrowed(text),
            Part::Fragment { fragment, glue, .. } if glue.is_empty() => Cow::Borrowed(fragment.as_bytes()),
            Part::Fragment { fragment, glue, .. } => Cow::Owned([glue, fragment.as_bytes()].concat()),
        }
    }

    /// The part as body chunks: the text, or the glue (if any) and then the fragment.
    fn chunks(&self) -> (Bytes, Option<Bytes>) {
        match self {
            Part::Text { text, .. } => (text.clone(), None),
            Part::Fragment { fragment, glue, .. } if glue.is_empty() => (shared(fragment), None),
            Part::Fragment { fragment, glue, .. } => (glue.clone(), Some(shared(fragment))),
        }
    }

    /// The identity of the part as the part after it sees it, and the bytes its piece may use as
    /// a dictionary: a fragment's own bytes (never its glue), or the text.
    fn as_before(&self) -> (Before, &[u8]) {
        match self {
            Part::Text { text, sha } => (Before::Text(*sha), text),
            Part::Fragment { fragment, sha, .. } => (Before::Fragment(*sha), fragment.as_bytes()),
        }
    }
}

impl PageParts {
    /// The body `text` makes with each of `fragments` (cached HTML, in order) spliced in at its
    /// byte offset in `text`, split into parts; or that body whole, when no fragment is big enough
    /// for a part of its own.
    ///
    /// # Panics
    ///
    /// If the offsets go backwards or past the end of `text`.
    pub fn splice(text: &Bytes, fragments: Vec<(usize, Arc<String>)>) -> Result<Self, Bytes> {
        let len = text.len() + fragments.iter().map(|(_, fragment)| fragment.len()).sum::<usize>();
        let mut between = Gathered::default();
        let mut runs = Vec::with_capacity(fragments.len());
        let mut position = 0;
        for (offset, fragment) in fragments {
            between.push(text.slice(position..offset));
            position = offset;
            if fragment.len() < MIN_FRAGMENT {
                between.push(shared(&fragment));
            } else {
                runs.push((between.take(), fragment));
            }
        }
        between.push(text.slice(position..));
        let rest = between.take();
        if runs.is_empty() {
            return Err(rest);
        }
        let shas = fragment_shas(runs.iter().map(|(_, fragment)| fragment));
        let mut parts = Vec::with_capacity(runs.len() * 2 + 1);
        for ((gap, fragment), sha) in runs.into_iter().zip(shas) {
            let follows_fragment = matches!(parts.last(), Some(Part::Fragment { .. }));
            let glue = if follows_fragment && gap.len() <= MAX_GLUE {
                gap
            } else {
                if !gap.is_empty() {
                    parts.push(text_part(gap));
                }
                Bytes::new()
            };
            parts.push(Part::Fragment { fragment, sha, glue });
        }
        if !rest.is_empty() {
            parts.push(text_part(rest));
        }
        Ok(Self { len, parts })
    }

    /// The body's length.
    pub fn body_len(&self) -> usize {
        self.len
    }

    /// Whether `body` is still these parts' plain body (a HEAD response's is empty).
    pub fn fits(&self, body: &impl HttpBody) -> bool {
        body.size_hint().exact() == Some(self.len as u64)
    }

    /// The body, sent part by part.
    pub fn plain_body(self: &Arc<Self>) -> PlainBody {
        PlainBody { page: self.clone(), next: 0, fragment: None, remaining: self.len as u64 }
    }

    /// The weak ETag's value: 32 hex digits of a SHA-256 over the parts (the same body split the
    /// same way always gets the same one), as `Rack::ETag`'s is of the body.
    pub fn etag(&self) -> String {
        let mut hasher = Sha256::new();
        for part in &self.parts {
            match part {
                Part::Text { text, sha } => {
                    hasher.update(b"T");
                    hasher.update((text.len() as u64).to_le_bytes());
                    hasher.update(sha);
                }
                Part::Fragment { sha, glue, fragment } => {
                    hasher.update(b"F");
                    hasher.update((glue.len() as u64).to_le_bytes());
                    hasher.update(glue);
                    hasher.update((fragment.len() as u64).to_le_bytes());
                    hasher.update(sha);
                }
            }
        }
        hex::encode(&hasher.finalize()[..16])
    }

    /// The whole gzip member for the body. `mtime` and the Unix OS code go in the header, as
    /// `Zlib::GzipWriter` writes them.
    pub fn gzip(&self, mtime: u32) -> Vec<u8> {
        let pieces = self.pieces();
        let mut out = Vec::with_capacity(pieces.iter().map(|piece| piece.deflated.len()).sum::<usize>() + 20);
        out.extend_from_slice(&[0x1f, 0x8b, 8, 0]);
        out.extend_from_slice(&mtime.to_le_bytes());
        out.extend_from_slice(&[0, 3]);
        for piece in &pieces {
            out.extend_from_slice(&piece.deflated);
        }
        // An empty final block (fixed Huffman), after the sync flushes that ended every piece.
        out.extend_from_slice(&[0x03, 0x00]);
        let crc = Crc::concatenated(pieces.iter().map(|piece| piece.crc));
        out.extend_from_slice(&crc.to_le_bytes());
        out.extend_from_slice(&(self.len as u32).to_le_bytes());
        out
    }

    /// Each part's piece: stored ones where they fit, the rest compressed and stored.
    fn pieces(&self) -> Vec<Piece> {
        let befores: Vec<(Before, &[u8])> =
            std::iter::once((Before::Nothing, &b""[..])).chain(self.parts.iter().map(Part::as_before)).collect();
        // Only look up under the locks; compressing happens outside them.
        let stored: Vec<Option<Piece>> = self
            .parts
            .iter()
            .zip(&befores)
            .map(|(part, (before, _))| match part {
                Part::Text { sha, .. } => {
                    let key = (*sha, *before);
                    TEXT_PIECES.lock_for(&key).get(&key, Piece::clone)
                }
                Part::Fragment { fragment, glue, .. } => {
                    let key = fragment_key(fragment);
                    FRAGMENTS.lock_for(&key).get(&key, |known| known.piece_after(*before, glue)).flatten()
                }
            })
            .collect();
        let mut pieces = Vec::with_capacity(self.parts.len());
        let mut new_texts = Vec::new();
        let mut new_fragments = Vec::new();
        for ((part, (before, dictionary)), stored) in self.parts.iter().zip(&befores).zip(stored) {
            if let Some(piece) = stored {
                pieces.push(piece);
                continue;
            }
            let bytes = part.bytes();
            let compressed = Piece { deflated: compress(dictionary, &bytes), crc: Crc::of(&bytes) };
            match part {
                Part::Text { .. } if compressed.deflated.len() > MAX_STORED_TEXT_PIECE => {}
                Part::Text { sha, .. } => new_texts.push(((*sha, *before), compressed.clone())),
                Part::Fragment { fragment, glue, .. } => new_fragments
                    .push((fragment.clone(), FragmentPiece { before: *before, glue: glue.to_vec().into(), piece: compressed.clone() })),
            }
            pieces.push(compressed);
        }
        for (key, piece) in new_texts {
            TEXT_PIECES.lock_for(&key).insert(key, piece);
        }
        for (fragment, piece) in new_fragments {
            let key = fragment_key(&fragment);
            FRAGMENTS.lock_for(&key).update(&key, |known| known.store(piece));
        }
        pieces
    }
}

fn text_part(text: Bytes) -> Part {
    let sha = text_sha(&TEXT_SHAS, &text);
    Part::Text { text, sha }
}

/// The SHA-256 of `text`, remembered by its bytes: a page repeats its texts (a room's layout) from
/// one request to the next, and finding one again costs a fast hash and a compare, a small part of
/// hashing it. The compare is what makes this sound, since the SHA-256 is what stored pieces are
/// found by: two texts whose fast hashes collide still get their own.
fn text_sha<S: BuildHasher + Default>(shas: &Shards<Generations<Box<[u8]>, Sha, S>>, text: &[u8]) -> Sha {
    if let Some(sha) = shas.lock_for(text).get(text, |sha| *sha) {
        return sha;
    }
    let sha = Sha256::digest(text).into();
    if text.len() <= MAX_STORED_TEXT {
        shas.lock_for(text).insert(text.into(), sha);
    }
    sha
}

/// A fragment's bytes as a `Bytes` that shares them.
fn shared(fragment: &Arc<String>) -> Bytes {
    struct Shared(Arc<String>);
    impl AsRef<[u8]> for Shared {
        fn as_ref(&self) -> &[u8] {
            self.0.as_bytes()
        }
    }
    Bytes::from_owner(Shared(fragment.clone()))
}

/// Body bytes gathered piece by piece (text, and fragments too small for a part of their own),
/// joined only when there's more than one piece.
#[derive(Default)]
struct Gathered(Vec<Bytes>);

impl Gathered {
    fn push(&mut self, bytes: Bytes) {
        if !bytes.is_empty() {
            self.0.push(bytes);
        }
    }

    fn take(&mut self) -> Bytes {
        match self.0.len() {
            0 => Bytes::new(),
            1 => self.0.pop().expect("one piece"),
            _ => std::mem::take(&mut self.0).concat().into(),
        }
    }
}

/// A page's plain body: its parts' bytes one after another, never joined into one buffer.
pub struct PlainBody {
    page: Arc<PageParts>,
    /// The index of the next part to send.
    next: usize,
    /// A fragment due after the glue just sent.
    fragment: Option<Bytes>,
    remaining: u64,
}

impl HttpBody for PlainBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        let this = self.get_mut();
        let chunk = this.fragment.take().or_else(|| {
            let part = this.page.parts.get(this.next)?;
            this.next += 1;
            let (chunk, fragment) = part.chunks();
            this.fragment = fragment;
            Some(chunk)
        });
        Poll::Ready(chunk.map(|chunk| {
            this.remaining -= chunk.len() as u64;
            Ok(Frame::data(chunk))
        }))
    }

    fn is_end_stream(&self) -> bool {
        self.remaining == 0
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.remaining)
    }
}

/// Raw deflate of `text` at level 6 with `dictionary` (its last 32 KB) preset, sync-flushed so it
/// ends on a byte boundary with no final block.
fn compress(dictionary: &[u8], text: &[u8]) -> Bytes {
    let mut deflate = Compress::new(Compression::default(), false);
    if !dictionary.is_empty() {
        deflate
            .set_dictionary(&dictionary[dictionary.len().saturating_sub(WINDOW)..])
            .expect("a fresh raw deflate stream takes a dictionary");
    }
    let mut out = Vec::with_capacity(text.len() / 4 + 64);
    loop {
        let consumed = deflate.total_in() as usize;
        if out.capacity() - out.len() < 1024 {
            out.reserve(out.capacity().max(4096));
        }
        deflate.compress_vec(&text[consumed..], &mut out, FlushCompress::Sync).expect("deflate doesn't fail on valid input");
        // Done once all input is in and the flush left spare room in the output.
        if deflate.total_in() as usize == text.len() && out.len() < out.capacity() {
            break;
        }
    }
    // Stored pieces live on, so they keep only what they use (`Bytes` keeps a `Vec`'s capacity).
    out.shrink_to_fit();
    out.into()
}

// --- CRC-32 from the pieces' --------------------------------------------------------------------

/// gzip's CRC-32 of a part, kept so a page's comes from its parts' without reading the body: the
/// CRC of `a ‖ b` is the CRC of `a` times x^(8·|b|), plus the CRC of `b`, as polynomials over GF(2)
/// modulo the CRC's (zlib's `crc32_combine_op`). A part keeps its x^(8·|b|) too, so each step is
/// one multiplication; `crc32fast::Hasher::combine` works it out from the length every time, which
/// for a room page's parts costs more than a CRC of the whole page.
#[derive(Clone, Copy)]
struct Crc {
    value: u32,
    shift: u32,
}

/// The CRC-32 polynomial, in the reflected bit order gzip's CRC uses (bit 31 is x^0).
const POLYNOMIAL: u32 = 0xedb8_8320;

/// x^(2^k) modulo the polynomial, for k in 0..32 (zlib's `x2n_table`).
const X_TO_THE_2_TO_THE: [u32; 32] = {
    let mut table = [0; 32];
    let mut power = 1 << 30; // x^1
    let mut k = 0;
    while k < 32 {
        table[k] = power;
        power = multiply(power, power);
        k += 1;
    }
    table
};

impl Crc {
    fn of(bytes: &[u8]) -> Self {
        Self { value: crc32fast::hash(bytes), shift: x_to_the_8(bytes.len() as u64) }
    }

    /// The CRC of `crcs`' parts, one after the other.
    fn concatenated(crcs: impl Iterator<Item = Crc>) -> u32 {
        crcs.fold(0, |value, next| multiply(value, next.shift) ^ next.value)
    }
}

/// x^(8·n) modulo the polynomial (zlib's `x2nmodp(n, 3)`). x's order divides 2^32 − 1, so
/// x^(2^32) is x^(2^0).
fn x_to_the_8(mut n: u64) -> u32 {
    let mut power = 1 << 31; // x^0
    let mut k = 3;
    while n != 0 {
        if n & 1 == 1 {
            power = multiply(X_TO_THE_2_TO_THE[k % 32], power);
        }
        n >>= 1;
        k += 1;
    }
    power
}

/// `a` times `b` modulo the polynomial (zlib's `multmodp`), without branching on the bits.
const fn multiply(a: u32, mut b: u32) -> u32 {
    let mut product = 0;
    let mut bit = 32;
    while bit > 0 {
        bit -= 1;
        product ^= b & ((a >> bit) & 1).wrapping_neg();
        b = (b >> 1) ^ (POLYNOMIAL & (b & 1).wrapping_neg());
    }
    product
}

// --- What's remembered -------------------------------------------------------------------------

/// A fragment seen in a page: its SHA-256, and its pieces for the predecessors it's followed, most
/// recently stored last.
struct KnownFragment {
    /// Keeps the fragment's address from being reused while the entry exists.
    _pin: Weak<String>,
    sha: Sha,
    pieces: Vec<Arc<FragmentPiece>>,
}

struct FragmentPiece {
    before: Before,
    glue: Box<[u8]>,
    /// The glue and the fragment.
    piece: Piece,
}

impl KnownFragment {
    fn piece_after(&self, before: Before, glue: &[u8]) -> Option<Piece> {
        self.pieces.iter().find(|piece| piece.before == before && *piece.glue == *glue).map(|piece| piece.piece.clone())
    }

    /// Stores `piece`, dropping the oldest when there are already [`PIECES_PER_FRAGMENT`].
    fn store(&mut self, piece: FragmentPiece) {
        self.pieces.retain(|stored| stored.before != piece.before || stored.glue != piece.glue);
        if self.pieces.len() == PIECES_PER_FRAGMENT {
            self.pieces.remove(0);
        }
        self.pieces.push(Arc::new(piece));
    }

    /// What the entry holds: itself, and its pieces with their glue.
    fn cost(&self) -> usize {
        ENTRY_OVERHEAD + self.pieces.iter().map(|piece| piece.piece.deflated.len() + piece.glue.len() + ENTRY_OVERHEAD).sum::<usize>()
    }
}

/// Known fragments by the address of their `Arc`: while an entry exists, its `_pin` keeps the
/// address from being reused, so the entry at an address is that fragment's.
type KnownFragments = Generations<usize, KnownFragment>;

impl KnownFragments {
    /// `fragment`'s SHA-256, hashing (and remembering) it the first time it's seen.
    fn sha(&mut self, fragment: &Arc<String>) -> Sha {
        let key = fragment_key(fragment);
        self.get(&key, |known| known.sha).unwrap_or_else(|| {
            let sha = Sha256::digest(fragment.as_bytes()).into();
            self.insert(key, KnownFragment { _pin: Arc::downgrade(fragment), sha, pieces: Vec::new() });
            sha
        })
    }
}

/// A map in two generations, bounded by what its entries cost: reading an old entry promotes it,
/// and when the young generation costs more than half the budget it becomes the old one (dropping
/// the previous old one). Bounded, and what pages keep using stays. Hashed with foldhash: text keys
/// are tens of KB, and SipHash would take a third as long as the SHA-256 finding them saves.
pub(super) struct Generations<K, V, S = RandomState> {
    young: HashMap<K, V, S>,
    old: HashMap<K, V, S>,
    young_cost: usize,
    budget: usize,
    cost: fn(&K, &V) -> usize,
}

impl<K: Hash + Eq, V, S: BuildHasher + Default> Generations<K, V, S> {
    pub(super) fn with_budget(budget: usize, cost: fn(&K, &V) -> usize) -> Self {
        Self { young: HashMap::default(), old: HashMap::default(), young_cost: 0, budget, cost }
    }

    /// What `read` makes of the entry for `key`, promoting it if it's old.
    pub(super) fn get<Q, R>(&mut self, key: &Q, read: impl FnOnce(&V) -> R) -> Option<R>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        if let Some(value) = self.young.get(key) {
            return Some(read(value));
        }
        let (key, value) = self.old.remove_entry(key)?;
        let found = read(&value);
        self.insert(key, value);
        Some(found)
    }

    pub(super) fn insert(&mut self, key: K, value: V) {
        self.young_cost += (self.cost)(&key, &value);
        match self.young.entry(key) {
            // Another request stored the same meanwhile.
            Entry::Occupied(mut entry) => {
                self.young_cost -= (self.cost)(entry.key(), entry.get());
                entry.insert(value);
            }
            Entry::Vacant(entry) => {
                entry.insert(value);
            }
        }
        self.rotate_when_full();
    }

    /// Changes the entry for `key`, if there is one, promoting it if it's old and counting what it
    /// costs now.
    fn update(&mut self, key: &K, change: impl FnOnce(&mut V)) {
        if let Some(value) = self.young.get_mut(key) {
            let before = (self.cost)(key, value);
            change(value);
            self.young_cost = self.young_cost - before + (self.cost)(key, value);
            self.rotate_when_full();
        } else if let Some((key, mut value)) = self.old.remove_entry(key) {
            change(&mut value);
            self.insert(key, value);
        }
    }

    fn rotate_when_full(&mut self) {
        if self.young_cost > self.budget / 2 {
            self.old = std::mem::take(&mut self.young);
            self.young_cost = 0;
        }
    }
}

/// Shards of a cache.
pub(super) const SHARDS: usize = 16;

/// A cache split by key hash into [`SHARDS`] parts, each behind its own lock: a page's parts are
/// looked up by every request on every core, and one lock for the whole cache made them wait for
/// each other. Each shard has an equal part of the cache's budget.
pub(super) struct Shards<T> {
    shards: Box<[Mutex<T>]>,
    hasher: RandomState,
}

impl<T> Shards<T> {
    pub(super) fn new(make: impl Fn() -> T) -> Self {
        Self { shards: (0..SHARDS).map(|_| Mutex::new(make())).collect(), hasher: RandomState::default() }
    }

    /// The shard that holds `key`.
    pub(super) fn lock_for<K: Hash + ?Sized>(&self, key: &K) -> MutexGuard<'_, T> {
        lock(&self.shards[self.hasher.hash_one(key) as usize % SHARDS])
    }
}

static FRAGMENTS: LazyLock<Shards<KnownFragments>> =
    LazyLock::new(|| Shards::new(|| Generations::with_budget(MAX_FRAGMENT_BYTES / SHARDS, |_, known: &KnownFragment| known.cost())));
static TEXT_PIECES: LazyLock<Shards<Generations<(Sha, Before), Piece>>> = LazyLock::new(|| {
    Shards::new(|| Generations::with_budget(MAX_TEXT_PIECE_BYTES / SHARDS, |_, piece: &Piece| piece.deflated.len() + ENTRY_OVERHEAD))
});
static TEXT_SHAS: LazyLock<Shards<Generations<Box<[u8]>, Sha>>> = LazyLock::new(|| {
    Shards::new(|| Generations::<Box<[u8]>, Sha>::with_budget(MAX_TEXT_BYTES / SHARDS, |text, _| text.len() + ENTRY_OVERHEAD))
});

pub(super) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn fragment_key(fragment: &Arc<String>) -> usize {
    Arc::as_ptr(fragment) as usize
}

/// Each fragment's SHA-256, hashing (and remembering) the ones not seen before.
fn fragment_shas<'a>(fragments: impl Iterator<Item = &'a Arc<String>>) -> Vec<Sha> {
    fragments.map(|fragment| FRAGMENTS.lock_for(&fragment_key(fragment)).sha(fragment)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    fn gunzip(bytes: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        flate2::read::GzDecoder::new(bytes).read_to_end(&mut out).unwrap();
        out
    }

    fn stored_pieces(fragment: &Arc<String>) -> Vec<Arc<FragmentPiece>> {
        let key = fragment_key(fragment);
        FRAGMENTS.lock_for(&key).get(&key, |known| known.pieces.clone()).expect("a known fragment")
    }

    fn message(n: usize) -> Arc<String> {
        Arc::new(format!("<div id=\"message_{n}\" class=\"message\">{}</div>\n", "<button>Boost</button> hello there ".repeat(40 + n % 7)))
    }

    /// A page as a template records it (its text, and where each fragment went), and the body it
    /// stands for.
    #[derive(Default, Clone)]
    struct Page {
        plain: String,
        text: String,
        fragments: Vec<(usize, Arc<String>)>,
    }

    impl Page {
        fn text(mut self, text: &str) -> Self {
            self.plain.push_str(text);
            self.text.push_str(text);
            self
        }

        fn fragment(mut self, fragment: &Arc<String>) -> Self {
            self.plain.push_str(fragment);
            self.fragments.push((self.text.len(), fragment.clone()));
            self
        }

        fn parts(&self) -> PageParts {
            PageParts::splice(&Bytes::from(self.text.clone()), self.fragments.clone()).expect("a fragment big enough for a part")
        }

        fn gzip(&self, mtime: u32) -> Vec<u8> {
            self.parts().gzip(mtime)
        }

        fn etag(&self) -> String {
            self.parts().etag()
        }
    }

    fn page(head: &str, messages: &[Arc<String>], tail: &str) -> Page {
        messages.iter().fold(Page::default().text(head), |page, message| page.text("  ").fragment(message)).text(tail)
    }

    async fn plain(parts: PageParts) -> Vec<u8> {
        use http_body_util::BodyExt;
        let body = Arc::new(parts).plain_body();
        assert_eq!(body.size_hint().exact(), Some(body.remaining));
        body.collect().await.unwrap().to_bytes().to_vec()
    }

    #[test]
    fn decodes_to_the_body_and_reuses_pieces() {
        let messages: Vec<_> = (0..30).map(message).collect();
        let page = page("<html><head>layout</head><body>", &messages, "</body></html>");
        let gz = page.gzip(1234);
        assert_eq!(gunzip(&gz), page.plain.as_bytes());
        assert_eq!(&gz[4..8], &1234u32.to_le_bytes());
        assert_eq!(gz[9], 3);
        let piece = stored_pieces(&messages[5])[0].clone();
        assert_eq!(page.gzip(1234), gz, "the same page is the same stored pieces");
        assert!(Arc::ptr_eq(&piece, &stored_pieces(&messages[5])[0]));
    }

    #[tokio::test]
    async fn the_plain_body_is_the_page() {
        let messages: Vec<_> = (900..910).map(message).collect();
        let small = Arc::new("<i>small</i>".to_string());
        let page = page("<p>", &messages[..5], "")
            .fragment(&small)
            .text(&"x".repeat(MAX_GLUE + 1))
            .fragment(&messages[5])
            .fragment(&messages[6])
            .text("</p>");
        let parts = page.parts();
        assert_eq!(parts.body_len(), page.plain.len());
        assert!(parts.fits(&http_body_util::Full::new(Bytes::from(page.plain.clone()))));
        assert!(!parts.fits(&http_body_util::Empty::<Bytes>::new()), "a HEAD response's body");
        assert_eq!(plain(parts).await, page.plain.as_bytes());
    }

    /// The gzip members and ETags these pages got when they were split by searching the body for its
    /// fragments, before the fragments' offsets were recorded.
    #[test]
    fn the_same_etags_and_gzip_as_when_split_by_searching() {
        let sha = |gz: Vec<u8>| hex::encode(sha2::Sha256::digest(gz));
        let messages: Vec<_> = (0..30).map(message).collect();
        let whole = page("<html><head>layout</head><body>", &messages, "</body></html>");
        assert_eq!(whole.etag(), "79e220089f196fbabb4a04d3a545749e");
        assert_eq!(sha(whole.gzip(7)), "20bebddcfa29cdde1b76e1572479c130eb4cfd9bfb0e032e5332a2e3b453cfe3");

        let small = Arc::new("<i>small</i>".to_string());
        let mixed = Page::default()
            .text("<p>")
            .fragment(&messages[0])
            .text(&"x".repeat(300))
            .fragment(&messages[1])
            .text("  ")
            .fragment(&small)
            .text("  ")
            .fragment(&messages[2])
            .text("</p>");
        assert_eq!(mixed.etag(), "ca77a29a2de60cdd36f203de61f1f9cb");
        assert_eq!(sha(mixed.gzip(7)), "1de43a2a5729028f5084ac91c02de84b5518b61093f2a6896da5ed785e3ce724");
    }

    #[test]
    fn a_changed_layout_or_neighbour_decodes_correctly() {
        let messages: Vec<_> = (100..110).map(message).collect();
        page("<p>", &messages, "</p>").gzip(0);
        for page in [page("<p>changed", &messages, "</p>"), page("<p>", &messages, "</p>changed")] {
            assert_eq!(gunzip(&page.gzip(0)), page.plain.as_bytes());
        }
        // Drop one message: the one after it now follows a different predecessor.
        let mut fewer = messages.clone();
        fewer.remove(4);
        let page = page("<p>", &fewer, "</p>");
        assert_eq!(gunzip(&page.gzip(0)), page.plain.as_bytes());
        // Different glue between the same fragments.
        let page = messages.iter().fold(Page::default().text("<p>"), |page, message| page.text("\n    ").fragment(message));
        assert_eq!(gunzip(&page.gzip(0)), page.plain.as_bytes());
    }

    #[test]
    fn small_repeated_and_far_apart_fragments() {
        let messages: Vec<_> = (200..206).map(message).collect();
        let small = Arc::new("<i>small</i>".to_string());
        let with_small = page("<p>", &messages[..1], "  ").fragment(&small);
        let with_small = messages[1..3].iter().fold(with_small, |page, message| page.text("  ").fragment(message)).text("</p>");
        assert_eq!(gunzip(&with_small.gzip(0)), with_small.plain.as_bytes());
        // A small fragment is text to the parts.
        let as_text = page("<p>", &messages[..1], &format!("  {small}"));
        let as_text = messages[1..3].iter().fold(as_text, |page, message| page.text("  ").fragment(message)).text("</p>");
        assert_eq!(with_small.etag(), as_text.etag());
        let only_small = Page::default().text("<p>").fragment(&small).text("</p>");
        let whole = PageParts::splice(&Bytes::from(only_small.text), only_small.fragments).expect_err("no part");
        assert_eq!(whole, only_small.plain.as_bytes());

        let (a, b) = (message(300), message(301));
        let page = Page::default().fragment(&a).fragment(&b).fragment(&a).text(&"x".repeat(MAX_GLUE + 1)).fragment(&b).fragment(&a);
        for _ in 0..2 {
            assert_eq!(gunzip(&page.gzip(0)), page.plain.as_bytes());
        }
    }

    #[test]
    fn a_fragment_keeps_a_piece_for_each_predecessor() {
        let messages: Vec<_> = (600..606).map(message).collect();
        let room = page("<p>", &messages, "</p>");
        // The same last message after a different one, as in search results.
        let search = page("<q>", &[messages[1].clone(), messages[5].clone()], "</q>");
        room.gzip(0);
        search.gzip(0);
        let before = stored_pieces(&messages[5]);
        assert_eq!(before.len(), 2, "one after message 604, one after 601");
        room.gzip(0);
        search.gzip(0);
        let after = stored_pieces(&messages[5]);
        assert!(before.iter().zip(&after).all(|(a, b)| Arc::ptr_eq(a, b)), "both pages reuse theirs");
    }

    #[test]
    fn a_piece_follows_its_predecessors_bytes_not_its_arc() {
        let messages: Vec<_> = (800..803).map(message).collect();
        let body = page("<p>", &messages, "</p>");
        body.gzip(0);
        let stored = stored_pieces(&messages[1]);
        // The first message rendered again into a new `Arc`, with the same bytes.
        let rerendered = page("<p>", &[Arc::new(messages[0].to_string()), messages[1].clone(), messages[2].clone()], "</p>");
        assert_eq!(gunzip(&rerendered.gzip(0)), rerendered.plain.as_bytes());
        let after = stored_pieces(&messages[1]);
        assert_eq!(after.len(), 1, "no second piece for the same predecessor");
        assert!(Arc::ptr_eq(&stored[0], &after[0]), "the piece after it is reused");
    }

    #[test]
    fn fragments_a_page_keeps_showing_stay_while_the_rest_age_out() {
        fn remember(known: &mut KnownFragments, fragment: &Arc<String>) {
            known.sha(fragment);
            let piece = Piece { deflated: Bytes::from(vec![0; 1000]), crc: Crc::of(b"") };
            let piece = FragmentPiece { before: Before::Nothing, glue: Box::default(), piece };
            known.update(&fragment_key(fragment), |entry| entry.store(piece));
        }
        fn pieces(known: &mut KnownFragments, fragment: &Arc<String>) -> Option<Vec<Arc<FragmentPiece>>> {
            known.get(&fragment_key(fragment), |entry| entry.pieces.clone())
        }

        // Room for a few dozen fragments, where the old bound only cleared them all at once.
        let budget = 64 * 1024;
        let mut known: KnownFragments = Generations::with_budget(budget, |_, entry| entry.cost());
        let page: Vec<_> = (900..903).map(message).collect();
        for fragment in &page {
            remember(&mut known, fragment);
        }
        let stored: Vec<_> = page.iter().map(|fragment| pieces(&mut known, fragment).unwrap()).collect();
        let others: Vec<_> = (1_000..1_300).map(message).collect();
        for (n, fragment) in others.iter().enumerate() {
            remember(&mut known, fragment);
            if n % 10 == 0 {
                // The page, shown again.
                for fragment in &page {
                    known.sha(fragment);
                }
            }
            let held: usize = known.young.values().chain(known.old.values()).map(KnownFragment::cost).sum();
            // Each generation may overshoot half the budget by the entry that filled it.
            assert!(held <= budget + 2 * 2048, "{held} bytes held");
        }
        for (fragment, stored) in page.iter().zip(&stored) {
            let now = pieces(&mut known, fragment).expect("still known");
            assert!(Arc::ptr_eq(&stored[0], &now[0]), "with the same piece");
        }
        assert!(pieces(&mut known, &others[0]).is_none(), "one not seen again ages out");
    }

    #[test]
    fn etags_follow_the_content() {
        let messages: Vec<_> = (400..410).map(message).collect();
        let body = page("<p>", &messages, "</p>");
        assert_eq!(body.etag(), page("<p>", &messages, "</p>").etag());
        assert_eq!(body.etag().len(), 32);
        assert_ne!(body.etag(), page("<p>", &messages, "</p>!").etag());
        assert_ne!(body.etag(), page("<q>", &messages, "</p>").etag());
        let glued = messages.iter().fold(Page::default().text("<p>"), |page, message| page.text(" ").fragment(message)).text("</p>");
        assert_ne!(body.etag(), glued.etag());
    }

    #[test]
    fn stays_close_to_whole_body_compression() {
        let messages: Vec<_> = (500..540).map(message).collect();
        let page = page(&"<head>layout</head>".repeat(200), &messages, &"<footer/>".repeat(300));
        let mut whole = flate2::write::GzEncoder::new(Vec::new(), Compression::default());
        whole.write_all(page.plain.as_bytes()).unwrap();
        let whole = whole.finish().unwrap().len();
        let spliced = page.gzip(0).len();
        // Each piece costs its flush marker and block header, not a recompressed message.
        assert!(spliced < whole + 40 * (messages.len() + 2), "{spliced} bytes spliced vs {whole} whole");
    }

    #[test]
    fn generations_stay_within_their_budget_and_keep_what_is_read() {
        let budget = 64 * 1024;
        let size = 1024;
        let mut map: Generations<u8, usize> = Generations::with_budget(budget, |_, size| *size);
        for n in 0..=255u8 {
            map.insert(n, size);
            assert!(map.get(&0, |_| ()).is_some(), "what's read stays");
            let held = (map.young.len() + map.old.len()) * size;
            // Each generation may overshoot half the budget by the entry that filled it.
            assert!(held <= budget + 2 * size, "{held} bytes held");
        }
        assert!(map.get(&255, |_| ()).is_some(), "the latest is kept");
        assert!(map.get(&1, |_| ()).is_none(), "what isn't read ages out");

        let mut map: Generations<u8, usize> = Generations::with_budget(budget, |_, size| *size);
        map.insert(1, size);
        map.insert(1, size);
        assert_eq!(map.young_cost, size, "an entry stored twice counts once");
    }

    #[test]
    fn a_large_one_off_text_is_served_but_not_remembered() {
        // Incompressible, so its piece is as large as the text: over both stores' bounds.
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        let text: Vec<u8> = (0..MAX_STORED_TEXT.max(MAX_STORED_TEXT_PIECE) + 1)
            .map(|_| {
                state = state.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
                (state >> 56) as u8
            })
            .collect();
        let fragment = message(990);
        let parts = PageParts::splice(&Bytes::from(text.clone()), vec![(text.len(), fragment.clone())]).unwrap();
        let mut plain = text.clone();
        plain.extend_from_slice(fragment.as_bytes());
        assert_eq!(gunzip(&parts.gzip(0)), plain);
        let sha: Sha = Sha256::digest(&text).into();
        assert!(TEXT_SHAS.lock_for(&text[..]).get(&text[..], |_| ()).is_none(), "hashed, not remembered");
        let key = (sha, Before::Nothing);
        assert!(TEXT_PIECES.lock_for(&key).get(&key, |_| ()).is_none(), "compressed, not stored");
    }

    #[test]
    fn a_text_part_is_its_texts_sha256_the_first_time_and_after() {
        let text = Bytes::from(b"<html><head>layout</head><body>".repeat(50));
        for text in [text.slice(0..10), text.slice(5..), text] {
            let expected: Sha = Sha256::digest(&text).into();
            for _ in 0..2 {
                let Part::Text { sha, .. } = text_part(text.clone()) else { unreachable!() };
                assert_eq!(sha, expected);
            }
        }
    }

    /// Hashes every key alike, as texts whose fast hashes collide.
    #[derive(Default)]
    struct Colliding;

    impl std::hash::Hasher for Colliding {
        fn finish(&self) -> u64 {
            0
        }

        fn write(&mut self, _: &[u8]) {}
    }

    #[test]
    fn a_text_is_known_by_its_bytes_not_its_fast_hash() {
        // A small budget, so texts also age out and come back through the old generation.
        let shas: Shards<Generations<Box<[u8]>, Sha, std::hash::BuildHasherDefault<Colliding>>> = Shards::new(|| {
            Generations::<Box<[u8]>, Sha, std::hash::BuildHasherDefault<Colliding>>::with_budget(8 * 1024, |text, _| {
                text.len() + ENTRY_OVERHEAD
            })
        });
        // The same length, all in one bucket.
        let texts: Vec<String> = (0..30).map(|n| format!("<h1>Room {n:02}</h1>").repeat(40)).collect();
        for _ in 0..3 {
            for text in &texts {
                let sha: Sha = Sha256::digest(text).into();
                assert_eq!(text_sha(&shas, text.as_bytes()), sha);
                assert_eq!(shas.lock_for(text.as_bytes()).get(text.as_bytes(), |sha| *sha), Some(sha), "remembered as its own");
            }
        }
    }

    #[test]
    fn stored_pieces_keep_only_what_they_use() {
        let text = "<p>repeated</p>".repeat(100_000);
        let piece = compress(b"", text.as_bytes());
        let vec: Vec<u8> = piece.into();
        assert!(vec.capacity() < 64 * 1024, "{} bytes of capacity for {}", vec.capacity(), vec.len());
    }

    #[test]
    fn crcs_concatenate() {
        let bytes: Vec<u8> = (0..100_000u32).map(|n| (n.wrapping_mul(2_654_435_761) >> 24) as u8).collect();
        for cuts in [&[][..], &[0, 0, 1], &[1, 2, 3, 4, 5], &[1000, 1001, 70_000, 99_999, 100_000]] {
            let bounds: Vec<usize> = std::iter::once(0).chain(cuts.iter().copied()).chain([bytes.len()]).collect();
            let crcs = bounds.windows(2).map(|range| Crc::of(&bytes[range[0]..range[1]]));
            assert_eq!(Crc::concatenated(crcs), crc32fast::hash(&bytes), "cut at {cuts:?}");
        }
    }
}
