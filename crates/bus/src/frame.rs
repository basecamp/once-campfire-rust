use std::sync::{Arc, OnceLock};

/// A frame's text, shared by every subscriber that receives it, with one encoding of the text
/// that its consumer computes at most once (for example a compressed form).
#[derive(Clone)]
pub struct Frame(Arc<Payload>);

struct Payload {
    text: Box<str>,
    encoded: OnceLock<Box<[u8]>>,
}

impl Frame {
    pub fn as_str(&self) -> &str {
        &self.0.text
    }

    pub fn len(&self) -> usize {
        self.0.text.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.text.is_empty()
    }

    /// The text encoded by `encode`, computed by the first caller and shared with the others.
    pub fn encoded(&self, encode: impl FnOnce(&str) -> Box<[u8]>) -> &[u8] {
        self.0.encoded.get_or_init(|| encode(&self.0.text))
    }
}

impl From<String> for Frame {
    fn from(text: String) -> Self {
        Frame(Arc::new(Payload { text: text.into_boxed_str(), encoded: OnceLock::new() }))
    }
}

impl From<&str> for Frame {
    fn from(text: &str) -> Self {
        text.to_string().into()
    }
}

impl std::fmt::Debug for Frame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Frame").field(&self.as_str()).finish()
    }
}

impl PartialEq for Frame {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl PartialEq<&str> for Frame {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}
