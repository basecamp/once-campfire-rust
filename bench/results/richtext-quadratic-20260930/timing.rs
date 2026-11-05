//! Times rich text on bodies whose work grew with the square of their size. Copied into
//! crates/richtext/examples/ to run:
//! `SHAPE=<ol|wrapper|fonts|gallery|attachments> cargo run --release -p campfire_richtext --example quadratic_timing -- <items>...`
//!
//! ol:          an `<ol>` of N items. Plain text numbered each item by scanning its list.
//! wrapper:     a `<font>` around N `<b>`s. Rendering moved the children up one at a time.
//! fonts:       N sibling `<font>`s. SanitizeTags detached them one at a time.
//! gallery:     a `<div>` of N gallery attachments, each followed by a `<b>`. Loading copied and
//!              searched the parent's children once per attachment.
//! attachments: a `<div>` of N sibling content attachments. Not fixed: each is replaced with
//!              `Dom::replace_with_html`, which searches and shifts the parent's children.

use std::time::{Duration, Instant};

use campfire_richtext::{
    AttachableResolver, Content, GidLookup, RenderContext, SignedLookup, mentioned_users, message_presentation, to_plain_text,
};

struct NoRecords;

impl AttachableResolver for NoRecords {
    fn locate_signed(&self, _: &str) -> SignedLookup {
        SignedLookup::Invalid
    }
    fn find_gid(&self, _: &str) -> GidLookup {
        GidLookup::NotFound
    }
}

/// The fastest of three runs, and what the last one returned.
fn best<T>(f: impl Fn() -> T) -> (Duration, T) {
    let mut fastest = Duration::MAX;
    let mut out = None;
    for _ in 0..3 {
        let started = Instant::now();
        let value = f();
        fastest = fastest.min(started.elapsed());
        out = Some(value);
    }
    (fastest, out.unwrap())
}

fn main() {
    let ctx = RenderContext { resolver: &NoRecords, request_host: Some("once.campfire.test".into()) };
    let shape = std::env::var("SHAPE").unwrap_or_else(|_| "ol".into());
    for n in std::env::args().skip(1).map(|a| a.parse::<usize>().unwrap()) {
        let body = match shape.as_str() {
            "ol" => format!("<ol>{}</ol>", "<li>x</li>".repeat(n)),
            "wrapper" => format!("<font>{}</font>", "<b>x</b>".repeat(n)),
            "fonts" => "<font>x</font>".repeat(n),
            "gallery" => {
                format!("<div>{}</div>", "<action-text-attachment presentation=\"gallery\"></action-text-attachment><b>x</b>".repeat(n))
            }
            "attachments" => format!(
                "<div>{}</div>",
                "<action-text-attachment content-type=\"text/html\" content=\"x\"></action-text-attachment>".repeat(n)
            ),
            _ => panic!("unknown SHAPE"),
        };
        let (presented, html) = best(|| message_presentation(&body, &ctx).map(|h| h.len()));
        let (rendered, body_html) = best(|| Content::load(&body, &ctx).and_then(|c| c.to_rendered_html_with_layout(&ctx)).map(|h| h.len()));
        let (plain, text) = best(|| to_plain_text(&body, &ctx).map(|t| t.len()));
        let (mentions, users) = best(|| mentioned_users(&body, &ctx).map(|u| u.len()));
        println!(
            "{shape} {n} items {} bytes: presentation {presented:?} ({html:?}), body html {rendered:?} ({body_html:?}), plain text {plain:?} ({text:?}), mentions {mentions:?} ({users:?})",
            body.len()
        );
    }
}
