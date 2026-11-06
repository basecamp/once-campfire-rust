//! Message bodies whose work used to grow with the square of their size. They have their own test
//! binary so that these large bodies do not compete for cores with the timing tests in
//! `hardening.rs`.

use std::time::{Duration, Instant};

use campfire_richtext::{
    AttachableResolver, Content, GidLookup, RenderContext, SignedLookup, mentioned_users, message_presentation, to_plain_text,
};

struct NoRecords;

impl AttachableResolver for NoRecords {
    fn locate_signed(&self, _sgid: &str) -> SignedLookup {
        SignedLookup::Invalid
    }

    fn find_gid(&self, _gid: &str) -> GidLookup {
        GidLookup::NotFound
    }
}

fn ctx() -> RenderContext<'static> {
    RenderContext { resolver: &NoRecords, request_host: Some("once.campfire.test".into()) }
}

fn assert_quick(started: Instant, what: &str) {
    // Generous enough for a debug build.
    let bound = if cfg!(debug_assertions) { Duration::from_secs(5) } else { Duration::from_secs(1) };
    assert!(started.elapsed() < bound, "{what} took {:?}", started.elapsed());
}

#[test]
fn a_long_ordered_list_converts_to_plain_text_in_linear_time() {
    // Each item's number was found by scanning its whole list again, on the database writer
    // thread when a message is indexed for search.
    let body = format!("<ol>{}</ol>", "<li>x</li>".repeat(32 * 1024));
    let started = Instant::now();
    let text = to_plain_text(&body, &ctx()).unwrap();
    assert_quick(started, "plain text for a 32K item list");
    assert!(text.ends_with("32768. x"), "{}", &text[text.len() - 20..]);
}

#[test]
fn a_wide_disallowed_wrapper_renders_in_linear_time() {
    // The wrapper's children were moved up one at a time, each move searching and shifting the
    // parent's children.
    let body = format!("<font>{}</font>", "<b>x</b>".repeat(64 * 1024));
    let started = Instant::now();
    let html = Content::load(&body, &ctx()).unwrap().to_rendered_html_with_layout(&ctx()).unwrap();
    assert_quick(started, "rendering a <font> around 64K children");
    assert_eq!(html.matches("<b>x</b>").count(), 64 * 1024);
}

#[test]
fn many_sibling_disallowed_tags_are_dropped_in_linear_time() {
    // Each disallowed tag was detached on its own, each detach filtering the parent's children.
    let body = "<font>x</font>".repeat(64 * 1024);
    let started = Instant::now();
    let html = message_presentation(&body, &ctx()).unwrap();
    assert_quick(started, "dropping 64K sibling <font> tags");
    assert!(!html.contains("font"), "{html}");
}

#[test]
fn gallery_detection_is_linear_in_the_attachments_of_one_element() {
    // Each gallery attachment copied and searched its parent's children to find the one before it.
    let body =
        format!("<div>{}</div>", "<action-text-attachment presentation=\"gallery\"></action-text-attachment><b>x</b>".repeat(16 * 1024));
    let started = Instant::now();
    let users = mentioned_users(&body, &ctx()).unwrap();
    assert_quick(started, "loading 16K gallery attachments");
    assert!(users.is_empty());
}
