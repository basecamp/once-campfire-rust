//! Regression tests for places where the port deliberately diverges from the Rails pipeline to
//! close a hole or bound the work a message body can cause (see "Known differences" in README.md).

use std::time::{Duration, Instant};

use campfire_richtext::dom::{Dom, MAX_ATTRIBUTES};
use campfire_richtext::{AttachableResolver, GidLookup, RenderContext, SignedLookup, editable_value, message_presentation, to_plain_text};

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

fn presentation(body: &str) -> String {
    message_presentation(body, &ctx()).unwrap()
}

/// Every element and attribute name in `html` as a browser would parse it.
fn parsed_markup(html: &str) -> Vec<String> {
    let mut dom = Dom::new();
    let root = dom.parse_fragment(html).unwrap();
    let mut names = Vec::new();
    for node in dom.descendants(root) {
        if let Some(name) = dom.local_name(node) {
            if name == "div" && dom.attr(node, "class") == Some("lexxy-content") {
                continue; // the layout's wrapper
            }
            names.push(name.to_string());
            names.extend(dom.attrs(node).into_iter().map(|(attr, _)| format!("{name}[{attr}]")));
        }
    }
    names
}

fn assert_quick(started: Instant, what: &str) {
    // Generous enough for a debug build; release takes a few milliseconds.
    let bound = if cfg!(debug_assertions) { Duration::from_secs(5) } else { Duration::from_secs(1) };
    assert!(started.elapsed() < bound, "{what} took {:?}", started.elapsed());
}

// --- Autolinking inside attribute values ---------------------------------------------------------

#[test]
fn a_url_after_a_greater_than_sign_in_an_attribute_cannot_break_out_of_it() {
    // Rails serializes the title unescaped (`title="x> http://..."`), so auto_link's "inside a
    // tag" check misses, the inserted <a href="..."> closes the attribute, and the <img> after it
    // becomes live markup.
    let body = r#"<p title="x> http://evil.test/ <img src=x onerror=alert(1)>">hi</p>"#;
    let html = presentation(body);
    let markup = parsed_markup(&html);
    assert!(!markup.iter().any(|m| m == "img" || m.contains("onerror")), "{html}");
    assert_eq!(markup, ["p", "p[title]"], "{html}");
}

#[test]
fn an_email_address_after_a_greater_than_sign_in_an_attribute_cannot_break_out_of_it() {
    let body = r#"<p><abbr title="x> me@evil.test <img src=x onerror=alert(1)>">hi</abbr></p>"#;
    let html = presentation(body);
    let markup = parsed_markup(&html);
    assert_eq!(markup, ["p", "abbr", "abbr[title]"], "{html}");
}

#[test]
fn name_attributes_cant_clobber_the_pages_globals() {
    let html = presentation(r#"<p><a name="body" href="/x">x</a><span name="cookie">y</span></p>"#);
    assert_eq!(parsed_markup(&html), ["p", "a", "a[href]", "span"], "{html}");
}

#[test]
fn urls_in_text_are_still_linked() {
    let html = presentation("<p>see http://example.com/a?b=1&amp;c=2 and me@example.com</p>");
    assert!(html.contains(
        "<p>see <a target=\"_blank\" href=\"http://example.com/a?b=1&amp;c=2\">http://example.com/a?b=1&amp;c=2</a> and <a target=\"_blank\" href=\"mailto:me@example.com\">me@example.com</a></p>"
    ), "{html}");
}

// --- Bounded work ----------------------------------------------------------------------------------

#[test]
fn many_bare_domains_autolink_in_linear_time() {
    let body = "<p>www.a.com</p>".repeat(64 * 1024 / 16);
    let started = Instant::now();
    let html = presentation(&body);
    assert_quick(started, "autolinking 64 KB of bare domains");
    assert_eq!(html.matches("<a target=\"_blank\" href=\"http://www.a.com\">").count(), 64 * 1024 / 16);
}

/// Content attachments nested `levels` deep, each saying which level it is.
fn nested_content_attachments(levels: usize, padding: &str) -> String {
    let mut body = String::new();
    for level in (1..=levels).rev() {
        let content = format!("<p>level {level}{padding}</p>{body}").replace('&', "&amp;").replace('"', "&quot;");
        body = format!("<action-text-attachment content-type=\"text/html\" content=\"{content}\"></action-text-attachment>");
    }
    body
}

#[test]
fn content_attachments_render_eight_levels_deep() {
    let html = presentation(&nested_content_attachments(12, ""));
    for level in 1..=12 {
        assert_eq!(html.contains(&format!("level {level}<")), level <= 8, "level {level} in {html}");
    }
}

#[test]
fn deeply_nested_content_attachments_render_quickly() {
    let body = nested_content_attachments(200, &"x".repeat(1000));
    assert!(body.len() > 200_000);
    let started = Instant::now();
    presentation(&body);
    assert_quick(started, "rendering 200 nested content attachments");
}

/// Both the page and the search index (which is written inside the database transaction) have to
/// refuse `body`.
fn assert_refused_quickly(body: &str, what: &str) {
    let started = Instant::now();
    assert!(message_presentation(body, &ctx()).is_err(), "{what} rendered");
    assert!(to_plain_text(body, &ctx()).is_err(), "{what} was indexed");
    assert_quick(started, what);
}

#[test]
fn deeply_nested_elements_are_refused_quickly() {
    // Gumbo's depth limit is enforced as the tree is built. Checked on the finished tree, 400 KB
    // of nested <div>s took 16 seconds, since html5ever's scope checks walk every open element.
    assert_refused_quickly(&"<div>".repeat(80_000), "400 KB of nested <div>s");
    assert_refused_quickly(&"<a><b>".repeat(80_000), "480 KB of <a><b>");
    assert_refused_quickly(&"<a><div><div>".repeat(30_000), "390 KB of <a><div><div>");
}

#[test]
fn the_rest_of_a_body_is_not_read_once_it_is_too_deep() {
    // Gumbo stops there. Tokenizing the rest of a 16 MB body (kit's request body limit) only to
    // throw it away took 200 ms a parse, and a message is parsed several times. Stopping once the
    // next token has been read isn't enough, as it can be all the rest: a comment took 130 ms.
    let too_deep = "<div>".repeat(401);
    let rest = "x".repeat(16 * 1024 * 1024);
    for (what, body) in [
        ("tags", format!("{too_deep}{}", "<a><b>".repeat(rest.len() / 6))),
        ("a comment", format!("{too_deep}<!--{rest}")),
        ("a tag name", format!("{too_deep}<{rest}")),
    ] {
        let started = Instant::now();
        assert!(message_presentation(&body, &ctx()).is_err() && to_plain_text(&body, &ctx()).is_err());
        // Copying the body to parse it is all that's left
        let bound = if cfg!(debug_assertions) { Duration::from_secs(1) } else { Duration::from_millis(100) };
        assert!(started.elapsed() < bound, "refusing 16 MB of {what} took {:?}", started.elapsed());
    }
}

#[test]
fn a_tag_with_too_many_attributes_is_refused_quickly() {
    // Each attribute is checked against the tag's others for a duplicate, up to Gumbo's limit
    let attributes: Vec<String> = (1..=64_000).map(|i| format!("a{i}=1")).collect();
    assert_refused_quickly(&format!("<b {}>x</b>", attributes.join(" ")), "a tag with 64,000 attributes");
}

#[test]
fn html_tags_in_the_body_parse_in_linear_time() {
    // Each one's attributes go to the fragment's root <html> element, unless it has them already.
    // Checking every one against all the root had collected made 800 KB of them take 1.5 seconds.
    let body: String = (0..200)
        .map(|tag| {
            let names: Vec<String> = (1..=MAX_ATTRIBUTES).map(|i| format!("a{}", tag * MAX_ATTRIBUTES + i)).collect();
            format!("<html {}>", names.join(" "))
        })
        .collect();
    assert!(body.len() > 500_000);
    let started = Instant::now();
    assert_eq!(to_plain_text(&body, &ctx()).unwrap(), "");
    assert_eq!(presentation(&body), presentation(""));
    assert_quick(started, "550 KB of <html> tags, each with 400 new attributes");
}

#[test]
fn elements_misplaced_in_a_table_parse_in_linear_time() {
    // Foster parenting inserts each of them before the table. Finding the table from the front of
    // its parent's children made that quadratic: 480 KB of them took 1.5 seconds.
    let body = format!("<table>{}", "<br>".repeat(200_000));
    let started = Instant::now();
    assert!(to_plain_text(&body, &ctx()).is_ok());
    assert_quick(started, "800 KB of <br>s in a table");
}

/// Every SGID names a user who has since been deleted.
struct DeletedUsers;

impl AttachableResolver for DeletedUsers {
    fn locate_signed(&self, _sgid: &str) -> SignedLookup {
        SignedLookup::MissingRecord { model_name: "User".into() }
    }

    fn find_gid(&self, _gid: &str) -> GidLookup {
        GidLookup::NotFound
    }
}

const DELETED_MENTION: &str = r#"<p>Hi <action-text-attachment sgid="eyJfcmFpbHMiOnsiZGF0YSI6ImdpZDovL2NhbXBmaXJlL1VzZXIvNj9leHBpcmVzX2luIiwicHVyIjoiYXR0YWNoYWJsZSJ9fQ==--fc4f83a239475557295b8e2f5ff55482bebc9bfe" content-type="application/vnd.campfire.mention"></action-text-attachment>, welcome</p>"#;

#[test]
fn a_mention_of_a_deleted_user_leaves_the_rest_of_the_message() {
    let ctx = RenderContext { resolver: &DeletedUsers, request_host: None };
    let html = message_presentation(DELETED_MENTION, &ctx).unwrap();
    assert!(html.contains("Hi") && html.contains('☒') && html.contains("welcome"), "{html}");
}

#[test]
fn a_mention_of_a_deleted_user_leaves_the_editor() {
    let ctx = RenderContext { resolver: &DeletedUsers, request_host: None };
    let value = editable_value(DELETED_MENTION, &ctx).unwrap().unwrap();
    assert!(!value.contains("action-text-attachment") && value.contains("welcome"), "{value}");
}
