//! `Rails::HTML5::SafeListSanitizer` with a `Rails::HTML::PermitScrubber`, over Loofah's HTML5
//! scrubbing helpers (rails-html-sanitizer 1.7.1, loofah 2.25.2).
//!
//! Every sanitization layer in the pipeline is this one scrubber with a different pair of tag and
//! attribute allowlists, so it is written once here rather than expressed through ammonia, whose
//! URL, comment, foreign-content and attribute-escaping rules differ from Loofah's.

use std::sync::LazyLock;

use regex::Regex;

use crate::dom::{Attr, Dom, NodeId, ParseError};

/// `Rails::HTML::Concern::Scrubber::SafeList::DEFAULT_ALLOWED_TAGS`
pub const DEFAULT_ALLOWED_TAGS: &[&str] = &[
    "a", "abbr", "acronym", "address", "b", "big", "blockquote", "br", "cite", "code", "dd", "del", "dfn", "div", "dl", "dt", "em", "h1",
    "h2", "h3", "h4", "h5", "h6", "hr", "i", "img", "ins", "kbd", "li", "mark", "ol", "p", "pre", "samp", "small", "span", "strong", "sub",
    "sup", "time", "tt", "ul", "var",
];

/// `Rails::HTML::Concern::Scrubber::SafeList::DEFAULT_ALLOWED_ATTRIBUTES` without `name`, which let
/// a message clobber the page's DOM globals (`<img name="body">` shadows `document.body`). Nothing
/// Campfire's composer writes has one.
pub const DEFAULT_ALLOWED_ATTRIBUTES: &[&str] =
    &["abbr", "alt", "cite", "class", "datetime", "height", "href", "lang", "src", "title", "width", "xml:lang"];

/// `ContentFilters::EDITOR_FORMATTING_TAGS` (reference/app/helpers/content_filters.rb)
pub const EDITOR_FORMATTING_TAGS: &[&str] = &["s", "u", "mark", "table", "thead", "tbody", "tfoot", "tr", "th", "td"];

/// `ContentFilters::EDITOR_FORMATTING_ATTRIBUTES`
pub const EDITOR_FORMATTING_ATTRIBUTES: &[&str] = &["data-language"];

/// `ActionText::Attachment::ATTRIBUTES`
pub const ATTACHMENT_ATTRIBUTES: &[&str] = &[
    "sgid", "content-type", "url", "href", "filename", "filesize", "width", "height", "previewable", "presentation", "caption", "content",
];

/// A tag and attribute allowlist, as passed to `sanitize(html, tags:, attributes:)`.
#[derive(Debug, Clone)]
pub struct SafeList {
    pub tags: Vec<&'static str>,
    pub attributes: Vec<&'static str>,
}

impl SafeList {
    fn allows_tag(&self, name: &str) -> bool {
        self.tags.contains(&name)
    }

    fn allows_attribute(&self, name: &str) -> bool {
        self.attributes.contains(&name)
    }

    /// Action View's `sanitize(html)` with no options: the sanitizer's class-level defaults.
    pub fn defaults() -> &'static Self {
        static LIST: LazyLock<SafeList> =
            LazyLock::new(|| SafeList { tags: DEFAULT_ALLOWED_TAGS.to_vec(), attributes: DEFAULT_ALLOWED_ATTRIBUTES.to_vec() });
        &LIST
    }

    /// `ActionText::ContentHelper.allowed_tags`/`allowed_attributes` as configured at boot: Action
    /// Text's defaults, then Lexxy's additions (lexxy/engine.rb, "lexxy.sanitization"), then
    /// Campfire's (reference/lib/rails_ext/action_text_allowed_tags.rb).
    pub fn action_text() -> &'static Self {
        static LIST: LazyLock<SafeList> = LazyLock::new(|| {
            let mut tags = DEFAULT_ALLOWED_TAGS.to_vec();
            tags.extend(["action-text-attachment", "figure", "figcaption"]);
            tags.extend(["video", "audio", "source", "embed", "table", "tbody", "tr", "th", "td"]);
            for tag in EDITOR_FORMATTING_TAGS {
                if !tags.contains(tag) {
                    tags.push(tag);
                }
            }
            let mut attributes = DEFAULT_ALLOWED_ATTRIBUTES.to_vec();
            attributes.extend(ATTACHMENT_ATTRIBUTES);
            attributes.extend(["controls", "poster", "data-language", "style", "value", "start"]);
            for attribute in EDITOR_FORMATTING_ATTRIBUTES {
                if !attributes.contains(attribute) {
                    attributes.push(attribute);
                }
            }
            SafeList { tags, attributes }
        });
        &LIST
    }

    /// `ContentFilters::SanitizeAttributes`: SanitizeTags' tags, Action Text's attributes plus `class`.
    pub fn content_filter() -> &'static Self {
        static LIST: LazyLock<SafeList> = LazyLock::new(|| {
            let mut attributes = SafeList::action_text().attributes.clone();
            if !attributes.contains(&"class") {
                attributes.push("class");
            }
            SafeList { tags: sanitize_tags_allowed_tags().to_vec(), attributes }
        });
        &LIST
    }

    /// `MessagesHelper::AUTO_LINK_ALLOWED_TAGS`/`AUTO_LINK_ALLOWED_ATTRIBUTES`.
    pub fn auto_link() -> &'static Self {
        static LIST: LazyLock<SafeList> = LazyLock::new(|| {
            let mut tags = DEFAULT_ALLOWED_TAGS.to_vec();
            for tag in EDITOR_FORMATTING_TAGS {
                if !tags.contains(tag) {
                    tags.push(tag);
                }
            }
            let mut attributes = DEFAULT_ALLOWED_ATTRIBUTES.to_vec();
            attributes.extend(EDITOR_FORMATTING_ATTRIBUTES);
            SafeList { tags, attributes }
        });
        &LIST
    }
}

/// `ContentFilters::SanitizeTags::ALLOWED_TAGS`
pub fn sanitize_tags_allowed_tags() -> &'static [&'static str] {
    static TAGS: LazyLock<Vec<&'static str>> = LazyLock::new(|| {
        let mut tags = vec![
            "a", "abbr", "acronym", "address", "b", "big", "blockquote", "br", "cite", "code", "dd", "del", "dfn", "div", "dl", "dt", "em",
            "h1", "h2", "h3", "h4", "h5", "h6", "hr", "i", "ins", "kbd", "li", "ol", "p", "pre", "samp", "small", "span", "strong", "sub",
            "sup", "time", "tt", "ul", "var",
        ];
        tags.extend(EDITOR_FORMATTING_TAGS);
        tags.extend(["action-text-attachment", "figure", "figcaption"]);
        tags
    });
    &TAGS
}

/// `SafeListSanitizer#sanitize(html, tags:, attributes:)`.
pub fn sanitize(html: &str, list: &SafeList) -> Result<String, ParseError> {
    if html.is_empty() {
        return Ok(String::new());
    }
    let (dom, fragment) = scrubbed(html, list)?;
    Ok(dom.to_html(fragment))
}

/// `sanitize`, serialized with `<` and `>` escaped in attribute values too, so that the result can
/// be scanned with regular expressions (auto_link) without mistaking an attribute for text.
/// Rails serializes them raw; the two are the same DOM.
pub fn sanitize_with_escaped_attribute_brackets(html: &str, list: &SafeList) -> Result<String, ParseError> {
    if html.is_empty() {
        return Ok(String::new());
    }
    let (dom, fragment) = scrubbed(html, list)?;
    Ok(dom.to_html_with_escaped_attribute_brackets(fragment))
}

fn scrubbed(html: &str, list: &SafeList) -> Result<(Dom, NodeId), ParseError> {
    let mut dom = Dom::new();
    let fragment = dom.parse_fragment(html)?;
    for child in dom.children(fragment).to_vec() {
        scrub_bottom_up(&mut dom, child, list);
    }
    Ok((dom, fragment))
}

/// `Loofah::Scrubber#traverse_conditionally_bottom_up`: children (as they were before any of them
/// was scrubbed) first, then the node itself.
fn scrub_bottom_up(dom: &mut Dom, node: NodeId, list: &SafeList) {
    for child in dom.children(node).to_vec() {
        scrub_bottom_up(dom, child, list);
    }
    scrub(dom, node, list);
}

/// `Rails::HTML::PermitScrubber#scrub`
fn scrub(dom: &mut Dom, node: NodeId, list: &SafeList) {
    if dom.is_text(node) {
        return;
    }
    let keep = dom.local_name(node).is_some_and(|name| list.allows_tag(name));
    if !keep {
        // Unwrap HTML elements (and comments, which have no children); drop foreign (SVG,
        // MathML) elements together with their contents, since they carry a namespace.
        let foreign = dom.is_element(node) && !dom.is_html_element(node);
        if foreign {
            dom.detach(node);
        } else {
            dom.unwrap(node);
        }
        return;
    }
    scrub_attributes(dom, node, list);
}

/// `PermitScrubber#scrub_attributes` with an attribute allowlist. Attributes are visited in order,
/// and each allowed one re-escapes every URL attribute on the node as it goes, so a later URL is
/// checked in its re-escaped form (" javascript:" has become "%20javascript:" and passes).
fn scrub_attributes(dom: &mut Dom, node: NodeId, list: &SafeList) {
    let Some(element) = dom.element_mut(node) else { return };
    let is_a = &*element.name.local == "a";
    let mut i = 0;
    while let Some(attr) = element.attrs.get(i) {
        let (scrubbed, blank_src) = {
            let name = attr.qualified_name();
            let scrubbed = !list.allows_attribute(&name) || (ATTR_VAL_IS_URI.contains(&&*name) && !allowed_uri(&attr.value));
            (scrubbed, name == "src" && attr.value.chars().all(char::is_whitespace))
        };
        if scrubbed {
            element.attrs.remove(i);
            continue;
        }
        // A blank src goes, but the escaping still runs for the attributes after it
        if blank_src {
            element.attrs.remove(i);
        } else {
            i += 1;
        }
        force_correct_attribute_escaping(&mut element.attrs, is_a);
    }
    scrub_style(dom, node);
}

/// Where Loofah's `scrub_css_attribute` runs `style` through its CSS scrubber, this keeps only what
/// Lexxy writes: highlight colors (`color` and `background-color`, which Lexxy's own paste filter
/// also limits `style` to) with plain color values. A message's presentation drops `style` in
/// auto_link anyway, but the HTML body bots and webhooks get (`Presenter::body_html`) keeps it.
fn scrub_style(dom: &mut Dom, node: NodeId) {
    let Some(style) = dom.attr(node, "style") else { return };
    let declarations: Vec<(String, &str)> = style
        .split(';')
        .filter(|declaration| !declaration.trim().is_empty())
        .map(|declaration| {
            let (property, value) = declaration.split_once(':').unwrap_or((declaration, ""));
            (property.trim().to_ascii_lowercase(), value.trim())
        })
        .collect();
    let allowed = |(property, value): &(String, &str)| ALLOWED_STYLE_PROPERTIES.contains(&property.as_str()) && is_plain_color(value);
    if declarations.iter().all(allowed) && !declarations.is_empty() {
        return;
    }
    let scrubbed: String = declarations.iter().filter(|d| allowed(d)).map(|(property, value)| format!("{property}: {value};")).collect();
    if scrubbed.is_empty() {
        dom.remove_attr(node, "style");
    } else {
        dom.set_attr(node, "style", &scrubbed);
    }
}

/// Lexxy's `ALLOWED_STYLE_PROPERTIES`.
const ALLOWED_STYLE_PROPERTIES: &[&str] = &["color", "background-color"];

/// A color keyword, a hex color, a custom property (`var(--highlight-1)`, as Lexxy's highlights
/// are), or an `rgb()`/`hsl()` color: nothing that can load a URL, escape, or run an expression.
fn is_plain_color(value: &str) -> bool {
    static PLAIN_COLOR: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"(?i)\A(?:[a-z]+|#[0-9a-f]{3,8}|var\(\s*--[a-z0-9_-]+\s*\)|(?:rgb|rgba|hsl|hsla)\([0-9a-z.,%\s/+-]*\))\z").unwrap()
    });
    PLAIN_COLOR.is_match(value)
}

/// `Loofah::HTML5::SafeList::ATTR_VAL_IS_URI`
const ATTR_VAL_IS_URI: &[&str] = &["action", "cite", "href", "longdesc", "poster", "preload", "src", "xlink:href", "xml:base"];

/// `Loofah::HTML5::Scrub.force_correct_attribute_escaping!` (libxml2 builds only, which CRuby is):
/// spaces and double quotes in `href`, `action`, `src` and an `a`'s `name` become `%20` and `%22`.
/// The value is written back through `Nokogiri::XML::Attr#value=`, where libxml2 drops the C0
/// controls XML 1.0 doesn't allow.
fn force_correct_attribute_escaping(attrs: &mut [Attr], is_a: bool) {
    for attr in attrs {
        let qualifies = {
            let name = attr.qualified_name();
            matches!(&*name, "href" | "action" | "src") || (name == "name" && is_a)
        };
        if qualifies && attr.value.chars().any(is_escaped) {
            attr.value = escape_attribute_value(&attr.value);
        }
    }
}

/// What `force_correct_attribute_escaping` changes: spaces, double quotes, and the C0 controls
/// other than tab, newline and carriage return.
fn is_escaped(c: char) -> bool {
    c == ' ' || c == '"' || (c < ' ' && !matches!(c, '\t' | '\n' | '\r'))
}

fn escape_attribute_value(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            ' ' => escaped.push_str("%20"),
            '"' => escaped.push_str("%22"),
            '\t' | '\n' | '\r' => escaped.push(c),
            c if c < ' ' => {}
            c => escaped.push(c),
        }
    }
    escaped
}

const ALLOWED_PROTOCOLS: &[&str] = &[
    "afs", "aim", "callto", "data", "ed2k", "fax", "ftp", "gopher", "http", "https", "irc", "line", "mailto", "modem", "news", "nntp",
    "rsync", "rtsp", "sftp", "sms", "ssh", "tag", "tel", "telnet", "urn", "webcal", "xmpp",
];

const ALLOWED_URI_DATA_MEDIATYPES: &[&str] = &["image/gif", "image/jpeg", "image/png", "text/css", "text/plain"];

/// `Loofah::HTML5::Scrub::CONTROL_CHARACTERS`: /[`\u0000- \u007f\u0080-ā]/
fn is_control_character(c: char) -> bool {
    c == '`' || c <= '\u{20}' || c == '\u{7f}' || ('\u{80}'..='\u{101}').contains(&c)
}

/// `Loofah::HTML5::Scrub.allowed_uri?`
pub fn allowed_uri(uri: &str) -> bool {
    let without_controls: String = uri.chars().filter(|&c| !is_control_character(c)).collect();
    let decoded = decode_numeric_character_references(&cgi_unescape_html(&without_controls));
    let mut s: String = decoded.chars().filter(|&c| !is_control_character(c)).collect();
    s = s.replace("&Tab;", "").replace("&NewLine;", "");
    s = s.replace("&colon;", ":");
    s = s.to_lowercase();
    let Some(protocol) = protocol_before_separator(&s) else { return true };
    if !ALLOWED_PROTOCOLS.contains(&protocol) {
        return false;
    }
    if protocol == "data" {
        return data_uri_mediatype(&s).is_some_and(|m| ALLOWED_URI_DATA_MEDIATYPES.contains(&m.as_str()));
    }
    true
}

/// Matches `\A[a-z][a-z0-9+\-.]*` followed by `PROTOCOL_SEPARATOR`
/// (`/:|(&#0*58)|(&#x0*3a)|(%|&#37;)3A/i`) and returns the scheme.
fn protocol_before_separator(s: &str) -> Option<&str> {
    let bytes = s.as_bytes();
    if !bytes.first()?.is_ascii_lowercase() {
        return None;
    }
    let mut end = 1;
    while end < bytes.len() && (bytes[end].is_ascii_lowercase() || bytes[end].is_ascii_digit() || matches!(bytes[end], b'+' | b'-' | b'.'))
    {
        end += 1;
    }
    // The class can't contain the start of a separator, so the scheme is the longest run.
    separator_len(&s[end..]).map(|_| &s[..end])
}

fn separator_len(s: &str) -> Option<usize> {
    let lower = s.to_ascii_lowercase();
    if lower.starts_with(':') {
        return Some(1);
    }
    if let Some(rest) = lower.strip_prefix("&#x") {
        let zeros = rest.len() - rest.trim_start_matches('0').len();
        if rest[zeros..].starts_with("3a") {
            return Some(3 + zeros + 2);
        }
    }
    if let Some(rest) = lower.strip_prefix("&#") {
        let zeros = rest.len() - rest.trim_start_matches('0').len();
        if rest[zeros..].starts_with("58") {
            return Some(2 + zeros + 2);
        }
    }
    if lower.starts_with("%3a") {
        return Some(3);
    }
    if lower.starts_with("&#37;3a") {
        return Some(7);
    }
    None
}

/// `Loofah::HTML5::Scrub.data_uri_mediatype`
fn data_uri_mediatype(s: &str) -> Option<String> {
    let rest = s.strip_prefix("data:").unwrap_or(s);
    let (metadata, _) = rest.split_once(',')?;
    let metadata = metadata.strip_suffix(";base64").unwrap_or(metadata);
    let mediatype =
        metadata.split(';').next().unwrap_or("").trim_matches(|c: char| matches!(c, ' ' | '\t' | '\n' | '\u{0b}' | '\u{0c}' | '\r' | '\0'));
    let tchar = |c: char| c.is_ascii_alphanumeric() || "!#$%&'*+-.^_`|~".contains(c);
    let valid = mediatype
        .split_once('/')
        .is_some_and(|(t, sub)| !t.is_empty() && !sub.is_empty() && t.chars().all(tchar) && sub.chars().all(tchar));
    Some(if valid { mediatype.to_string() } else { "text/plain".to_string() })
}

/// `CGI.unescapeHTML`: the five named entities plus terminated numeric references.
pub fn cgi_unescape_html(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(pos) = rest.find('&') {
        out.push_str(&rest[..pos]);
        rest = &rest[pos..];
        let (replacement, consumed) = unescape_one(rest);
        match replacement {
            Some(r) => {
                out.push_str(&r);
                rest = &rest[consumed..];
            }
            None => {
                out.push('&');
                rest = &rest[1..];
            }
        }
    }
    out.push_str(rest);
    out
}

fn unescape_one(s: &str) -> (Option<String>, usize) {
    for (name, value) in [("&apos;", "'"), ("&amp;", "&"), ("&quot;", "\""), ("&gt;", ">"), ("&lt;", "<")] {
        if s.starts_with(name) {
            return (Some(value.to_string()), name.len());
        }
    }
    let Some(end) = s.find(';') else { return (None, 0) };
    let body = &s[1..end];
    let code = if let Some(hex) = body.strip_prefix("#x").or_else(|| body.strip_prefix("#X")) {
        numeric_reference(hex, 16)
    } else if let Some(dec) = body.strip_prefix('#') {
        numeric_reference(dec, 10)
    } else {
        None
    };
    match code.and_then(char::from_u32) {
        Some(c) => (Some(c.to_string()), end + 1),
        None => (None, 0),
    }
}

/// The code point of a numeric reference's digits, when `CGI.unescapeHTML` decodes it: any number
/// of leading zeros, and below U+10FFFF (cgi's `escape.c` skips `cc >= charlimit`, 0x10ffff for
/// UTF-8). Surrogates are left alone too, as a Rust string can't hold them.
fn numeric_reference(digits: &str, radix: u32) -> Option<u32> {
    if digits.is_empty() || !digits.chars().all(|c| c.is_digit(radix)) {
        return None;
    }
    let significant = digits.trim_start_matches('0');
    // Past 7 decimal or 6 hex digits, it's past U+10FFFF anyway (and `u32` stays in range).
    if significant.len() > if radix == 16 { 6 } else { 7 } {
        return None;
    }
    u32::from_str_radix(if significant.is_empty() { "0" } else { significant }, radix).ok().filter(|&code| code < 0x10ffff)
}

/// `Loofah::HTML5::Scrub.decode_numeric_character_references`: `&#(x[0-9a-f]+|[0-9]+);?` (case
/// insensitive), skipping references with too many significant digits or invalid code points.
fn decode_numeric_character_references(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    let mut copied = 0;
    while i < bytes.len() {
        if bytes[i] == b'&' && bytes.get(i + 1) == Some(&b'#') {
            let start = i + 2;
            let hex = matches!(bytes.get(start), Some(b'x') | Some(b'X'));
            let digits_start = if hex { start + 1 } else { start };
            let mut end = digits_start;
            while end < bytes.len() && (if hex { bytes[end].is_ascii_hexdigit() } else { bytes[end].is_ascii_digit() }) {
                end += 1;
            }
            if end > digits_start {
                let digits = &s[digits_start..end];
                let full_end = if bytes.get(end) == Some(&b';') { end + 1 } else { end };
                let significant = digits.trim_start_matches('0');
                let limit = if hex { 6 } else { 7 };
                let decoded = (significant.len() <= limit)
                    .then(|| u32::from_str_radix(if significant.is_empty() { "0" } else { significant }, if hex { 16 } else { 10 }).ok())
                    .flatten()
                    .and_then(char::from_u32);
                out.push_str(&s[copied..i]);
                match decoded {
                    Some(c) => out.push(c),
                    None => out.push_str(&s[i..full_end]),
                }
                i = full_end;
                copied = i;
                continue;
            }
        }
        i += 1;
    }
    out.push_str(&s[copied..]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scrubs_like_rails() {
        let list = SafeList::content_filter();
        assert_eq!(sanitize("<div><a href=\"javascript:alert(1)\">x</a></div>", list).unwrap(), "<div><a>x</a></div>");
        assert_eq!(sanitize("<a href=\"/x\" onmouseover=\"alert(1)\">x</a>", list).unwrap(), "<a href=\"/x\">x</a>");
        assert_eq!(sanitize("<a href=\"data:text/html,pwned\">x</a>", list).unwrap(), "<a>x</a>");
        assert_eq!(sanitize("<a href=\"a b\">x</a><!-- c -->", list).unwrap(), "<a href=\"a%20b\">x</a>");
        assert_eq!(sanitize("<svg><a>x</a></svg>y<script>z</script>", list).unwrap(), "yz");
    }

    /// The outputs are rails-html-sanitizer's, from the reference image.
    #[test]
    fn re_escapes_url_attributes_as_each_attribute_is_scrubbed() {
        let list = &SafeList { tags: vec!["img", "a"], attributes: vec!["src", "href", "name", "title", "alt"] };
        // The blank src is removed, and the escaping it still triggers lets the href through
        assert_eq!(sanitize("<img src=\" \" href=\" javascript:alert(1)\">", list).unwrap(), "<img href=\"%20javascript:alert(1)\">");
        assert_eq!(sanitize("<img href=\" javascript:alert(1)\">", list).unwrap(), "<img>");
        assert_eq!(
            sanitize("<a href=\"a b\" name=\"c d\" title=\"x y\">t</a>", list).unwrap(),
            "<a href=\"a%20b\" name=\"c%20d\" title=\"x y\">t</a>"
        );
        assert_eq!(sanitize("<img src=\"a b\" alt=\"q\">", list).unwrap(), "<img src=\"a%20b\" alt=\"q\">");
    }

    #[test]
    fn unwraps_disallowed_wrappers_keeping_children_in_order() {
        let list = SafeList::defaults();
        // A disallowed HTML wrapper (`<font>`) is unwrapped: its children stay, in order, in its
        // place. A foreign wrapper is dropped with its contents (see `scrubs_like_rails`).
        assert_eq!(sanitize("<p>a<font><b>1</b><i>2</i></font>b</p>", list).unwrap(), "<p>a<b>1</b><i>2</i>b</p>");
        // A wide wrapper unwraps in one pass, not a move per child (which was quadratic).
        let wide = format!("<font>{}</font>", "<b>x</b>".repeat(2000));
        let out = sanitize(&wide, list).unwrap();
        assert_eq!(out.matches("<b>x</b>").count(), 2000);
        assert!(!out.contains("font"));
    }

    #[test]
    fn keeps_only_lexxys_highlight_colors_in_style() {
        let list = SafeList::action_text();
        let highlight = "<mark style=\"color: var(--highlight-1);background-color: var(--highlight-bg-2);\">x</mark>";
        assert_eq!(sanitize(highlight, list).unwrap(), highlight);
        assert_eq!(
            sanitize("<span style=\"color: #f00; position: fixed; top: 0\">x</span>", list).unwrap(),
            "<span style=\"color: #f00;\">x</span>"
        );
        let rgb = "<span style=\"COLOR: rgb(1 2 3 / 50%)\">x</span>";
        assert_eq!(sanitize(rgb, list).unwrap(), rgb);
        for hostile in [
            "background-color: url(https://evil.test/beacon)",
            "color: expression(alert(1))",
            "background-color: red; background-image: url(x)",
            "color: \\72 ed",
            "color: red /* */",
            "width: 100000px",
            "",
        ] {
            let html = sanitize(&format!("<span style=\"{hostile}\">x</span>"), list).unwrap();
            assert!(
                !html.contains("url") && !html.contains("expression") && !html.contains('\\') && !html.contains("width"),
                "{hostile}: {html}"
            );
        }
        assert_eq!(sanitize("<span style=\"position: fixed\">x</span>", list).unwrap(), "<span>x</span>");
    }

    #[test]
    fn checks_uris_like_loofah() {
        assert!(!allowed_uri("javascript:alert(1)"));
        assert!(!allowed_uri("java\nscript:alert(1)"));
        assert!(!allowed_uri("javascript&#58;alert(1)"));
        assert!(!allowed_uri("&#106;avascript:alert(1)"));
        assert!(!allowed_uri("javascript&colon;alert(1)"));
        assert!(allowed_uri("/rooms/1"));
        assert!(allowed_uri("https://example.com"));
        assert!(allowed_uri("data:image/png;base64,xx"));
        assert!(!allowed_uri("data:text/html,xx"));
        // Rails' SafeListSanitizer drops this href: CGI.unescapeHTML turns the zero-padded `&` into
        // `&#106;`, which Loofah then decodes.
        let list = SafeList::content_filter();
        assert_eq!(sanitize("<a href=\"&amp;#0000000000038;#106;avascript:alert(1)\">x</a>", list).unwrap(), "<a>x</a>");
    }

    #[test]
    fn unescapes_html_like_cgi() {
        // `CGI.unescapeHTML(s)` in the reference.
        for (escaped, unescaped) in [
            ("&#65;", "A"),
            ("&#X41;", "A"),
            ("&#00000000065;", "A"),
            ("&#0000000000000000000000065;", "A"),
            ("&#x000000041;", "A"),
            ("&#x0000000000000000000041;", "A"),
            ("&#00000000000000000000000000000000106;avascript", "javascript"),
            ("&#0;", "\0"),
            ("&#65535;", "\u{ffff}"),
            ("&#1114110;", "\u{10fffe}"),
            ("&#1114111;", "&#1114111;"),
            ("&#x10FFFF;", "&#x10FFFF;"),
            ("&#x110000;", "&#x110000;"),
            ("&#99999999999999999999;", "&#99999999999999999999;"),
            ("&#18446744073709551615;", "&#18446744073709551615;"),
            ("&#x10000000000000041;", "&#x10000000000000041;"),
            ("&#;", "&#;"),
            ("&#x;", "&#x;"),
            ("&#65", "&#65"),
            ("&#0x41;", "&#0x41;"),
            ("&amp;#65;", "&#65;"),
        ] {
            assert_eq!(cgi_unescape_html(escaped), unescaped, "{escaped}");
        }
    }
}
