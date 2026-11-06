//! `ActionText::Content`: loading (canonicalization), attachment rendering, and plain text.
//!
//! Each Ruby step that serializes a node and parses the markup back (`Fragment#replace` with a
//! string, `inner_html=`, a filter returning HTML) does the same here, in the same parse context,
//! because those round trips are where the output takes its shape.

use ruby_compat::strip;
use serde_json::Value;

use crate::Error;
use crate::attachables::{self, Attachable, Attachment, PlainTextRepresentation, RenderContext, attachment_from_node};
use crate::dom::{Dom, NodeId};
use crate::plain_text;
use crate::ruby::{self, is_blank, presence};
use crate::sanitizer::{self, ATTACHMENT_ATTRIBUTES, SafeList};

pub const ATTACHMENT_TAG: &str = "action-text-attachment";

/// A fragment of rich text living in a DOM arena.
pub struct Content {
    pub dom: Dom,
    pub root: NodeId,
}

impl Content {
    /// `ActionText::Content.new(html)`, which is also how a stored body loads: canonicalized.
    pub fn load(html: &str, ctx: &RenderContext) -> Result<Content, Error> {
        let mut dom = Dom::new();
        let root = load_into(&mut dom, html, ctx)?;
        Ok(Content { dom, root })
    }

    /// `ActionText::Content.new(html, canonicalize: false)`, i.e. `Fragment.from_html`.
    pub fn wrap(html: &str) -> Result<Content, Error> {
        let mut dom = Dom::new();
        let root = dom.parse_fragment(strip(html)).map_err(Error::Parse)?;
        Ok(Content { dom, root })
    }

    pub fn to_html(&self) -> String {
        self.dom.to_html(self.root)
    }

    /// `ActionText::Content#to_plain_text`
    pub fn to_plain_text(&self, ctx: &RenderContext) -> Result<String, Error> {
        let mut dom = self.dom.clone();
        let root = self.root;
        for node in attachment_nodes(&dom, root) {
            sanitize_content_attribute(&mut dom, node)?;
            let attachment = attachment_from_node(&dom, node, ctx)?;
            match attachables::attachment_plain_text(&attachment) {
                PlainTextRepresentation::Html(text) => dom.replace_with_html(node, &text).map_err(Error::Parse)?,
                PlainTextRepresentation::Content(content) => {
                    let fragment = load_into(&mut dom, &content, ctx)?;
                    let children = dom.children(fragment).to_vec();
                    dom.replace_with_nodes(node, &children);
                }
            }
        }
        Ok(plain_text::node_to_plain_text(&dom, root))
    }

    /// `render_action_text_content(content)`: attachments and galleries rendered, then sanitized
    /// with Action Text's allowlist.
    pub fn render(&self, ctx: &RenderContext) -> Result<String, Error> {
        self.render_nested(ctx, 0)
    }

    /// `render`, for content `depth` content attachments down.
    fn render_nested(&self, ctx: &RenderContext, depth: usize) -> Result<String, Error> {
        let mut dom = self.dom.clone();
        let root = self.root;
        render_attachments(&mut dom, root, ctx, depth)?;
        render_attachment_galleries(&mut dom, root, ctx, depth)?;
        sanitizer::sanitize(&dom.to_html(root), SafeList::action_text()).map_err(Error::Parse)
    }

    /// `Content#to_s`: the content partial inside `layouts/action_text/contents/_content.html.erb`,
    /// which Campfire overrides with a `lexxy-content` wrapper.
    pub fn to_rendered_html_with_layout(&self, ctx: &RenderContext) -> Result<String, Error> {
        Ok(format!("<div class=\"lexxy-content\">\n  {}\n</div>\n", self.render(ctx)?))
    }
}

/// Loads (and canonicalizes) `html` as a new fragment in `dom`.
pub fn load_into(dom: &mut Dom, html: &str, ctx: &RenderContext) -> Result<NodeId, Error> {
    let root = dom.parse_fragment(strip(html)).map_err(Error::Parse)?;
    convert_trix_attachments(dom, root, ctx)?;
    for node in attachment_nodes(dom, root) {
        dom.set_inner_html(node, "").map_err(Error::Parse)?;
    }
    for gallery in attachment_gallery_nodes(dom, root) {
        let html = format!("<div>{}</div>", dom.inner_html(gallery));
        dom.replace_with_html(gallery, &html).map_err(Error::Parse)?;
    }
    Ok(root)
}

pub fn attachment_nodes(dom: &Dom, root: NodeId) -> Vec<NodeId> {
    dom.descendants(root).into_iter().filter(|&n| dom.local_name(n) == Some(ATTACHMENT_TAG)).collect()
}

// --- Trix attachments --------------------------------------------------------------------------

/// `ActionText::TrixAttachment::ATTRIBUTES`, in order
const TRIX_ATTRIBUTES: &[(&str, &str)] = &[
    ("sgid", "sgid"),
    ("contentType", "content-type"),
    ("url", "url"),
    ("href", "href"),
    ("filename", "filename"),
    ("filesize", "filesize"),
    ("width", "width"),
    ("height", "height"),
    ("previewable", "previewable"),
    ("content", "content"),
    ("caption", "caption"),
    ("presentation", "presentation"),
];

/// `fragment_by_converting_trix_attachments`: `figure[data-trix-attachment]` (or any element
/// carrying the attribute) becomes an `<action-text-attachment>`, or disappears if it has none of
/// the attachment attributes.
fn convert_trix_attachments(dom: &mut Dom, root: NodeId, ctx: &RenderContext) -> Result<(), Error> {
    let nodes: Vec<NodeId> = dom.descendants(root).into_iter().filter(|&n| dom.has_attr(n, "data-trix-attachment")).collect();
    for node in nodes {
        let mut attributes: Vec<(&str, Value)> = Vec::new();
        for name in ["data-trix-attachment", "data-trix-attributes"] {
            let parsed = match dom.attr(node, name) {
                // Unparseable JSON is logged and treated as no attributes
                Some(json) => ruby::json_parse(json).unwrap_or(Value::Null),
                None => Value::Null,
            };
            match parsed {
                Value::Null | Value::Bool(false) => {}
                Value::Object(map) => {
                    for (key, value) in map {
                        if let Some(&(trix, _)) = TRIX_ATTRIBUTES.iter().find(|(trix, _)| *trix == key) {
                            if let Some(existing) = attributes.iter_mut().find(|(k, _)| *k == trix) {
                                existing.1 = value;
                            } else {
                                attributes.push((trix, value));
                            }
                        }
                    }
                }
                _ => return Err(Error::Raised("NoMethodError: merge")),
            }
        }
        let mut element_attrs: Vec<(&str, String)> = Vec::new();
        for name in ATTACHMENT_ATTRIBUTES {
            let trix_name = TRIX_ATTRIBUTES.iter().find(|(_, dashed)| dashed == name).map(|(t, _)| *t);
            if let Some((_, value)) = attributes.iter().find(|(k, _)| Some(*k) == trix_name) {
                element_attrs.push((name, ruby::json_value_to_s(value)));
            }
        }
        let replacement = if element_attrs.is_empty() {
            String::new()
        } else {
            let pairs: Vec<(&str, &str)> = element_attrs.iter().map(|(k, v)| (*k, v.as_str())).collect();
            let element = dom.create_element(ATTACHMENT_TAG, &pairs);
            // Attachment.from_node resolves the attachable, which may raise
            attachment_from_node(dom, element, ctx)?;
            dom.to_html(element)
        };
        dom.replace_with_html(node, &replacement).map_err(Error::Parse)?;
    }
    Ok(())
}

// --- Rendering ---------------------------------------------------------------------------------

/// `render_attachments`' first step: an attachment's `content` attribute is sanitized with Action
/// Text's allowlist, and dropped if that leaves nothing.
fn sanitize_content_attribute(dom: &mut Dom, node: NodeId) -> Result<(), Error> {
    if let Some(content) = dom.remove_attr(node, "content") {
        let sanitized = sanitizer::sanitize(&content, SafeList::action_text()).map_err(Error::Parse)?;
        if !is_blank(&sanitized) {
            dom.set_attr(node, "content", &sanitized);
        }
    }
    Ok(())
}

/// `Attachment#with_full_attributes`: a new node carrying the node's attachment attributes, the
/// attachable's own (sgid and content type, for a user), and the node's sgid if it had one.
fn node_with_full_attributes(dom: &mut Dom, node: NodeId, attachable: &Attachable) -> Result<NodeId, Error> {
    let mut attrs: Vec<(&str, String)> = Vec::new();
    for &name in ATTACHMENT_ATTRIBUTES {
        let value = match (name, attachable) {
            ("sgid", Attachable::User(user)) => Some(dom.attr(node, "sgid").unwrap_or(&user.attachable_sgid).to_string()),
            ("content-type", Attachable::User(_)) => Some(attachables::MENTION_CONTENT_TYPE.to_string()),
            _ => dom.attr(node, name).map(str::to_string),
        };
        if let Some(value) = value {
            attrs.push((name, value));
        }
    }
    if attrs.is_empty() {
        // from_attributes returns nil, and the render block calls #node on it
        return Err(Error::Raised("NoMethodError: node for nil"));
    }
    let pairs: Vec<(&str, &str)> = attrs.iter().map(|(k, v)| (*k, v.as_str())).collect();
    Ok(dom.create_element(ATTACHMENT_TAG, &pairs))
}

/// How deep content attachments render inside one another. Each level parses and sanitizes
/// everything nested below it again, so Rails' unbounded nesting makes rendering quadratic in the
/// body's size; deeper content attachments render empty. Nothing Campfire's composer makes nests
/// them at all.
pub const MAX_CONTENT_ATTACHMENT_DEPTH: usize = 8;

/// `render_action_text_attachment`, with nested content attachments rendered through
/// `ContentAttachment#to_html` (the content partial, without the layout).
pub fn render_attachment_html(attachment: &Attachment, ctx: &RenderContext) -> Result<String, Error> {
    render_attachment_html_at(attachment, ctx, 0)
}

fn render_attachment_html_at(attachment: &Attachment, ctx: &RenderContext, depth: usize) -> Result<String, Error> {
    attachables::render_attachment(attachment, &|content: &str| {
        if depth >= MAX_CONTENT_ATTACHMENT_DEPTH {
            return Ok(String::new());
        }
        let content = Content::load(content, ctx)?;
        Ok(format!("{}\n", content.render_nested(ctx, depth + 1)?))
    })
}

fn render_attachments(dom: &mut Dom, root: NodeId, ctx: &RenderContext, depth: usize) -> Result<(), Error> {
    for node in attachment_nodes(dom, root) {
        sanitize_content_attribute(dom, node)?;
        let attachment = attachment_from_node(dom, node, ctx)?;
        let full = node_with_full_attributes(dom, node, &attachment.attachable)?;
        let attachment = Attachment { attachable: attachment.attachable, caption: presence(dom.attr(full, "caption")).map(str::to_string) };
        let html = render_attachment_html_at(&attachment, ctx, depth)?;
        dom.set_inner_html(full, &html).map_err(Error::Parse)?;
        let replacement = dom.to_html(full);
        dom.replace_with_html(node, &replacement).map_err(Error::Parse)?;
    }
    Ok(())
}

const GALLERY_ATTACHMENT_PRESENTATION: &str = "gallery";

fn is_gallery_attachment(dom: &Dom, node: NodeId) -> bool {
    dom.local_name(node) == Some(ATTACHMENT_TAG) && dom.attr(node, "presentation") == Some(GALLERY_ATTACHMENT_PRESENTATION)
}

/// `AttachmentGallery.find_attachment_gallery_nodes`: `div:has(A + A)` for gallery attachments A,
/// whose children are all gallery attachments or newline/space text.
pub fn attachment_gallery_nodes(dom: &Dom, root: NodeId) -> Vec<NodeId> {
    // The gallery attachments whose previous element sibling is one too (`A + A`), from one pass
    // over each parent's element children rather than a copy and a search per attachment.
    let nodes = dom.descendants(root);
    let mut follows_gallery = std::collections::HashSet::new();
    for &parent in std::iter::once(&root).chain(nodes.iter()) {
        let mut previous_is_gallery = false;
        for child in dom.element_children(parent) {
            let is_gallery = is_gallery_attachment(dom, child);
            if is_gallery && previous_is_gallery {
                follows_gallery.insert(child);
            }
            previous_is_gallery = is_gallery;
        }
    }
    nodes
        .into_iter()
        .filter(|&div| dom.local_name(div) == Some("div"))
        .filter(|&div| dom.descendants(div).into_iter().any(|n| follows_gallery.contains(&n)))
        .filter(|&div| {
            dom.children(div).iter().all(|&child| match dom.text(child) {
                Some(text) => text.chars().all(|c| c == '\n' || c == ' '),
                None => is_gallery_attachment(dom, child),
            })
        })
        .collect()
}

fn render_attachment_galleries(dom: &mut Dom, root: NodeId, ctx: &RenderContext, depth: usize) -> Result<(), Error> {
    for gallery in attachment_gallery_nodes(dom, root) {
        let members: Vec<NodeId> = dom.descendants(gallery).into_iter().filter(|&n| is_gallery_attachment(dom, n)).collect();
        let mut rendered = String::new();
        for member in &members {
            let attachment = attachment_from_node(dom, *member, ctx)?;
            let full = node_with_full_attributes(dom, *member, &attachment.attachable)?;
            let html = render_attachment_html_at(&attachment, ctx, depth)?;
            dom.set_inner_html(full, &html).map_err(Error::Parse)?;
            rendered.push_str(&dom.to_html(full));
        }
        let html = format!("<div class=\"attachment-gallery attachment-gallery--{}\">\n  {}\n</div>", members.len(), rendered);
        dom.replace_with_html(gallery, &html).map_err(Error::Parse)?;
    }
    Ok(())
}
