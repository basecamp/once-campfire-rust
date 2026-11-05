//! A small arena DOM that behaves like the Nokogiri HTML5 (Gumbo) trees Action Text works on.
//!
//! Parsing goes through html5ever, which implements the same WHATWG tree construction
//! algorithm as Gumbo. Serialization reproduces Nokogiri's `html_standard_serialize`
//! (`Nokogiri::HTML5::Node#write_to`), which is what `ActionText::HtmlConversion.node_to_html`
//! and Loofah's `to_html` produce for HTML5 documents.

use std::borrow::Cow;
use std::cell::{Cell, RefCell};

use html5ever::interface::{ElemName, ElementFlags, NodeOrText, QuirksMode, TreeSink, create_element};
use html5ever::tendril::StrTendril;
use html5ever::tokenizer::{BufferQueue, Token, TokenSink, TokenSinkResult, Tokenizer, TokenizerOpts};
use html5ever::tree_builder::{TreeBuilder, TreeBuilderOpts};
use html5ever::{Attribute, LocalName, Namespace, QualName, TokenizerResult, local_name, ns};

pub type NodeId = usize;

/// Gumbo's defaults, which Nokogiri raises `ArgumentError` for exceeding
/// (`Nokogiri::Gumbo::DEFAULT_MAX_TREE_DEPTH`, `DEFAULT_MAX_ATTRIBUTES`).
pub const MAX_TREE_DEPTH: usize = 400;
pub const MAX_ATTRIBUTES: usize = 400;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ParseError {
    #[error("Document tree depth limit exceeded")]
    TreeDepthExceeded,
    #[error("Attributes per element limit exceeded")]
    TooManyAttributes,
}

#[derive(Debug, Clone)]
pub struct Attr {
    pub name: QualName,
    pub value: String,
}

impl Attr {
    /// The attribute's name as Nokogiri reports and serializes it ("xlink:href", "href").
    pub fn qualified_name(&self) -> Cow<'_, str> {
        match &self.name.prefix {
            Some(prefix) => Cow::Owned(format!("{}:{}", prefix, self.name.local)),
            None => Cow::Borrowed(&self.name.local),
        }
    }

    /// Whether `qualified_name() == name`, without building the qualified name.
    pub fn has_name(&self, name: &str) -> bool {
        match &self.name.prefix {
            Some(prefix) => {
                name.strip_prefix(&**prefix).and_then(|rest| rest.strip_prefix(':')).is_some_and(|local| local == &*self.name.local)
            }
            None => name == &*self.name.local,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ElementData {
    pub name: QualName,
    pub attrs: Vec<Attr>,
}

#[derive(Debug, Clone)]
pub enum NodeData {
    Document,
    Fragment,
    Element(ElementData),
    Text(String),
    Comment(String),
    Doctype(String),
}

#[derive(Debug, Clone)]
pub struct Node {
    pub data: NodeData,
    pub parent: Option<NodeId>,
    pub children: Vec<NodeId>,
}

#[derive(Debug, Default, Clone)]
pub struct Dom {
    nodes: Vec<Node>,
}

/// The context a fragment is parsed in: Nokogiri parses `Document#fragment` in a `body`
/// context and `Node#fragment` (used by `replace`, `inner_html=`) in the node's own context.
#[derive(Debug, Clone)]
pub struct Context {
    pub name: QualName,
}

impl Context {
    pub fn body() -> Self {
        Self::html("body")
    }

    pub fn html(local: &str) -> Self {
        Context { name: QualName::new(None, ns!(html), LocalName::from(local)) }
    }
}

impl Dom {
    pub fn new() -> Self {
        Dom { nodes: Vec::new() }
    }

    pub fn node(&self, id: NodeId) -> &Node {
        &self.nodes[id]
    }

    /// The number of nodes in the arena, so a caller can size a per-node table.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    fn push(&mut self, data: NodeData) -> NodeId {
        self.nodes.push(Node { data, parent: None, children: Vec::new() });
        self.nodes.len() - 1
    }

    pub fn new_fragment(&mut self) -> NodeId {
        self.push(NodeData::Fragment)
    }

    pub fn create_element(&mut self, local: &str, attrs: &[(&str, &str)]) -> NodeId {
        let attrs =
            attrs.iter().map(|(k, v)| Attr { name: QualName::new(None, ns!(), LocalName::from(*k)), value: v.to_string() }).collect();
        self.push(NodeData::Element(ElementData { name: QualName::new(None, ns!(html), LocalName::from(local)), attrs }))
    }

    pub fn create_text(&mut self, text: &str) -> NodeId {
        self.push(NodeData::Text(text.to_string()))
    }

    pub fn children(&self, id: NodeId) -> &[NodeId] {
        &self.nodes[id].children
    }

    pub fn parent(&self, id: NodeId) -> Option<NodeId> {
        self.nodes[id].parent
    }

    pub fn element(&self, id: NodeId) -> Option<&ElementData> {
        match &self.nodes[id].data {
            NodeData::Element(e) => Some(e),
            _ => None,
        }
    }

    pub fn element_mut(&mut self, id: NodeId) -> Option<&mut ElementData> {
        match &mut self.nodes[id].data {
            NodeData::Element(e) => Some(e),
            _ => None,
        }
    }

    pub fn is_element(&self, id: NodeId) -> bool {
        matches!(self.nodes[id].data, NodeData::Element(_))
    }

    pub fn is_text(&self, id: NodeId) -> bool {
        matches!(self.nodes[id].data, NodeData::Text(_))
    }

    /// Nokogiri's `Node#name`: the local name for elements, "text", "comment" and so on otherwise.
    pub fn name(&self, id: NodeId) -> Cow<'_, str> {
        match &self.nodes[id].data {
            NodeData::Element(e) => Cow::Borrowed(&*e.name.local),
            NodeData::Text(_) => Cow::Borrowed("text"),
            NodeData::Comment(_) => Cow::Borrowed("comment"),
            NodeData::Fragment => Cow::Borrowed("#document-fragment"),
            NodeData::Document => Cow::Borrowed("document"),
            NodeData::Doctype(n) => Cow::Owned(n.clone()),
        }
    }

    /// The element's local name, if `id` is an element.
    pub fn local_name(&self, id: NodeId) -> Option<&str> {
        self.element(id).map(|e| &*e.name.local)
    }

    pub fn is_html_element(&self, id: NodeId) -> bool {
        self.element(id).is_some_and(|e| e.name.ns == ns!(html))
    }

    pub fn attr(&self, id: NodeId, name: &str) -> Option<&str> {
        self.element(id)?.attrs.iter().find(|a| a.has_name(name)).map(|a| a.value.as_str())
    }

    pub fn has_attr(&self, id: NodeId, name: &str) -> bool {
        self.attr(id, name).is_some()
    }

    /// Nokogiri's `node[name] = value`: updates in place, or appends a new attribute.
    pub fn set_attr(&mut self, id: NodeId, name: &str, value: &str) {
        if let Some(element) = self.element_mut(id) {
            if let Some(attr) = element.attrs.iter_mut().find(|a| a.has_name(name)) {
                attr.value = value.to_string();
            } else {
                element.attrs.push(Attr { name: QualName::new(None, ns!(), LocalName::from(name)), value: value.to_string() });
            }
        }
    }

    pub fn remove_attr(&mut self, id: NodeId, name: &str) -> Option<String> {
        let element = self.element_mut(id)?;
        let index = element.attrs.iter().position(|a| a.has_name(name))?;
        Some(element.attrs.remove(index).value)
    }

    pub fn attrs(&self, id: NodeId) -> Vec<(String, String)> {
        self.element(id).map(|e| e.attrs.iter().map(|a| (a.qualified_name().into_owned(), a.value.clone())).collect()).unwrap_or_default()
    }

    /// Element children only (Nokogiri's `Node#elements`).
    pub fn element_children(&self, id: NodeId) -> Vec<NodeId> {
        self.children(id).iter().copied().filter(|&c| self.is_element(c)).collect()
    }

    /// All descendants in document order, excluding `id` itself.
    pub fn descendants(&self, id: NodeId) -> Vec<NodeId> {
        let mut out = Vec::new();
        let mut stack: Vec<NodeId> = self.children(id).iter().rev().copied().collect();
        while let Some(n) = stack.pop() {
            out.push(n);
            stack.extend(self.children(n).iter().rev().copied());
        }
        out
    }

    pub fn ancestors(&self, id: NodeId) -> Vec<NodeId> {
        let mut out = Vec::new();
        let mut current = self.parent(id);
        while let Some(p) = current {
            out.push(p);
            current = self.parent(p);
        }
        out
    }

    pub fn detach(&mut self, id: NodeId) {
        if let Some(parent) = self.nodes[id].parent.take() {
            self.nodes[parent].children.retain(|&c| c != id);
        }
    }

    /// Detaches every node in `ids`, filtering each parent's children once rather than once per
    /// node: detaching the N children of one parent one at a time scans that list N times.
    pub fn detach_all(&mut self, ids: &[NodeId]) {
        let mut gone = vec![false; self.nodes.len()];
        let mut parents = Vec::new();
        for &id in ids {
            if let Some(parent) = self.nodes[id].parent.take() {
                gone[id] = true;
                parents.push(parent);
            }
        }
        parents.sort_unstable();
        parents.dedup();
        for parent in parents {
            self.nodes[parent].children.retain(|&c| !gone[c]);
        }
    }

    pub fn append(&mut self, parent: NodeId, child: NodeId) {
        self.detach(child);
        self.nodes[child].parent = Some(parent);
        self.nodes[parent].children.push(child);
    }

    pub fn insert_before(&mut self, reference: NodeId, new: NodeId) {
        self.detach(new);
        let parent = self.nodes[reference].parent.expect("reference node has a parent");
        let index = self.nodes[parent].children.iter().position(|&c| c == reference).unwrap();
        self.nodes[new].parent = Some(parent);
        self.nodes[parent].children.insert(index, new);
    }

    /// Replaces `id` with its children, in order, at `id`'s own place under its parent: Nokogiri's
    /// `node.before(node.children); node.remove`. Done as one splice so unwrapping a wide element
    /// is O(children + siblings), not O(children) insertions that each search and shift the
    /// parent's child list (which is quadratic in the number of children).
    pub fn unwrap(&mut self, id: NodeId) {
        let Some(parent) = self.parent(id) else { return };
        let children = std::mem::take(&mut self.nodes[id].children);
        for &child in &children {
            self.nodes[child].parent = Some(parent);
        }
        let index = self.nodes[parent].children.iter().position(|&c| c == id).expect("child of its parent");
        self.nodes[parent].children.splice(index..=index, children);
        self.nodes[id].parent = None;
    }

    /// The text content of a node, as libxml2's `xmlNodeGetContent` computes it.
    pub fn text_content(&self, id: NodeId) -> String {
        match &self.nodes[id].data {
            NodeData::Text(t) | NodeData::Comment(t) => t.clone(),
            _ => {
                let mut out = String::new();
                for d in self.descendants(id) {
                    if let NodeData::Text(t) = &self.nodes[d].data {
                        out.push_str(t);
                    }
                }
                out
            }
        }
    }

    pub fn text(&self, id: NodeId) -> Option<&str> {
        match &self.nodes[id].data {
            NodeData::Text(t) => Some(t),
            _ => None,
        }
    }

    pub fn deep_clone(&mut self, id: NodeId) -> NodeId {
        let data = self.nodes[id].data.clone();
        let copy = self.push(data);
        for child in self.nodes[id].children.clone() {
            let child_copy = self.deep_clone(child);
            self.nodes[child_copy].parent = Some(copy);
            self.nodes[copy].children.push(child_copy);
        }
        copy
    }

    // --- Parsing -----------------------------------------------------------------------------

    /// `Nokogiri::HTML5::Document#fragment(html)`: a new fragment parsed in a `body` context.
    pub fn parse_fragment(&mut self, html: &str) -> Result<NodeId, ParseError> {
        let fragment = self.new_fragment();
        for node in self.parse_nodes(html, &Context::body())? {
            self.append(fragment, node);
        }
        Ok(fragment)
    }

    /// The context Nokogiri uses when parsing markup for a node (`Node#fragment`).
    pub fn context_for(&self, id: NodeId) -> Context {
        match &self.nodes[id].data {
            NodeData::Element(e) => Context { name: e.name.clone() },
            // Gumbo falls back to a body context for anything that isn't an element
            _ => Context::body(),
        }
    }

    /// Parses `html` in the given context and returns the resulting top-level nodes, detached.
    pub fn parse_nodes(&mut self, html: &str, context: &Context) -> Result<Vec<NodeId>, ParseError> {
        let parsed = parse_with_html5ever(html, context)?;
        let root = parsed.fragment_root();
        let children = parsed.nodes[root].children.clone();
        let offset = self.nodes.len();
        for node in parsed.nodes {
            self.nodes.push(Node {
                data: node.data,
                parent: node.parent.map(|p| p + offset),
                children: node.children.into_iter().map(|c| c + offset).collect(),
            });
        }
        let top: Vec<NodeId> = children.into_iter().map(|c| c + offset).collect();
        for &n in &top {
            self.nodes[n].parent = None;
        }
        Ok(top)
    }

    /// Nokogiri's `node.inner_html = html` (via `children=`), parsed in the node's context.
    pub fn set_inner_html(&mut self, id: NodeId, html: &str) -> Result<(), ParseError> {
        let context = self.context_for(id);
        let new_children = self.parse_nodes(html, &context)?;
        for child in self.nodes[id].children.clone() {
            self.nodes[child].parent = None;
        }
        self.nodes[id].children.clear();
        for child in new_children {
            self.append(id, child);
        }
        Ok(())
    }

    /// Nokogiri's `node.replace(html)`: the markup is parsed in the context of the node's parent.
    pub fn replace_with_html(&mut self, id: NodeId, html: &str) -> Result<(), ParseError> {
        let Some(parent) = self.parent(id) else { return Ok(()) };
        let context = self.context_for(parent);
        let replacements = self.parse_nodes(html, &context)?;
        for n in replacements {
            self.insert_before(id, n);
        }
        self.detach(id);
        Ok(())
    }

    /// Replaces a node with other (detached) nodes.
    pub fn replace_with_nodes(&mut self, id: NodeId, replacements: &[NodeId]) {
        if self.parent(id).is_none() {
            return;
        }
        for &n in replacements {
            self.insert_before(id, n);
        }
        self.detach(id);
    }

    // --- Serialization -----------------------------------------------------------------------

    /// `node.to_html` for an HTML5 document: the node itself (children only for a fragment).
    pub fn to_html(&self, id: NodeId) -> String {
        self.serialize(id, AttributeEscaping::Nokogiri)
    }

    /// `to_html`, but with `<` and `>` escaped in attribute values as well, as the current HTML
    /// serialization algorithm does. Nokogiri leaves them raw, which lets auto_link's regular
    /// expressions mistake an attribute value for text.
    pub fn to_html_with_escaped_attribute_brackets(&self, id: NodeId) -> String {
        self.serialize(id, AttributeEscaping::Brackets)
    }

    fn serialize(&self, id: NodeId, escaping: AttributeEscaping) -> String {
        let mut out = String::new();
        match self.nodes[id].data {
            NodeData::Fragment | NodeData::Document => self.serialize_children(id, escaping, &mut out),
            _ => self.serialize_node(id, escaping, &mut out),
        }
        out
    }

    pub fn inner_html(&self, id: NodeId) -> String {
        let mut out = String::new();
        self.serialize_children(id, AttributeEscaping::Nokogiri, &mut out);
        out
    }

    fn serialize_children(&self, id: NodeId, escaping: AttributeEscaping, out: &mut String) {
        for &child in &self.nodes[id].children {
            self.serialize_node(child, escaping, out);
        }
    }

    fn serialize_node(&self, id: NodeId, escaping: AttributeEscaping, out: &mut String) {
        match &self.nodes[id].data {
            NodeData::Element(e) => {
                out.push('<');
                push_tag_name(&e.name, out);
                for attr in &e.attrs {
                    out.push(' ');
                    push_qualified_name(&attr.name, out);
                    out.push_str("=\"");
                    escape_attribute(&attr.value, escaping, out);
                    out.push('"');
                }
                out.push('>');
                if e.name.ns == ns!(html) && is_void_element(&e.name.local) {
                    return;
                }
                self.serialize_children(id, escaping, out);
                out.push_str("</");
                push_tag_name(&e.name, out);
                out.push('>');
            }
            NodeData::Text(text) => {
                let raw = self
                    .parent(id)
                    .is_some_and(|p| self.element(p).is_some_and(|e| e.name.ns == ns!(html) && is_raw_text_element(&e.name.local)));
                if raw {
                    out.push_str(text);
                } else {
                    escape_text(text, out);
                }
            }
            NodeData::Comment(text) => {
                out.push_str("<!--");
                out.push_str(text);
                out.push_str("-->");
            }
            NodeData::Doctype(name) => {
                out.push_str("<!DOCTYPE ");
                out.push_str(name);
                out.push('>');
            }
            NodeData::Fragment | NodeData::Document => self.serialize_children(id, escaping, out),
        }
    }
}

fn push_tag_name(name: &QualName, out: &mut String) {
    if name.ns == ns!(html) || name.ns == ns!(svg) || name.ns == ns!(mathml) {
        out.push_str(&name.local);
    } else {
        push_qualified_name(name, out);
    }
}

fn push_qualified_name(name: &QualName, out: &mut String) {
    if let Some(prefix) = &name.prefix {
        out.push_str(prefix);
        out.push(':');
    }
    out.push_str(&name.local);
}

pub fn is_void_element(local: &str) -> bool {
    matches!(
        local,
        "area"
            | "base"
            | "basefont"
            | "bgsound"
            | "br"
            | "col"
            | "embed"
            | "frame"
            | "hr"
            | "img"
            | "input"
            | "keygen"
            | "link"
            | "meta"
            | "param"
            | "source"
            | "track"
            | "wbr"
    )
}

fn is_raw_text_element(local: &str) -> bool {
    matches!(local, "style" | "script" | "xmp" | "iframe" | "noembed" | "noframes" | "plaintext" | "noscript")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AttributeEscaping {
    Nokogiri,
    Brackets,
}

fn escape_attribute(value: &str, escaping: AttributeEscaping, out: &mut String) {
    for c in value.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '\u{a0}' => out.push_str("&nbsp;"),
            '"' => out.push_str("&quot;"),
            '<' if escaping == AttributeEscaping::Brackets => out.push_str("&lt;"),
            '>' if escaping == AttributeEscaping::Brackets => out.push_str("&gt;"),
            _ => out.push(c),
        }
    }
}

fn escape_text(value: &str, out: &mut String) {
    for c in value.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '\u{a0}' => out.push_str("&nbsp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            _ => out.push(c),
        }
    }
}

// --- html5ever glue ------------------------------------------------------------------------------

struct ParsedTree {
    nodes: Vec<Node>,
}

impl ParsedTree {
    /// html5ever puts the fragment's nodes under an `html` element beneath the document.
    fn fragment_root(&self) -> NodeId {
        self.nodes[0]
            .children
            .iter()
            .copied()
            .find(|&c| matches!(&self.nodes[c].data, NodeData::Element(e) if e.name.local == local_name!("html")))
            .unwrap_or(0)
    }
}

/// `html5ever::driver::parse_fragment`, held to Gumbo's limits as it goes, the way Gumbo is,
/// rather than checked once the tree is built: html5ever's scope checks walk the stack of open
/// elements, so building a deeply nested tree in full takes quadratic time.
fn parse_with_html5ever(html: &str, context: &Context) -> Result<ParsedTree, ParseError> {
    let sink = Sink { nodes: RefCell::new(vec![Node { data: NodeData::Document, parent: None, children: vec![] }]) };
    let context_element = create_element(&sink, context.name.clone(), Vec::new());
    let tree_builder = TreeBuilder::new_for_fragment(sink, context_element, None, tree_builder_opts());
    let tokenizer_opts = TokenizerOpts {
        initial_state: Some(tree_builder.tokenizer_state_for_context_elem(false)),
        max_attributes: Some(MAX_ATTRIBUTES),
        // Gumbo drops a byte order mark only at the start. html5ever drops one at the start of
        // every feed, and input is fed again after each </script>.
        discard_bom: false,
        ..TokenizerOpts::default()
    };
    let input = BufferQueue::default();
    input.push_back(StrTendril::from(html.strip_prefix('\u{feff}').unwrap_or(html)));
    let tokenizer = Tokenizer::new(DepthLimit::new(tree_builder, &input), tokenizer_opts);
    while let TokenizerResult::Script(_) = tokenizer.feed(&input) {}
    tokenizer.end();

    if tokenizer.sink.exceeded.get() {
        Err(ParseError::TreeDepthExceeded)
    } else if tokenizer.too_many_attributes() {
        Err(ParseError::TooManyAttributes)
    } else {
        Ok(tokenizer.sink.tree_builder.sink.finish())
    }
}

fn tree_builder_opts() -> TreeBuilderOpts {
    TreeBuilderOpts {
        // Gumbo parses without scripting, so <noscript> content is markup rather than raw text
        scripting_enabled: false,
        quirks_mode: QuirksMode::NoQuirks,
        ..TreeBuilderOpts::default()
    }
}

/// Gumbo's tree depth limit. Before it reads each token, Gumbo stops, as if the input had ended
/// there, once the stack of open elements holds more than `max_tree_depth` elements, and Nokogiri
/// adds one to the limit for a fragment's `html` element (nokogiri's ext/nokogiri/gumbo.c).
///
/// So the stack is checked as soon as a token has been handled, before the tokenizer reads on:
/// the next token can be all the rest of the input. Gumbo doesn't check after the end of the
/// input, though, when text left pending in a table can reopen formatting elements past the limit.
struct DepthLimit<'input> {
    tree_builder: TreeBuilder<NodeId, Sink>,
    input: &'input BufferQueue,
    exceeded: Cell<bool>,
}

impl<'input> DepthLimit<'input> {
    const MAX_OPEN_ELEMENTS: usize = MAX_TREE_DEPTH + 1;

    fn new(tree_builder: TreeBuilder<NodeId, Sink>, input: &'input BufferQueue) -> Self {
        DepthLimit { tree_builder, input, exceeded: Cell::new(false) }
    }

    /// Stops the tokenizer by taking away the rest of the input. The few tokens it can still make
    /// from what it has already read are ignored.
    fn stop(&self) {
        self.exceeded.set(true);
        self.input.replace_with(BufferQueue::default());
    }
}

impl TokenSink for DepthLimit<'_> {
    type Handle = NodeId;

    fn process_token(&self, token: Token, line_number: u64) -> TokenSinkResult<NodeId> {
        if self.exceeded.get() {
            return TokenSinkResult::Continue;
        }
        let reads_on = !matches!(token, Token::EOFToken);
        let result = self.tree_builder.process_token(token, line_number);
        if reads_on && self.tree_builder.open_elements_len() > Self::MAX_OPEN_ELEMENTS {
            self.stop();
        }
        result
    }

    fn end(&self) {
        self.tree_builder.end();
    }

    fn adjusted_current_node_present_but_not_in_html_namespace(&self) -> bool {
        self.tree_builder.adjusted_current_node_present_but_not_in_html_namespace()
    }
}

struct Sink {
    nodes: RefCell<Vec<Node>>,
}

#[derive(Debug)]
struct OwnedName(QualName);

impl ElemName for OwnedName {
    fn ns(&self) -> &Namespace {
        &self.0.ns
    }

    fn local_name(&self) -> &LocalName {
        &self.0.local
    }
}

impl Sink {
    fn new_node(&self, data: NodeData) -> NodeId {
        let mut nodes = self.nodes.borrow_mut();
        nodes.push(Node { data, parent: None, children: vec![] });
        nodes.len() - 1
    }

    /// The `html` element html5ever puts the fragment's nodes under (see `ParsedTree::fragment_root`).
    fn is_fragment_root(&self, id: NodeId) -> bool {
        self.nodes.borrow()[id].parent == Some(self.get_document())
    }

    fn detach(&self, id: NodeId) {
        let mut nodes = self.nodes.borrow_mut();
        if let Some(parent) = nodes[id].parent.take() {
            nodes[parent].children.retain(|&c| c != id);
        }
    }

    fn insert_at(&self, parent: NodeId, index: usize, child: NodeOrText<NodeId>) {
        match child {
            NodeOrText::AppendNode(node) => {
                self.detach(node);
                let mut nodes = self.nodes.borrow_mut();
                nodes[node].parent = Some(parent);
                nodes[parent].children.insert(index, node);
            }
            NodeOrText::AppendText(text) => {
                // Adjacent text merges into the preceding text node, as in the DOM
                let previous = if index > 0 { Some(self.nodes.borrow()[parent].children[index - 1]) } else { None };
                if let Some(prev) = previous
                    && let NodeData::Text(existing) = &mut self.nodes.borrow_mut()[prev].data
                {
                    existing.push_str(&text);
                    return;
                }
                let node = self.new_node(NodeData::Text(text.to_string()));
                let mut nodes = self.nodes.borrow_mut();
                nodes[node].parent = Some(parent);
                nodes[parent].children.insert(index, node);
            }
        }
    }
}

impl TreeSink for Sink {
    type Handle = NodeId;
    type Output = ParsedTree;
    type ElemName<'a> = OwnedName;

    fn finish(self) -> ParsedTree {
        ParsedTree { nodes: self.nodes.into_inner() }
    }

    fn parse_error(&self, _msg: Cow<'static, str>) {}

    fn get_document(&self) -> NodeId {
        0
    }

    fn elem_name<'a>(&'a self, target: &'a NodeId) -> OwnedName {
        match &self.nodes.borrow()[*target].data {
            NodeData::Element(e) => OwnedName(e.name.clone()),
            _ => panic!("not an element"),
        }
    }

    fn create_element(&self, name: QualName, attrs: Vec<Attribute>, _flags: ElementFlags) -> NodeId {
        // The tokenizer has already dropped duplicate attributes
        let attrs = attrs.into_iter().map(|a| Attr { name: a.name, value: a.value.to_string() }).collect();
        self.new_node(NodeData::Element(ElementData { name, attrs }))
    }

    fn create_comment(&self, text: StrTendril) -> NodeId {
        self.new_node(NodeData::Comment(text.to_string()))
    }

    fn create_pi(&self, _target: StrTendril, data: StrTendril) -> NodeId {
        self.new_node(NodeData::Comment(data.to_string()))
    }

    fn append(&self, parent: &NodeId, child: NodeOrText<NodeId>) {
        let index = self.nodes.borrow()[*parent].children.len();
        self.insert_at(*parent, index, child);
    }

    fn append_based_on_parent_node(&self, element: &NodeId, prev_element: &NodeId, child: NodeOrText<NodeId>) {
        if self.nodes.borrow()[*element].parent.is_some() {
            self.append_before_sibling(element, child);
        } else {
            self.append(prev_element, child);
        }
    }

    fn append_doctype_to_document(&self, name: StrTendril, _public_id: StrTendril, _system_id: StrTendril) {
        let node = self.new_node(NodeData::Doctype(name.to_string()));
        self.append(&0, NodeOrText::AppendNode(node));
    }

    fn get_template_contents(&self, target: &NodeId) -> NodeId {
        // libxml2 has no template contents: Nokogiri keeps them as ordinary children
        *target
    }

    fn same_node(&self, x: &NodeId, y: &NodeId) -> bool {
        x == y
    }

    fn set_quirks_mode(&self, _mode: QuirksMode) {}

    fn append_before_sibling(&self, sibling: &NodeId, new_node: NodeOrText<NodeId>) {
        let parent = self.nodes.borrow()[*sibling].parent.expect("sibling has a parent");
        // From the end: foster parenting inserts before an open table, which is its parent's last
        // child, so a body of thousands of misplaced elements in a table stays linear.
        let index = self.nodes.borrow()[parent].children.iter().rposition(|c| c == sibling).unwrap();
        self.insert_at(parent, index, new_node);
    }

    fn add_attrs_if_missing(&self, target: &NodeId, attrs: Vec<Attribute>) {
        // An <html> tag in the body gives its attributes to the fragment's root <html> element,
        // which parse_nodes never reads. Merging them there would compare each one with all the
        // root had collected, so a body of <html> tags would take quadratic time.
        if self.is_fragment_root(*target) {
            return;
        }
        if let NodeData::Element(e) = &mut self.nodes.borrow_mut()[*target].data {
            for a in attrs {
                if !e.attrs.iter().any(|existing| existing.name == a.name) {
                    e.attrs.push(Attr { name: a.name, value: a.value.to_string() });
                }
            }
        }
    }

    fn remove_from_parent(&self, target: &NodeId) {
        self.detach(*target);
    }

    fn reparent_children(&self, node: &NodeId, new_parent: &NodeId) {
        let children = std::mem::take(&mut self.nodes.borrow_mut()[*node].children);
        for child in children {
            self.nodes.borrow_mut()[child].parent = None;
            self.append(new_parent, NodeOrText::AppendNode(child));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(html: &str) -> String {
        let mut dom = Dom::new();
        let f = dom.parse_fragment(html).unwrap();
        dom.to_html(f)
    }

    #[test]
    fn serializes_like_nokogiri() {
        assert_eq!(roundtrip("<td>x</td>"), "x");
        assert_eq!(roundtrip("<p><table><tr><td>a</td></tr></table>"), "<p></p><table><tbody><tr><td>a</td></tr></tbody></table>");
        assert_eq!(
            roundtrip("<a title='a<b>c' href=\"x&y\u{a0}z\">t&lt;\u{a0}>\"'</a>"),
            "<a title=\"a<b>c\" href=\"x&amp;y&nbsp;z\">t&lt;&nbsp;&gt;\"'</a>"
        );
        let mut dom = Dom::new();
        let f = dom.parse_fragment("<a title='a<b>c'>d>e</a>").unwrap();
        assert_eq!(dom.to_html_with_escaped_attribute_brackets(f), "<a title=\"a&lt;b&gt;c\">d&gt;e</a>");
        assert_eq!(roundtrip("<pre>\n\nx</pre>"), "<pre>\nx</pre>");
        assert_eq!(roundtrip("<noscript><b>x</b></noscript>"), "<noscript><b>x</b></noscript>");
        assert_eq!(roundtrip("<?php x ?>"), "<!--?php x ?-->");
        assert_eq!(roundtrip("<SVG viewBox='0 0 1 1'><CLIPPATH/></SVG>"), "<svg viewBox=\"0 0 1 1\"><clipPath></clipPath></svg>");
    }

    #[test]
    fn drops_only_a_leading_byte_order_mark() {
        assert_eq!(roundtrip("\u{feff}\u{feff}x"), "\u{feff}x");
        assert_eq!(roundtrip("<script></script>\u{feff}x"), "<script></script>\u{feff}x");
    }

    fn parse(html: &str) -> Result<(), ParseError> {
        Dom::new().parse_fragment(html).map(|_| ())
    }

    fn numbered_attributes(range: std::ops::RangeInclusive<usize>) -> String {
        range.map(|i| format!("a{i}={i}")).collect::<Vec<_>>().join(" ")
    }

    // Gumbo's answers here come from Nokogiri 1.19.4 in the reference image.

    #[test]
    fn enforces_gumbo_tree_depth_limit() {
        use ParseError::TreeDepthExceeded;
        let b = |n| "<b>".repeat(n);
        assert_eq!(parse(&b(400)), Ok(()));
        assert_eq!(parse(&b(401)), Err(TreeDepthExceeded));
        assert_eq!(parse(&format!("{}x", b(401))), Err(TreeDepthExceeded));
        assert_eq!(parse(&format!("{}<p>", b(400))), Err(TreeDepthExceeded));
        assert_eq!(parse(&format!("{}<table><td>", b(397))), Err(TreeDepthExceeded));
    }

    #[test]
    fn counts_open_elements_as_gumbo_does_rather_than_the_final_trees_depth() {
        use ParseError::TreeDepthExceeded;
        let b = |n| "<b>".repeat(n);
        let s = |n| "<span>".repeat(n);
        let div = |n| "<div>".repeat(n);
        // A void element never goes on the stack of open elements
        assert_eq!(parse(&format!("{}<br>", b(400))), Ok(()));
        assert_eq!(parse(&format!("{}</p>", b(400))), Ok(()));
        // Closing everything again doesn't undo having been too deep
        assert_eq!(parse(&format!("{}{}", b(401), "</b>".repeat(401))), Err(TreeDepthExceeded));
        // The adoption agency moves blocks back up, and what counts is how deep they were
        assert_eq!(parse(&format!("<b>{}{}</b>", s(300), div(10)).repeat(3)), Ok(()));
        assert_eq!(parse(&format!("<b>{}{}</b>{}", s(390), div(10), div(300))), Err(TreeDepthExceeded));
        // Text pending in a table reopens the <b>s past the limit when the input ends, and Gumbo
        // doesn't check after that
        let bs: String = (1..=399).map(|i| format!("<b id={i}>")).collect();
        let reopened = format!("<p>{bs}</p><div><div><table>x");
        assert_eq!(parse(&reopened), Ok(()));
        assert_eq!(parse(&format!("{reopened}<!---->")), Err(TreeDepthExceeded));
    }

    #[test]
    fn enforces_gumbo_attribute_limit() {
        use ParseError::TooManyAttributes;
        assert_eq!(parse(&format!("<p {}>x</p>", numbered_attributes(1..=400))), Ok(()));
        assert_eq!(parse(&format!("<p {}>x</p>", numbered_attributes(1..=401))), Err(TooManyAttributes));
        assert_eq!(parse(&format!("<p {}>x</p>", ["a=1"; 1000].join(" "))), Ok(()));
        assert_eq!(parse(&format!("<textarea><p {}>", numbered_attributes(1..=401))), Ok(()));
    }

    #[test]
    fn counts_attributes_as_gumbos_tokenizer_does() {
        use ParseError::TooManyAttributes;
        let attributes = numbered_attributes(1..=400);
        // Before dropping a duplicate, on end tags, and on a tag the input ends inside
        assert_eq!(parse(&format!("<p {attributes} a1=again>x</p>")), Err(TooManyAttributes));
        assert_eq!(parse(&format!("<p>x</p {attributes} a401>")), Err(TooManyAttributes));
        assert_eq!(parse(&format!("<p {attributes} a401")), Err(TooManyAttributes));
    }

    #[test]
    fn keeps_what_the_tokenizer_already_deduplicated() {
        assert_eq!(roundtrip("<p title=a TITLE=b id=c title=d>x</p>"), "<p title=\"a\" id=\"c\">x</p>");
        assert_eq!(
            roundtrip("<svg xlink:href=a href=b viewbox=c><a xlink:href=d>x</a></svg>"),
            "<svg xlink:href=\"a\" href=\"b\" viewBox=\"c\"><a xlink:href=\"d\">x</a></svg>"
        );
    }
}
