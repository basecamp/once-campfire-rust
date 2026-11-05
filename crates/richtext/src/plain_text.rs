//! `ActionText::PlainTextConversion`

use crate::dom::{Dom, NodeId};
use crate::ruby::{chomp_newlines, is_blank};

/// `PlainTextConversion.node_to_plain_text`: a bottom-up reduction keyed on each node's name.
pub fn node_to_plain_text(dom: &Dom, node: NodeId) -> String {
    let ordinals = element_child_ordinals(dom);
    chomp_newlines(&plain_text_for(dom, &ordinals, node)).to_string()
}

/// Each element's 0-based position among its parent's element children, computed in one pass so
/// that an ordered list item's number (`bullet_for_li`) is a lookup. Recomputing the position by
/// scanning the whole list for every `<li>` was O(items) per item, i.e. quadratic for a long `<ol>`.
fn element_child_ordinals(dom: &Dom) -> Vec<usize> {
    let mut ordinals = vec![0usize; dom.node_count()];
    for parent in 0..dom.node_count() {
        let mut index = 0;
        for &child in dom.children(parent) {
            if dom.is_element(child) {
                ordinals[child] = index;
                index += 1;
            }
        }
    }
    ordinals
}

fn plain_text_for(dom: &Dom, ordinals: &[usize], node: NodeId) -> String {
    let child_values = || -> Vec<String> { dom.children(node).iter().map(|&c| plain_text_for(dom, ordinals, c)).collect() };
    let name = dom.name(node);
    match name.as_ref() {
        "script" | "style" | "unsupported" => String::new(),
        "h1" | "p" => plain_text_for_block(&child_values()),
        "ul" | "ol" => {
            let text = plain_text_for_block(&child_values());
            if list_depth(dom, node) > 0 { format!("\n{text}") } else { text }
        }
        "br" => "\n".to_string(),
        // Text nodes, and elements that happen to be named "text" (SVG's), use `node.text`
        "text" => chomp_newlines(&dom.text_content(node)).to_string(),
        "div" => format!("{}\n", chomp_newlines(&child_values().concat())),
        "figcaption" => format!("[{}]", chomp_newlines(&child_values().concat())),
        "blockquote" => {
            let text = plain_text_for_block(&child_values());
            if is_blank(&text) {
                return "“”".to_string();
            }
            // `text.insert(text.rindex(/\S/) + 1, "”")`, then `text.index(/\S/)` for "“"
            let is_space = |c: char| matches!(c, ' ' | '\t' | '\n' | '\u{0b}' | '\u{0c}' | '\r');
            let mut text = text;
            if let Some((i, c)) = text.char_indices().rev().find(|&(_, c)| !is_space(c)) {
                text.insert(i + c.len_utf8(), '”');
            }
            if let Some((i, _)) = text.char_indices().find(|&(_, c)| !is_space(c)) {
                text.insert(i, '“');
            }
            text
        }
        "li" => {
            let bullet = bullet_for_li(dom, ordinals, node);
            let text = chomp_newlines(&child_values().concat()).to_string();
            let depth = list_depth(dom, node);
            let indentation = if depth > 1 { "  ".repeat(depth - 1) } else { String::new() };
            format!("{indentation}{bullet} {text}\n")
        }
        _ => child_values().concat(),
    }
}

fn plain_text_for_block(child_values: &[String]) -> String {
    format!("{}\n\n", chomp_newlines(&child_values.concat()))
}

fn is_list(name: &str) -> bool {
    name == "ul" || name == "ol"
}

fn list_depth(dom: &Dom, node: NodeId) -> usize {
    dom.ancestors(node).into_iter().filter(|&a| is_list(&dom.name(a))).count()
}

fn bullet_for_li(dom: &Dom, ordinals: &[usize], node: NodeId) -> String {
    let list = dom.ancestors(node).into_iter().map(|a| dom.name(a).into_owned()).find(|n| is_list(n));
    if list.as_deref() == Some("ol") {
        // `list_node.children.index(node)`: the item's place among its parent's element children.
        format!("{}.", ordinals[node] + 1)
    } else {
        "•".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plain_text(html: &str) -> String {
        let mut dom = Dom::new();
        let root = dom.parse_fragment(html).unwrap();
        node_to_plain_text(&dom, root)
    }

    #[test]
    fn numbers_ordered_list_items_by_position() {
        assert_eq!(plain_text("<ol><li>a</li><li>b</li><li>c</li></ol>"), "1. a\n2. b\n3. c");
        // Numbering is a lookup, not a search of the list per item, so a long `<ol>` stays linear.
        let text = plain_text(&format!("<ol>{}</ol>", "<li>x</li>".repeat(2000)));
        assert!(text.starts_with("1. x\n2. x\n"));
        assert!(text.ends_with("\n1999. x\n2000. x"));
    }
}
