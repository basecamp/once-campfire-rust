//! `ContentFilters::TextMessagePresentationFilters` (reference/app/helpers/content_filters/*.rb):
//! RemoveSoloUnfurledLinkText, SanitizeTags, SanitizeAttributes, applied in that order.

use ruby_compat::strip;

use crate::Error;
use crate::attachables::{OPENGRAPH_EMBED_CONTENT_TYPE, RenderContext, opengraph_embed_from_node};
use crate::content::{ATTACHMENT_TAG, Content};
use crate::ruby::is_blank;
use crate::sanitizer::{self, SafeList, sanitize_tags_allowed_tags};
use crate::uri::{self, UriError};

pub fn apply(content: Content, ctx: &RenderContext) -> Result<Content, Error> {
    let content = remove_solo_unfurled_link_text(content, ctx)?;
    let content = sanitize_tags(content);
    sanitize_attributes(content)
}

// --- RemoveSoloUnfurledLinkText -----------------------------------------------------------------

/// A message that is nothing but a link to what it unfurls shows just the unfurl.
pub fn remove_solo_unfurled_link_text(content: Content, ctx: &RenderContext) -> Result<Content, Error> {
    let unfurled_links: Vec<_> = content
        .dom
        .descendants(content.root)
        .into_iter()
        .filter(|&n| {
            content.dom.local_name(n) == Some(ATTACHMENT_TAG) && content.dom.attr(n, "content-type") == Some(OPENGRAPH_EMBED_CONTENT_TYPE)
        })
        .collect();
    let solo_unfurled_url = if unfurled_links.len() == 1 {
        opengraph_embed_from_node(&content.dom, unfurled_links[0], ctx)?.and_then(|embed| embed.href)
    } else {
        None
    };
    let plain_text = content.to_plain_text(ctx)?;
    let applicable = normalize_tweet_url(solo_unfurled_url.as_deref())? == normalize_tweet_url(Some(&plain_text))?;
    if !applicable {
        return Ok(content);
    }

    let Content { mut dom, root } = content;
    let is_trix_body = dom.descendants(root).into_iter().any(|n| dom.local_name(n) == Some("div"));
    if is_trix_body {
        // Every div gets the unfurl as its only content
        let unfurl = dom.to_html(unfurled_links[0]);
        for div in dom.descendants(root).into_iter().filter(|&n| dom.local_name(n) == Some("div")).collect::<Vec<_>>() {
            dom.set_inner_html(div, &unfurl).map_err(Error::Parse)?;
        }
    } else {
        for p in dom.descendants(root).into_iter().filter(|&n| dom.local_name(n) == Some("p")).collect::<Vec<_>>() {
            let has_attachment = dom.descendants(p).into_iter().any(|n| dom.local_name(n) == Some(ATTACHMENT_TAG));
            if !has_attachment {
                dom.detach(p);
            }
        }
    }
    Ok(Content { dom, root })
}

const TWITTER_DOMAINS: &[&str] = &["x.com", "twitter.com"];

fn normalize_tweet_url(url: Option<&str>) -> Result<Option<String>, Error> {
    let Some(url) = url else { return Ok(None) };
    let is_twitter_url = !is_blank(url) && TWITTER_DOMAINS.iter().any(|d| strip(url).contains(d));
    if !is_twitter_url {
        return Ok(Some(url.to_string()));
    }
    match uri::parse(url) {
        Err(UriError::InvalidUri) => Ok(Some(url.to_string())),
        Err(UriError::InvalidComponent) => Err(Error::Raised("URI::InvalidComponentError")),
        Ok(mut parsed) => {
            if parsed.host.as_deref().map(str::to_lowercase).as_deref() == Some("x.com") {
                parsed.host = Some("twitter.com".to_string());
            }
            parsed.query = None;
            Ok(Some(parsed.to_s()))
        }
    }
}

// --- SanitizeTags ------------------------------------------------------------------------------

/// Removes every element outside the allowlist, together with its contents.
pub fn sanitize_tags(content: Content) -> Content {
    let Content { mut dom, root } = content;
    let allowed = sanitize_tags_allowed_tags();
    let disallowed: Vec<_> =
        dom.descendants(root).into_iter().filter(|&n| dom.local_name(n).is_some_and(|name| !allowed.contains(&name))).collect();
    dom.detach_all(&disallowed);
    Content { dom, root }
}

// --- SanitizeAttributes ------------------------------------------------------------------------

/// Scrubs attributes with Rails' safe-list sanitizer over SanitizeTags' own tags.
pub fn sanitize_attributes(content: Content) -> Result<Content, Error> {
    let html = sanitizer::sanitize(&content.to_html(), SafeList::content_filter()).map_err(Error::Parse)?;
    Content::wrap(&html)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_tweet_urls_like_rails() {
        // `RemoveSoloUnfurledLinkText#normalize_tweet_url` in the reference.
        for (url, normalized) in [
            ("HTTPS://x.com/dhh/status/1?s=20", "https://twitter.com/dhh/status/1"),
            ("https://x.com/dhh/status/1?s=20", "https://twitter.com/dhh/status/1"),
            ("HTTP://twitter.com/a", "http://twitter.com/a"),
            ("Https://X.com/dhh/status/1", "Https://X.com/dhh/status/1"),
        ] {
            assert_eq!(normalize_tweet_url(Some(url)).unwrap().as_deref(), Some(normalized), "{url}");
        }
    }
}
