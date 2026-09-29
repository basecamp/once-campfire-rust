//! turbo-rails: `Turbo::StreamsChannel`, the `<turbo-stream>` tags its broadcasts carry
//! (`Turbo::Streams::ActionHelper#turbo_stream_action_tag`) and the `broadcast_*_to` helpers
//! (`Turbo::Streams::Broadcasts`).
use std::sync::Arc;

use rails_compat::Secrets;
use serde_json::Value;

use crate::channel::{Channel, ChannelError, ChannelResult, Params, Subscription};
use crate::{Server, naming};

pub const STREAMS_CHANNEL: &str = "Turbo::StreamsChannel";

type Verifier = Arc<dyn Fn(&str) -> Option<String> + Send + Sync>;
type Guard = Arc<dyn Fn(&str) -> bool + Send + Sync>;

/// `Turbo::StreamsChannel`, optionally with a guard prepended the way Campfire prepends
/// `RoomStreamsAreAuthorized` (reference/app/channels/concerns/room_streams_are_authorized.rb).
#[derive(Clone)]
pub struct StreamsChannel {
    verifier: Verifier,
    guard: Option<Guard>,
}

impl StreamsChannel {
    /// Verifies signed stream names with `Turbo.signed_stream_verifier`.
    pub fn new(secrets: Arc<Secrets>) -> Self {
        Self::with_verifier(move |signed| rails_compat::turbo::verified_stream_name(&secrets, signed))
    }

    pub fn with_verifier(verifier: impl Fn(&str) -> Option<String> + Send + Sync + 'static) -> Self {
        Self { verifier: Arc::new(verifier), guard: None }
    }

    /// Rejects any subscription whose verified stream name `guarded` returns true for, before
    /// the stock behavior runs. The guard also sees names that failed verification, as `""`
    /// (Ruby's `nil.to_s`).
    pub fn guarded_by(mut self, guarded: impl Fn(&str) -> bool + Send + Sync + 'static) -> Self {
        self.guard = Some(Arc::new(guarded));
        self
    }

    /// `verified_stream_name_from_params`, for channels (like `RoomMessagesChannel`) that take
    /// signed stream names too.
    pub fn verified_stream_name_from_params(&self, params: &Params) -> ChannelResult<Option<String>> {
        verified_stream_name_from_params(params, |signed| (self.verifier)(signed))
    }
}

/// `params[:signed_stream_name]` verified with `verifier`. A missing or `null` name is simply
/// unverified; any other non-string makes `MessageVerifier#verified` raise.
pub fn verified_stream_name_from_params(
    params: &Params,
    verifier: impl Fn(&str) -> Option<String>,
) -> ChannelResult<Option<String>> {
    match params.get("signed_stream_name") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(signed)) => Ok(verifier(signed)),
        Some(other) => Err(ChannelError(format!("undefined method 'valid_encoding?' for {other}"))),
    }
}

impl<U: Send + Sync + 'static> Channel<U> for StreamsChannel {
    async fn subscribed(&mut self, sub: &mut Subscription<U>) -> ChannelResult {
        let stream_name = self.verified_stream_name_from_params(&sub.params())?;
        if let Some(guard) = &self.guard
            && guard(stream_name.as_deref().unwrap_or(""))
        {
            sub.reject();
            return Ok(());
        }
        match stream_name {
            Some(stream_name) => sub.stream_from(stream_name),
            None => sub.reject(),
        }
        Ok(())
    }
}

/// Turbo Stream actions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Append,
    Prepend,
    Replace,
    Update,
    Remove,
    Before,
    After,
    Refresh,
}

impl Action {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Append => "append",
            Self::Prepend => "prepend",
            Self::Replace => "replace",
            Self::Update => "update",
            Self::Remove => "remove",
            Self::Before => "before",
            Self::After => "after",
            Self::Refresh => "refresh",
        }
    }
}

/// Where the action applies. Records are passed as their `dom_id` (`dom_id(@room, :list)`), and
/// `targets` as a CSS selector (records become `#<dom_id>`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Target<'a> {
    None,
    Target(&'a str),
    Targets(&'a str),
}

/// `turbo_stream_action_tag(action, target:, targets:, template:, **attributes)`.
///
/// Extra attributes come first, then `action`, then `target`/`targets`, exactly as the tag
/// helper's hash is built. Attribute names are used as given (Rails only dasherizes the tag
/// name, so `maintain_scroll: true` renders `maintain_scroll="true"`), and a `None` value omits
/// the attribute. `template` is already-safe HTML; `remove` and `refresh` never carry one.
pub fn action_tag(action: Action, target: Target<'_>, template: Option<&str>, attributes: &[(&str, Option<&str>)]) -> String {
    let mut tag = String::from("<turbo-stream");
    for (name, value) in attributes {
        if let Some(value) = value {
            push_attribute(&mut tag, name, value);
        }
    }
    push_attribute(&mut tag, "action", action.as_str());
    match target {
        Target::Target(target) => push_attribute(&mut tag, "target", target),
        Target::Targets(targets) => push_attribute(&mut tag, "targets", targets),
        Target::None => {}
    }
    tag.push('>');
    if !matches!(action, Action::Remove | Action::Refresh) {
        tag.push_str("<template>");
        tag.push_str(template.unwrap_or(""));
        tag.push_str("</template>");
    }
    tag.push_str("</turbo-stream>");
    tag
}

/// `turbo_stream_refresh_tag(request_id:)`.
pub fn refresh_tag(request_id: Option<&str>) -> String {
    action_tag(Action::Refresh, Target::None, None, &[("request-id", request_id.filter(|id| !id.is_empty()))])
}

fn push_attribute(tag: &mut String, name: &str, value: &str) {
    tag.push(' ');
    tag.push_str(name);
    tag.push_str("=\"");
    tag.push_str(&html_escape(value));
    tag.push('"');
}

/// `ERB::Util.unwrapped_html_escape`.
pub fn html_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&#39;"),
            c => escaped.push(c),
        }
    }
    escaped
}

/// `Turbo::StreamsChannel.broadcast_*_to`. Streamables are the stream name parts (GID params
/// and symbols); blank ones are dropped and nothing is sent if none remain, as in
/// `broadcast_stream_to`.
impl<U: Send + Sync + 'static> Server<U> {
    pub fn broadcast_stream_to(&self, streamables: &[&str], content: &str) -> usize {
        let streamables: Vec<&str> = streamables.iter().copied().filter(|s| !s.trim().is_empty()).collect();
        if streamables.is_empty() {
            return 0;
        }
        self.broadcast(&naming::stream_name_from(&streamables), content)
    }

    pub fn broadcast_action_to(
        &self,
        streamables: &[&str],
        action: Action,
        target: Target<'_>,
        html: Option<&str>,
        attributes: &[(&str, Option<&str>)],
    ) -> usize {
        self.broadcast_stream_to(streamables, &action_tag(action, target, html, attributes))
    }

    pub fn broadcast_append_to(&self, streamables: &[&str], target: &str, html: &str) -> usize {
        self.broadcast_action_to(streamables, Action::Append, Target::Target(target), Some(html), &[])
    }

    pub fn broadcast_prepend_to(&self, streamables: &[&str], target: &str, html: &str) -> usize {
        self.broadcast_action_to(streamables, Action::Prepend, Target::Target(target), Some(html), &[])
    }

    pub fn broadcast_replace_to(&self, streamables: &[&str], target: &str, html: &str) -> usize {
        self.broadcast_action_to(streamables, Action::Replace, Target::Target(target), Some(html), &[])
    }

    pub fn broadcast_update_to(&self, streamables: &[&str], target: &str, html: &str) -> usize {
        self.broadcast_action_to(streamables, Action::Update, Target::Target(target), Some(html), &[])
    }

    pub fn broadcast_remove_to(&self, streamables: &[&str], target: &str) -> usize {
        self.broadcast_action_to(streamables, Action::Remove, Target::Target(target), None, &[])
    }

    pub fn broadcast_refresh_to(&self, streamables: &[&str], request_id: Option<&str>) -> usize {
        self.broadcast_stream_to(streamables, &refresh_tag(request_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_tag() {
        assert_eq!(
            action_tag(Action::Append, Target::Target("messages_room_1"), Some("<div id=\"m\">Hi &amp; bye</div>"), &[]),
            r#"<turbo-stream action="append" target="messages_room_1"><template><div id="m">Hi &amp; bye</div></template></turbo-stream>"#
        );
    }

    #[test]
    fn remove_tag_has_no_template() {
        assert_eq!(
            action_tag(Action::Remove, Target::Target("message_1"), None, &[]),
            r#"<turbo-stream action="remove" target="message_1"></turbo-stream>"#
        );
    }

    #[test]
    fn attributes_come_before_action_and_are_not_dasherized() {
        assert_eq!(
            action_tag(Action::Replace, Target::Target("presentation_message_1"), Some("x"), &[("maintain_scroll", Some("true"))]),
            r#"<turbo-stream maintain_scroll="true" action="replace" target="presentation_message_1"><template>x</template></turbo-stream>"#
        );
    }

    #[test]
    fn targets_and_escaping() {
        assert_eq!(
            action_tag(Action::Update, Target::Targets("#a > b[data-x='1']"), None, &[]),
            r##"<turbo-stream action="update" targets="#a &gt; b[data-x=&#39;1&#39;]"><template></template></turbo-stream>"##
        );
    }

    #[test]
    fn refresh_tags() {
        assert_eq!(refresh_tag(None), r#"<turbo-stream action="refresh"></turbo-stream>"#);
        assert_eq!(refresh_tag(Some("abc")), r#"<turbo-stream request-id="abc" action="refresh"></turbo-stream>"#);
    }
}
