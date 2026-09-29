//! `ActionCable::Channel::Base`: one instance per subscription, driven by the connection.
use std::sync::Arc;

use serde::Serialize;
use serde_json::{Map, Value};
use futures_util::future::BoxFuture;
use futures_util::stream::{AbortHandle, AbortRegistration};

use crate::pubsub::Subscriber;
use crate::{json, naming, protocol, Server};

pub type Params = Map<String, Value>;

/// An exception escaping a channel callback. Rails logs it (`Subscriptions#execute_command`) and
/// sends nothing: a subscription whose `subscribed` raised is neither confirmed nor rejected, and
/// stays registered.
#[derive(Debug)]
pub struct ChannelError(pub String);

impl<E: std::error::Error> From<E> for ChannelError {
    fn from(error: E) -> Self {
        Self(error.to_string())
    }
}

pub type ChannelResult<T = ()> = Result<T, ChannelError>;

/// A channel class. The server builds a fresh instance per subscription from the factory
/// registered under the Ruby class name (see [`crate::ServerBuilder::channel`]).
///
/// Rails' `on_subscribe`/`on_unsubscribe` callbacks run after `subscribed`/`unsubscribed`, so put
/// them at the end of these methods (guarding on [`Subscription::rejected`] where the Ruby does
/// `unless: :subscription_rejected?`).
pub trait Channel<U: Send + Sync + 'static>: Send + 'static {
    fn subscribed(&mut self, sub: &mut Subscription<U>) -> impl Future<Output = ChannelResult> + Send {
        let _ = sub;
        async { Ok(()) }
    }

    /// Also runs when a subscription is rejected, as in Rails (`reject_subscription` removes the
    /// subscription, which calls `unsubscribe_from_channel`).
    fn unsubscribed(&mut self, sub: &mut Subscription<U>) -> impl Future<Output = ChannelResult> + Send {
        let _ = sub;
        async { Ok(()) }
    }

    /// Dispatches a `perform` from the client. Return `Ok(false)` when `action` isn't one of the
    /// channel's public methods, which Rails logs as "Unable to process". `action` is
    /// `data["action"]`, or `"receive"` when that's blank.
    fn perform(&mut self, action: &str, data: &Params, sub: &mut Subscription<U>) -> impl Future<Output = ChannelResult<bool>> + Send {
        let _ = (action, data, sub);
        async { Ok(false) }
    }
}

pub(crate) trait DynChannel<U: Send + Sync + 'static>: Send + 'static {
    fn subscribed<'a>(&'a mut self, sub: &'a mut Subscription<U>) -> BoxFuture<'a, ChannelResult>;
    fn unsubscribed<'a>(&'a mut self, sub: &'a mut Subscription<U>) -> BoxFuture<'a, ChannelResult>;
    fn perform<'a>(&'a mut self, action: &'a str, data: &'a Params, sub: &'a mut Subscription<U>) -> BoxFuture<'a, ChannelResult<bool>>;
}

impl<U: Send + Sync + 'static, C: Channel<U>> DynChannel<U> for C {
    fn subscribed<'a>(&'a mut self, sub: &'a mut Subscription<U>) -> BoxFuture<'a, ChannelResult> {
        Box::pin(Channel::subscribed(self, sub))
    }

    fn unsubscribed<'a>(&'a mut self, sub: &'a mut Subscription<U>) -> BoxFuture<'a, ChannelResult> {
        Box::pin(Channel::unsubscribed(self, sub))
    }

    fn perform<'a>(&'a mut self, action: &'a str, data: &'a Params, sub: &'a mut Subscription<U>) -> BoxFuture<'a, ChannelResult<bool>> {
        Box::pin(Channel::perform(self, action, data, sub))
    }
}

/// A channel with no callbacks: `ApplicationCable::Channel` itself and `HeartbeatChannel`
/// subscribe, confirm and do nothing else.
pub struct EmptyChannel;

impl<U: Send + Sync + 'static> Channel<U> for EmptyChannel {}

/// The per-subscription state a channel works with: params, the connection's identity, streams,
/// rejection and transmissions.
pub struct Subscription<U: Send + Sync + 'static> {
    pub(crate) server: Server<U>,
    pub(crate) class_name: String,
    /// The raw identifier the client subscribed with; params are parsed from it when asked for
    /// (subscriptions live as long as their sockets, and are many).
    pub(crate) identifier: Arc<str>,
    pub(crate) encoded_identifier: Arc<str>,
    pub(crate) current_user: Arc<U>,
    pub(crate) streams: Vec<(String, AbortHandle)>,
    /// Streams started by the last callback, for the connection to start reading once that
    /// callback's own frames (transmissions, the confirmation) are queued ahead of them.
    pub(crate) started: Vec<(Subscriber, AbortRegistration)>,
    pub(crate) rejected: bool,
    pub(crate) unsubscribed: bool,
    pub(crate) transmissions: Vec<String>,
}

impl<U: Send + Sync + 'static> Subscription<U> {
    /// The raw identifier string the client subscribed with.
    pub fn identifier(&self) -> &str {
        &self.identifier
    }

    /// The decoded identifier, including `channel`.
    pub fn params(&self) -> Params {
        match serde_json::from_str::<Value>(&self.identifier) {
            Ok(Value::Object(params)) => params,
            _ => Params::new(),
        }
    }

    pub fn param(&self, key: &str) -> Option<Value> {
        self.params().remove(key)
    }

    /// `identified_by :current_user`.
    pub fn current_user(&self) -> &Arc<U> {
        &self.current_user
    }

    pub fn server(&self) -> &Server<U> {
        &self.server
    }

    pub fn channel_name(&self) -> String {
        naming::channel_name(&self.class_name)
    }

    /// `broadcasting_for` for this channel's class.
    pub fn broadcasting_for(&self, broadcastables: &[&str]) -> String {
        naming::broadcasting_for(&self.class_name, broadcastables)
    }

    /// `broadcast_to` for this channel's class.
    pub fn broadcast_to<T: Serialize + ?Sized>(&self, broadcastables: &[&str], message: &T) {
        self.server.broadcast(&self.broadcasting_for(broadcastables), message);
    }

    pub fn stream_from(&mut self, broadcasting: impl Into<String>) {
        if self.unsubscribed {
            return;
        }
        let broadcasting = broadcasting.into();
        // The hub wraps each broadcast for this identifier once, for every subscriber sharing it.
        let subscriber = self.server.hub().subscribe(&broadcasting, Some(self.encoded_identifier.clone()));
        let (handle, registration) = AbortHandle::new_pair();
        self.started.push((subscriber, registration));
        self.streams.push((broadcasting, handle));
    }

    pub fn stream_for(&mut self, broadcastables: &[&str]) {
        let broadcasting = self.broadcasting_for(broadcastables);
        self.stream_from(broadcasting);
    }

    pub fn stop_stream_from(&mut self, broadcasting: &str) {
        self.streams.retain(|(name, handle)| {
            let keep = name != broadcasting;
            if !keep {
                handle.abort();
            }
            keep
        });
    }

    pub fn stop_all_streams(&mut self) {
        for (_, handle) in self.streams.drain(..) {
            handle.abort();
        }
        self.started.clear();
    }

    /// `stream_or_reject_for`.
    pub fn stream_or_reject_for(&mut self, broadcastables: Option<&[&str]>) {
        match broadcastables {
            Some(broadcastables) => self.stream_for(broadcastables),
            None => self.reject(),
        }
    }

    pub fn streams(&self) -> impl Iterator<Item = &str> {
        self.streams.iter().map(|(name, _)| name.as_str())
    }

    pub fn reject(&mut self) {
        self.rejected = true;
    }

    /// `subscription_rejected?`
    pub fn rejected(&self) -> bool {
        self.rejected
    }

    /// Sends `{"identifier":...,"message":...}` to this subscriber only.
    pub fn transmit<T: Serialize + ?Sized>(&mut self, message: &T) {
        self.transmissions.push(protocol::message(&self.encoded_identifier, &json::encode(message)));
    }
}

impl<U: Send + Sync + 'static> Drop for Subscription<U> {
    fn drop(&mut self) {
        self.stop_all_streams();
    }
}
