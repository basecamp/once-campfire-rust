//! `ActionCable::Connection::Base` and `Connection::Subscriptions`: one task per socket.
//!
//! Commands are handled one at a time in arrival order. The connection reads its streams straight
//! from the hub's per-broadcasting ring buffers, so there's no per-connection queue: a client
//! that stops reading falls behind by the ring's capacity, which closes the connection with
//! `reconnect: true`. Frames that are ready together go out in one socket write.
//!
//! The socket's read half lives in a task of its own that hands incoming messages over in order,
//! so it's only polled when the socket is readable, not every time a delivery wakes the
//! connection.
use std::sync::Arc;

use futures_util::stream::{AbortRegistration, Abortable, SelectAll};
use futures_util::{FutureExt, StreamExt};
use tokio::io::{ReadHalf, WriteHalf};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::channel::{DynChannel, Params, Subscription};
use crate::protocol::{self, DisconnectReason};
use crate::pubsub::{Deliveries, Frame, Subscriber};
use crate::server::{ConnectRequest, Identified, internal_channel};
use crate::socket::{Incoming, Reader, Writer};
use crate::{Server, json};

struct Entry<U: Send + Sync + 'static> {
    channel: Box<dyn DynChannel<U>>,
    sub: Subscription<U>,
}

/// Why the socket is going away, once we've decided to close it.
struct Close {
    reason: Option<DisconnectReason>,
    reconnect: Value,
}

impl Close {
    /// A stream fell behind (`reason: nil`, as `Connection::Base#close` without one).
    fn lagged() -> Self {
        Close { reason: None, reconnect: Value::Bool(true) }
    }
}

struct Connection<U: Send + Sync + 'static> {
    server: Server<U>,
    user: Arc<U>,
    /// Keyed by the raw identifier string, in subscription order (a Ruby hash).
    subscriptions: Vec<Entry<U>>,
    pending: Vec<Frame>,
    /// Streams the last command's callbacks started, to read from once its frames are queued.
    started: Vec<(Subscriber, AbortRegistration)>,
}

/// The most subscriptions one connection may hold, and the longest identifier it may subscribe with.
const MAX_SUBSCRIPTIONS: usize = 64;
const MAX_IDENTIFIER_BYTES: usize = 4096;

/// Incoming messages buffered between the reader task and the connection. A client that sends
/// commands faster than they're handled is held back by TCP once this fills.
const INCOMING_CAPACITY: usize = 16;

/// The upgraded HTTP connection a WebSocket runs on.
pub(crate) type Io = hyper_util::rt::TokioIo<hyper::upgrade::Upgraded>;
type Sink = Writer<WriteHalf<Io>>;

pub(crate) async fn run<U: Identified + Send + Sync + 'static>(server: Server<U>, io: Io, deflate: bool, request: ConnectRequest) {
    let (read, write) = tokio::io::split(io);
    let mut sink = Writer::new(write, deflate);
    let (reader, mut incoming) = spawn_reader(Reader::new(read, deflate));
    let config = server.config().clone();

    // handle_open: connect, subscribe to the internal channel, welcome, then process whatever
    // arrived meanwhile (the socket buffers it for us, like MessageBuffer).
    let Some(user) = server.authenticator().connect(&request).await else {
        return reject_unauthorized(sink, incoming, reader, &config).await;
    };

    // The internal channel carries raw payloads; every subscription stream carries frames.
    let mut internal = SelectAll::new();
    let identifier = user.connection_identifier();
    if !identifier.is_empty() {
        internal.push(server.hub().subscribe(&internal_channel(&identifier), None).deliveries());
        // A ban or sign-out that disconnected this user between the check above and that
        // subscription went unheard, so check again now that it would be heard (Rails has this
        // gap).
        if server.authenticator().connect(&request).await.is_none() {
            return reject_unauthorized(sink, incoming, reader, &config).await;
        }
    }
    // Only connecting needs the request. Its header values are slices of the HTTP read buffer, so
    // keeping it would hold that buffer (8 KB) for as long as the socket is open.
    drop(request);
    let mut deliveries = SelectAll::<Deliveries>::new();

    let mut heartbeat = server.heartbeat();
    heartbeat.mark_unchanged();
    let mut restarts = server.restarts();

    let mut connection =
        Connection { server, user: Arc::new(user), subscriptions: Vec::new(), pending: Vec::new(), started: Vec::new() };

    let mut close: Option<Close> = None;
    if sink.send(&[protocol::welcome().into()]).await.is_err() {
        reader.abort();
        connection.handle_close().await;
        return;
    }

    loop {
        tokio::select! {
            message = incoming.recv() => match message {
                Some(Incoming::Text(text)) => connection.dispatch(&text).await,
                Some(Incoming::Binary) => tracing::error!("Couldn't handle non-string message: Array"),
                Some(Incoming::Ping(payload)) => {
                    if sink.pong(&payload).await.is_err() {
                        break;
                    }
                }
                Some(Incoming::Pong) => {}
                // Complete the closing handshake (RFC 6455 §5.5.1) before letting the socket go.
                Some(Incoming::Close(code)) => {
                    let _ = sink.close_reply(code).await;
                    break;
                }
                Some(Incoming::Invalid) => {
                    let _ = sink.close(1002).await;
                    break;
                }
                None => break,
            },
            Some(delivery) = deliveries.next() => {
                // Whatever else is ready already goes out in the same write.
                let mut delivery = Some(delivery);
                while let Some(result) = delivery.take() {
                    match result {
                        Ok(frame) => connection.pending.push(frame),
                        Err(_) => {
                            close = Some(Close::lagged());
                            break;
                        }
                    }
                    if connection.pending.len() < config.max_write_batch {
                        delivery = deliveries.next().now_or_never().flatten();
                    }
                }
            }
            Some(message) = internal.next() => match message {
                Ok(message) => match process_internal_message(message.as_str()) {
                    Some(remote) => close = Some(remote),
                    None => continue,
                },
                Err(_) => continue,
            },
            Ok(()) = heartbeat.changed() => {
                // One frame per beat, shared by every connection.
                let ping = heartbeat.borrow_and_update().clone();
                connection.pending.push(ping);
            }
            Ok(()) = restarts.recv() => {
                close = Some(Close { reason: Some(DisconnectReason::ServerRestart), reconnect: Value::Bool(true) });
            }
        }

        deliveries.extend(connection.started.drain(..).map(|(subscriber, registration)| Abortable::new(subscriber.deliveries(), registration)));
        if !connection.flush(&mut sink).await {
            break;
        }
        if let Some(Close { reason, reconnect }) = close.take() {
            let _ = sink.send(&[protocol::disconnect(reason, &reconnect).into()]).await;
            close_socket(&mut sink, &mut incoming, config.close_timeout).await;
            break;
        }
    }

    reader.abort();
    connection.handle_close().await;
}

/// `InternalChannel#process_internal_message`.
fn process_internal_message(message: &str) -> Option<Close> {
    let message: Value = serde_json::from_str(message).ok()?;
    (message.get("type")? == "disconnect").then(|| Close {
        reason: Some(DisconnectReason::Remote),
        reconnect: message.get("reconnect").cloned().unwrap_or(Value::Bool(true)),
    })
}

/// `Connection::Base#respond_to_invalid_request` for an unauthorized connection: tell the client
/// not to reconnect, and close.
async fn reject_unauthorized(mut sink: Sink, mut incoming: mpsc::Receiver<Incoming>, reader: JoinHandle<()>, config: &crate::Config) {
    tracing::error!("An unauthorized connection attempt was rejected");
    let frame = protocol::disconnect(Some(DisconnectReason::Unauthorized), &Value::Bool(false));
    let _ = sink.send(&[frame.into()]).await;
    close_socket(&mut sink, &mut incoming, config.close_timeout).await;
    reader.abort();
}

/// Reads the socket until it closes or errors, handing each message to the connection. It stops
/// after a close frame, as the connection does.
fn spawn_reader(mut reader: Reader<ReadHalf<Io>>) -> (JoinHandle<()>, mpsc::Receiver<Incoming>) {
    let (sender, receiver) = mpsc::channel(INCOMING_CAPACITY);
    let reader = tokio::spawn(async move {
        loop {
            let message = match reader.next().await {
                Ok(message) => message,
                Err(error) if error.kind() == std::io::ErrorKind::InvalidData => Incoming::Invalid,
                Err(_) => break,
            };
            let close = matches!(message, Incoming::Close(_) | Incoming::Invalid);
            if sender.send(message).await.is_err() || close {
                break;
            }
        }
    });
    (reader, receiver)
}

/// Sends a normal close (1000, no reason, as `ClientSocket#close` defaults) and waits briefly
/// for the client to finish the handshake.
async fn close_socket(sink: &mut Sink, incoming: &mut mpsc::Receiver<Incoming>, timeout: std::time::Duration) {
    if sink.close(1000).await.is_ok() {
        let _ = tokio::time::timeout(timeout, async {
            while let Some(message) = incoming.recv().await {
                if matches!(message, Incoming::Close(_) | Incoming::Invalid) {
                    break;
                }
            }
        })
        .await;
    }
}

impl<U: Send + Sync + 'static> Connection<U> {
    /// Writes the pending frames in order, in one vectored write where the socket takes it.
    async fn flush(&mut self, sink: &mut Sink) -> bool {
        if self.pending.is_empty() {
            return true;
        }
        let written = sink.send(&self.pending).await.is_ok();
        self.pending.clear();
        written
    }

    /// `Subscriptions#execute_command`. Anything malformed raises in Rails, which is logged and
    /// otherwise ignored; the connection stays open.
    async fn dispatch(&mut self, text: &str) {
        let data = match serde_json::from_str::<Value>(text) {
            Ok(Value::Object(data)) => data,
            _ => return tracing::error!(message = text, "Could not execute command"),
        };
        match data.get("command").and_then(Value::as_str) {
            Some("subscribe") => self.add(&data).await,
            Some("unsubscribe") => self.remove(&data).await,
            Some("message") => self.perform_action(&data).await,
            _ => tracing::error!(message = text, "Received unrecognized command"),
        }
    }

    /// `Subscriptions#add`. A repeated identifier (byte for byte) is ignored without a reply.
    async fn add(&mut self, data: &Params) {
        let Some(identifier) = data.get("identifier").and_then(Value::as_str) else {
            return tracing::error!("Could not execute command: missing identifier");
        };
        let Ok(Value::Object(params)) = serde_json::from_str::<Value>(identifier) else {
            return tracing::error!(identifier, "Could not execute command: invalid identifier");
        };
        if self.position(identifier).is_some() {
            return;
        }
        // Bounds on what one socket can make the server hold (Rails has none). A page subscribes
        // to six channels with identifiers of a few hundred bytes.
        if self.subscriptions.len() >= MAX_SUBSCRIPTIONS || identifier.len() > MAX_IDENTIFIER_BYTES {
            return tracing::error!(subscriptions = self.subscriptions.len(), "Could not execute command: subscription limit reached");
        }
        let class_name = params.get("channel").and_then(Value::as_str).unwrap_or_default().to_string();
        let Some(factory) = self.server.channel_factory(&class_name) else {
            return tracing::error!(channel = class_name, "Subscription class not found");
        };

        let channel = factory();
        let sub = Subscription {
            server: self.server.clone(),
            class_name,
            identifier: identifier.into(),
            encoded_identifier: json::encode(identifier).into(),
            current_user: self.user.clone(),
            streams: Vec::new(),
            rejected: false,
            unsubscribed: false,
            transmissions: Vec::new(),
            started: Vec::new(),
        };
        self.subscriptions.push(Entry { channel, sub });
        self.subscribe_to_channel(identifier).await;
    }

    /// `Channel::Base#subscribe_to_channel`.
    async fn subscribe_to_channel(&mut self, identifier: &str) {
        let index = self.position(identifier).expect("just added");
        let Entry { channel, sub } = &mut self.subscriptions[index];
        let result = channel.subscribed(sub).await;
        self.pending.extend(sub.transmissions.drain(..).map(Frame::from));
        self.started.append(&mut sub.started);

        if let Err(error) = result {
            return tracing::error!(identifier, error = error.0, "Could not execute command");
        }
        if sub.rejected {
            self.remove_subscription(index).await;
            self.pending.push(protocol::rejection(identifier).into());
        } else {
            self.pending.push(protocol::confirmation(identifier).into());
        }
    }

    /// `Subscriptions#remove`: no reply either way.
    async fn remove(&mut self, data: &Params) {
        match self.find(data) {
            Some(index) => self.remove_subscription(index).await,
            None => tracing::error!("Unable to find subscription with identifier"),
        }
    }

    /// `Subscriptions#remove_subscription` → `Channel::Base#unsubscribe_from_channel`.
    async fn remove_subscription(&mut self, index: usize) {
        let Entry { mut channel, mut sub } = self.subscriptions.remove(index);
        sub.unsubscribed = true;
        if let Err(error) = channel.unsubscribed(&mut sub).await {
            tracing::error!(error = error.0, "Could not execute command");
        }
        sub.stop_all_streams();
        self.pending.extend(sub.transmissions.drain(..).map(Frame::from));
        self.started.append(&mut sub.started);
    }

    /// `Subscriptions#perform_action` → `Channel::Base#perform_action`.
    async fn perform_action(&mut self, data: &Params) {
        let Some(index) = self.find(data) else {
            return tracing::error!("Unable to find subscription with identifier");
        };
        let payload = match data.get("data").and_then(Value::as_str).map(serde_json::from_str::<Value>) {
            Some(Ok(Value::Object(payload))) => payload,
            _ => return tracing::error!("Could not execute command: invalid data"),
        };
        let action = match payload.get("action") {
            None | Some(Value::Null) => "receive".to_string(),
            Some(Value::String(action)) if action.trim().is_empty() => "receive".to_string(),
            Some(Value::String(action)) => action.clone(),
            Some(_) => return tracing::error!("Could not execute command: invalid action"),
        };

        let Entry { channel, sub } = &mut self.subscriptions[index];
        let result = channel.perform(&action, &payload, sub).await;
        self.pending.extend(sub.transmissions.drain(..).map(Frame::from));
        self.started.append(&mut sub.started);
        match result {
            Ok(true) => {}
            Ok(false) => tracing::error!(action, "Unable to process"),
            Err(error) => tracing::error!(action, error = error.0, "Could not execute command"),
        }
    }

    /// `Connection::Base#handle_close`: unsubscribe everything.
    async fn handle_close(&mut self) {
        while !self.subscriptions.is_empty() {
            self.remove_subscription(0).await;
        }
    }

    fn find(&self, data: &Params) -> Option<usize> {
        data.get("identifier").and_then(Value::as_str).and_then(|identifier| self.position(identifier))
    }

    fn position(&self, identifier: &str) -> Option<usize> {
        self.subscriptions.iter().position(|entry| &*entry.sub.identifier == identifier)
    }
}
