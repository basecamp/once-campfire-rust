//! A tiny app on top of the cable server, and a raw WebSocket client for driving it.
#![allow(dead_code)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use campfire_cable::turbo::StreamsChannel;
use campfire_cable::{
    Authenticate, Channel, ChannelResult, Config, ConnectRequest, EmptyChannel, Identified, Params, Server, Subscription,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

pub struct User {
    pub id: u64,
    pub room_ids: Vec<u64>,
}

impl Identified for User {
    fn connection_identifier(&self) -> String {
        format!("user-{}", self.id)
    }
}

/// `Cookie: session_token=<user id>`; users 1 and 2 exist, and both are members of room 1 only.
struct CookieAuth;

impl Authenticate<User> for CookieAuth {
    async fn connect(&self, request: &ConnectRequest) -> Option<User> {
        let cookie = request.headers.get("cookie")?.to_str().ok()?;
        let id: u64 = cookie.strip_prefix("session_token=")?.parse().ok()?;
        (id == 1 || id == 2).then(|| User { id, room_ids: vec![1] })
    }
}

/// Like `RoomChannel` plus `TypingNotificationsChannel`'s actions.
#[derive(Default)]
struct RoomChannel {
    room: Option<String>,
    log: Log,
}

pub type Log = Arc<Mutex<Vec<String>>>;

impl Channel<User> for RoomChannel {
    async fn subscribed(&mut self, sub: &mut Subscription<User>) -> ChannelResult {
        let room_id = sub.param("room_id").as_ref().and_then(Value::as_u64);
        match room_id.filter(|id| sub.current_user().room_ids.contains(id)) {
            Some(id) => {
                let room = format!("room-{id}");
                sub.stream_for(&[&room]);
                self.room = Some(room);
            }
            None => sub.reject(),
        }
        self.log.lock().unwrap().push(format!("subscribed rejected={}", sub.rejected()));
        Ok(())
    }

    async fn unsubscribed(&mut self, sub: &mut Subscription<User>) -> ChannelResult {
        self.log.lock().unwrap().push(format!("unsubscribed rejected={}", sub.rejected()));
        Ok(())
    }

    async fn perform(&mut self, action: &str, data: &Params, sub: &mut Subscription<User>) -> ChannelResult<bool> {
        match action {
            "start" => {
                let room = self.room.clone().unwrap();
                sub.broadcast_to(&[&room], &json!({ "action": "start", "user": { "id": sub.current_user().id } }));
            }
            "echo" => sub.transmit(data),
            "receive" => sub.transmit(&json!({ "received": data })),
            _ => return Ok(false),
        }
        Ok(true)
    }
}

pub struct TestServer {
    pub server: Server<User>,
    pub url: String,
    pub origin: String,
    pub log: Log,
}

pub async fn start(config: Config) -> TestServer {
    let log = Log::default();
    let room_log = log.clone();
    let server = Server::builder(config, CookieAuth)
        .channel("RoomChannel", move || RoomChannel { room: None, log: room_log.clone() })
        .channel("HeartbeatChannel", || EmptyChannel)
        .channel("Turbo::StreamsChannel", || {
            StreamsChannel::with_test_verifier().guarded_by(|name| name.split_once(':').map(|(_, s)| s) == Some("messages"))
        })
        .build();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = server.router::<()>("/cable");
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    TestServer { server, url: format!("ws://{addr}/cable"), origin: format!("http://{addr}"), log }
}

pub fn test_config() -> Config {
    Config { assume_ssl: false, ..Config::default() }
}

trait TestVerifier {
    fn with_test_verifier() -> Self;
}

/// Signed names in tests are `signed(<name>)`.
impl TestVerifier for StreamsChannel {
    fn with_test_verifier() -> Self {
        StreamsChannel::with_verifier(|signed| {
            signed.strip_prefix("signed(").and_then(|s| s.strip_suffix(')')).map(str::to_string)
        })
    }
}

pub type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub struct Client {
    pub socket: Socket,
    pub protocol: Option<String>,
}

impl TestServer {
    pub async fn connect(&self, user_id: u64) -> Client {
        self.connect_with(Some(user_id), &self.origin).await
    }

    pub async fn connect_with(&self, user_id: Option<u64>, origin: &str) -> Client {
        let mut request = self.url.as_str().into_client_request().unwrap();
        let headers = request.headers_mut();
        headers.insert("origin", HeaderValue::from_str(origin).unwrap());
        headers.insert("sec-websocket-protocol", HeaderValue::from_static("actioncable-v1-json, actioncable-unsupported"));
        if let Some(id) = user_id {
            headers.insert("cookie", HeaderValue::from_str(&format!("session_token={id}")).unwrap());
        }
        let (socket, response) = tokio_tungstenite::connect_async(request).await.expect("upgrade");
        let protocol = response.headers().get("sec-websocket-protocol").map(|v| v.to_str().unwrap().to_string());
        Client { socket, protocol }
    }
}

pub fn identifier(value: Value) -> String {
    value.to_string()
}

impl Client {
    pub async fn send(&mut self, command: Value) {
        self.socket.send(Message::Text(command.to_string().into())).await.unwrap();
    }

    pub async fn send_raw(&mut self, text: &str) {
        self.socket.send(Message::Text(text.into())).await.unwrap();
    }

    pub async fn subscribe(&mut self, identifier: &str) {
        self.send(json!({ "command": "subscribe", "identifier": identifier })).await;
    }

    pub async fn unsubscribe(&mut self, identifier: &str) {
        self.send(json!({ "command": "unsubscribe", "identifier": identifier })).await;
    }

    pub async fn perform(&mut self, identifier: &str, data: Value) {
        self.send(json!({ "command": "message", "identifier": identifier, "data": data.to_string() })).await;
    }

    /// The next frame, as raw text, skipping pings.
    pub async fn next(&mut self) -> Frame {
        loop {
            match self.next_including_pings().await {
                Frame::Text(text) if text.starts_with(r#"{"type":"ping""#) => continue,
                frame => return frame,
            }
        }
    }

    pub async fn next_including_pings(&mut self) -> Frame {
        let message = tokio::time::timeout(Duration::from_secs(5), self.socket.next()).await.expect("frame within 5s");
        match message {
            Some(Ok(Message::Text(text))) => Frame::Text(text.to_string()),
            Some(Ok(Message::Close(frame))) => Frame::Close(frame.map(|f| (u16::from(f.code), f.reason.to_string()))),
            Some(Ok(other)) => panic!("unexpected message {other:?}"),
            Some(Err(error)) => Frame::Error(error.to_string()),
            None => Frame::End,
        }
    }

    pub async fn next_text(&mut self) -> String {
        match self.next().await {
            Frame::Text(text) => text,
            other => panic!("expected a text frame, got {other:?}"),
        }
    }

    /// Asserts nothing arrives (pings aside) for a little while.
    pub async fn assert_silent(&mut self) {
        let result = tokio::time::timeout(Duration::from_millis(200), self.next()).await;
        assert!(result.is_err(), "expected no frame, got {result:?}");
    }
}

#[derive(Debug, PartialEq)]
pub enum Frame {
    Text(String),
    Close(Option<(u16, String)>),
    Error(String),
    End,
}
