//! Frame sequences recorded from the reference app's `/cable`, replayed against this server.
//!
//! `tests/golden/reference.json` holds the fixture values the reference run used (user, rooms,
//! signed stream names) and the frames it answered each step with. It was recorded from the
//! Docker reference (Thruster, Puma, Redis adapter). To re-record:
//!
//!   parity/bin/reference up --seed default --port 3141
//!   parity/bin/reference runner --port 3141 crates/cable/tests/golden/fixtures.rb > target/cable-fixtures.json
//!   CABLE_REFERENCE_URL=ws://127.0.0.1:3141/cable CABLE_REFERENCE_FIXTURES=$PWD/target/cable-fixtures.json \
//!     cargo test -p campfire_cable --test golden -- --ignored record_reference
//!
//! (The fixtures script starts a fresh session each time, because the script signs it out.)
//!
//! The replay builds Campfire's channels over the same fixture values, so every frame must match
//! byte for byte; only ping timestamps are normalized.
use std::collections::BTreeMap;
use std::time::Duration;

use campfire_cable::turbo::StreamsChannel;
use campfire_cable::{Authenticate, Channel, ChannelResult, Config, ConnectRequest, EmptyChannel, Identified, Params, Server, Subscription};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

const GOLDEN: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/golden/reference.json");

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Recording {
    tokens: BTreeMap<String, String>,
    sessions: BTreeMap<String, Vec<Exchange>>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Exchange {
    step: String,
    frames: Vec<String>,
}

/// One scripted step: what the client does, then what it collects.
enum Step {
    Connect { cookie: bool, origin_ok: bool },
    HttpGet,
    Send(String),
    AwaitPing,
    /// Signing out (`reset_remote_connections`), or `Server::disconnect` on the replay.
    RemoteDisconnect,
}

fn identifier(value: Value) -> String {
    value.to_string()
}

fn subscribe(identifier: &str) -> String {
    json!({ "command": "subscribe", "identifier": identifier }).to_string()
}

fn unsubscribe(identifier: &str) -> String {
    json!({ "command": "unsubscribe", "identifier": identifier }).to_string()
}

fn perform(identifier: &str, data: Value) -> String {
    json!({ "command": "message", "identifier": identifier, "data": data.to_string() }).to_string()
}

fn script(tokens: &BTreeMap<String, String>) -> Vec<(&'static str, Vec<(String, Step)>)> {
    let t = |name: &str| tokens[name].clone();
    let room_id: u64 = t("ROOM_ID").parse().unwrap();
    let closed_room_id: u64 = t("CLOSED_ROOM_ID").parse().unwrap();

    let heartbeat = identifier(json!({ "channel": "HeartbeatChannel" }));
    let room = identifier(json!({ "channel": "RoomChannel", "room_id": room_id }));
    let closed_room = identifier(json!({ "channel": "RoomChannel", "room_id": closed_room_id }));
    let typing = identifier(json!({ "channel": "TypingNotificationsChannel", "room_id": room_id }));
    let turbo = |signed: Value| identifier(json!({ "channel": "Turbo::StreamsChannel", "signed_stream_name": signed }));

    let step = |name: &str, step: Step| (name.to_string(), step);
    vec![
        ("authenticated", vec![
            step("connect", Step::Connect { cookie: true, origin_ok: true }),
            step("subscribe heartbeat", Step::Send(subscribe(&heartbeat))),
            step("subscribe heartbeat again", Step::Send(subscribe(&heartbeat))),
            step("subscribe member room", Step::Send(subscribe(&room))),
            step("subscribe non-member room", Step::Send(subscribe(&closed_room))),
            step("subscribe unknown channel", Step::Send(subscribe(&identifier(json!({ "channel": "NopeChannel" }))))),
            step("subscribe base channel", Step::Send(subscribe(&identifier(json!({ "channel": "ApplicationCable::Channel" }))))),
            step("subscribe turbo rooms", Step::Send(subscribe(&turbo(json!(t("ROOMS_SIGNED")))))),
            step("subscribe turbo forged", Step::Send(subscribe(&turbo(json!("InJvb21zIg==--0000"))))),
            step("subscribe turbo unsigned", Step::Send(subscribe(&identifier(json!({ "channel": "Turbo::StreamsChannel" }))))),
            step("subscribe turbo guarded room messages", Step::Send(subscribe(&turbo(json!(t("ROOM_MESSAGES_SIGNED")))))),
            step("subscribe typing", Step::Send(subscribe(&typing))),
            step("perform typing start", Step::Send(perform(&typing, json!({ "action": "start" })))),
            step("perform unknown action", Step::Send(perform(&typing, json!({ "action": "dance" })))),
            step("perform default receive", Step::Send(perform(&typing, json!({ "text": "hi" })))),
            step("unsubscribe typing", Step::Send(unsubscribe(&typing))),
            step("perform after unsubscribe", Step::Send(perform(&typing, json!({ "action": "start" })))),
            step("unknown command", Step::Send(json!({ "command": "dance" }).to_string())),
            step("invalid json", Step::Send("not json".to_string())),
            step("ping", Step::AwaitPing),
        ]),
        ("remote disconnect", vec![
            step("connect", Step::Connect { cookie: true, origin_ok: true }),
            step("subscribe member room", Step::Send(subscribe(&room))),
            step("sign out", Step::RemoteDisconnect),
        ]),
        ("unauthenticated", vec![step("connect", Step::Connect { cookie: false, origin_ok: true })]),
        ("cross origin", vec![step("connect", Step::Connect { cookie: true, origin_ok: false })]),
        ("plain http", vec![step("get", Step::HttpGet)]),
    ]
}

struct Target {
    url: String,
    origin: String,
    cookie: String,
    /// Present on the replay; the reference is disconnected by signing out over HTTP.
    server: Option<Server<User>>,
}

async fn run_script(target: &Target, tokens: &BTreeMap<String, String>) -> BTreeMap<String, Vec<Exchange>> {
    let mut sessions = BTreeMap::new();
    for (session, steps) in script(tokens) {
        let mut socket = None;
        let mut exchanges = Vec::new();
        for (name, step) in steps {
            let frames = match step {
                Step::Connect { cookie, origin_ok } => {
                    let mut request = target.url.as_str().into_client_request().unwrap();
                    let headers = request.headers_mut();
                    let origin = if origin_ok { target.origin.clone() } else { "http://evil.example".to_string() };
                    headers.insert("origin", origin.parse().unwrap());
                    headers.insert("sec-websocket-protocol", "actioncable-v1-json, actioncable-unsupported".parse().unwrap());
                    if cookie {
                        headers.insert("cookie", target.cookie.parse().unwrap());
                    }
                    match tokio_tungstenite::connect_async(request).await {
                        Ok((ws, response)) => {
                            let protocol = response.headers().get("sec-websocket-protocol").map(|v| v.to_str().unwrap().to_string());
                            let mut frames = vec![format!("upgrade {} protocol={}", response.status().as_u16(), protocol.unwrap_or_default())];
                            socket = Some(ws);
                            frames.extend(collect(socket.as_mut().unwrap(), false).await);
                            frames
                        }
                        Err(tokio_tungstenite::tungstenite::Error::Http(response)) => {
                            let body = String::from_utf8_lossy(response.body().as_deref().unwrap_or_default()).to_string();
                            vec![format!("http {} {}", response.status().as_u16(), body)]
                        }
                        Err(error) => vec![format!("error {error}")],
                    }
                }
                Step::HttpGet => vec![http_get(&target.url.replace("ws://", "http://")).await],
                Step::Send(text) => {
                    let ws = socket.as_mut().unwrap();
                    ws.send(Message::Text(text.into())).await.unwrap();
                    collect(ws, false).await
                }
                Step::AwaitPing => collect(socket.as_mut().unwrap(), true).await,
                Step::RemoteDisconnect => {
                    match &target.server {
                        Some(server) => {
                            server.disconnect(&format!("gid://campfire/User/{}", tokens["USER_ID"]), true);
                        }
                        None => sign_out(target, &tokens["ROOM_ID"]).await,
                    }
                    collect(socket.as_mut().unwrap(), false).await
                }
            };
            exchanges.push(Exchange { step: name, frames });
        }
        sessions.insert(session.to_string(), exchanges);
    }
    sessions
}

/// Frames until the socket has been quiet for a moment (or, when awaiting a ping, the first
/// ping). Pings are dropped otherwise, since when they land is timing, not protocol.
async fn collect(ws: &mut tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>, await_ping: bool) -> Vec<String> {
    let mut frames = Vec::new();
    let quiet = if await_ping { Duration::from_secs(4) } else { Duration::from_millis(400) };
    while let Ok(message) = tokio::time::timeout(quiet, ws.next()).await {
        match message {
            Some(Ok(Message::Text(text))) => {
                let text = text.to_string();
                if text.starts_with(r#"{"type":"ping""#) {
                    if await_ping {
                        frames.push(normalize_ping(&text));
                        break;
                    }
                    continue;
                }
                frames.push(text);
            }
            Some(Ok(Message::Close(frame))) => {
                frames.push(format!("close {:?}", frame.map(|f| (u16::from(f.code), f.reason.to_string()))));
            }
            Some(Ok(_)) => {}
            Some(Err(_)) | None => {
                frames.push("end".to_string());
                break;
            }
        }
    }
    frames
}

fn normalize_ping(text: &str) -> String {
    let ping: Value = serde_json::from_str(text).unwrap();
    assert!(ping["message"].is_i64(), "ping carries an integer timestamp: {text}");
    text.replace(&ping["message"].to_string(), "<unix>")
}

async fn http_get(url: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let (host, path) = url.strip_prefix("http://").unwrap().split_once('/').unwrap();
    let mut stream = tokio::net::TcpStream::connect(host).await.unwrap();
    stream.write_all(format!("GET /{path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    let (head, body) = response.split_once("\r\n\r\n").unwrap();
    let status = head.split(' ').nth(1).unwrap();
    let content_type = head.lines().find_map(|l| l.to_ascii_lowercase().strip_prefix("content-type: ").map(str::to_string));
    // Rails sends a content-length body; axum may chunk it.
    let body = if head.to_ascii_lowercase().contains("transfer-encoding: chunked") { dechunk(body) } else { body.to_string() };
    format!("http {status} {} {body}", content_type.unwrap_or_default())
}

/// `DELETE /session` with a CSRF token from a room page, like the sign-out button.
async fn sign_out(target: &Target, room_id: &str) {
    let base = target.origin.clone();
    let host = base.strip_prefix("http://").unwrap().to_string();
    let page = raw_http(&host, &format!("GET /rooms/{room_id} HTTP/1.1\r\nHost: {host}\r\nCookie: {}\r\nConnection: close\r\n\r\n", target.cookie)).await;
    let token = page.split(r#"<meta name="csrf-token" content=""#).nth(1).unwrap().split('"').next().unwrap().to_string();
    let session_cookie = page
        .lines()
        .find_map(|l| l.to_ascii_lowercase().starts_with("set-cookie: _campfire_session=").then(|| l["set-cookie: _campfire_session=".len()..].to_string()))
        .map(|v| v.split(';').next().unwrap().to_string())
        .unwrap();
    let body = format!("_method=delete&authenticity_token={}", token.replace('+', "%2B").replace('/', "%2F").replace('=', "%3D"));
    let response = raw_http(&host, &format!(
        "POST /session HTTP/1.1\r\nHost: {host}\r\nOrigin: {base}\r\nCookie: {}; _campfire_session={session_cookie}\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        target.cookie, body.len()
    )).await;
    assert!(response.starts_with("HTTP/1.1 302") || response.starts_with("HTTP/1.1 303"), "sign out failed: {}", &response[..200.min(response.len())]);
}

async fn raw_http(host: &str, request: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(host).await.unwrap();
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    String::from_utf8_lossy(&response).to_string()
}

fn dechunk(body: &str) -> String {
    let mut out = String::new();
    let mut rest = body;
    while let Some((size, after)) = rest.split_once("\r\n") {
        let size = usize::from_str_radix(size.trim(), 16).unwrap_or(0);
        if size == 0 {
            break;
        }
        out.push_str(&after[..size]);
        rest = &after[size + 2..];
    }
    out
}

#[tokio::test]
#[ignore = "needs a running reference app; see the module docs"]
async fn record_reference() {
    let url = std::env::var("CABLE_REFERENCE_URL").expect("CABLE_REFERENCE_URL");
    let fixtures: Value =
        serde_json::from_str(&std::fs::read_to_string(std::env::var("CABLE_REFERENCE_FIXTURES").expect("CABLE_REFERENCE_FIXTURES")).unwrap()).unwrap();
    let tokens: BTreeMap<String, String> = serde_json::from_value(fixtures["tokens"].clone()).unwrap();
    let origin = url.replace("ws://", "http://").trim_end_matches("/cable").to_string();
    let target = Target { url, origin, cookie: fixtures["cookie"].as_str().unwrap().to_string(), server: None };
    let sessions = run_script(&target, &tokens).await;
    let recording = Recording { tokens, sessions };
    std::fs::write(GOLDEN, serde_json::to_string_pretty(&recording).unwrap() + "\n").unwrap();
}

#[tokio::test]
async fn replays_reference_frames() {
    let golden: Recording = serde_json::from_str(&std::fs::read_to_string(GOLDEN).unwrap()).unwrap();
    let target = start_campfire_like_server(&golden.tokens).await;
    let sessions = run_script(&target, &golden.tokens).await;
    for (session, expected) in &golden.sessions {
        for (expected, actual) in expected.iter().zip(&sessions[session]) {
            assert_eq!(actual, expected, "session {session:?}, step {:?}", expected.step);
        }
        assert_eq!(sessions[session].len(), expected.len());
    }
}

// Campfire's channels (reference/app/channels), over the fixture values the reference used.

struct User {
    id: u64,
    name: String,
    room_ids: Vec<u64>,
}

impl Identified for User {
    fn connection_identifier(&self) -> String {
        format!("gid://campfire/User/{}", self.id)
    }
}

struct FixtureAuth {
    cookie: String,
    tokens: BTreeMap<String, String>,
}

impl Authenticate<User> for FixtureAuth {
    async fn connect(&self, request: &ConnectRequest) -> Option<User> {
        (request.headers.get("cookie")?.to_str().ok()? == self.cookie).then(|| User {
            id: self.tokens["USER_ID"].parse().unwrap(),
            name: self.tokens["USER_NAME"].clone(),
            room_ids: vec![self.tokens["ROOM_ID"].parse().unwrap()],
        })
    }
}

/// `RoomChannel#subscribed`: `stream_for` a room the user is a member of, else reject.
fn subscribe_to_room(sub: &mut Subscription<User>) -> Option<u64> {
    let room_id = sub.param("room_id").as_ref().and_then(Value::as_u64).filter(|id| sub.current_user().room_ids.contains(id));
    match room_id {
        Some(id) => sub.stream_for(&[&format!("room-{id}")]),
        None => sub.reject(),
    }
    room_id
}

struct RoomChannel;

#[async_trait::async_trait]
impl Channel<User> for RoomChannel {
    async fn subscribed(&mut self, sub: &mut Subscription<User>) -> ChannelResult {
        subscribe_to_room(sub);
        Ok(())
    }
}

#[derive(Default)]
struct TypingNotificationsChannel {
    room: Option<String>,
}

#[async_trait::async_trait]
impl Channel<User> for TypingNotificationsChannel {
    async fn subscribed(&mut self, sub: &mut Subscription<User>) -> ChannelResult {
        self.room = subscribe_to_room(sub).map(|id| format!("room-{id}"));
        Ok(())
    }

    async fn perform(&mut self, action: &str, _data: &Params, sub: &mut Subscription<User>) -> ChannelResult<bool> {
        if action != "start" && action != "stop" {
            return Ok(false);
        }
        let user = sub.current_user();
        let message = json!({ "action": action, "user": { "id": user.id, "name": user.name } });
        sub.broadcast_to(&[self.room.as_deref().unwrap()], &message);
        Ok(true)
    }
}

fn room_gid_param(id: &str) -> String {
    campfire_cable::naming::gid_param(&rails_compat::global_id::GlobalId {
        app: "campfire".into(),
        model_name: "Rooms::Open".into(),
        id: id.into(),
    })
}

async fn start_campfire_like_server(tokens: &BTreeMap<String, String>) -> Target {
    let cookie = "session_token=replay".to_string();
    let signed: BTreeMap<String, String> = [
        (tokens["ROOMS_SIGNED"].clone(), "rooms".to_string()),
        (tokens["ROOM_MESSAGES_SIGNED"].clone(), format!("{}:messages", room_gid_param(&tokens["ROOM_ID"]))),
    ]
    .into();
    let turbo = StreamsChannel::with_verifier(move |name| signed.get(name).cloned())
        .guarded_by(|name| name.split_once(':').map(|(_, suffix)| suffix) == Some("messages"));

    let server = Server::builder(Config { assume_ssl: false, ..Config::default() }, FixtureAuth { cookie: cookie.clone(), tokens: tokens.clone() })
        .channel("ApplicationCable::Channel", || EmptyChannel)
        .channel("HeartbeatChannel", || EmptyChannel)
        .channel("RoomChannel", || RoomChannel)
        .channel("TypingNotificationsChannel", TypingNotificationsChannel::default)
        .channel("Turbo::StreamsChannel", move || turbo.clone())
        .build();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = server.router::<()>("/cable");
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    Target { url: format!("ws://{addr}/cable"), origin: format!("http://{addr}"), cookie, server: Some(server) }
}
