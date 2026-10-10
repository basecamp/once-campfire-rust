use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use p256::SecretKey;
use p256::ecdsa::signature::Verifier;
use p256::ecdsa::{Signature, VerifyingKey};
use p256::elliptic_curve::sec1::ToEncodedPoint;

use super::*;
use crate::integrations::test_support::{FakeResolver, FakeServer, MappingDialer, Route, TestDb, network};

/// `DnsTestHelper::WEB_PUSH_PUBLIC_TEST_IP`
const PUBLIC_IP: &str = "142.250.185.206";
/// parity/.env.reference
const VAPID_PUBLIC_KEY: &str = "BEYXTBB5_jNhNzXDmx5KEU55Vbbd-u--Lk9rM5OFQvUkPIBwZJ9QzAq0zdEzFw6yTV8cTriz_qYBVicY02_VxTQ=";
const VAPID_PRIVATE_KEY: &str = "qfXLHghuG1rSHZUVo9SscNRI-0EIHRbIrfeGCqbAwak=";
/// `WebPush::Notification#vapid_identification`, which the gem's vectors were made with.
const REFERENCE_SUBJECT: &str = "mailto:support@37signals.com";

fn expected() -> serde_json::Value {
    serde_json::from_str(include_str!("../testdata/web_push_expected.json")).unwrap()
}

fn vapid() -> VapidConfig {
    VapidConfig::new(REFERENCE_SUBJECT, VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY).unwrap()
}

struct Receiver {
    key: SecretKey,
    auth: [u8; 16],
}

impl Receiver {
    fn new() -> Self {
        Self { key: SecretKey::random(&mut p256::elliptic_curve::rand_core::OsRng), auth: rand::random() }
    }

    fn subscription(&self, id: i64, endpoint: &str) -> PushSubscription {
        let p256dh = encode64_nopad(self.key.public_key().to_encoded_point(false).as_bytes());
        PushSubscription::new(1, Some(endpoint), Some(&p256dh), Some(&encode64_nopad(&self.auth)), None).with_id(id)
    }

    fn open(&self, body: &[u8]) -> String {
        let (_, plaintext) = encryption::decrypt(body, &self.key, &self.auth).expect("decrypts");
        String::from_utf8(plaintext.strip_suffix(b"\x02\x00").expect("gem padding").to_vec()).unwrap()
    }
}

trait WithId {
    fn with_id(self, id: i64) -> Self;
}

impl WithId for PushSubscription {
    fn with_id(mut self, id: i64) -> Self {
        self.id = id;
        self
    }
}

struct PushService {
    server: FakeServer,
    resolver: Arc<FakeResolver>,
    dialer: Arc<MappingDialer>,
    net: Network,
}

async fn push_service(status: u16, reason: &str) -> PushService {
    let mut route = Route::new("POST", "*", "", status);
    route.reason = reason.into();
    let routes = ["/fcm/send/abc", "/fcm/send/123", "/fcm/send/456", "/fcm/send/567", "/fcm/send/789"]
        .iter()
        .map(|path| Route { path: path.to_string(), ..route.clone() })
        .collect();
    let server = FakeServer::start_tls(routes).await;
    let resolver = Arc::new(FakeResolver::new([("fcm.googleapis.com", vec![PUBLIC_IP])]));
    let dialer =
        Arc::new(MappingDialer { public: HashSet::from([PUBLIC_IP.parse().unwrap()]), to: server.addr, dialed: Mutex::new(Vec::new()) });
    let net = network(resolver.clone(), dialer.clone());
    PushService { server, resolver, dialer, net }
}

fn notification(subscription: PushSubscription) -> Notification {
    Notification { title: "t".into(), body: "b".into(), path: "/".into(), badge: 0, subscription }
}

#[test]
fn encodes_the_message_like_json_generate() {
    let expected = expected();
    let notification = Notification {
        title: "Designers <&> \"quotes\" é 😀".into(),
        body: "Kevin: line\nbreak\ttab \u{2028} \u{1f} / \\ ".into(),
        path: "/rooms/1".into(),
        badge: 3,
        subscription: PushSubscription::new(1, None, None, None, None),
    };
    assert_eq!(notification.encoded_message(), expected["message"].as_str().unwrap());
}

#[test]
fn the_longest_payload_fits_a_push_message() {
    use campfire_db::{MAX_PAYLOAD_BODY_BYTES, MAX_PAYLOAD_TITLE_BYTES};
    let receiver = Receiver::new();
    let notification = Notification {
        title: "\"".repeat(MAX_PAYLOAD_TITLE_BYTES / 2),
        body: "\u{1}".repeat(MAX_PAYLOAD_BODY_BYTES / 6),
        path: format!("/rooms/{}", i64::MIN),
        badge: i64::MIN,
        subscription: receiver.subscription(1, "https://fcm.googleapis.com/fcm/send/abc"),
    };
    let subscription = &notification.subscription;
    let body = encryption::encrypt(
        notification.encoded_message().as_bytes(),
        subscription.p256dh_key.as_deref(),
        subscription.auth_key.as_deref(),
    );
    assert_eq!(receiver.open(&body.unwrap()), notification.encoded_message());
}

#[test]
fn signs_the_vapid_header_like_the_gem() {
    let expected = expected();
    let now = expected["now"].as_i64().unwrap();
    let authorization = vapid().authorization("https://fcm.googleapis.com", now);
    let (t, k) = authorization.strip_prefix("vapid t=").unwrap().split_once(",k=").unwrap();
    assert_eq!(k, expected["authorization_k"].as_str().unwrap());
    let segments: Vec<&str> = t.split('.').collect();
    assert_eq!(segments[0], expected["jwt_header_segment"].as_str().unwrap());
    assert_eq!(segments[1], expected["jwt_payload_segment"].as_str().unwrap());

    let key = VerifyingKey::from_sec1_bytes(&decode64(VAPID_PUBLIC_KEY).unwrap()).unwrap();
    let signature = Signature::from_slice(&decode64(segments[2]).unwrap()).unwrap();
    key.verify(format!("{}.{}", segments[0], segments[1]).as_bytes(), &signature).unwrap();
}

#[test]
fn signs_with_the_configured_subject() {
    let vapid = VapidConfig::new("mailto:ops@example.com", VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY).unwrap();
    let authorization = vapid.authorization("https://fcm.googleapis.com", 0);
    let jwt = authorization.strip_prefix("vapid t=").unwrap().split(',').next().unwrap();
    let claims: serde_json::Value = serde_json::from_slice(&decode64(jwt.split('.').nth(1).unwrap()).unwrap()).unwrap();
    assert_eq!(claims["sub"], "mailto:ops@example.com");
}

#[test]
fn rejects_bad_vapid_keys_up_front() {
    let other_public_key = encode64_nopad(Receiver::new().key.public_key().to_encoded_point(false).as_bytes());
    let keys = |public_key: &str, private_key: &str| VapidConfig::new(REFERENCE_SUBJECT, public_key, private_key).map(|_| ());
    assert_eq!(keys("", VAPID_PRIVATE_KEY), Err(VapidError::InvalidPublicKey));
    assert_eq!(keys("dGVzdF9rZXk", VAPID_PRIVATE_KEY), Err(VapidError::InvalidPublicKey));
    assert_eq!(keys(VAPID_PUBLIC_KEY, "not base64!"), Err(VapidError::InvalidPrivateKey));
    assert_eq!(keys(VAPID_PUBLIC_KEY, &encode64_nopad(&[0xff; 32])), Err(VapidError::InvalidPrivateKey));
    assert_eq!(keys(&other_public_key, VAPID_PRIVATE_KEY), Err(VapidError::Mismatched));
    assert_eq!(keys(VAPID_PUBLIC_KEY.trim_end_matches('='), VAPID_PRIVATE_KEY.trim_end_matches('=')), Ok(()));
}

fn config(public_key: Option<&str>, private_key: Option<&str>) -> crate::config::Config {
    crate::config::Config {
        vapid_public_key: public_key.map(Into::into),
        vapid_private_key: private_key.map(Into::into),
        vapid_subject: REFERENCE_SUBJECT.into(),
        ..crate::config::Config::from_lookup(|name| (name == "SECRET_KEY_BASE_DUMMY").then(|| "1".into())).unwrap()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn web_push_is_off_without_a_valid_key_pair() {
    let t = tokio::task::spawn_blocking(TestDb::new).await.unwrap();
    let pool = |public_key, private_key| {
        crate::integrations::web_push_pool(&config(public_key, private_key), &t.db, Arc::new(campfire_jobs::Backlog::new(10_000)))
    };
    assert!(pool(None, Some(VAPID_PRIVATE_KEY)).is_none());
    assert!(pool(Some("dGVzdF9rZXk"), Some(VAPID_PRIVATE_KEY)).is_none());
    assert!(pool(Some(VAPID_PUBLIC_KEY), Some(VAPID_PRIVATE_KEY)).is_some());
    assert_eq!(VapidConfig::from_config(&config(Some(VAPID_PUBLIC_KEY), None)).map(|_| ()), Err(VapidError::Missing));
}

#[tokio::test]
async fn delivers_to_the_pinned_address_with_the_gems_headers() {
    let service = push_service(201, "Created").await;
    let receiver = Receiver::new();
    let notification = notification(receiver.subscription(1, "https://fcm.googleapis.com/fcm/send/abc"));

    assert_eq!(notification.deliver(&service.net, &vapid()).await.unwrap(), Some(201));

    assert_eq!(service.resolver.lookups(), ["fcm.googleapis.com"]);
    assert_eq!(*service.dialer.dialed.lock().unwrap(), [format!("{PUBLIC_IP}:443").parse().unwrap()]);
    let received = service.server.received();
    assert_eq!(received.len(), 1);
    let request = &received[0];
    assert_eq!((request.method.as_str(), request.target.as_str()), ("POST", "/fcm/send/abc"));

    let expected = expected();
    let mut expected_headers: Vec<(String, String)> = serde_json::from_value(expected["headers"].clone()).unwrap();
    for (name, value) in expected_headers.iter_mut() {
        if name == "Content-Length" {
            *value = request.body.len().to_string();
        }
    }
    let names: Vec<&str> = request.headers.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(
        names,
        [
            "Content-Type", "Ttl", "Urgency", "Content-Encoding", "Content-Length", "Authorization", "Accept-Encoding", "Accept",
            "User-Agent", "Connection", "Host"
        ]
    );
    for (name, value) in &expected_headers {
        assert_eq!(request.header(name), Some(value.as_str()), "{name}");
    }
    assert_eq!(request.header("Host"), Some("fcm.googleapis.com"));
    assert_eq!(request.header("User-Agent"), Some("Ruby"));
    assert!(
        request.header("Authorization").unwrap().starts_with(&format!("vapid t={}.", expected["jwt_header_segment"].as_str().unwrap()))
    );
    assert_eq!(receiver.open(&request.body), notification.encoded_message());
}

#[tokio::test]
async fn skips_endpoints_it_may_not_deliver_to() {
    let service = push_service(201, "Created").await;
    service.resolver.set("updates.push.services.mozilla.com", vec![vec!["10.0.0.5".parse().unwrap()]]);
    let receiver = Receiver::new();
    for endpoint in [
        "https://updates.push.services.mozilla.com/wpush/v2/x", // resolves privately
        "https://web.push.apple.com/QaBC123",                   // doesn't resolve
        "https://attacker.example.com/collect",
        "https://fcm.googleapis.com:22/fcm/send/abc",
        "http://fcm.googleapis.com/fcm/send/abc",
        "https://evilfcm.googleapis.com.attacker.example/webhook",
    ] {
        let delivered = notification(receiver.subscription(1, endpoint)).deliver(&service.net, &vapid()).await.unwrap();
        assert_eq!(delivered, None, "{endpoint}");
    }
    assert!(service.server.received().is_empty());
    assert_eq!(service.resolver.lookups(), ["updates.push.services.mozilla.com", "web.push.apple.com"]);
}

#[tokio::test]
async fn raises_what_the_gem_raises() {
    let receiver = Receiver::new();
    let subscription = receiver.subscription(1, "https://fcm.googleapis.com/fcm/send/abc");
    for (status, reason, kind, invalidates) in [
        (410, "Gone", "WebPush::ExpiredSubscription", true),
        (404, "Not Found", "WebPush::InvalidSubscription", true),
        (403, "Forbidden", "WebPush::Unauthorized", false),
        (400, "UnauthorizedRegistration", "WebPush::Unauthorized", false),
        (400, "Bad Request", "WebPush::ResponseError", false),
        (413, "Payload Too Large", "WebPush::PayloadTooLarge", false),
        (429, "Too Many Requests", "WebPush::TooManyRequests", false),
        (503, "Service Unavailable", "WebPush::PushServiceError", false),
        (302, "Found", "WebPush::ResponseError", false),
    ] {
        let service = push_service(status, reason).await;
        let error = notification(subscription.clone()).deliver(&service.net, &vapid()).await.unwrap_err();
        assert_eq!((error.class_name(), error.invalidates_subscription()), (kind, invalidates), "{status} {reason}");
    }
}

#[tokio::test]
async fn only_the_subscriptions_own_faults_invalidate_it() {
    // A key that isn't a point on the curve (as in the fixtures) can never be delivered to.
    let service = push_service(201, "Created").await;
    let bad_key =
        PushSubscription::new(1, Some("https://fcm.googleapis.com/fcm/send/abc"), Some("dGVzdF9rZXk"), Some("dGVzdF9hdXRo"), None);
    let error = notification(bad_key).deliver(&service.net, &vapid()).await.unwrap_err();
    assert_eq!((error.class_name(), error.invalidates_subscription()), ("OpenSSL::PKey::EC::Point::Error", true));

    // A certificate that doesn't verify may be our fault (an empty CA store, a skewed clock).
    let untrusted = Network { tls: crate::integrations::net::tls_config(rustls::RootCertStore::empty()), ..service.net.clone() };
    let receiver = Receiver::new();
    let error =
        notification(receiver.subscription(1, "https://fcm.googleapis.com/fcm/send/abc")).deliver(&untrusted, &vapid()).await.unwrap_err();
    assert_eq!((error.class_name(), error.invalidates_subscription()), ("OpenSSL::SSL::SSLError", false));

    let blank = PushSubscription::new(1, Some("https://fcm.googleapis.com/fcm/send/abc"), Some(""), Some("dGVzdF9hdXRo"), None);
    let error = notification(blank).deliver(&service.net, &vapid()).await.unwrap_err();
    assert_eq!((error.class_name(), error.invalidates_subscription()), ("ArgumentError", false));
}

#[tokio::test]
async fn test_notification() {
    let service = push_service(201, "Created").await;
    let receiver = Receiver::new();
    let subscription = receiver.subscription(1, "https://fcm.googleapis.com/fcm/send/abc");
    deliver_test_notification(&service.net, &vapid(), &subscription, 4, "http://example.com/users/me/push_subscriptions").await.unwrap();

    let message: serde_json::Value = serde_json::from_str(&receiver.open(&service.server.received()[0].body)).unwrap();
    assert_eq!(message["title"], "Campfire Test");
    assert!(uuid::Uuid::parse_str(message["options"]["body"].as_str().unwrap()).is_ok());
    assert_eq!(message["options"]["icon"], "/account/logo");
    assert_eq!(message["options"]["data"], serde_json::json!({ "path": "http://example.com/users/me/push_subscriptions", "badge": 4 }));

    let expired = push_service(410, "Gone").await;
    assert!(deliver_test_notification(&expired.net, &vapid(), &subscription, 4, "/").await.is_err());
}

/// `test/models/room/push_test.rb`: "deliver new message ..." and "destroys invalid subscriptions".
#[tokio::test(flavor = "multi_thread")]
async fn pushes_messages_and_destroys_expired_subscriptions() {
    use campfire_db::{Membership, Message, NewMessage};
    let t = Arc::new(tokio::task::spawn_blocking(TestDb::new).await.unwrap());
    let receivers: Vec<(i64, Receiver)> =
        ["david_chrome", "jason_chrome", "jz_chrome", "kevin_chrome"].iter().map(|label| (TestDb::id(label), Receiver::new())).collect();
    let keys: Vec<(i64, String, String)> = receivers
        .iter()
        .map(|(id, r)| (*id, encode64_nopad(r.key.public_key().to_encoded_point(false).as_bytes()), encode64_nopad(&r.auth)))
        .collect();
    t.db.write(move |tx| {
        for (id, p256dh, auth) in &keys {
            tx.conn().execute("UPDATE push_subscriptions SET p256dh_key = ?, auth_key = ? WHERE id = ?", (p256dh, auth, id))?;
        }
        Membership::find(tx.conn(), TestDb::id("kevin_designers"))?.update_involvement(tx, campfire_db::Involvement::Invisible)
    })
    .await
    .unwrap();

    let create = |t: Arc<TestDb>| async move {
        let attributes = NewMessage {
            room_id: TestDb::id("designers"),
            creator_id: TestDb::id("david"),
            client_message_id: Some("earth".into()),
            body: Some("Hey @kevin".into()),
            attachment_blob_id: None,
        };
        t.db.write(move |tx| Message::create(tx, attributes)).await.unwrap()
    };

    // Delivered: nothing is destroyed.
    let ok = push_service(201, "Created").await;
    let destroyed = Arc::new(Mutex::new(Vec::new()));
    let log = destroyed.clone();
    let pool = Pool::new(ok.net.clone(), vapid(), move |id| -> Result<(), String> {
        log.lock().unwrap().push(id);
        Ok(())
    });
    let message = create(t.clone()).await;
    let (queued_pool, now, rich_text) = (pool.clone(), t.db.env().now(), t.db.env().rich_text.clone());
    let payload = t.db.read(move |conn| push_message(&queued_pool, conn, &*rich_text, &message, now)).await.unwrap();
    assert_eq!(payload.title, "Designers");
    assert_eq!(payload.body, "David: Hey @kevin");
    pool.shutdown().await;
    let received = ok.server.received();
    assert_eq!(received.len(), 2);
    for request in &received {
        let (_, receiver) = receivers.iter().find(|(id, _)| request.target.ends_with(&expected_endpoint_suffix(&t, *id))).unwrap();
        let opened: serde_json::Value = serde_json::from_str(&receiver.open(&request.body)).unwrap();
        assert_eq!(opened["title"], "Designers");
        assert_eq!(opened["options"]["data"]["path"], payload.path);
    }
    assert!(destroyed.lock().unwrap().is_empty());

    // Expired: both subscriptions are destroyed by the handler, in the pool's worker thread.
    let gone = push_service(410, "Gone").await;
    let db = t.clone();
    let pool = Pool::new(gone.net.clone(), vapid(), move |id| {
        db.db.write_blocking(move |tx| match PushSubscription::find(tx.conn(), id) {
            Ok(subscription) => subscription.destroy(tx),
            Err(_) => Ok(()),
        })
    });
    let before = t.db.read(PushSubscription::count).await.unwrap();
    let message = create(t.clone()).await;
    let (queued_pool, now, rich_text) = (pool.clone(), t.db.env().now(), t.db.env().rich_text.clone());
    t.db.read(move |conn| push_message(&queued_pool, conn, &*rich_text, &message, now)).await.unwrap();
    pool.shutdown().await;
    assert_eq!(t.db.read(PushSubscription::count).await.unwrap(), before - 2);
}

fn expected_endpoint_suffix(_t: &TestDb, id: i64) -> String {
    let endpoints = [("david_chrome", "123"), ("jason_chrome", "567"), ("jz_chrome", "456"), ("kevin_chrome", "789")];
    let label = endpoints.iter().find(|(label, _)| TestDb::id(label) == id).map(|(_, e)| *e).unwrap();
    format!("/fcm/send/{label}")
}

#[tokio::test]
async fn the_pool_keeps_subscriptions_it_failed_to_reach() {
    let service = push_service(201, "Created").await;
    let untrusted = Network { tls: crate::integrations::net::tls_config(rustls::RootCertStore::empty()), ..service.net.clone() };
    let destroyed = Arc::new(Mutex::new(Vec::new()));
    let log = destroyed.clone();
    let pool = Pool::new(untrusted, vapid(), move |id| -> Result<(), String> {
        log.lock().unwrap().push(id);
        Ok(())
    });
    pool.deliver_later(notification(Receiver::new().subscription(1, "https://fcm.googleapis.com/fcm/send/abc")));
    pool.shutdown().await;
    assert!(destroyed.lock().unwrap().is_empty());
}

#[tokio::test]
async fn the_pool_keeps_deliveries_past_rails_queue_limit() {
    let service = push_service(201, "Created").await;
    let pool = Pool::new(service.net.clone(), vapid(), |_| -> Result<(), String> { Ok(()) });
    let receiver = Receiver::new();
    let subscription = receiver.subscription(1, "https://fcm.googleapis.com/fcm/send/abc");
    for _ in 0..(50 + 10_000 + 5) {
        pool.deliver_later(notification(subscription.clone()));
    }
    assert_eq!(pool.pending(), 10_055, "Rails drops the last five; the lane keeps them");
}

/// A delivery that panics still gives its place in the queue back, so panics can't fill it up.
#[tokio::test]
async fn a_panicking_delivery_frees_its_slot() {
    let service = push_service(201, "Created").await;
    let panicking = Network { resolver: Arc::new(PanickingResolver), ..service.net.clone() };
    let pool = Pool::new(panicking, vapid(), |_| -> Result<(), String> { Ok(()) });
    pool.deliver_later(notification(Receiver::new().subscription(1, "https://fcm.googleapis.com/fcm/send/abc")));
    pool.shutdown().await;
    assert_eq!(pool.pending(), 0);
}

struct PanickingResolver;

impl crate::integrations::net::Resolver for PanickingResolver {
    fn lookup<'a>(&'a self, host: &'a str) -> crate::integrations::net::BoxFuture<'a, std::io::Result<Vec<std::net::IpAddr>>> {
        panic!("resolving {host} panicked");
    }
}
