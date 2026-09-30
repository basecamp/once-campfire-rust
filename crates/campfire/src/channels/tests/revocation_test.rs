//! Revocation, for every channel that carries a room: subscribe, revoke (remove the membership,
//! deactivate or ban), attempt a delivery, reconnect and resubscribe.
use serde_json::json;

use super::support::*;
use crate::channels::room_gid;

const DISCONNECT_RECONNECT: &str = r#"{"type":"disconnect","reason":"remote","reconnect":true}"#;
const DISCONNECT_FOR_GOOD: &str = r#"{"type":"disconnect","reason":"remote","reconnect":false}"#;
const UNAUTHORIZED: &str = r#"{"type":"disconnect","reason":"unauthorized","reconnect":false}"#;

/// Each room-carrying channel's identifier for the designers room, and the broadcasting it
/// streams from.
async fn room_channels(app: &TestApp) -> Vec<(String, String)> {
    let designers = app.room("designers").await;
    let gid = room_gid(&designers).to_param();
    let messages = format!("{gid}:messages");
    let signed = app.signed_stream_name(&[&gid, "messages"]);
    vec![
        (room_identifier("RoomChannel", designers.id), format!("room:{gid}")),
        (room_identifier("PresenceChannel", designers.id), format!("presence:{gid}")),
        (room_identifier("TypingNotificationsChannel", designers.id), format!("typing_notifications:{gid}")),
        (identifier(json!({ "channel": "RoomMessagesChannel", "signed_stream_name": signed })), messages.clone()),
    ]
}

enum Revoke {
    Membership,
    Deactivate,
    Ban,
}

async fn revoke(app: &TestApp, how: Revoke) {
    let (room_id, kevin) = (id("designers"), id("kevin"));
    app.db
        .write(move |tx| match how {
            Revoke::Membership => campfire_db::Room::find(tx.conn(), room_id)?.revoke_from(tx, &[kevin]),
            Revoke::Deactivate => campfire_db::User::find(tx.conn(), kevin)?.deactivate(tx),
            Revoke::Ban => campfire_db::User::find(tx.conn(), kevin)?.ban(tx),
        })
        .await
        .unwrap();
}

async fn subscribed_kevin(app: &TestApp) -> (Client, String) {
    let cookie = app.cookie_for("kevin").await;
    let mut kevin = app.connect_with_cookie(Some(&cookie)).await;
    assert_eq!(kevin.next_text().await, r#"{"type":"welcome"}"#);
    for (channel, _) in room_channels(app).await {
        kevin.confirm(&channel).await;
    }
    (kevin, cookie)
}

async fn assert_no_deliveries(app: &TestApp) {
    for (_, broadcasting) in room_channels(app).await {
        eventually(|| async { app.server.broadcast(&broadcasting, &json!({ "after": "revocation" })) == 0 }).await;
    }
}

#[tokio::test]
async fn removing_the_membership_disconnects_and_resubscribing_is_rejected() {
    let app = start().await;
    let (mut kevin, cookie) = subscribed_kevin(&app).await;
    // PresenceChannel's `present` told kevin's reads stream; nothing else is pending.
    kevin.assert_silent().await;

    revoke(&app, Revoke::Membership).await;
    assert_eq!(kevin.until_closed().await, vec![DISCONNECT_RECONNECT.to_string()]);
    assert_no_deliveries(&app).await;

    // The client reconnects and replays its subscriptions.
    let mut kevin = app.connect_with_cookie(Some(&cookie)).await;
    assert_eq!(kevin.next_text().await, r#"{"type":"welcome"}"#);
    for (channel, _) in room_channels(&app).await {
        kevin.reject(&channel).await;
    }
    // The stock channel doesn't serve the stream either.
    let designers = app.room("designers").await;
    let stock = identifier(json!({
        "channel": "Turbo::StreamsChannel",
        "signed_stream_name": app.signed_stream_name(&[&room_gid(&designers).to_param(), "messages"]),
    }));
    kevin.reject(&stock).await;
    // Rooms kevin is still in keep working.
    kevin.confirm(&room_identifier("RoomChannel", id("bender_and_kevin"))).await;
}

#[tokio::test]
async fn deactivating_disconnects_for_good() {
    let app = start().await;
    let (mut kevin, cookie) = subscribed_kevin(&app).await;

    revoke(&app, Revoke::Deactivate).await;
    assert_eq!(kevin.until_closed().await, vec![DISCONNECT_FOR_GOOD.to_string()]);
    assert_no_deliveries(&app).await;

    let mut kevin = app.connect_with_cookie(Some(&cookie)).await;
    assert_eq!(kevin.until_closed().await, vec![UNAUTHORIZED.to_string()]);
}

#[tokio::test]
async fn banning_disconnects_for_good() {
    let app = start().await;
    let (mut kevin, cookie) = subscribed_kevin(&app).await;

    revoke(&app, Revoke::Ban).await;
    assert_eq!(kevin.until_closed().await, vec![DISCONNECT_FOR_GOOD.to_string()]);
    assert_no_deliveries(&app).await;

    let mut kevin = app.connect_with_cookie(Some(&cookie)).await;
    assert_eq!(kevin.until_closed().await, vec![UNAUTHORIZED.to_string()]);
}

#[tokio::test]
async fn only_the_revoked_users_connections_are_closed() {
    let app = start().await;
    let (mut kevin, _) = subscribed_kevin(&app).await;
    let mut jz = app.connect("jz").await;
    let room = room_identifier("RoomChannel", id("designers"));
    jz.confirm(&room).await;

    revoke(&app, Revoke::Membership).await;
    kevin.until_closed().await;

    let designers = app.room("designers").await;
    app.server.broadcast(&format!("room:{}", room_gid(&designers).to_param()), &json!({ "still": "here" }));
    assert_eq!(jz.next_text().await, delivery(&room, r#"{"still":"here"}"#));
}

#[tokio::test]
async fn inactive_users_cannot_connect_with_a_surviving_session() {
    let app = start().await;
    let cookie = app.cookie_for("kevin").await;
    for status in [campfire_db::Status::Banned, campfire_db::Status::Deactivated] {
        app.db
            .write(move |tx| {
                tx.conn().execute("UPDATE users SET status = ? WHERE id = ?", rusqlite::params![status, id("kevin")])?;
                Ok(())
            })
            .await
            .unwrap();
        let mut client = app.connect_with_cookie(Some(&cookie)).await;
        assert_eq!(client.until_closed().await, vec![UNAUTHORIZED.to_string()]);
    }
}
