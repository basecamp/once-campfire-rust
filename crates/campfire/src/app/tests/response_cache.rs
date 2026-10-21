use super::*;
use std::io::Read;
use std::sync::atomic::Ordering;

async fn fixture() -> Option<(Test, String, i64, i64)> {
    let test = boot_seeded().await?;
    let session = &vectors().sessions[0];
    let name = session.user_name.clone();
    let (user, room) = test.booted.app.db.read(move |conn| {
        Ok(conn.query_row(
            "SELECT users.id, rooms.id FROM users JOIN memberships ON memberships.user_id = users.id JOIN rooms ON rooms.id = memberships.room_id WHERE users.name = ? AND rooms.type = 'Rooms::Closed' AND EXISTS (SELECT 1 FROM messages WHERE messages.room_id = rooms.id) ORDER BY rooms.id LIMIT 1",
            [name], |row| Ok((row.get(0)?, row.get(1)?)),
        )?)
    }).await.unwrap();
    Some((test, session.cookie_header.clone(), user, room))
}

fn hits(test: &Test) -> usize {
    test.booted.app.response_cache.hits.load(Ordering::Relaxed)
}

async fn warm(test: &Test, path: &str, cookie: &str) -> Reply {
    let mut reply = send(&test.booted.router, get_with_cookie(path, cookie)).await;
    // The first seeded session authentication writes its hourly activity refresh and must not
    // admit that pre-authentication generation. Subsequent requests can populate then hit.
    for _ in 0..3 {
        assert_eq!(reply.status, StatusCode::OK, "{}", reply.text());
        reply = send(&test.booted.router, get_with_cookie(path, cookie)).await;
    }
    reply
}

#[tokio::test]
async fn room_messages_sidebar_and_search_cache_complete_responses() {
    let Some((test, cookie, _user, room)) = fixture().await else { return };
    for path in [format!("/rooms/{room}"), format!("/rooms/{room}/messages"), "/users/me/sidebar".into(), "/searches?q=hello".into()] {
        let first = warm(&test, &path, &cookie).await;
        let before = hits(&test);
        let early = test.booted.app.response_cache.early_hits.load(Ordering::Relaxed);
        let hit = send(&test.booted.router, get_with_cookie(&path, &cookie)).await;
        assert_eq!(hits(&test), before + 1, "{path}");
        assert_eq!(test.booted.app.response_cache.early_hits.load(Ordering::Relaxed), early + 1, "{path}");
        assert_eq!(hit.status, first.status);
        assert_eq!(hit.body, first.body);
        assert_eq!(hit.header("etag"), first.header("etag"));
        assert_ne!(hit.header("x-request-id"), first.header("x-request-id"));
        assert_eq!(hit.header("content-length"), Some(hit.body.len().to_string().as_str()));
    }
}

#[tokio::test]
async fn expiring_credentials_never_enter_the_early_path() {
    use campfire_kit::cookies::{Cookie, CookieJar};
    let Some((test, cookie, _user, room)) = fixture().await else { return };
    let path = format!("/rooms/{room}");
    warm(&test, &path, &cookie).await;
    let mut jar = CookieJar::from_headers([cookie.as_str()], test.booted.app.secrets.clone(), test.booted.app.clock.clone());
    let token = jar.signed("session_token").unwrap();
    jar.set_signed("session_token", Cookie::new(token).expires(test.booted.app.clock.now() + jiff::SignedDuration::from_secs(5))).unwrap();
    let short = jar.set_cookie_headers(false, "campfire.test")[0].split(';').next().unwrap().to_string();
    warm(&test, &path, &short).await;
    let before = test.booted.app.response_cache.early_hits.load(Ordering::Relaxed);
    assert_eq!(send(&test.booted.router, get_with_cookie(&path, &short)).await.status, StatusCode::OK);
    assert_eq!(test.booted.app.response_cache.early_hits.load(Ordering::Relaxed), before);
    assert_eq!(send(&test.booted.router, get_with_cookie(&path, &cookie)).await.status, StatusCode::OK);
    assert_eq!(test.booted.app.response_cache.early_hits.load(Ordering::Relaxed), before + 1);
}

#[tokio::test]
async fn early_hits_preserve_preference_cookies_and_reject_body_or_transport_variants() {
    let Some((test, cookie, _user, room)) = fixture().await else { return };
    let path = format!("/rooms/{room}");
    let first = warm(&test, &path, &cookie).await;
    let hit = send(&test.booted.router, get_with_cookie(&path, &cookie)).await;
    for response in [&first, &hit] {
        let cookies: Vec<_> = response.headers.get_all(header::SET_COOKIE).iter().collect();
        assert_eq!(cookies.len(), 1);
        let cookie = cookies[0].to_str().unwrap();
        assert!(cookie.starts_with(&format!("last_room={room}; path=/; expires=")));
        assert!(cookie.ends_with("; samesite=lax"));
    }
    let matching_preference = format!("{cookie}; last_room={room}");
    warm(&test, &path, &matching_preference).await;
    let hit = send(&test.booted.router, get_with_cookie(&path, &matching_preference)).await;
    assert!(hit.headers.get_all(header::SET_COOKIE).iter().next().is_none());
    let before = test.booted.app.response_cache.early_hits.load(Ordering::Relaxed);
    let mut body = get_with_cookie(&path, &cookie);
    *body.body_mut() = Body::from("format=json");
    send(&test.booted.router, body).await;
    assert_eq!(test.booted.app.response_cache.early_hits.load(Ordering::Relaxed), before);
    let mut peer = get_with_cookie(&path, &cookie);
    peer.extensions_mut().insert(axum::extract::ConnectInfo("192.0.2.10:12345".parse::<std::net::SocketAddr>().unwrap()));
    assert_eq!(send(&test.booted.router, peer).await.status, StatusCode::OK);
    assert_eq!(test.booted.app.response_cache.early_hits.load(Ordering::Relaxed), before);
}

#[tokio::test]
async fn gzip_identity_head_and_conditional_requests_keep_their_contracts() {
    let Some((test, cookie, _user, room)) = fixture().await else { return };
    let path = format!("/rooms/{room}");
    let plain = warm(&test, &path, &cookie).await;
    let gzip_request = || {
        Request::get(&path)
            .header(header::HOST, "campfire.test")
            .header(header::COOKIE, &cookie)
            .header(header::ACCEPT_ENCODING, "gzip")
            .body(Body::empty())
            .unwrap()
    };
    let first = send(&test.booted.router, gzip_request()).await;
    let before = hits(&test);
    let hit = send(&test.booted.router, gzip_request()).await;
    assert_eq!(hits(&test), before + 1);
    assert_eq!(first.body, hit.body);
    assert_eq!(hit.header("content-encoding"), Some("gzip"));
    let mut decoded = String::new();
    flate2::read::GzDecoder::new(&hit.body[..]).read_to_string(&mut decoded).unwrap();
    assert_eq!(decoded, plain.text());
    let head = send(
        &test.booted.router,
        Request::head(&path)
            .header(header::HOST, "campfire.test")
            .header(header::COOKIE, &cookie)
            .header(header::ACCEPT_ENCODING, "gzip")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(head.status, StatusCode::OK);
    assert!(head.body.is_empty());
    assert_eq!(head.header("content-length"), hit.header("content-length"));
    let conditional = send(
        &test.booted.router,
        Request::get(&path)
            .header(header::HOST, "campfire.test")
            .header(header::COOKIE, &cookie)
            .header(header::ACCEPT_ENCODING, "gzip")
            .header(header::IF_NONE_MATCH, hit.header("etag").unwrap())
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(conditional.status, StatusCode::NOT_MODIFIED);
    assert!(conditional.body.is_empty());
    let forbidden = send(
        &test.booted.router,
        Request::get(&path)
            .header(header::HOST, "campfire.test")
            .header(header::COOKIE, &cookie)
            .header(header::ACCEPT_ENCODING, "gzip;q=0,identity;q=0")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(forbidden.status, StatusCode::NOT_ACCEPTABLE);
    assert_eq!(send(&test.booted.router, get_with_cookie(&path, &cookie)).await.body, plain.body);
}

#[tokio::test]
async fn foreign_and_local_writers_change_cached_room_content() {
    let Some((test, cookie, _user, room)) = fixture().await else { return };
    let path = format!("/rooms/{room}");
    let initial = warm(&test, &path, &cookie).await;
    let foreign = rusqlite::Connection::open(test.booted.app.db.path()).unwrap();
    foreign.execute("UPDATE rooms SET name = 'Changed externally without a timestamp' WHERE id = ?", [room]).unwrap();
    let after = send(&test.booted.router, get_with_cookie(&path, &cookie)).await;
    assert!(after.text().contains("Changed externally without a timestamp"));
    assert_ne!(after.header("etag"), initial.header("etag"));
    test.booted
        .app
        .db
        .write(move |tx| {
            tx.conn().execute("UPDATE rooms SET name = 'Changed through the app writer' WHERE id = ?", [room])?;
            Ok(())
        })
        .await
        .unwrap();
    let local = send(&test.booted.router, get_with_cookie(&path, &cookie)).await;
    assert!(local.text().contains("Changed through the app writer"));
}

#[tokio::test]
async fn revoked_sessions_and_changed_tokens_cannot_reuse_private_responses() {
    for mutation in ["DELETE FROM sessions WHERE user_id = ?", "UPDATE sessions SET token = 'rotated-token' WHERE user_id = ?"] {
        let Some((test, cookie, user, room)) = fixture().await else { return };
        let path = format!("/rooms/{room}");
        warm(&test, &path, &cookie).await;
        let before = hits(&test);
        let foreign = rusqlite::Connection::open(test.booted.app.db.path()).unwrap();
        foreign.execute(mutation, [user]).unwrap();
        let revoked = send(&test.booted.router, get_with_cookie(&path, &cookie)).await;
        assert_eq!(revoked.status, StatusCode::FOUND);
        assert_eq!(revoked.header("location"), Some("http://campfire.test/session/new"));
        assert_eq!(hits(&test), before);
        assert!(revoked.body.is_empty());
    }
}

#[tokio::test]
async fn changed_users_and_room_permissions_are_checked_before_cache_hits() {
    let Some((test, cookie, user, room)) = fixture().await else { return };
    let path = format!("/rooms/{room}");
    warm(&test, &path, &cookie).await;
    let foreign = rusqlite::Connection::open(test.booted.app.db.path()).unwrap();
    foreign.execute("UPDATE users SET name = 'Renamed current user' WHERE id = ?", [user]).unwrap();
    let renamed = send(&test.booted.router, get_with_cookie(&path, &cookie)).await;
    assert!(renamed.text().contains("Renamed current user"));
    warm(&test, &path, &cookie).await;
    let before = hits(&test);
    foreign.execute("DELETE FROM memberships WHERE user_id = ? AND room_id = ?", rusqlite::params![user, room]).unwrap();
    let denied = send(&test.booted.router, get_with_cookie(&path, &cookie)).await;
    assert_eq!(denied.status, StatusCode::FOUND);
    assert_eq!(hits(&test), before);
    assert!(!denied.text().contains("Renamed current user"));
}

#[tokio::test]
async fn request_origin_frame_user_agent_target_cookies_and_bypass_are_isolated() {
    let Some((test, cookie, _user, room)) = fixture().await else { return };
    let path = format!("/rooms/{room}");
    let full = warm(&test, &path, &cookie).await;
    for (name, value) in [
        ("host", "other.example"),
        ("turbo-frame", "room"),
        ("user-agent", "Campfire variant"),
        ("cookie", "unrelated=variant"),
        ("cache-control", "no-cache"),
    ] {
        let before = hits(&test);
        let mut request = get_with_cookie(&path, &cookie);
        let value = if name == "cookie" { format!("{cookie}; {value}") } else { value.into() };
        request.headers_mut().insert(axum::http::HeaderName::from_bytes(name.as_bytes()).unwrap(), value.parse().unwrap());
        let reply = send(&test.booted.router, request).await;
        assert_eq!(reply.status, StatusCode::OK, "{name}: {}", reply.text());
        assert_eq!(hits(&test), before, "{name}");
        if name == "turbo-frame" {
            assert!(!reply.text().contains("<!DOCTYPE html>"));
        }
    }
    let before = hits(&test);
    assert_eq!(send(&test.booted.router, get_with_cookie(&format!("{path}?variant=1"), &cookie)).await.status, StatusCode::OK);
    assert_eq!(hits(&test), before);
    let anon = send(&test.booted.router, get(&path)).await;
    assert_eq!(anon.status, StatusCode::FOUND);
    let forged = send(&test.booted.router, get_with_cookie(&path, &vectors().forged.cookie_header)).await;
    assert_eq!(forged.status, StatusCode::FOUND);
    assert_eq!(send(&test.booted.router, get_with_cookie(&path, &cookie)).await.body, full.body);
}

#[tokio::test]
async fn fresh_trace_headers_do_not_disable_hits_or_replay_old_request_ids() {
    let Some((test, cookie, _user, room)) = fixture().await else { return };
    let path = format!("/rooms/{room}");
    warm(&test, &path, &cookie).await;
    let before = hits(&test);
    for index in 1..=3 {
        let mut request = get_with_cookie(&path, &cookie);
        request.headers_mut().insert("x-request-start", format!("t={index}").parse().unwrap());
        request.headers_mut().insert("x-request-id", format!("trace-{index}").parse().unwrap());
        let reply = send(&test.booted.router, request).await;
        assert_eq!(reply.header("x-request-id"), Some(format!("trace-{index}").as_str()));
        assert_eq!(hits(&test), before + index);
    }
}

#[tokio::test]
async fn timestamp_free_foreign_edits_invalidate_nested_fragments_and_validators() {
    let Some((test, cookie, _user, room)) = fixture().await else { return };
    let foreign = rusqlite::Connection::open(test.booted.app.db.path()).unwrap();
    let (message, creator): (i64, i64) = foreign.query_row(
        "SELECT messages.id, messages.creator_id FROM messages JOIN boosts ON boosts.message_id = messages.id WHERE messages.room_id = ? ORDER BY messages.id LIMIT 1", [room], |row| Ok((row.get(0)?, row.get(1)?)),
    ).unwrap();
    let path = format!("/rooms/{room}/@{message}");
    let before = warm(&test, &path, &cookie).await;
    foreign.execute("UPDATE users SET name = 'Foreign renamed creator' WHERE id = ?", [creator]).unwrap();
    foreign
        .execute(
            "UPDATE action_text_rich_texts SET body = '<p>Foreign replacement body</p>' WHERE record_type = 'Message' AND record_id = ?",
            [message],
        )
        .unwrap();
    foreign.execute("UPDATE boosts SET content = 'Foreign replacement boost' WHERE message_id = ?", [message]).unwrap();
    let after = send(&test.booted.router, get_with_cookie(&path, &cookie)).await;
    for changed in ["Foreign renamed creator", "Foreign replacement body", "Foreign replacement boost"] {
        assert!(after.text().contains(changed), "missing {changed}");
    }
    assert_ne!(after.header("etag"), before.header("etag"));
    // The paginated representation has its own validator, not just the surrounding room page.
    let messages_path = format!("/rooms/{room}/messages");
    let old = warm(&test, &messages_path, &cookie).await;
    foreign.execute("UPDATE users SET name = 'Second foreign creator name' WHERE id = ?", [creator]).unwrap();
    let mut request = get_with_cookie(&messages_path, &cookie);
    request.headers_mut().insert(header::IF_NONE_MATCH, old.headers[header::ETAG].clone());
    let conditional = send(&test.booted.router, request).await;
    assert_eq!(conditional.status, StatusCode::OK, "the old body validator cannot yield 304");
    assert_ne!(conditional.header("etag"), old.header("etag"));
    foreign.execute("UPDATE users SET name = 'Third foreign creator name' WHERE id = ?", [creator]).unwrap();
    let mut request = get_with_cookie(&messages_path, &cookie);
    request.headers_mut().insert(header::IF_MODIFIED_SINCE, "Sat, 01 Jan 2050 00:00:00 GMT".parse().unwrap());
    assert_eq!(send(&test.booted.router, request).await.status, StatusCode::OK);
    let current = warm(&test, &messages_path, &cookie).await;
    let mut request = get_with_cookie(&messages_path, &cookie);
    request.headers_mut().insert(header::IF_MODIFIED_SINCE, "Sat, 01 Jan 2050 00:00:00 GMT".parse().unwrap());
    let cached_ims = send(&test.booted.router, request).await;
    assert_eq!(cached_ims.status, StatusCode::OK, "a cache hit cannot invent a Last-Modified validator either");
    assert_eq!(cached_ims.body, current.body);
    assert!(cached_ims.header("last-modified").is_none());
}

#[tokio::test]
async fn flash_is_rendered_and_consumed_fresh_instead_of_reusing_or_populating_a_response() {
    let Some((test, cookie, _user, room)) = fixture().await else { return };
    let path = format!("/rooms/{room}");
    warm(&test, &path, &cookie).await;
    let denied = send(&test.booted.router, get_with_cookie("/rooms/0", &cookie)).await;
    assert_eq!(denied.status, StatusCode::FOUND);
    let flash_cookie = denied
        .headers
        .get_all(header::SET_COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .find(|value| value.starts_with("_campfire_session="))
        .unwrap()
        .split(';')
        .next()
        .unwrap();
    let health = send(&test.booted.router, get_with_cookie("/up", &format!("{cookie}; {flash_cookie}"))).await;
    assert_eq!(health.status, StatusCode::OK);
    assert!(
        health.headers.get_all(header::SET_COOKIE).iter().all(|value| !value.to_str().unwrap().starts_with("_campfire_session=")),
        "an unrelated health response must not consume pending flash"
    );
    let before = hits(&test);
    let flashed = send(&test.booted.router, get_with_cookie(&path, &format!("{cookie}; {flash_cookie}"))).await;
    assert_eq!(flashed.status, StatusCode::OK);
    assert!(flashed.text().contains("Room not found or inaccessible"));
    assert_eq!(hits(&test), before);
    assert!(
        flashed.headers.get_all(header::SET_COOKIE).iter().any(|value| value.to_str().unwrap().starts_with("_campfire_session=")),
        "flash consumption updates its cookie"
    );
    let clean = send(&test.booted.router, get_with_cookie(&path, &cookie)).await;
    assert_eq!(hits(&test), before + 1);
    assert!(!clean.text().contains("Room not found or inaccessible"));
    assert!(
        clean.headers.get_all(header::SET_COOKIE).iter().any(|value| value.to_str().unwrap().starts_with("last_room=")),
        "remember_last_room still runs on a cache hit"
    );
    assert!(
        !clean.headers.get_all(header::SET_COOKIE).iter().any(|value| value.to_str().unwrap().starts_with("session_token=")),
        "an old session refresh cookie is not replayed"
    );
}

#[tokio::test]
async fn cached_opengraph_fragments_preserve_actual_origin_filtering() {
    let Some((test, cookie, _user, room)) = fixture().await else { return };
    let foreign = rusqlite::Connection::open(test.booted.app.db.path()).unwrap();
    let message: i64 =
        foreign.query_row("SELECT id FROM messages WHERE room_id = ? ORDER BY created_at DESC LIMIT 1", [room], |row| row.get(0)).unwrap();
    let embed = r#"<action-text-attachment content-type="application/vnd.actiontext.opengraph-embed" url="https://one.example/embed.png" href="https://one.example/host-sensitive" filename="Origin-sensitive embed"></action-text-attachment>"#;
    foreign
        .execute(
            "UPDATE action_text_rich_texts SET body = ? WHERE record_type = 'Message' AND record_id = ?",
            rusqlite::params![embed, message],
        )
        .unwrap();
    let path = format!("/rooms/{room}");
    let request = |host: &str| {
        let mut request = get_with_cookie(&path, &cookie);
        request.headers_mut().insert(header::HOST, host.parse().unwrap());
        request
    };
    for _ in 0..4 {
        assert_eq!(send(&test.booted.router, request("one.example")).await.status, StatusCode::OK);
    }
    let same_origin = send(&test.booted.router, request("one.example")).await;
    assert!(!same_origin.text().contains("https://one.example/host-sensitive"));
    let elsewhere = send(&test.booted.router, request("two.example")).await;
    assert!(elsewhere.text().contains("https://one.example/host-sensitive"), "the second origin must render its own nested fragment");
    let before = hits(&test);
    assert_eq!(send(&test.booted.router, request("two.example")).await.body, elsewhere.body);
    assert_eq!(hits(&test), before + 1);
    assert_eq!(send(&test.booted.router, request("one.example")).await.body, same_origin.body);
}

#[tokio::test]
async fn delayed_detached_render_does_not_poison_a_new_generation() {
    use crate::controllers::presenters::{Presenter, page};
    let Some((test, _cookie, _user, room)) = fixture().await else { return };
    let app = test.booted.app.clone();
    let foreign = rusqlite::Connection::open(app.db.path()).unwrap();
    let id: i64 =
        foreign.query_row("SELECT id FROM messages WHERE room_id = ? ORDER BY created_at DESC LIMIT 1", [room], |row| row.get(0)).unwrap();
    foreign
        .execute(
            "UPDATE action_text_rich_texts SET body = '<p>old detached render body</p>' WHERE record_type = 'Message' AND record_id = ?",
            [id],
        )
        .unwrap();
    let old_renderer = page::DetachedRenderer::new(&app);
    let render_app = app.clone();
    let old = app
        .read(move |conn| {
            let message = campfire_db::Message::find(conn, id)?;
            Presenter::new(conn, &render_app, None).message(&message)
        })
        .await
        .unwrap();
    foreign
        .execute(
            "UPDATE action_text_rich_texts SET body = '<p>new detached render body</p>' WHERE record_type = 'Message' AND record_id = ?",
            [id],
        )
        .unwrap();
    // A broadcast has gathered its view, then another connection commits before
    // render_detached captures the fragment namespace.
    old_renderer.render(&app, None, |ctx| campfire_views::messages::message(ctx, &old));
    let fresh_renderer = page::DetachedRenderer::new(&app);
    let render_app = app.clone();
    let fresh = app
        .read(move |conn| {
            let message = campfire_db::Message::find(conn, id)?;
            Presenter::new(conn, &render_app, None).message(&message)
        })
        .await
        .unwrap();
    let html = fresh_renderer.render(&app, None, |ctx| campfire_views::messages::message(ctx, &fresh));
    assert!(html.contains("new detached render body"), "a delayed old broadcast must not populate the new fragment generation");
    assert!(!html.contains("old detached render body"));
}

#[tokio::test]
async fn repeated_header_values_cannot_impersonate_a_frame_header() {
    let Some((test, cookie, _user, room)) = fixture().await else { return };
    let path = format!("/rooms/{room}");
    let embedded = || {
        let mut request = get_with_cookie(&path, &cookie);
        for value in ["value", "", "turbo-frame", "room"] {
            request.headers_mut().append("t", value.parse().unwrap());
        }
        request
    };
    for _ in 0..4 {
        let full = send(&test.booted.router, embedded()).await;
        assert_eq!(full.status, StatusCode::OK);
        assert!(full.text().contains("<!DOCTYPE html>"));
    }
    let frame = || {
        let mut request = get_with_cookie(&path, &cookie);
        request.headers_mut().insert("t", "value".parse().unwrap());
        request.headers_mut().insert("turbo-frame", "room".parse().unwrap());
        request
    };
    let before = hits(&test);
    let first = send(&test.booted.router, frame()).await;
    assert_eq!(first.status, StatusCode::OK);
    assert_eq!(hits(&test), before, "repeated values must not alias a separate header");
    assert!(!first.text().contains("<!DOCTYPE html>"));
    let cached = send(&test.booted.router, frame()).await;
    assert_eq!(hits(&test), before + 1);
    assert_eq!(cached.body, first.body);
}
