//! `test/models/user_test.rb`, `user/bot_test.rb`, `user/role_test.rb`, plus Bannable.

use super::*;
use crate::{
    Ban, Membership, Message, NewUser, PasswordDigest, PushSubscription, Role, Room, RoomType, Search, Session, Status, User, UserChanges,
    Webhook,
};

fn user(t: &TestDb, label: &str) -> User {
    let user_id = id(label);
    t.read(|c| User::find(c, user_id))
}

fn create_new_user(t: &TestDb) -> User {
    t.write(|tx| {
        User::create(
            tx,
            NewUser {
                name: "User".into(),
                email_address: Some("user@example.com".into()),
                password_digest: Some(PasswordDigest::create("secret123456", 4).unwrap()),
                ..Default::default()
            },
        )
    })
}

#[test]
fn user_does_not_prevent_very_long_passwords() {
    let t = TestDb::new();
    let mut david = user(&t, "david");
    t.write(move |tx| {
        david.update(
            tx,
            UserChanges { password_digest: Some(PasswordDigest::create(&"secret".repeat(50), 4).unwrap()), ..Default::default() },
        )
    });
    assert!(user(&t, "david").authenticate(&"secret".repeat(50)));
}

#[test]
fn creating_users_grants_membership_to_the_open_rooms() {
    let t = TestDb::new();
    let before = t.read(Membership::count);
    let open_rooms = t.read(|c| Room::count_of_type(c, RoomType::Open));
    let user = create_new_user(&t);
    assert_eq!(t.read(Membership::count), before + open_rooms);
    // grant_membership_to_open_rooms leaves involvement to the column default.
    assert!(t.read(|c| user.memberships(c)).iter().all(|m| m.involved_in(crate::Involvement::Mentions)));
}

#[test]
fn creating_subsequent_users_makes_them_members() {
    let t = TestDb::new();
    let user = create_new_user(&t);
    assert!(user.is_member());
    assert!(user.is_active());
    assert!(user.password_digest.as_deref().unwrap().starts_with("$2a$04$"));
}

#[test]
fn deactivating_a_user_deletes_push_subscriptions_searches_memberships_for_non_direct_rooms_and_changes_their_email_address() {
    let t = TestDb::new();
    let david = id("david");
    let memberships = t.read(Membership::count);
    let without_directs = t.read(|c| Membership::count_without_direct_rooms(c, david));
    let subscriptions = t.read(PushSubscription::count);
    let davids_subscriptions = t.read(|c| PushSubscription::for_user(c, david)).len() as i64;
    let searches = t.read(Search::count);
    let davids_searches = t.read(|c| Search::count_for_user(c, david));

    let mut user = user(&t, "david");
    t.write(move |tx| user.deactivate(tx));

    assert_eq!(t.read(Membership::count), memberships - without_directs);
    assert_eq!(t.read(PushSubscription::count), subscriptions - davids_subscriptions);
    assert_eq!(t.read(Search::count), searches - davids_searches);

    let reloaded = t.read(|c| User::find(c, david));
    let email = reloaded.email_address.unwrap();
    assert!(email.starts_with("david-deactivated-") && email.ends_with("@37signals.com"), "{email}");
    assert_eq!(email.len(), "david-deactivated-2e7de450-cf04-4fa8-9b02-ff5ab2d733e7@37signals.com".len());
    assert_eq!(reloaded.status, Status::Deactivated);
    assert_eq!(t.events(), vec![Event::DisconnectUser { user_id: david, reconnect: false }]);
}

#[test]
fn deactivating_a_user_deletes_their_sessions() {
    let t = TestDb::new();
    assert_eq!(t.read(|c| Session::count_for_user(c, id("david"))), 1);
    let mut david = user(&t, "david");
    t.write(move |tx| david.deactivate(tx));
    assert_eq!(t.read(|c| Session::count_for_user(c, id("david"))), 0);
}

#[test]
fn initials_and_title() {
    let t = TestDb::new();
    let mut jz = user(&t, "jz");
    assert_eq!(jz.initials(), "J");
    assert_eq!(jz.title(), "JZ – Designer");
    assert_eq!(user(&t, "bender").initials(), "BB");
    jz.name = "Émile Zola".into();
    assert_eq!(jz.initials(), "Z", "Ruby's \\b sees É as a word character, \\w doesn't");
    jz.bio = Some("  ".into());
    assert_eq!(jz.title(), "Émile Zola");
}

// User::Bot

#[test]
fn create_bot() {
    let t = TestDb::new();
    let bot = t.write(|tx| User::create_bot(tx, "Bender", None));
    let token = bot.bot_token.clone().unwrap();
    assert_eq!(token.len(), 12);
    assert_eq!(bot.bot_key(), format!("{}-{token}", bot.id));
    assert_eq!(bot.role, Role::Bot);
    assert!(bot.password_digest.is_none());
}

#[test]
fn create_bot_with_webhook() {
    let t = TestDb::new();
    let bot = t.write(|tx| User::create_bot(tx, "Bot", Some("http://x")));
    assert_eq!(t.read(|c| bot.webhook_url(c)).as_deref(), Some("http://x"));

    let mut b = bot.clone();
    t.write(move |tx| b.update_bot(tx, UserChanges { name: Some("Bot2".into()), ..Default::default() }, Some("")));
    assert!(t.read(|c| Webhook::find_by_user(c, bot.id)).is_none());
    assert_eq!(t.read(|c| User::find(c, bot.id)).name, "Bot2");
}

#[test]
fn reset_bot_key() {
    let t = TestDb::new();
    let bot = t.write(|tx| User::create_bot(tx, "Bender", None));
    let first = bot.bot_key();
    let mut b = bot.clone();
    let second = t.write(move |tx| {
        b.reset_bot_key(tx)?;
        Ok(b.bot_key())
    });
    assert_ne!(first, second);
    assert!(t.read(|c| User::authenticate_bot(c, &first)).is_none());
    assert!(t.read(|c| User::authenticate_bot(c, &second)).is_some());
}

#[test]
fn authenticate_bot() {
    let t = TestDb::new();
    let bot = t.write(|tx| User::create_bot(tx, "Bender", None));
    assert_eq!(t.read(|c| User::authenticate_bot(c, &bot.bot_key())).unwrap().id, bot.id);
    assert!(t.read(|c| User::authenticate_bot(c, "nonsense")).is_none());
    assert!(t.read(|c| User::authenticate_bot(c, &format!("{}-", bot.id))).is_none());
}

#[test]
fn deliver_message_by_webhook() {
    let t = TestDb::new();
    let bender = user(&t, "bender");
    t.write(move |tx| bender.deliver_webhook_later(tx, id("first")));
    assert_eq!(t.events(), vec![Event::DeliverWebhook { bot_id: id("bender"), message_id: id("first") }]);

    // No webhook, no job.
    let jz = user(&t, "jz");
    t.write(move |tx| jz.deliver_webhook_later(tx, id("first")));
    assert_eq!(t.events().len(), 1);
}

#[test]
fn webhook_payload() {
    let t = TestDb::new();
    let message = t.read(|c| Message::find(c, id("first")));
    let webhook = t.read(|c| Ok(Webhook::find_by_user(c, id("bender"))?.unwrap()));
    let payload = t.read(|c| webhook.payload(c, &BasicRichText, &message, "/rooms/1/bot/key/messages", "/rooms/1/@2"));
    let (jason, designers) = (id("jason"), id("designers"));
    assert_eq!(
        payload,
        format!(
            r#"{{"user":{{"id":{jason},"name":"Jason"}},"room":{{"id":{designers},"name":"Designers","path":"/rooms/1/bot/key/messages"}},"message":{{"id":{},"body":{{"html":"First post!","plain":"First post!"}},"path":"/rooms/1/@2"}}}}"#,
            message.id
        )
    );
}

/// `ActiveSupport::JSON` escapes `<`, `>` and `&`, and an unnamed room's name is `null`.
#[test]
fn webhook_payload_escapes_html_entities() {
    let t = TestDb::new();
    let (first, designers) = (id("first"), id("designers"));
    t.write(move |tx| {
        tx.conn().execute("UPDATE rooms SET name = NULL WHERE id = ?", [designers])?;
        tx.conn().execute(
            "UPDATE action_text_rich_texts SET body = '<p>Tom & Jerry</p>' WHERE record_type = 'Message' AND record_id = ?",
            [first],
        )?;
        Ok(())
    });
    let message = t.read(|c| Message::find(c, first));
    let webhook = t.read(|c| Ok(Webhook::find_by_user(c, id("bender"))?.unwrap()));
    let payload = t.read(|c| webhook.payload(c, &BasicRichText, &message, "/rooms/1/bot/key/messages", "/rooms/1/@2"));
    assert_eq!(
        payload,
        format!(
            r#"{{"user":{{"id":{},"name":"Jason"}},"room":{{"id":{designers},"name":null,"path":"/rooms/1/bot/key/messages"}},"message":{{"id":{first},"body":{{"html":"\u003cp\u003eTom \u0026 Jerry\u003c/p\u003e","plain":"Tom \u0026 Jerry"}},"path":"/rooms/1/@2"}}}}"#,
            id("jason")
        )
    );
}

// User::Role

#[test]
fn can_administer() {
    let t = TestDb::new();
    let mut admin = user(&t, "david");
    assert!(admin.can_administer(None, false));
    admin.role = Role::Member;
    assert!(!admin.can_administer(None, false));

    let member = user(&t, "kevin");
    assert!(member.can_administer(Some(member.id), false), "creator");
    assert!(member.can_administer(Some(id("jz")), true), "new record");
    let designers = t.read(|c| Room::find(c, id("designers")));
    assert!(!member.can_administer(Some(designers.creator_id), false));
}

// User::Bannable

#[test]
fn ban_creates_bans_from_session_ips_and_removes_sessions() {
    let t = TestDb::new();
    let kevin = id("kevin");
    t.write(move |tx| {
        Session::start(tx, kevin, Some("ua"), Some("8.8.8.8"))?;
        Session::start(tx, kevin, Some("ua"), Some("8.8.8.8"))?;
        Session::start(tx, kevin, Some("ua"), Some(""))?;
        Ok(())
    });
    t.sink.take();

    let mut user = user(&t, "kevin");
    t.write(move |tx| user.ban(tx));

    assert_eq!(t.read(|c| Ban::for_user(c, kevin)).iter().map(|b| b.ip_address.clone()).collect::<Vec<_>>(), ["8.8.8.8"]);
    assert!(t.read(|c| Ban::banned(c, "8.8.8.8")));
    assert_eq!(t.read(|c| Session::count_for_user(c, kevin)), 0);
    assert_eq!(t.read(|c| User::find(c, kevin)).status, Status::Banned);
    assert_eq!(t.events(), vec![Event::DisconnectUser { user_id: kevin, reconnect: false }, Event::RemoveBannedContent { user_id: kevin }]);

    let mut user = t.read(|c| User::find(c, kevin));
    t.write(move |tx| user.unban(tx));
    assert!(!t.read(|c| Ban::banned(c, "8.8.8.8")));
    assert_eq!(t.read(|c| User::find(c, kevin)).status, Status::Active);
}

#[test]
fn ban_rejects_private_session_ips() {
    let t = TestDb::new();
    let kevin = id("kevin");
    t.write(move |tx| Session::start(tx, kevin, None, Some("192.168.1.1")).map(|_| ()));
    let mut user = user(&t, "kevin");
    assert!(t.try_write(move |tx| user.ban(tx)).is_err());
    assert_eq!(t.read(|c| User::find(c, kevin)).status, Status::Active, "rolled back");
}

#[test]
fn remove_banned_content() {
    let t = TestDb::new();
    let jz = user(&t, "jz");
    let removed = t.write(move |tx| jz.remove_banned_content(tx));
    assert_eq!(removed.len(), 5);
    assert!(t.read(|c| Message::by_creator(c, id("jz"))).is_empty());
}

#[test]
fn session_creation_rechecks_status_after_password_verification() {
    for banned in [false, true] {
        let t = TestDb::new();
        let candidate = create_new_user(&t);
        let user_id = candidate.id;
        t.write(move |tx| {
            let mut user = User::find(tx.conn(), user_id)?;
            if banned { user.ban(tx) } else { user.deactivate(tx) }
        });
        let authenticated = User::authenticated(Some(candidate), "secret123456").unwrap();
        let result = t.try_write(move |tx| Session::start(tx, authenticated.id, None, Some("8.8.8.8")));
        assert!(matches!(result, Err(crate::Error::RecordNotFound("User"))), "{result:?}");
        assert_eq!(t.read(|conn| Session::count_for_user(conn, user_id)), 0);
    }
}

#[test]
fn session_lookup_rejects_inactive_users_with_surviving_session_rows() {
    let t = TestDb::new();
    let user_id = create_new_user(&t).id;
    let session = t.write(move |tx| Session::start(tx, user_id, None, None));
    for status in [Status::Banned, Status::Deactivated] {
        t.write(move |tx| {
            tx.conn().execute("UPDATE users SET status = ? WHERE id = ?", rusqlite::params![status, user_id])?;
            Ok(())
        });
        assert!(t.read(|conn| Session::find_by_token(conn, &session.token)).is_none());
        assert_eq!(t.read(|conn| Session::count_for_user(conn, user_id)), 1);
    }
}
