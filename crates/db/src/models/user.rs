//! `reference/app/models/user.rb` and `user/*.rb` (Role, Bot, Bannable, Mentionable; Avatar
//! and Transferable are signed ids, which live in `rails_compat`).

use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef};
use rusqlite::{Connection, params};

use crate::database::Tx;
use crate::error::{OptionalExt, Result};
use crate::events::Event;
use crate::models::{Ban, Membership, Message, Session, Webhook};
use crate::sql::{self, CachedStatements, columns, placeholders, query_all, query_one};
use crate::time::{SQLITE_NOW, Timestamp};

/// `enum :role, %i[ member administrator bot ]`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Role {
    #[default]
    Member = 0,
    Administrator = 1,
    Bot = 2,
}

impl Role {
    pub fn name(self) -> &'static str {
        match self {
            Role::Member => "member",
            Role::Administrator => "administrator",
            Role::Bot => "bot",
        }
    }

    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "member" => Some(Role::Member),
            "administrator" => Some(Role::Administrator),
            "bot" => Some(Role::Bot),
            _ => None,
        }
    }
}

/// `enum :status, %i[ active deactivated banned ], default: :active`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Status {
    #[default]
    Active = 0,
    Deactivated = 1,
    Banned = 2,
}

impl Status {
    pub fn name(self) -> &'static str {
        match self {
            Status::Active => "active",
            Status::Deactivated => "deactivated",
            Status::Banned => "banned",
        }
    }

    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "active" => Some(Status::Active),
            "deactivated" => Some(Status::Deactivated),
            "banned" => Some(Status::Banned),
            _ => None,
        }
    }
}

macro_rules! integer_enum_sql {
    ($ty:ty, $($variant:path = $value:literal),+) => {
        impl ToSql for $ty {
            fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
                Ok(ToSqlOutput::from(*self as i64))
            }
        }

        impl FromSql for $ty {
            fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
                match value.as_i64()? {
                    $($value => Ok($variant),)+
                    other => Err(FromSqlError::OutOfRange(other)),
                }
            }
        }
    };
}

integer_enum_sql!(Role, Role::Member = 0, Role::Administrator = 1, Role::Bot = 2);
integer_enum_sql!(Status, Status::Active = 0, Status::Deactivated = 1, Status::Banned = 2);

#[derive(Debug, Clone, PartialEq)]
pub struct User {
    pub id: i64,
    pub name: String,
    pub email_address: Option<String>,
    pub password_digest: Option<String>,
    pub role: Role,
    pub status: Status,
    pub bio: Option<String>,
    pub bot_token: Option<String>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

/// Attributes for `User.create!`.
#[derive(Debug, Clone, Default)]
pub struct NewUser {
    pub name: String,
    pub email_address: Option<String>,
    /// `has_secure_password`'s `password=`, already hashed.
    pub password_digest: Option<PasswordDigest>,
    pub role: Role,
    pub bio: Option<String>,
    pub bot_token: Option<String>,
}

/// Attributes for `user.update`. `None` leaves an attribute alone.
#[derive(Debug, Clone, Default)]
pub struct UserChanges {
    pub name: Option<String>,
    pub email_address: Option<Option<String>>,
    pub password_digest: Option<PasswordDigest>,
    pub role: Option<Role>,
    pub status: Option<Status>,
    pub bio: Option<Option<String>>,
    pub bot_token: Option<Option<String>>,
}

const INSERT: &str = r#"INSERT INTO "users" ("bio", "bot_token", "created_at", "email_address", "name", "password_digest", "role", "status", "updated_at") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING "id""#;

columns! {
    User, "users", user_columns {
        id: "id",
        name: "name",
        email_address: "email_address",
        password_digest: "password_digest",
        role: "role",
        status: "status",
        bio: "bio",
        bot_token: "bot_token",
        created_at: "created_at",
        updated_at: "updated_at",
    }
}

impl User {
    /// `SELECT` and the columns [`User::find_by_sql`] reads, `FROM "users"`: queries built outside
    /// this crate start with it, since users are read by position (see `sql::columns!`).
    pub const SELECT: &str = concat!("SELECT ", user_columns!(), r#" FROM "users""#);

    // Finders and scopes

    /// `User.find_by_sql`: the users a query starting with [`User::SELECT`] returns.
    pub fn find_by_sql(conn: &Connection, sql: &str, params: impl rusqlite::Params) -> Result<Vec<Self>> {
        query_all(conn, sql, params, Self::from_row)
    }

    pub fn find(conn: &Connection, id: i64) -> Result<Self> {
        Self::find_by_id(conn, id)?.or_not_found("User")
    }

    pub fn find_by_id(conn: &Connection, id: i64) -> Result<Option<Self>> {
        query_one(conn, concat!("SELECT ", user_columns!(), r#" FROM "users" WHERE "users"."id" = ? LIMIT 1"#), [id], Self::from_row)
    }

    /// `User.active.find(id)`
    pub fn find_active(conn: &Connection, id: i64) -> Result<Self> {
        query_one(
            conn,
            concat!("SELECT ", user_columns!(), r#" FROM "users" WHERE "users"."status" = 0 AND "users"."id" = ? LIMIT 1"#),
            [id],
            Self::from_row,
        )?
        .or_not_found("User")
    }

    pub fn find_by_email_address(conn: &Connection, email_address: &str) -> Result<Option<Self>> {
        query_one(
            conn,
            concat!("SELECT ", user_columns!(), r#" FROM "users" WHERE "users"."email_address" = ? LIMIT 1"#),
            [email_address],
            Self::from_row,
        )
    }

    pub fn all(conn: &Connection) -> Result<Vec<Self>> {
        query_all(conn, Self::SELECT, [], Self::from_row)
    }

    pub fn count(conn: &Connection) -> Result<i64> {
        sql::count(conn, r#"SELECT COUNT(*) FROM "users""#, [])
    }

    /// `User.where(id: ids)`
    pub fn where_ids(conn: &Connection, ids: &[i64]) -> Result<Vec<Self>> {
        let sql = format!(r#"{} WHERE "users"."id" IN ({})"#, Self::SELECT, placeholders(ids.len()));
        query_all(conn, &sql, rusqlite::params_from_iter(ids), Self::from_row)
    }

    /// `User.active.ordered`
    pub fn active_ordered(conn: &Connection) -> Result<Vec<Self>> {
        query_all(
            conn,
            concat!("SELECT ", user_columns!(), r#" FROM "users" WHERE "users"."status" = 0 ORDER BY LOWER(name)"#),
            [],
            Self::from_row,
        )
    }

    /// `User.active`
    pub fn active(conn: &Connection) -> Result<Vec<Self>> {
        query_all(conn, concat!("SELECT ", user_columns!(), r#" FROM "users" WHERE "users"."status" = 0"#), [], Self::from_row)
    }

    /// `User.active.filtered_by(query).ordered`
    pub fn active_filtered_by_ordered(conn: &Connection, query: &str) -> Result<Vec<Self>> {
        query_all(
            conn,
            concat!("SELECT ", user_columns!(), r#" FROM "users" WHERE "users"."status" = 0 AND (name like ?) ORDER BY LOWER(name)"#),
            [format!("%{query}%")],
            Self::from_row,
        )
    }

    /// `User.active.ordered.without_bots`
    pub fn active_ordered_without_bots(conn: &Connection) -> Result<Vec<Self>> {
        query_all(
            conn,
            concat!("SELECT ", user_columns!(), r#" FROM "users" WHERE "users"."status" = 0 AND "users"."role" != 2 ORDER BY LOWER(name)"#),
            [],
            Self::from_row,
        )
    }

    /// `User.active_bots.ordered`
    pub fn active_bots_ordered(conn: &Connection) -> Result<Vec<Self>> {
        query_all(
            conn,
            concat!("SELECT ", user_columns!(), r#" FROM "users" WHERE "users"."status" = 0 AND "users"."role" = 2 ORDER BY LOWER(name)"#),
            [],
            Self::from_row,
        )
    }

    /// `User.active_bots.find(id)`
    pub fn find_active_bot(conn: &Connection, id: i64) -> Result<Self> {
        query_one(
            conn,
            concat!(
                "SELECT ",
                user_columns!(),
                r#" FROM "users" WHERE "users"."status" = 0 AND "users"."role" = 2 AND "users"."id" = ? LIMIT 1"#
            ),
            [id],
            Self::from_row,
        )?
        .or_not_found("User")
    }

    /// `User.active.find_by(email_address:)`: the lookup half of `authenticate_by`.
    pub fn find_active_by_email_address(conn: &Connection, email_address: &str) -> Result<Option<Self>> {
        query_one(
            conn,
            concat!("SELECT ", user_columns!(), r#" FROM "users" WHERE "users"."status" = 0 AND "users"."email_address" = ? LIMIT 1"#),
            [email_address],
            Self::from_row,
        )
    }

    /// The password half of `User.active.authenticate_by(email_address:, password:)`, given
    /// what [`User::find_active_by_email_address`] found. Blocking (bcrypt), so it runs with no
    /// connection held. A blank password returns nil before the lookup in Rails; callers skip
    /// the lookup for one too.
    pub fn authenticated(candidate: Option<Self>, password: &str) -> Option<Self> {
        if password.is_empty() {
            return None;
        }
        match candidate {
            Some(user) => user.authenticate(password).then_some(user),
            None => {
                // authenticate_by hashes anyway so a missing account takes as long as a wrong password.
                rails_compat::password::verify(password, DUMMY_DIGEST);
                None
            }
        }
    }

    /// `User.authenticate_bot(bot_key)`: `"#{id}-#{bot_token}"`
    pub fn authenticate_bot(conn: &Connection, bot_key: &str) -> Result<Option<Self>> {
        // Ruby's `split("-")` drops trailing empty fields; a key without a token finds nothing.
        let mut parts = bot_key.split('-');
        let (Some(id), Some(token)) = (parts.next(), parts.next()) else {
            return Ok(None);
        };
        query_one(
            conn,
            concat!(
                "SELECT ",
                user_columns!(),
                r#" FROM "users" WHERE "users"."status" = 0 AND "users"."role" = 2 AND "users"."id" = ? AND "users"."bot_token" = ? LIMIT 1"#
            ),
            params![id, token],
            Self::from_row,
        )
    }

    // Creating

    /// `User.create!`: inserts, then grants memberships to every open room after commit.
    pub fn create(tx: &mut Tx<'_>, attributes: NewUser) -> Result<Self> {
        let now = tx.now();
        let password_digest = attributes.password_digest.map(PasswordDigest::into_string);
        let id: i64 = tx.conn().query_row_cached(
            INSERT,
            params![
                attributes.bio,
                attributes.bot_token,
                now,
                attributes.email_address,
                attributes.name,
                password_digest,
                attributes.role,
                Status::Active,
                now
            ],
            |r| r.get(0),
        )?;
        tx.after_commit(move |tx| grant_membership_to_open_rooms(tx, id));
        Self::find(tx.conn(), id)
    }

    /// `User.create_bot!`
    pub fn create_bot(tx: &mut Tx<'_>, name: &str, webhook_url: Option<&str>) -> Result<Self> {
        let user = Self::create(
            tx,
            NewUser { name: name.to_string(), bot_token: Some(generate_bot_token()), role: Role::Bot, ..Default::default() },
        )?;
        if let Some(url) = webhook_url {
            Webhook::create(tx, user.id, Some(url))?;
        }
        Ok(user)
    }

    // Updating

    /// `user.update(attributes)`: writes only what changed, and nothing at all (not even
    /// `updated_at`) when nothing did.
    pub fn update(&mut self, tx: &mut Tx<'_>, changes: UserChanges) -> Result<()> {
        let mut sets: Vec<(&str, Box<dyn rusqlite::ToSql>)> = Vec::new();
        if let Some(name) = changes.name.filter(|n| *n != self.name) {
            self.name = name.clone();
            sets.push(("name", Box::new(name)));
        }
        if let Some(email) = changes.email_address.filter(|e| *e != self.email_address) {
            self.email_address = email.clone();
            sets.push(("email_address", Box::new(email)));
        }
        if let Some(digest) = changes.password_digest.map(PasswordDigest::into_string) {
            self.password_digest = Some(digest.clone());
            sets.push(("password_digest", Box::new(digest)));
        }
        if let Some(role) = changes.role.filter(|r| *r != self.role) {
            self.role = role;
            sets.push(("role", Box::new(role)));
        }
        if let Some(status) = changes.status.filter(|s| *s != self.status) {
            self.status = status;
            sets.push(("status", Box::new(status)));
        }
        if let Some(bio) = changes.bio.filter(|b| *b != self.bio) {
            self.bio = bio.clone();
            sets.push(("bio", Box::new(bio)));
        }
        if let Some(token) = changes.bot_token.filter(|t| *t != self.bot_token) {
            self.bot_token = token.clone();
            sets.push(("bot_token", Box::new(token)));
        }
        if sets.is_empty() {
            return Ok(());
        }
        let now = tx.now();
        self.updated_at = now;
        sets.push(("updated_at", Box::new(now)));
        let assignments: Vec<String> = sets.iter().map(|(c, _)| format!(r#""{c}" = ?"#)).collect();
        let sql = format!(r#"UPDATE "users" SET {} WHERE "users"."id" = ?"#, assignments.join(", "));
        let mut values: Vec<&dyn rusqlite::ToSql> = sets.iter().map(|(_, v)| v.as_ref()).collect();
        values.push(&self.id);
        tx.conn().execute_cached(&sql, values.as_slice())?;
        Ok(())
    }

    /// `update_bot!`: the webhook first, then the user, in one transaction.
    pub fn update_bot(&mut self, tx: &mut Tx<'_>, changes: UserChanges, webhook_url: Option<&str>) -> Result<()> {
        let webhook = Webhook::find_by_user(tx.conn(), self.id)?;
        match (webhook_url.filter(|u| !u.trim().is_empty()), webhook) {
            (Some(url), Some(mut webhook)) => webhook.update_url(tx, url)?,
            (Some(url), None) => {
                Webhook::create(tx, self.id, Some(url))?;
            }
            (None, Some(webhook)) => webhook.destroy(tx)?,
            (None, None) => {}
        }
        self.update(tx, changes)
    }

    pub fn reset_bot_key(&mut self, tx: &mut Tx<'_>) -> Result<()> {
        self.update(tx, UserChanges { bot_token: Some(Some(generate_bot_token())), ..Default::default() })
    }

    /// `deactivate`: disconnects sockets first (mid-transaction, as Rails does), then removes
    /// non-direct memberships, push subscriptions, searches and sessions, and scrambles the
    /// email address.
    pub fn deactivate(&mut self, tx: &mut Tx<'_>) -> Result<()> {
        self.close_remote_connections(tx, false);
        let conn = tx.conn();
        conn.execute_cached(
            r#"DELETE FROM "memberships" WHERE ("memberships"."id") IN (SELECT "memberships"."id" FROM "memberships" INNER JOIN "rooms" AS "room" ON "room"."id" = "memberships"."room_id" WHERE "memberships"."user_id" = ? AND "room"."type" != ?)"#,
            params![self.id, "Rooms::Direct"],
        )?;
        conn.execute_cached(r#"DELETE FROM "push_subscriptions" WHERE "push_subscriptions"."user_id" = ?"#, [self.id])?;
        conn.execute_cached(r#"DELETE FROM "searches" WHERE "searches"."user_id" = ?"#, [self.id])?;
        conn.execute_cached(r#"DELETE FROM "sessions" WHERE "sessions"."user_id" = ?"#, [self.id])?;
        let email = self.deactivated_email_address();
        self.update(tx, UserChanges { status: Some(Status::Deactivated), email_address: Some(email), ..Default::default() })
    }

    fn deactivated_email_address(&self) -> Option<String> {
        let uuid = sql::uuid();
        self.email_address.as_ref().map(|e| e.replace('@', &format!("-deactivated-{uuid}@")))
    }

    /// `User::Bannable#ban`
    pub fn ban(&mut self, tx: &mut Tx<'_>) -> Result<()> {
        // create_bans_from_sessions: `sessions.pluck(:ip_address).compact_blank.uniq`
        let ips: Vec<Option<String>> =
            query_all(tx.conn(), r#"SELECT "sessions"."ip_address" FROM "sessions" WHERE "sessions"."user_id" = ?"#, [self.id], |r| {
                r.get(0)
            })?;
        let mut seen = Vec::new();
        for ip in ips.into_iter().flatten().filter(|ip| !ip.trim().is_empty()) {
            if !seen.contains(&ip) {
                // reference/app/models/user/bannable.rb lets an ineligible session IP abort the
                // entire ban. Skip its IP ban so the account and sessions can still be revoked.
                if Ban::validate(&ip).is_empty() {
                    Ban::create(tx, self.id, &ip)?;
                }
                seen.push(ip);
            }
        }
        // apply_ban
        self.close_remote_connections(tx, false);
        tx.conn().execute_cached(r#"DELETE FROM "sessions" WHERE "sessions"."user_id" = ?"#, [self.id])?;
        tx.emit_after_commit(Event::RemoveBannedContent { user_id: self.id });
        self.update(tx, UserChanges { status: Some(Status::Banned), ..Default::default() })
    }

    pub fn unban(&mut self, tx: &mut Tx<'_>) -> Result<()> {
        tx.conn().execute_cached(r#"DELETE FROM "bans" WHERE "bans"."user_id" = ?"#, [self.id])?;
        self.update(tx, UserChanges { status: Some(Status::Active), ..Default::default() })
    }

    /// `remove_banned_content`: destroys every message the user wrote. Returns them so the
    /// caller can `broadcast_remove` each one.
    pub fn remove_banned_content(&self, tx: &mut Tx<'_>) -> Result<Vec<Message>> {
        let messages = Message::by_creator(tx.conn(), self.id)?;
        for message in &messages {
            message.destroy(tx)?;
        }
        Ok(messages)
    }

    /// `reset_remote_connections`: disconnect this user's sockets and let them reconnect.
    pub fn reset_remote_connections(&self, tx: &mut Tx<'_>) {
        self.close_remote_connections(tx, true);
    }

    /// After commit: a connection authenticating meanwhile then either finds the sessions gone or
    /// is already listening for this (see `campfire_cable`'s connection setup).
    fn close_remote_connections(&self, tx: &mut Tx<'_>, reconnect: bool) {
        tx.emit_after_commit(Event::DisconnectUser { user_id: self.id, reconnect });
    }

    /// `deliver_webhook_later(message)`: only bots with a webhook.
    pub fn deliver_webhook_later(&self, tx: &mut Tx<'_>, message_id: i64) -> Result<()> {
        if Webhook::find_by_user(tx.conn(), self.id)?.is_some() {
            tx.emit_after_commit(Event::DeliverWebhook { bot_id: self.id, message_id });
        }
        Ok(())
    }

    // Associations

    pub fn memberships(&self, conn: &Connection) -> Result<Vec<Membership>> {
        Membership::for_user(conn, self.id)
    }

    pub fn sessions(&self, conn: &Connection) -> Result<Vec<Session>> {
        Session::for_user(conn, self.id)
    }

    pub fn webhook(&self, conn: &Connection) -> Result<Option<Webhook>> {
        Webhook::find_by_user(conn, self.id)
    }

    pub fn webhook_url(&self, conn: &Connection) -> Result<Option<String>> {
        Ok(self.webhook(conn)?.and_then(|w| w.url))
    }

    // Attributes

    /// `name.scan(/\b\w/).join`. Ruby's `\w` is ASCII-only, but `\b` treats any Unicode
    /// letter or digit as a word character, so "Émile" contributes nothing.
    pub fn initials(&self) -> String {
        let mut initials = String::new();
        let mut previous: Option<char> = None;
        for c in self.name.chars() {
            let ascii_word = c.is_ascii_alphanumeric() || c == '_';
            let boundary = previous.is_none_or(|p| !(p.is_alphanumeric() || p == '_'));
            if ascii_word && boundary {
                initials.push(c);
            }
            previous = Some(c);
        }
        initials
    }

    /// `[ name, bio ].compact_blank.join(" – ")`
    pub fn title(&self) -> String {
        [Some(self.name.as_str()), self.bio.as_deref()]
            .into_iter()
            .flatten()
            .filter(|s| !s.trim().is_empty())
            .collect::<Vec<_>>()
            .join(" – ")
    }

    pub fn bot_key(&self) -> String {
        format!("{}-{}", self.id, self.bot_token.as_deref().unwrap_or(""))
    }

    /// `can_administer?(record)`: administrators, the record's creator, or a new record.
    pub fn can_administer(&self, record_creator_id: Option<i64>, record_is_new: bool) -> bool {
        self.is_administrator() || record_creator_id == Some(self.id) || record_is_new
    }

    /// `has_secure_password`'s `authenticate`.
    pub fn authenticate(&self, password: &str) -> bool {
        match self.password_digest.as_deref() {
            Some(digest) if !digest.is_empty() => rails_compat::password::verify(password, digest),
            _ => false,
        }
    }

    pub fn is_member(&self) -> bool {
        self.role == Role::Member
    }

    pub fn is_administrator(&self) -> bool {
        self.role == Role::Administrator
    }

    pub fn is_bot(&self) -> bool {
        self.role == Role::Bot
    }

    pub fn is_active(&self) -> bool {
        self.status == Status::Active
    }

    pub fn is_deactivated(&self) -> bool {
        self.status == Status::Deactivated
    }

    pub fn is_banned(&self) -> bool {
        self.status == Status::Banned
    }

    /// `attachable_plain_text_representation`
    pub fn attachable_plain_text_representation(&self) -> String {
        format!("@{}", self.name)
    }

    pub fn reload(&mut self, conn: &Connection) -> Result<()> {
        *self = Self::find(conn, self.id)?;
        Ok(())
    }
}

/// `MENTION_CONTENT_TYPE`
pub const MENTION_CONTENT_TYPE: &str = "application/vnd.campfire.mention";

/// `User.generate_bot_token`
pub fn generate_bot_token() -> String {
    sql::alphanumeric(12)
}

/// A password hashed for `password_digest`. Hashing takes about 250 ms at cost 12, so it's done
/// before the write that saves it, never on the writer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PasswordDigest(String);

impl PasswordDigest {
    /// `BCrypt::Password.create(password, cost:)`. Blocking.
    pub fn create(password: &str, cost: u32) -> Result<Self> {
        password_digest(password, cost).map(Self)
    }

    /// [`PasswordDigest::create`] on the blocking pool.
    pub async fn hash(password: String, cost: u32) -> Result<Self> {
        tokio::task::spawn_blocking(move || Self::create(&password, cost)).await.map_err(crate::Error::other)?
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

/// `BCrypt::Password.create(password, cost:)`, in the `$2a$` format bcrypt-ruby writes.
pub fn password_digest(password: &str, cost: u32) -> Result<String> {
    rails_compat::password::digest_with_cost(password, cost).map_err(crate::Error::other)
}

const DUMMY_DIGEST: &str = "$2a$12$FiKmSp4UhLvSB4Sd/ZUjQunyKP6.NjDRHdr5LnKUVk.BUn4Mq12WS";

/// `after_create_commit :grant_membership_to_open_rooms`
fn grant_membership_to_open_rooms(tx: &mut Tx<'_>, user_id: i64) -> Result<()> {
    let room_ids: Vec<i64> =
        query_all(tx.conn(), r#"SELECT "rooms"."id" FROM "rooms" WHERE "rooms"."type" = ?"#, ["Rooms::Open"], |r| r.get(0))?;
    for room_ids in room_ids.chunks(crate::models::room::MEMBERSHIP_INSERT_BATCH) {
        let rows: Vec<String> = room_ids.iter().map(|_| format!("({SQLITE_NOW}, ?, {SQLITE_NOW}, ?)")).collect();
        let sql = format!(
            r#"INSERT INTO "memberships" ("created_at","room_id","updated_at","user_id") VALUES {} ON CONFLICT  DO NOTHING RETURNING "id""#,
            rows.join(", ")
        );
        let values: Vec<i64> = room_ids.iter().flat_map(|room_id| [*room_id, user_id]).collect();
        let mut stmt = tx.conn().prepare(&sql)?;
        let mut rows = stmt.query(rusqlite::params_from_iter(values))?;
        while rows.next()?.is_some() {}
    }
    Ok(())
}
