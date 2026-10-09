//! `reference/app/models/message.rb` and `message/*.rb` (Attachment, Mentionee, Pagination,
//! Searchable; Broadcasts belong to the app).

use rusqlite::{Connection, params};

use crate::database::Tx;
use crate::error::{OptionalExt, Result};
use crate::events::Event;
use crate::models::{Attachment, Blob, Boost, RichTextRecord, Room, Sound, User};
use crate::rich_text::RichText;
use crate::sql::{self, CachedStatements, columns, placeholders, query_all, query_one};
use crate::time::Timestamp;

/// `Message::Pagination::PAGE_SIZE`
pub const PAGE_SIZE: i64 = 40;

const RECORD_TYPE: &str = "Message";

#[derive(Debug, Clone, PartialEq)]
pub struct Message {
    pub id: i64,
    pub room_id: i64,
    pub creator_id: i64,
    pub client_message_id: String,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

columns! {
    Message, "messages", message_columns {
        id: "id",
        room_id: "room_id",
        creator_id: "creator_id",
        client_message_id: "client_message_id",
        created_at: "created_at",
        updated_at: "updated_at",
    }
}

/// Attributes for `room.messages.create!` / `create_with_attachment!`.
#[derive(Debug, Clone, Default)]
pub struct NewMessage {
    pub room_id: i64,
    pub creator_id: i64,
    /// Defaults to a random UUID (`before_create`; "Bots don't care").
    pub client_message_id: Option<String>,
    /// The stored Action Text body, if one was assigned.
    pub body: Option<String>,
    /// An already-saved blob to attach as `attachment`.
    pub attachment_blob_id: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentType {
    Attachment,
    Sound,
    Text,
}

impl ContentType {
    pub fn name(self) -> &'static str {
        match self {
            ContentType::Attachment => "attachment",
            ContentType::Sound => "sound",
            ContentType::Text => "text",
        }
    }
}

const SELECT_IN_ROOM: &str = concat!("SELECT ", message_columns!(), r#" FROM "messages" WHERE "messages"."room_id" = ?"#);

const SELECT_REACHABLE: &str = concat!(
    "SELECT ",
    message_columns!(),
    r#" FROM "messages" INNER JOIN "rooms" ON "messages"."room_id" = "rooms"."id" INNER JOIN "memberships" ON "rooms"."id" = "memberships"."room_id""#
);

impl Message {
    pub fn find(conn: &Connection, id: i64) -> Result<Self> {
        Self::find_by_id(conn, id)?.or_not_found("Message")
    }

    pub fn find_by_id(conn: &Connection, id: i64) -> Result<Option<Self>> {
        query_one(
            conn,
            concat!("SELECT ", message_columns!(), r#" FROM "messages" WHERE "messages"."id" = ? LIMIT 1"#),
            [id],
            Self::from_row,
        )
    }

    /// `Message.last`
    pub fn last(conn: &Connection) -> Result<Option<Self>> {
        query_one(
            conn,
            concat!("SELECT ", message_columns!(), r#" FROM "messages" ORDER BY "messages"."id" DESC LIMIT 1"#),
            [],
            Self::from_row,
        )
    }

    pub fn count(conn: &Connection) -> Result<i64> {
        sql::count(conn, r#"SELECT COUNT(*) FROM "messages""#, [])
    }

    /// `user.messages`
    pub fn by_creator(conn: &Connection, creator_id: i64) -> Result<Vec<Self>> {
        query_all(
            conn,
            concat!("SELECT ", message_columns!(), r#" FROM "messages" WHERE "messages"."creator_id" = ?"#),
            [creator_id],
            Self::from_row,
        )
    }

    /// `room.messages`
    pub fn for_room(conn: &Connection, room_id: i64) -> Result<Vec<Self>> {
        query_all(conn, SELECT_IN_ROOM, [room_id], Self::from_row)
    }

    /// `room.messages.find(id)`
    pub fn find_in_room(conn: &Connection, room_id: i64, id: i64) -> Result<Self> {
        let sql = format!(r#"{SELECT_IN_ROOM} AND "messages"."id" = ? LIMIT 1"#);
        query_one(conn, &sql, [room_id, id], Self::from_row)?.or_not_found("Message")
    }

    /// `room.messages.count`
    pub fn count_in_room(conn: &Connection, room_id: i64) -> Result<i64> {
        sql::count(conn, r#"SELECT COUNT(*) FROM "messages" WHERE "messages"."room_id" = ?"#, [room_id])
    }

    /// `Current.user.reachable_messages.find(id)`
    pub fn find_reachable(conn: &Connection, user_id: i64, id: i64) -> Result<Self> {
        let sql = format!(r#"{SELECT_REACHABLE} WHERE "memberships"."user_id" = ? AND "messages"."id" = ? LIMIT 1"#);
        query_one(conn, &sql, [user_id, id], Self::from_row)?.or_not_found("Message")
    }

    // Message::Pagination

    /// `room.messages.last_page`: the newest 40, oldest first.
    pub fn last_page(conn: &Connection, room_id: i64) -> Result<Vec<Self>> {
        let sql = format!(r#"{SELECT_IN_ROOM} ORDER BY "messages"."created_at" DESC LIMIT {PAGE_SIZE}"#);
        Ok(reversed(query_all(conn, &sql, [room_id], Self::from_row)?))
    }

    /// `room.messages.first_page`
    pub fn first_page(conn: &Connection, room_id: i64) -> Result<Vec<Self>> {
        let sql = format!(r#"{SELECT_IN_ROOM} ORDER BY "messages"."created_at" ASC LIMIT {PAGE_SIZE}"#);
        query_all(conn, &sql, [room_id], Self::from_row)
    }

    /// `room.messages.page_before(message)`
    pub fn page_before(conn: &Connection, room_id: i64, message: &Message) -> Result<Vec<Self>> {
        let sql = format!(r#"{SELECT_IN_ROOM} AND (created_at < ?) ORDER BY "messages"."created_at" DESC LIMIT {PAGE_SIZE}"#);
        Ok(reversed(query_all(conn, &sql, params![room_id, message.created_at], Self::from_row)?))
    }

    /// `room.messages.page_after(message)`
    pub fn page_after(conn: &Connection, room_id: i64, message: &Message) -> Result<Vec<Self>> {
        let sql = format!(r#"{SELECT_IN_ROOM} AND (created_at > ?) ORDER BY "messages"."created_at" ASC LIMIT {PAGE_SIZE}"#);
        query_all(conn, &sql, params![room_id, message.created_at], Self::from_row)
    }

    /// `room.messages.page_around(message)`: up to 40 before, the message, up to 40 after.
    pub fn page_around(conn: &Connection, room_id: i64, message: &Message) -> Result<Vec<Self>> {
        let mut page = Self::page_before(conn, room_id, message)?;
        page.push(message.clone());
        page.extend(Self::page_after(conn, room_id, message)?);
        Ok(page)
    }

    /// `room.messages.page_created_since(time)`
    pub fn page_created_since(conn: &Connection, room_id: i64, time: Timestamp) -> Result<Vec<Self>> {
        let sql = format!(r#"{SELECT_IN_ROOM} AND (created_at > ?) ORDER BY "messages"."created_at" ASC LIMIT {PAGE_SIZE}"#);
        query_all(conn, &sql, params![room_id, time], Self::from_row)
    }

    /// `room.messages.without(excluding).page_updated_since(time)`
    pub fn page_updated_since(conn: &Connection, room_id: i64, time: Timestamp, excluding: &[i64]) -> Result<Vec<Self>> {
        let sql = page_updated_since_sql(excluding.len());
        let mut values: Vec<rusqlite::types::Value> = vec![room_id.into()];
        values.extend(excluding.iter().map(|id| rusqlite::types::Value::from(*id)));
        values.push(time.to_db().into());
        Ok(reversed(query_all(conn, &sql, rusqlite::params_from_iter(values), Self::from_row)?))
    }

    /// `room.messages.before(message).exists?`
    pub fn exists_before(conn: &Connection, room_id: i64, message: &Message) -> Result<bool> {
        sql::exists(
            conn,
            r#"SELECT 1 FROM "messages" WHERE "messages"."room_id" = ? AND (created_at < ?) LIMIT 1"#,
            params![room_id, message.created_at],
        )
    }

    /// `room.messages.after(message).exists?`
    pub fn exists_after(conn: &Connection, room_id: i64, message: &Message) -> Result<bool> {
        sql::exists(
            conn,
            r#"SELECT 1 FROM "messages" WHERE "messages"."room_id" = ? AND (created_at > ?) LIMIT 1"#,
            params![room_id, message.created_at],
        )
    }

    /// `room.messages.paged?`: more than a page (`count > PAGE_SIZE`). Asks whether a row exists
    /// past the first page rather than counting the whole room, which grows without bound.
    pub fn paged(conn: &Connection, room_id: i64) -> Result<bool> {
        sql::exists(conn, &format!(r#"SELECT 1 FROM "messages" WHERE "messages"."room_id" = ? LIMIT 1 OFFSET {PAGE_SIZE}"#), [room_id])
    }

    // Message::Searchable

    /// `room.messages.search(query)`: FTS5 `MATCH`, in message order.
    pub fn search_in_room(conn: &Connection, room_id: i64, query: &str) -> Result<Vec<Self>> {
        let query = match_terms(query);
        if query.is_empty() {
            return Ok(Vec::new());
        }
        query_all(
            conn,
            concat!(
                "SELECT ",
                message_columns!(),
                r#" FROM "messages" join message_search_index idx on messages.id = idx.rowid WHERE "messages"."room_id" = ? AND (idx.body match ?) ORDER BY "messages"."created_at" ASC"#
            ),
            params![room_id, query],
            Self::from_row,
        )
    }

    /// `Current.user.reachable_messages.search(query).last(100)`
    pub fn search_reachable(conn: &Connection, user_id: i64, query: &str) -> Result<Vec<Self>> {
        let query = match_terms(query);
        if query.is_empty() {
            return Ok(Vec::new());
        }
        if let Some(messages) = search_index_page(conn, user_id, &query)? {
            return Ok(messages);
        }
        // A sparse membership set can miss most global matches. Let SQLite choose the
        // room-first plan instead of scanning the remaining inaccessible history.
        let sql = format!(
            r#"{SELECT_REACHABLE} join message_search_index idx on messages.id = idx.rowid WHERE "memberships"."user_id" = ? AND (idx.body match ?) ORDER BY "messages"."id" DESC LIMIT 100"#
        );
        Ok(reversed(query_all(conn, &sql, params![user_id, query], Self::from_row)?))
    }

    // Creating, updating, destroying

    /// `room.messages.create!` (or `create_with_attachment!` given a blob), in one transaction:
    /// the message, its body (which touches the message), its attachment, the room touch, the
    /// search index and `room.receive`'s unread memberships, so that a message is never
    /// committed without them. The push job follows the commit. Rails' reference indexes and
    /// marks unread after commit, in commits of their own; current Rails does them in the
    /// creating transaction too.
    pub fn create(tx: &mut Tx<'_>, attributes: NewMessage) -> Result<Self> {
        Self::create_prepared(tx, attributes, None)
    }

    /// [`Message::create`] given the body's plain text, which a caller can compute ahead of the
    /// write for a body without attachments (only attachments read records), keeping that work
    /// off the writer. `None` computes it here.
    pub fn create_prepared(tx: &mut Tx<'_>, attributes: NewMessage, plain_text: Option<String>) -> Result<Self> {
        // Before any statement, so that the transaction's statements run back to back.
        let body_text = match (plain_text, &attributes.body) {
            (Some(text), _) => text,
            (None, Some(body)) => tx.rich_text().to_plain_text(tx.conn(), body),
            (None, None) => String::new(),
        };
        let now = tx.now();
        let client_message_id = attributes.client_message_id.unwrap_or_else(sql::uuid);
        let id: i64 = tx.conn().query_row_cached(
            r#"INSERT INTO "messages" ("client_message_id", "created_at", "creator_id", "room_id", "updated_at") VALUES (?, ?, ?, ?, ?) RETURNING "id""#,
            params![client_message_id, now, attributes.creator_id, attributes.room_id, now],
            |r| r.get(0),
        )?;
        let mut message = Self {
            id,
            room_id: attributes.room_id,
            creator_id: attributes.creator_id,
            client_message_id,
            created_at: now,
            updated_at: now,
        };

        let mut touched = false;
        if let Some(body) = &attributes.body {
            RichTextRecord::create(tx, RECORD_TYPE, id, "body", body)?;
            touched = true;
        }
        if let Some(blob_id) = attributes.attachment_blob_id {
            Attachment::create(tx, RECORD_TYPE, id, "attachment", blob_id)?;
            touched = true;
        }
        if touched {
            message.updated_at = Self::touch_row(tx, id)?;
        }
        Room::touch(tx, message.room_id)?;

        // `plain_text_body`: the body's text, else the attachment's filename.
        let index_text = if !body_text.trim().is_empty() {
            body_text
        } else if attributes.attachment_blob_id.is_some() {
            message.attachment(tx.conn())?.map(|(_, blob)| blob.filename).unwrap_or_default()
        } else {
            String::new()
        };
        tx.conn().execute_cached("insert into message_search_index(rowid, body) values (?, ?)", params![message.id, index_text])?;
        Room::receive(tx, message.room_id, &message)?;
        Ok(message)
    }

    /// `message.update!(body:)`: the body changes, which touches the message and its room;
    /// the search index follows after commit.
    pub fn update_body(&mut self, tx: &mut Tx<'_>, body: &str) -> Result<()> {
        match RichTextRecord::find_for(tx.conn(), RECORD_TYPE, self.id, "body")? {
            Some(record) if record.body.as_deref() == Some(body) => return Ok(()),
            Some(mut record) => record.update_body(tx, body)?,
            None => {
                RichTextRecord::create(tx, RECORD_TYPE, self.id, "body", body)?;
            }
        }
        self.touch(tx)
    }

    /// `touch` (from a boost, or the body): the message, then its room, then a reindex
    /// after commit.
    pub fn touch(&mut self, tx: &mut Tx<'_>) -> Result<()> {
        self.updated_at = Self::touch_row(tx, self.id)?;
        Room::touch(tx, self.room_id)?;
        let id = self.id;
        tx.after_commit(move |tx| Message::find(tx.conn(), id)?.update_in_index(tx));
        Ok(())
    }

    fn touch_row(tx: &Tx<'_>, id: i64) -> Result<Timestamp> {
        let now = tx.now();
        tx.conn().execute_cached(r#"UPDATE "messages" SET "updated_at" = ? WHERE "messages"."id" = ?"#, params![now, id])?;
        Ok(now)
    }

    /// `update!(attachment: blob or nil)`: `has_one_attached` replaces the attachment. The old
    /// one is destroyed (its blob purged after commit, `dependent: :purge_later`) and each
    /// attachment change touches the message (`belongs_to :record, touch: true`), and so its room.
    pub fn replace_attachment(&mut self, tx: &mut Tx<'_>, blob_id: Option<i64>) -> Result<()> {
        if let Some(attachment) = Attachment::find_for(tx.conn(), RECORD_TYPE, self.id, "attachment")? {
            attachment.delete(tx)?;
            tx.emit_after_commit(Event::PurgeBlob { blob_id: attachment.blob_id });
            self.touch(tx)?;
        }
        if let Some(blob_id) = blob_id {
            Attachment::create(tx, RECORD_TYPE, self.id, "attachment", blob_id)?;
            self.touch(tx)?;
        }
        Ok(())
    }

    /// `destroy`: its attachment (blob purged later), boosts and body, then the message, and
    /// the room is touched. The search index entry goes after commit.
    pub fn destroy(&self, tx: &mut Tx<'_>) -> Result<()> {
        if let Some(attachment) = Attachment::find_for(tx.conn(), RECORD_TYPE, self.id, "attachment")? {
            attachment.delete(tx)?;
            tx.emit_after_commit(Event::PurgeBlob { blob_id: attachment.blob_id });
        }
        for boost in Boost::for_message(tx.conn(), self.id)? {
            boost.delete_row(tx)?;
        }
        if let Some(body) = RichTextRecord::find_for(tx.conn(), RECORD_TYPE, self.id, "body")? {
            body.delete(tx)?;
        }
        tx.conn().execute_cached(r#"DELETE FROM "messages" WHERE "messages"."id" = ?"#, [self.id])?;
        Room::touch(tx, self.room_id)?;
        let id = self.id;
        tx.after_commit(move |tx| remove_from_index(tx, id));
        Ok(())
    }

    fn update_in_index(&self, tx: &Tx<'_>) -> Result<()> {
        let body = self.plain_text_body(tx.conn(), tx.rich_text())?;
        tx.conn().execute_cached("update message_search_index set body = ? where rowid = ?", params![body, self.id])?;
        Ok(())
    }

    // Attributes and associations

    pub fn room(&self, conn: &Connection) -> Result<Room> {
        Room::find(conn, self.room_id)
    }

    pub fn creator(&self, conn: &Connection) -> Result<User> {
        User::find(conn, self.creator_id)
    }

    pub fn boosts(&self, conn: &Connection) -> Result<Vec<Boost>> {
        Boost::for_message_ordered(conn, self.id)
    }

    pub fn body(&self, conn: &Connection) -> Result<Option<RichTextRecord>> {
        RichTextRecord::find_for(conn, RECORD_TYPE, self.id, "body")
    }

    /// `message.body.body` as stored HTML, if any.
    pub fn body_html(&self, conn: &Connection) -> Result<Option<String>> {
        Ok(self.body(conn)?.and_then(|b| b.body))
    }

    /// The attachment and its blob, if attached.
    pub fn attachment(&self, conn: &Connection) -> Result<Option<(Attachment, Blob)>> {
        match Attachment::find_for(conn, RECORD_TYPE, self.id, "attachment")? {
            Some(attachment) => {
                let blob = attachment.blob(conn)?;
                Ok(Some((attachment, blob)))
            }
            None => Ok(None),
        }
    }

    /// `plain_text_body`: `body.to_plain_text.presence || attachment&.filename&.to_s || ""`
    pub fn plain_text_body(&self, conn: &Connection, rich_text: &dyn RichText) -> Result<String> {
        if let Some(html) = self.body_html(conn)? {
            let text = rich_text.to_plain_text(conn, &html);
            if !text.trim().is_empty() {
                return Ok(text);
            }
        }
        Ok(self.attachment(conn)?.map(|(_, blob)| blob.filename).unwrap_or_default())
    }

    /// `content_type`
    pub fn content_type(&self, conn: &Connection, rich_text: &dyn RichText) -> Result<ContentType> {
        if self.attachment(conn)?.is_some() {
            Ok(ContentType::Attachment)
        } else if self.sound(conn, rich_text)?.is_some() {
            Ok(ContentType::Sound)
        } else {
            Ok(ContentType::Text)
        }
    }

    /// `sound`: a body of exactly `/play <name>` naming a built-in sound.
    pub fn sound(&self, conn: &Connection, rich_text: &dyn RichText) -> Result<Option<&'static Sound>> {
        Ok(sound_in(&self.plain_text_body(conn, rich_text)?))
    }

    /// `mentionees`: mentioned users who are members of the room.
    pub fn mentionees(&self, conn: &Connection, rich_text: &dyn RichText) -> Result<Vec<User>> {
        let ids = match self.body_html(conn)? {
            Some(html) => rich_text.mentioned_user_ids(conn, &html),
            None => Vec::new(),
        };
        mentionees_in_room(conn, self.room_id, &ids)
    }

    pub fn reload(&mut self, conn: &Connection) -> Result<()> {
        *self = Self::find(conn, self.id)?;
        Ok(())
    }
}

/// `room.users.where(id: ids)`
pub fn mentionees_in_room(conn: &Connection, room_id: i64, user_ids: &[i64]) -> Result<Vec<User>> {
    if user_ids.is_empty() {
        return Ok(Vec::new());
    }
    let sql = format!(
        r#"{} INNER JOIN "memberships" ON "users"."id" = "memberships"."user_id" WHERE "memberships"."room_id" = ? AND "users"."id" IN ({})"#,
        User::SELECT,
        placeholders(user_ids.len())
    );
    let values: Vec<i64> = std::iter::once(room_id).chain(user_ids.iter().copied()).collect();
    query_all(conn, &sql, rusqlite::params_from_iter(values), User::from_row)
}

/// `plain_text_body.match(/\A\/play (?<name>\w+)\z/)` then `Sound.find_by_name`.
pub fn sound_in(plain_text: &str) -> Option<&'static Sound> {
    let name = plain_text.strip_prefix("/play ")?;
    // `\z` allows no trailing newline; `\w` is ASCII word characters.
    if name.is_empty() || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
        return None;
    }
    Sound::find_by_name(name)
}

fn remove_from_index(tx: &Tx<'_>, id: i64) -> Result<()> {
    tx.conn().execute_cached("delete from message_search_index where rowid = ?", [id])?;
    Ok(())
}

/// [`Message::search_reachable`]'s query: the user's id, then the `MATCH` terms. Ordered by the
/// index's rowid, which is the message id, so SQLite walks the matches newest first and stops at
/// the page; ordering by `created_at` collected and sorted every match first. Ids follow
/// `created_at`: messages are only inserted by [`Message::create`], on the one writer thread,
/// stamped inside their transaction.
const SEARCH_PROBE_LIMIT: usize = 1000;

pub(crate) fn search_reachable_sql() -> String {
    format!(
        "SELECT {}, memberships.user_id IS NOT NULL AS reachable FROM messages INNER JOIN rooms ON messages.room_id = rooms.id LEFT JOIN memberships ON rooms.id = memberships.room_id AND memberships.user_id = ? JOIN message_search_index idx ON messages.id = idx.rowid WHERE idx.body MATCH ? ORDER BY idx.rowid DESC LIMIT {SEARCH_PROBE_LIMIT}",
        message_columns!()
    )
}

fn search_index_page(conn: &Connection, user_id: i64, query: &str) -> Result<Option<Vec<Message>>> {
    let mut stmt = conn.prepare_cached(&search_reachable_sql())?;
    let mut rows = stmt.query(params![user_id, query])?;
    let mut messages = Vec::new();
    let mut examined = 0;
    while let Some(row) = rows.next()? {
        examined += 1;
        if row.get::<_, bool>("reachable")? {
            messages.push(Message::from_row(row)?);
            if messages.len() == 100 {
                return Ok(Some(reversed(messages)));
            }
        }
    }
    // Exhausting the global matches also proves that a short page is complete.
    Ok((examined < SEARCH_PROBE_LIMIT).then(|| reversed(messages)))
}

/// [`Message::page_updated_since`]'s query, leaving out `excluding` ids: the room's id, those ids,
/// then the time. Sorting on `+created_at`, which no index covers, has SQLite find the messages
/// updated since through `(room_id, updated_at)` and sort just those, instead of walking the whole
/// room by `(room_id, created_at)` looking for them.
pub(crate) fn page_updated_since_sql(excluding: usize) -> String {
    let without = if excluding == 0 { String::new() } else { format!(r#" AND "messages"."id" NOT IN ({})"#, placeholders(excluding)) };
    format!(r#"{SELECT_IN_ROOM}{without} AND (updated_at > ?) ORDER BY +"messages"."created_at" DESC LIMIT {PAGE_SIZE}"#)
}

fn reversed<T>(mut rows: Vec<T>) -> Vec<T> {
    rows.reverse();
    rows
}

/// Each word of a search as an FTS5 string, so every word must appear and none is read as query
/// syntax. Rails passes the words straight to `MATCH`, where `NOT`, `AND`, `OR` or `NEAR` in the
/// wrong place is a syntax error (a 500).
/// NULs separate words too: SQLite would end the query string at one.
fn match_terms(query: &str) -> String {
    query
        .split(|c: char| c.is_whitespace() || c == '\0')
        .filter(|word| !word.is_empty())
        .map(|word| format!("\"{}\"", word.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(" ")
}
