//! `RoomChannel` (reference/app/channels/room_channel.rb), and the room lookup that
//! `PresenceChannel` and `TypingNotificationsChannel` inherit from it.
use campfire_cable::{Channel, ChannelResult, Params, Subscription};
use campfire_db::{Database, Room};
use serde_json::Value;

use super::{CableUser, room_gid};

pub struct RoomChannel {
    db: Database,
    room: Option<Room>,
}

impl RoomChannel {
    pub fn new(db: Database) -> Self {
        Self { db, room: None }
    }
}

/// `RoomChannel#subscribed`: `stream_for @room` if the user is a member, else `reject`.
pub async fn subscribe(db: &Database, sub: &mut Subscription<CableUser>) -> ChannelResult<Option<Room>> {
    let room = find_room(db, sub).await?;
    match &room {
        Some(room) => sub.stream_for(&[&room_gid(room).to_param()]),
        None => sub.reject(),
    }
    Ok(room)
}

/// `current_user.rooms.find_by(id: params[:room_id])`.
async fn find_room(db: &Database, sub: &Subscription<CableUser>) -> ChannelResult<Option<Room>> {
    let Some(room_id) = sub.param("room_id").as_ref().and_then(cast_id) else {
        return Ok(None);
    };
    let user_id = sub.current_user().id;
    Ok(db.read(move |conn| Room::find_for_user(conn, user_id, room_id)).await?)
}

/// How Active Record casts a value for an integer `id` condition: numbers are truncated,
/// booleans are 1 and 0, and strings go through `to_i` unless they don't start like a number
/// (`non_numeric_string?`), which matches nothing. So does anything out of range.
pub fn cast_id(value: &Value) -> Option<i64> {
    match value {
        Value::Number(n) => n.as_i64().or_else(|| n.as_f64().filter(|f| f.is_finite() && f.abs() < 9.2e18).map(|f| f.trunc() as i64)),
        Value::Bool(b) => Some(i64::from(*b)),
        Value::String(s) => ruby_to_i(s),
        _ => None,
    }
}

/// `String#to_i`, for strings matching `/\A\s*[+-]?\d/`; `None` for the rest or on overflow.
fn ruby_to_i(s: &str) -> Option<i64> {
    let s = s.trim_start_matches(|c: char| c.is_ascii_whitespace());
    let (negative, digits) = match s.as_bytes().first() {
        Some(b'-') => (true, &s[1..]),
        Some(b'+') => (false, &s[1..]),
        _ => (false, s),
    };
    if !digits.starts_with(|c: char| c.is_ascii_digit()) {
        return None;
    }
    let mut value: i64 = 0;
    let mut previous_underscore = false;
    for c in digits.chars() {
        match c {
            '0'..='9' => {
                value = value.checked_mul(10)?.checked_add(i64::from(c as u8 - b'0'))?;
                previous_underscore = false;
            }
            // Ruby allows single underscores between digits.
            '_' if !previous_underscore => previous_underscore = true,
            _ => break,
        }
    }
    Some(if negative { -value } else { value })
}

impl Channel<CableUser> for RoomChannel {
    async fn subscribed(&mut self, sub: &mut Subscription<CableUser>) -> ChannelResult {
        self.room = subscribe(&self.db, sub).await?;
        Ok(())
    }

    async fn perform(&mut self, action: &str, _data: &Params, sub: &mut Subscription<CableUser>) -> ChannelResult<bool> {
        match action {
            "subscribed" if !sub.rejected() => self.subscribed(sub).await.map(|()| true),
            _ => Ok(false),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn casts_ids_like_active_record() {
        assert_eq!(cast_id(&json!(5)), Some(5));
        assert_eq!(cast_id(&json!(5.9)), Some(5));
        assert_eq!(cast_id(&json!("5")), Some(5));
        assert_eq!(cast_id(&json!(" 12abc")), Some(12));
        assert_eq!(cast_id(&json!("1_000")), Some(1000));
        assert_eq!(cast_id(&json!("-1")), Some(-1));
        assert_eq!(cast_id(&json!("abc")), None);
        assert_eq!(cast_id(&json!("")), None);
        assert_eq!(cast_id(&json!(true)), Some(1));
        assert_eq!(cast_id(&json!(null)), None);
        assert_eq!(cast_id(&json!("99999999999999999999")), None);
    }
}
