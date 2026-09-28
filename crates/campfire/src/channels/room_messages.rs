//! `RoomMessagesChannel` (reference/app/channels/room_messages_channel.rb) and the
//! `RoomStreamsAreAuthorized` guard prepended onto `Turbo::StreamsChannel`
//! (reference/app/channels/concerns/room_streams_are_authorized.rb).
//!
//! A room's message stream (`<room gid param>:messages`) is only served here, and only to a
//! current member of the room the verified stream name points at.
use campfire_cable::turbo::StreamsChannel;
use campfire_cable::{Channel, ChannelError, ChannelResult, Params, Subscription};
use campfire_db::{Connection, Database, Room, RoomType};
use rails_compat::global_id::GlobalId;

use super::CableUser;

pub const STREAM_SUFFIX: &str = "messages";

/// `RoomMessagesChannel.guarded_stream?`: true for the stream names this channel guards, whoever
/// is asking. Also the guard on `Turbo::StreamsChannel`.
pub fn guarded_stream(stream_name: &str) -> bool {
    stream_name.split_once(':').map(|(_, suffix)| suffix) == Some(STREAM_SUFFIX)
}

/// `RoomMessagesChannel.subscribable_room(user, stream_name)`.
pub fn subscribable_room(conn: &Connection, user_id: i64, stream_name: &str) -> Result<Option<Room>, ChannelError> {
    let Some((gid_param, STREAM_SUFFIX)) = stream_name.split_once(':') else {
        return Ok(None);
    };
    match room_from(conn, gid_param)? {
        Some(room) => Ok(Room::find_for_user(conn, user_id, room.id)?),
        None => Ok(None),
    }
}

/// `GlobalID::Locator.locate gid_param, only: Room`, with `RecordNotFound` as nil.
fn room_from(conn: &Connection, gid_param: &str) -> Result<Option<Room>, ChannelError> {
    // `GlobalID.parse` takes the URI form or its Base64 param.
    let Some(gid) = GlobalId::parse(gid_param).or_else(|| GlobalId::from_param(gid_param)) else {
        return Ok(None);
    };
    // `find_allowed?(gid.model_class, only: Room)`: Room or one of its STI subclasses, whose
    // `find` also requires the type to match.
    let required_type = match gid.model_name.as_str() {
        "Room" => None,
        name => match RoomType::from_class_name(name) {
            Some(room_type) => Some(room_type),
            None if KNOWN_MODELS.contains(&name) => return Ok(None),
            // `model_name.constantize` raises NameError.
            None => return Err(ChannelError(format!("uninitialized constant {name}"))),
        },
    };
    let Ok(id) = gid.id.parse::<i64>() else {
        return Ok(None);
    };
    let room = Room::find_by_id(conn, id)?;
    Ok(room.filter(|room| required_type.is_none_or(|t| t == room.room_type)))
}

/// Constants a GID could name that aren't rooms (`only: Room` turns them away without raising).
const KNOWN_MODELS: &[&str] = &[
    "Account", "Ban", "Boost", "Current", "Membership", "Message", "Push::Subscription", "Search", "Session", "User", "Webhook",
];

pub struct RoomMessagesChannel {
    db: Database,
    streams: StreamsChannel,
}

impl RoomMessagesChannel {
    pub fn new(db: Database, streams: StreamsChannel) -> Self {
        Self { db, streams }
    }

    /// `authorized_stream_name`: the verified stream name, if present and for a room the user
    /// belongs to.
    async fn authorized_stream_name(&self, sub: &Subscription<CableUser>) -> ChannelResult<Option<String>> {
        let Some(stream_name) = self.streams.verified_stream_name_from_params(&sub.params())? else {
            return Ok(None);
        };
        if stream_name.trim().is_empty() {
            return Ok(None);
        }
        let user_id = sub.current_user().id;
        let name = stream_name.clone();
        let room = self
            .db
            .read(move |conn| Ok(subscribable_room(conn, user_id, &name)))
            .await
            .map_err(|error| ChannelError(error.to_string()))??;
        Ok(room.map(|_| stream_name))
    }
}

impl Channel<CableUser> for RoomMessagesChannel {
    async fn subscribed(&mut self, sub: &mut Subscription<CableUser>) -> ChannelResult {
        match self.authorized_stream_name(sub).await? {
            Some(stream_name) => sub.stream_from(stream_name),
            None => sub.reject(),
        }
        Ok(())
    }

    /// Its public methods: `subscribed`, and `verified_stream_name_from_params` from
    /// `include Turbo::Streams::StreamName::ClassMethods` (which returns without transmitting).
    async fn perform(&mut self, action: &str, _data: &Params, sub: &mut Subscription<CableUser>) -> ChannelResult<bool> {
        if sub.rejected() {
            return Ok(false);
        }
        match action {
            "subscribed" => self.subscribed(sub).await.map(|()| true),
            "verified_stream_name_from_params" => self.streams.verified_stream_name_from_params(&sub.params()).map(|_| true),
            _ => Ok(false),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guards_only_message_streams() {
        assert!(guarded_stream("Z2lkOi8vY2FtcGZpcmUvUm9vbXM6Ok9wZW4vMQ:messages"));
        assert!(!guarded_stream("rooms"));
        assert!(!guarded_stream("Z2lk:rooms"));
        assert!(!guarded_stream("Z2lk:messages:more"));
        assert!(!guarded_stream(""));
        assert!(guarded_stream(":messages"));
    }
}
