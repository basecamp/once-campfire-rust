//! `TypingNotificationsChannel` (reference/app/channels/typing_notifications_channel.rb).
use campfire_cable::{Channel, ChannelError, ChannelResult, Params, Subscription};
use campfire_db::{Database, Room};
use serde::Serialize;

use super::{CableUser, room, room_gid};

pub struct TypingNotificationsChannel {
    db: Database,
    room: Option<Room>,
}

impl TypingNotificationsChannel {
    pub fn new(db: Database) -> Self {
        Self { db, room: None }
    }

    /// `broadcast_to @room, action:, user: current_user.slice(:id, :name)`.
    fn broadcast(&self, action: &'static str, sub: &Subscription<CableUser>) -> ChannelResult {
        // `@room` is only nil when `subscribed` failed, and Rails raises NoMethodError.
        let room = self.room.as_ref().ok_or_else(|| ChannelError("undefined method 'to_gid_param' for nil".into()))?;
        let user = sub.current_user();
        let payload = Payload { action, user: UserAttributes { id: user.id, name: &user.name } };
        sub.broadcast_to(&[&room_gid(room).to_param()], &payload);
        Ok(())
    }
}

#[derive(Serialize)]
pub struct Payload<'a> {
    pub action: &'static str,
    pub user: UserAttributes<'a>,
}

#[derive(Serialize)]
pub struct UserAttributes<'a> {
    pub id: i64,
    pub name: &'a str,
}

impl Channel<CableUser> for TypingNotificationsChannel {
    async fn subscribed(&mut self, sub: &mut Subscription<CableUser>) -> ChannelResult {
        self.room = room::subscribe(&self.db, sub).await?;
        Ok(())
    }

    async fn perform(&mut self, action: &str, _data: &Params, sub: &mut Subscription<CableUser>) -> ChannelResult<bool> {
        if sub.rejected() {
            return Ok(false);
        }
        match action {
            "start" => self.broadcast("start", sub)?,
            "stop" => self.broadcast("stop", sub)?,
            "subscribed" => self.room = room::subscribe(&self.db, sub).await?,
            _ => return Ok(false),
        }
        Ok(true)
    }
}
