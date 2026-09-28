//! `UnreadRoomsChannel` (reference/app/channels/unread_rooms_channel.rb).
use campfire_cable::{Channel, ChannelResult, Params, Subscription};

use super::CableUser;

pub struct UnreadRoomsChannel;

/// `UnreadRoomsChannel.stream_name_for(user_id)`: per user, so activity in a room only reaches
/// its members.
pub fn stream_name_for(user_id: i64) -> String {
    format!("user_{user_id}_unreads")
}

impl Channel<CableUser> for UnreadRoomsChannel {
    async fn subscribed(&mut self, sub: &mut Subscription<CableUser>) -> ChannelResult {
        let stream = stream_name_for(sub.current_user().id);
        sub.stream_from(stream);
        Ok(())
    }

    /// `subscribed` is public, so it's an action too.
    async fn perform(&mut self, action: &str, _data: &Params, sub: &mut Subscription<CableUser>) -> ChannelResult<bool> {
        match action {
            "subscribed" => self.subscribed(sub).await.map(|()| true),
            _ => Ok(false),
        }
    }
}
