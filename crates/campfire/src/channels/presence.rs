//! `PresenceChannel` (reference/app/channels/presence_channel.rb): a `RoomChannel` that marks the
//! membership connected while subscribed (`Membership::Connectable`) and tells the user's other
//! windows the room has been read.
use campfire_cable::{Channel, ChannelError, ChannelResult, Params, Subscription};
use campfire_db::{Database, Membership, Room};

use super::broadcasts::read_room;
use super::{CableUser, room};

pub struct PresenceChannel {
    db: Database,
    room: Option<Room>,
}

impl PresenceChannel {
    pub fn new(db: Database) -> Self {
        Self { db, room: None }
    }

    /// `present`: `membership.present`, then `broadcast_read_room`.
    async fn present(&self, sub: &Subscription<CableUser>) -> ChannelResult {
        self.with_membership(sub, |membership, tx| membership.present(tx)).await?;
        // `membership.room_id` finds the membership again.
        let room_id = self.membership(sub).await?.room_id;
        read_room(sub.server(), sub.current_user().id, room_id);
        Ok(())
    }

    /// `absent`: `membership.disconnected`.
    async fn absent(&self, sub: &Subscription<CableUser>) -> ChannelResult {
        self.with_membership(sub, |membership, tx| membership.disconnected(tx)).await
    }

    /// `refresh`: `membership.refresh_connection`.
    async fn refresh(&self, sub: &Subscription<CableUser>) -> ChannelResult {
        self.with_membership(sub, |membership, tx| membership.refresh_connection(tx)).await
    }

    /// `@room.memberships.find_by(user: current_user)`, which is nil (and so raises
    /// NoMethodError) once the membership is gone.
    async fn membership(&self, sub: &Subscription<CableUser>) -> ChannelResult<Membership> {
        let (room_id, user_id) = self.ids(sub)?;
        self.db.read(move |conn| Membership::find_by_room_and_user(conn, room_id, user_id)).await?.ok_or_else(nil_membership)
    }

    async fn with_membership(
        &self,
        sub: &Subscription<CableUser>,
        change: impl FnOnce(&mut Membership, &mut campfire_db::Tx<'_>) -> campfire_db::Result<()> + Send + 'static,
    ) -> ChannelResult {
        let (room_id, user_id) = self.ids(sub)?;
        let found = self
            .db
            .write(move |tx| match Membership::find_by_room_and_user(tx.conn(), room_id, user_id)? {
                Some(mut membership) => change(&mut membership, tx).map(|()| true),
                None => Ok(false),
            })
            .await?;
        if found { Ok(()) } else { Err(nil_membership()) }
    }

    fn ids(&self, sub: &Subscription<CableUser>) -> ChannelResult<(i64, i64)> {
        // `@room` is only nil after a rejection, when these callbacks don't run.
        let room = self.room.as_ref().ok_or_else(|| ChannelError("undefined method 'memberships' for nil".into()))?;
        Ok((room.id, sub.current_user().id))
    }
}

fn nil_membership() -> ChannelError {
    ChannelError("undefined method for nil (membership)".into())
}

impl Channel<CableUser> for PresenceChannel {
    /// `subscribed`, then `on_subscribe :present, unless: :subscription_rejected?`.
    async fn subscribed(&mut self, sub: &mut Subscription<CableUser>) -> ChannelResult {
        self.room = room::subscribe(&self.db, sub).await?;
        if !sub.rejected() {
            self.present(sub).await?;
        }
        Ok(())
    }

    /// `on_unsubscribe :absent, unless: :subscription_rejected?`.
    async fn unsubscribed(&mut self, sub: &mut Subscription<CableUser>) -> ChannelResult {
        if !sub.rejected() {
            self.absent(sub).await?;
        }
        Ok(())
    }

    async fn perform(&mut self, action: &str, _data: &Params, sub: &mut Subscription<CableUser>) -> ChannelResult<bool> {
        if sub.rejected() {
            return Ok(false);
        }
        match action {
            "present" => self.present(sub).await?,
            "absent" => self.absent(sub).await?,
            "refresh" => self.refresh(sub).await?,
            "subscribed" => self.room = room::subscribe(&self.db, sub).await?,
            _ => return Ok(false),
        }
        Ok(true)
    }
}
