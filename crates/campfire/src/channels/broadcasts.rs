//! Every broadcast the Rails app makes, with the stream names, targets and `<turbo-stream>`
//! markup its Turbo and Action Cable calls produce. The HTML inside comes from [`Partials`].
//!
//! Stream names: records are their GID param (`turbo_stream_from @room, :messages` is
//! `<room gid param>:messages`), symbols themselves. Targets are `dom_id`s: STI rooms use their
//! own `param_key` (`messages_rooms_open_1`), and a message's `to_key` is its
//! `client_message_id` (`message_<uuid>`).
use campfire_cable::turbo::{Action, Target};
use campfire_db::{Boost, Connection, Involvement, Membership, Message, Room};
use serde::Serialize;

use super::{Cable, read_rooms, room_gid, unread_rooms, user_gid};

/// The partials Turbo renders for broadcasts (`ApplicationController.render(partial:, locals:)`,
/// html format, no request). Each returns the rendered HTML.
pub trait Partials: Send + Sync {
    /// `messages/_message` with `message:`.
    fn message(&self, message: &Message) -> String;
    /// `messages/_presentation` with `message:`.
    fn message_presentation(&self, message: &Message) -> String;
    /// `messages/boosts/_boost` with `boost:`.
    fn boost(&self, boost: &Boost) -> String;
    /// `users/sidebars/rooms/_shared` with `room:`.
    fn shared_room(&self, room: &Room) -> String;
    /// `users/sidebars/rooms/_direct` with `membership:`.
    fn direct_room(&self, membership: &Membership) -> String;
}

/// `dom_id(record, prefix)`: the views' own, so a broadcast's target is the id a template renders.
pub use campfire_views::helpers::dom_id;

/// `Room.model_name.param_key` for the room's STI class: `Rooms::Open` is `rooms_open`.
pub fn room_param_key(room: &Room) -> String {
    room.room_type.class_name().replace("::", "_").to_ascii_lowercase()
}

/// `dom_id(room, prefix)`.
pub fn room_dom_id(room: &Room, prefix: &str) -> String {
    dom_id(&room_param_key(room), room.id, Some(prefix))
}

/// `dom_id(message, prefix)`: messages are keyed by `client_message_id`.
pub fn message_dom_id(message: &Message, prefix: Option<&str>) -> String {
    dom_id("message", &message.client_message_id, prefix)
}

pub const ROOMS: &str = "rooms";
/// How far a room's slowest reader may fall behind before a post waits for it.
pub const SETTLE_LAG: u64 = 256;
pub const MESSAGES: &str = "messages";
const MAINTAIN_SCROLL: &[(&str, Option<&str>)] = &[("maintain_scroll", Some("true"))];

/// `ActionCable.server.broadcast "user_#{id}_reads", { room_id: }`
/// (reference/app/channels/presence_channel.rb).
pub fn read_room(server: &Cable, user_id: i64, room_id: i64) -> usize {
    #[derive(Serialize)]
    struct ReadRoom {
        room_id: i64,
    }
    server.broadcast(&read_rooms::stream_name_for(user_id), &ReadRoom { room_id })
}

#[derive(Clone)]
pub struct Broadcasts {
    server: Cable,
}

impl Broadcasts {
    pub fn new(server: Cable) -> Self {
        Self { server }
    }

    fn room_messages(room: &Room) -> [String; 2] {
        [room_gid(room).to_param(), MESSAGES.to_string()]
    }

    fn user_rooms(user_id: i64) -> [String; 2] {
        [user_gid(user_id).to_param(), ROOMS.to_string()]
    }

    fn to(&self, streamables: &[String], action: Action, target: &str, html: Option<&str>, attributes: &[(&str, Option<&str>)]) {
        let streamables: Vec<&str> = streamables.iter().map(String::as_str).collect();
        self.server.broadcast_action_to(&streamables, action, Target::Target(target), html, attributes);
    }

    // Message::Broadcasts (reference/app/models/message/broadcasts.rb)

    /// `message.broadcast_create`: append the message to the room, then tell every member's
    /// unread stream (`room.memberships.pluck(:user_id)`). Used by MessagesController#create,
    /// Webhook replies, and `Messages::ByBotsController`.
    pub fn message_create(&self, conn: &Connection, room: &Room, message: &Message, partials: &dyn Partials) -> campfire_db::Result<()> {
        let html = partials.message(message);
        self.to(&Self::room_messages(room), Action::Append, &room_dom_id(room, MESSAGES), Some(&html), &[]);
        self.unread_room(conn, room)
    }

    /// Waits while a subscriber of the room's messages stream is more than [`SETTLE_LAG`]
    /// messages behind, for at most ten seconds: the poster waits for the room's readers rather
    /// than outrunning them, so they are not disconnected for lagging, and their queues stay short.
    pub async fn settle_messages(&self, room: &Room) {
        let [gid, messages] = Self::room_messages(room);
        let name = campfire_cable::naming::stream_name_from(&[&gid, &messages]);
        let hub = self.server.hub();
        hub.settle(&name, SETTLE_LAG.min(hub.limits().frames / 2), std::time::Duration::from_secs(10)).await;
    }

    /// `broadcast_unread_room`: `{ roomId: }` to each member's `user_<id>_unreads`.
    pub fn unread_room(&self, conn: &Connection, room: &Room) -> campfire_db::Result<()> {
        #[derive(Serialize)]
        struct UnreadRoom {
            #[serde(rename = "roomId")]
            room_id: i64,
        }
        for membership in Membership::for_room(conn, room.id)? {
            self.server.broadcast(&unread_rooms::stream_name_for(membership.user_id), &UnreadRoom { room_id: room.id });
        }
        Ok(())
    }

    /// `message.broadcast_remove`: MessagesController#destroy and `User#remove_banned_content`.
    pub fn message_remove(&self, room: &Room, message: &Message) {
        self.to(&Self::room_messages(room), Action::Remove, &message_dom_id(message, None), None, &[]);
    }

    /// MessagesController#update: replace `[message, :presentation]` with
    /// `messages/_presentation`, keeping the scroll position.
    pub fn message_replace(&self, room: &Room, message: &Message, partials: &dyn Partials) {
        let html = partials.message_presentation(message);
        let target = message_dom_id(message, Some("presentation"));
        self.to(&Self::room_messages(room), Action::Replace, &target, Some(&html), MAINTAIN_SCROLL);
    }

    // Messages::BoostsController (and its ByBots subclass)

    /// `broadcast_create`: append to `boosts_message_<client_message_id>`.
    pub fn boost_create(&self, room: &Room, message: &Message, boost: &Boost, partials: &dyn Partials) {
        let html = partials.boost(boost);
        let target = format!("boosts_message_{}", message.client_message_id);
        self.to(&Self::room_messages(room), Action::Append, &target, Some(&html), MAINTAIN_SCROLL);
    }

    /// `broadcast_remove`: `dom_id(boost)`.
    pub fn boost_remove(&self, room: &Room, boost: &Boost) {
        self.to(&Self::room_messages(room), Action::Remove, &dom_id("boost", boost.id, None), None, &[]);
    }

    // The sidebar's room lists (users/sidebars/show.html.erb streams from `:rooms` and
    // `[Current.user, :rooms]`).

    /// RoomsController#destroy: remove `[room, :list]` from everyone's `:rooms`.
    pub fn room_remove(&self, room: &Room) {
        self.to(&[ROOMS.to_string()], Action::Remove, &room_dom_id(room, "list"), None, &[]);
    }

    /// Rooms::OpensController#create: prepend to everyone's `shared_rooms`.
    pub fn open_room_create(&self, room: &Room, partials: &dyn Partials) {
        let html = partials.shared_room(room);
        self.to(&[ROOMS.to_string()], Action::Prepend, "shared_rooms", Some(&html), &[]);
    }

    /// Rooms::OpensController#update: replace `[room, :list]` on `:rooms`. `room` is the room as
    /// an open room (`becomes!(Rooms::Open)`), so the target names that class even when the
    /// room was closed before.
    pub fn open_room_update(&self, room: &Room, partials: &dyn Partials) {
        let html = partials.shared_room(room);
        self.to(&[ROOMS.to_string()], Action::Replace, &room_dom_id(room, "list"), Some(&html), &[]);
    }

    /// Rooms::ClosedsController#create: render once, prepend to each member's own stream
    /// (`room.users`).
    pub fn closed_room_create(&self, conn: &Connection, room: &Room, partials: &dyn Partials) -> campfire_db::Result<()> {
        let html = partials.shared_room(room);
        for user_id in room.user_ids(conn)? {
            self.to(&Self::user_rooms(user_id), Action::Prepend, "shared_rooms", Some(&html), &[]);
        }
        Ok(())
    }

    /// Rooms::ClosedsController#update: after `memberships.revise`, replace `[room, :list]` for
    /// each remaining member (`room` as a closed room).
    pub fn closed_room_update(&self, conn: &Connection, room: &Room, partials: &dyn Partials) -> campfire_db::Result<()> {
        let html = partials.shared_room(room);
        let target = room_dom_id(room, "list");
        for user_id in room.user_ids(conn)? {
            self.to(&Self::user_rooms(user_id), Action::Replace, &target, Some(&html), &[]);
        }
        Ok(())
    }

    /// Rooms::DirectsController#create: prepend `users/sidebars/rooms/_direct` to each member's
    /// `direct_rooms`, rendered per membership.
    pub fn direct_room_create(&self, conn: &Connection, room: &Room, partials: &dyn Partials) -> campfire_db::Result<()> {
        for membership in room.memberships(conn)? {
            let html = partials.direct_room(&membership);
            self.to(&Self::user_rooms(membership.user_id), Action::Prepend, "direct_rooms", Some(&html), &[]);
        }
        Ok(())
    }

    /// Rooms::InvolvementsController#update (`broadcast_visibility_changes`). `previous` is
    /// `involvement_previously_was` (the current value when the update changed nothing). Rails
    /// raises NoMethodError on a nil previous involvement (`nil.inquiry`) after the update has
    /// saved; that's the `Err`.
    pub fn involvement_change(
        &self,
        room: &Room,
        membership: &Membership,
        previous: Option<Involvement>,
        partials: &dyn Partials,
    ) -> Result<(), NilInquiry> {
        if room.direct() {
            return Ok(());
        }
        let streamables = Self::user_rooms(membership.user_id);
        if membership.involved_in(Involvement::Invisible) {
            self.to(&streamables, Action::Remove, &room_dom_id(room, "list"), None, &[]);
        } else if previous.ok_or(NilInquiry)? == Involvement::Invisible {
            let html = partials.shared_room(room);
            self.to(&streamables, Action::Prepend, "shared_rooms", Some(&html), &[]);
        }
        Ok(())
    }
}

/// `nil.inquiry`: NoMethodError.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NilInquiry;

impl std::fmt::Display for NilInquiry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("undefined method 'inquiry' for nil")
    }
}

impl std::error::Error for NilInquiry {}
