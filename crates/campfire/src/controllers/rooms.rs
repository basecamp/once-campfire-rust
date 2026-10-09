//! `RoomsController` (reference/app/controllers/rooms_controller.rb), plus what its subclasses
//! share: `set_room` over a `room_scope`, `ensure_can_administer`,
//! `ensure_permission_to_create_rooms`.
//!
//! The subclasses (`Rooms::OpensController` and friends) re-declare `before_action :set_room`
//! (and `ensure_can_administer`) with their own `only:`, which *replaces* the parent's callback.
//! So the actions they inherit from here but don't list (`destroy` for opens/closeds, `show` for
//! directs) run without `set_room` and raise on the nil `@room`, as in the reference.

pub mod closeds;
pub mod directs;
pub mod involvements;
pub mod opens;
pub mod refreshes;

use askama::Template;
use campfire_db::{Account, Message, Room, RoomType, User};
use campfire_kit::{Ctx, Error, Redirect, Result, StatusCode, halt};
use ruby_compat::integer_cast;

use crate::app::AppCtx;
use crate::concerns::{self, Before, before_actions, require_current_user};
use crate::controllers::presenters::page::{self, Rendered};
use crate::controllers::presenters::{Presenter, user_view};

/// `room_scope`: which of `Current.user.rooms` a controller may act on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    /// `Current.user.rooms` (RoomsController)
    All,
    /// `Current.user.rooms.without_directs` (opens, closeds)
    WithoutDirects,
    /// `Current.user.rooms.directs` (directs)
    Directs,
}

impl Scope {
    fn includes(self, room: &Room) -> bool {
        match self {
            Scope::All => true,
            Scope::WithoutDirects => room.room_type != RoomType::Direct,
            Scope::Directs => room.room_type == RoomType::Direct,
        }
    }
}

// --- Actions ------------------------------------------------------------------------------------

/// `index`: `redirect_to room_url(Current.user.rooms.last)` (inherited by the room-type
/// controllers). With no rooms, `room_url(nil)` raises.
pub async fn index(c: &mut Ctx) -> Result {
    before_actions(c, Before::default()).await?;
    let user_id = require_current_user(c)?.id;
    let room = c.app().read(move |conn| Room::last_for_user(conn, user_id)).await?;
    let Some(room) = room else {
        return Err(Error::internal(anyhow::anyhow!("No route matches room_url(nil)")));
    };
    let url = c.url_for(&campfire_routes::room(room.id));
    c.redirect_to(&url)
}

/// `show`, also `GET /rooms/:room_id/@:message_id`.
pub async fn show(c: &mut Ctx) -> Result {
    before_actions(c, Before::default()).await?;
    let room = set_room(c, Scope::All).await?;
    concerns::remember_last_room_visited(c, room.id);
    if let Some(response) = crate::response_cache::lookup(c) {
        return Ok(response);
    }
    render_show(c, room).await
}

/// `destroy` (RoomsController and `Rooms::DirectsController`).
pub async fn destroy(c: &mut Ctx) -> Result {
    before_actions(c, Before::default()).await?;
    let room = set_room(c, Scope::All).await?;
    ensure_can_administer(c, &room)?;
    destroy_room(c, room).await
}

/// `destroy` inherited by `Rooms::OpensController` and `Rooms::ClosedsController`, whose
/// `set_room` doesn't run for it: `nil.destroy` raises NoMethodError.
pub async fn destroy_without_room(c: &mut Ctx) -> Result {
    before_actions(c, Before::default()).await?;
    Err(Error::internal(anyhow::anyhow!("undefined method 'destroy' for nil")))
}

pub(crate) async fn destroy_room(c: &mut Ctx, room: Room) -> Result {
    let destroyed = room.clone();
    c.app().write(move |tx| destroyed.destroy(tx)).await?;
    // broadcast_remove_to :rooms, target: [ @room, :list ]
    c.app().broadcasts.room_remove(&room);
    redirect_to_root(c)
}

// --- Shared before-actions ----------------------------------------------------------------------

/// `set_room`: `room_scope.find_by(id: params[:room_id] || params[:id])`, or back to the root with
/// an alert.
pub async fn set_room(c: &mut Ctx, scope: Scope) -> Result<Room> {
    let user_id = require_current_user(c)?.id;
    let id = c.param_str("room_id").or_else(|| c.param_str("id")).and_then(integer_cast);
    let room = match id {
        Some(id) => room_for_user(c, user_id, id).await?,
        None => None,
    };
    match room.filter(|room| scope.includes(room)) {
        Some(room) => Ok(room),
        None => {
            let root = c.url_for(&campfire_routes::root());
            let redirect = Redirect { alert: Some("Room not found or inaccessible".into()), ..Redirect::default() };
            halt(c.redirect_to_with(&root, redirect)?)
        }
    }
}

/// `Room::find_for_user`, or as read earlier in the request's database version.
async fn room_for_user(c: &Ctx, user_id: i64, id: i64) -> Result<Option<Room>> {
    let generation = crate::response_cache::reads_generation(c);
    let reads = &c.app().response_cache.reads.rooms;
    if let Some(room) = generation.and_then(|generation| reads.get(generation, &(user_id, id))) {
        return Ok(Some(room));
    }
    let room = c.app().read(move |conn| Room::find_for_user(conn, user_id, id)).await?;
    if let (Some(generation), Some(room)) = (generation, &room) {
        reads.insert(generation, (user_id, id), room.clone());
    }
    Ok(room)
}

/// `ensure_can_administer`: `head :forbidden unless Current.user.can_administer?(@room)`.
pub fn ensure_can_administer(c: &mut Ctx, room: &Room) -> Result<()> {
    let allowed = require_current_user(c)?.can_administer(Some(room.creator_id), false);
    if !allowed {
        return halt(concerns::head(StatusCode::FORBIDDEN));
    }
    Ok(())
}

/// `ensure_permission_to_create_rooms`
pub async fn ensure_permission_to_create_rooms(c: &mut Ctx) -> Result<()> {
    let administrator = require_current_user(c)?.is_administrator();
    let account = c.app().read(Account::first).await?;
    let restricted = account.is_some_and(|account| account.settings().restrict_room_creation_to_administrators());
    if restricted && !administrator {
        return halt(concerns::head(StatusCode::FORBIDDEN));
    }
    Ok(())
}

// --- Helpers ------------------------------------------------------------------------------------

pub(crate) fn redirect_to_root(c: &mut Ctx) -> Result {
    let root = c.url_for(&campfire_routes::root());
    c.redirect_to(&root)
}

pub(crate) fn redirect_to_room(c: &mut Ctx, room_id: i64) -> Result {
    let url = c.url_for(&campfire_routes::room(room_id));
    c.redirect_to(&url)
}

/// `params.require(:room).permit(:name)`: `Some(name)` when the name was submitted.
pub(crate) fn room_name_param(c: &Ctx) -> Result<Option<Option<String>>> {
    let room = c.params.require("room")?;
    let permitted = room.permit(&campfire_kit::permit_keys(&["name"]));
    Ok(permitted.get("name").map(|name| name.as_str().map(str::to_string)))
}

/// `params.fetch(:user_ids, [])` as ids `User.where(id:)` can match.
pub(crate) fn user_ids_param(c: &Ctx) -> Vec<i64> {
    match c.param("user_ids") {
        Some(campfire_kit::Param::Array(values)) => values.iter().filter_map(|v| v.as_str()).filter_map(integer_cast).collect(),
        Some(value) => value.as_str().and_then(integer_cast).into_iter().collect(),
        None => Vec::new(),
    }
}

/// `User.where(id: ids)`, as ids of existing users (in id order, like the query).
pub(crate) fn existing_user_ids(conn: &campfire_db::Connection, ids: &[i64]) -> campfire_db::Result<Vec<i64>> {
    Ok(User::where_ids(conn, ids)?.into_iter().map(|user| user.id).collect())
}

/// Renders `users/sidebars/rooms/_shared` for `room` outside a request.
pub(crate) async fn render_shared_room(c: &Ctx, room: &Room) -> Result<Rendered> {
    let app = c.app().clone();
    let base_url = page::renderer_base_url(c);
    let renderer = page::DetachedRenderer::new(&app);
    let room = room.clone();
    let html = c
        .app()
        .read(move |conn| {
            let presenter = Presenter::new(conn, &app, None);
            let sidebar_room = presenter.sidebar_room(&room);
            let account = Account::first(conn)?;
            Ok(renderer.render_at(&app, account.as_ref(), &base_url, |_| {
                campfire_views::users::SidebarSharedPartial { room: sidebar_room }.render()
            }))
        })
        .await?
        .map_err(Error::internal)?;
    Ok(Rendered { shared_room: Some(html), ..Rendered::default() })
}

/// `rooms/show` with `find_messages`: the page around `params[:message_id]`, else the last page.
async fn render_show(c: &mut Ctx, room: Room) -> Result {
    let app = c.app().clone();
    let fragments = crate::response_cache::fragments(c);
    let user = require_current_user(c)?.clone();
    let message_id = c.param_str("message_id").and_then(integer_cast);
    let request_host = Some(c.request.host());
    let show = c
        .app()
        .read(move |conn| {
            let messages = match message_id.map(|id| Message::find_by_id(conn, id)).transpose()?.flatten() {
                Some(message) if message.room_id == room.id => Message::page_around(conn, room.id, &message)?,
                _ => Message::last_page(conn, room.id)?,
            };
            let presenter = Presenter::new(conn, &app, request_host);
            let original = Room::original(conn)?.is_some_and(|original| original.id == room.id);
            let room_gid = crate::channels::room_gid(&room).to_param();
            Ok(campfire_views::rooms::ShowView {
                room: presenter.room_view(&room, &user)?,
                updated_at: room.updated_at.jiff(),
                user: user_view(&app.secrets, &user),
                // The page's message fragments come from the store the render then uses.
                messages: campfire_views::fragment_cache::with(&fragments, || presenter.messages(&messages))?,
                invitation: original && !Message::paged(conn, room.id)?,
                join_code: Account::first(conn)?.map(|account| account.join_code).unwrap_or_default(),
                messages_stream_name: rails_compat::turbo::signed_stream_name(&app.secrets, &[&room_gid, "messages"]),
            })
        })
        .await?;
    page::framed_page!(c, StatusCode::OK, |ctx| campfire_views::rooms::Show { ctx, show: &show }).await
}

#[cfg(test)]
mod tests;
