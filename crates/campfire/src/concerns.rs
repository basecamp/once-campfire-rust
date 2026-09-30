//! `ApplicationController`'s concerns as before-action functions on [`Ctx`].
//!
//! `ApplicationController` does `include AllowBrowser, Authentication, Authorization,
//! BlockBannedRequests, SetCurrentRequest, SetPlatform, TrackedRoomVisit, VersionHeaders`. Ruby
//! includes a list right to left, so the `included` hooks (and their `before_action`s) run in
//! reverse, and the chain the reference actually runs is (dumped with
//! `reference-tools/campfire/callbacks.rb`):
//!
//! 1. `set_version_headers` (VersionHeaders)
//! 2. `Current.request = request` (SetCurrentRequest)
//! 3. `reject_banned_ip`, unless GET/HEAD (BlockBannedRequests)
//! 4. `require_authentication` (Authentication)
//! 5. `deny_bots` (Authentication)
//! 6. `verify_authenticity_token`, unless authenticated by bot key (Authentication's
//!    `protect_from_forgery with: :exception, unless: -> { authenticated_by.bot_key? }`)
//! 7. `allow_browser` (AllowBrowser)
//!
//! then the controller's own before-actions. [`before_actions`] runs 1–7 with a controller's
//! skips ([`Before`]); controllers then call their own (`set_room`, `ensure_can_administer`, ...)
//! in declaration order. Everything returns `Err(Error::Halt(..))` to stop the chain the way a
//! Rails callback that renders or redirects does, so actions just use `?`:
//!
//! ```ignore
//! pub async fn show(c: &mut Ctx) -> Result {
//!     before_actions(c, Before::default()).await?;
//!     let room = set_room(c).await?;          // RoomScoped
//!     ...
//! }
//! ```
//!
//! Current attributes (`Current.user`, `Current.session`) live in the `Ctx` extensions; read them
//! with [`current_user`] / [`current_session`].

// The frame later controller ports build on; parts are unused until they land.

pub mod platform;
pub mod user_agent;

use std::borrow::Cow;

use campfire_db::{Ban, Membership, PasswordDigest, Room, Session, User};
use campfire_kit::{Cookie, Ctx, Error, Result, SameSite, StatusCode, halt};
use ruby_compat::integer_cast;

use crate::app::AppCtx;

// --- Current -----------------------------------------------------------------------------------

/// `Current.user`
#[derive(Debug, Clone)]
pub struct CurrentUser(pub User);

/// `Current.session`
#[derive(Debug, Clone)]
pub struct CurrentSession(pub Session);

/// `authenticated_by` (`"".inquiry`, `"session"` or `"bot_key"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AuthenticatedBy {
    #[default]
    Nothing,
    Session,
    BotKey,
}

pub fn current_user(c: &Ctx) -> Option<&User> {
    c.current::<CurrentUser>().map(|current| &current.0)
}

/// `Current.user` where a before-action guarantees one (after `require_authentication`).
pub fn require_current_user(c: &Ctx) -> Result<&User> {
    current_user(c).ok_or_else(|| Error::internal(anyhow::anyhow!("no Current.user")))
}

pub fn current_session(c: &Ctx) -> Option<&Session> {
    c.current::<CurrentSession>().map(|current| &current.0)
}

/// `signed_in?`
pub fn signed_in(c: &Ctx) -> bool {
    current_user(c).is_some()
}

pub fn authenticated_by(c: &Ctx) -> AuthenticatedBy {
    c.current::<AuthenticatedBy>().copied().unwrap_or_default()
}

fn set_authenticated_by(c: &mut Ctx, by: AuthenticatedBy) {
    c.set_current(by);
}

// --- The ApplicationController chain ---------------------------------------------------------

/// How a controller's `allow_unauthenticated_access` / `require_unauthenticated_access` /
/// `allow_bot_access` / `skip_forgery_protection` change the chain for one action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Before {
    pub authentication: Authentication,
    /// `deny_bots` (skipped by `allow_bot_access`).
    pub deny_bots: bool,
    /// `verify_authenticity_token` (skipped by `skip_forgery_protection`).
    pub forgery_protection: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Authentication {
    /// `require_authentication`
    Required,
    /// `allow_unauthenticated_access`: skip `require_authentication`.
    Skipped,
    /// `require_unauthenticated_access`: skip `require_authentication`, then (after the rest of
    /// the chain, as it's declared in the subclass) `restore_authentication` and
    /// `redirect_signed_in_user_to_root`.
    RequireUnauthenticated,
}

impl Default for Before {
    fn default() -> Self {
        Self { authentication: Authentication::Required, deny_bots: true, forgery_protection: true }
    }
}

impl Before {
    pub fn allow_unauthenticated_access(self) -> Self {
        Self { authentication: Authentication::Skipped, ..self }
    }

    pub fn require_unauthenticated_access(self) -> Self {
        Self { authentication: Authentication::RequireUnauthenticated, ..self }
    }

    pub fn allow_bot_access(self) -> Self {
        Self { deny_bots: false, ..self }
    }

    pub fn skip_forgery_protection(self) -> Self {
        Self { forgery_protection: false, ..self }
    }
}

/// `ApplicationController`'s before-actions, in the order the reference runs them.
pub async fn before_actions(c: &mut Ctx, before: Before) -> Result<()> {
    set_version_headers(c);
    set_current_request(c);
    reject_banned_ip(c).await?;
    if before.authentication == Authentication::Required {
        require_authentication(c).await?;
    }
    if before.deny_bots {
        deny_bots(c)?;
    }
    if before.forgery_protection && authenticated_by(c) != AuthenticatedBy::BotKey {
        c.verify_authenticity_token()?;
    }
    allow_browser(c).await?;
    if before.authentication == Authentication::RequireUnauthenticated {
        restore_authentication(c).await?;
        redirect_signed_in_user_to_root(c)?;
    }
    Ok(())
}

// --- VersionHeaders ----------------------------------------------------------------------------

/// `set_version_headers`: `X-Version` and `X-Rev` (`config/initializers/version.rb`).
pub fn set_version_headers(c: &mut Ctx) {
    let config = &c.app().config;
    let (version, revision) = (config.app_version.clone(), config.git_revision.clone());
    c.set_header("x-version", &version);
    // Rails drops a header set to nil.
    if let Some(revision) = revision {
        c.set_header("x-rev", &revision);
    }
}

// --- SetCurrentRequest -------------------------------------------------------------------------

/// `Current.request = request`. `default_url_options` then carry the request's host and
/// protocol, which is what `Ctx::url_for` already does, so there's nothing to store.
pub fn set_current_request(_c: &mut Ctx) {}

// --- BlockBannedRequests -----------------------------------------------------------------------

/// `reject_banned_ip`, unless the request is safe (GET/HEAD): 429 for a banned `remote_ip`.
pub async fn reject_banned_ip(c: &mut Ctx) -> Result<()> {
    if c.request.is_get() || c.request.is_head() {
        return Ok(());
    }
    let ip = c.request.remote_ip()?.to_string();
    let banned = c.app().read(move |conn| Ban::banned(conn, &ip)).await?;
    if banned {
        return halt(head(StatusCode::TOO_MANY_REQUESTS));
    }
    Ok(())
}

// --- Authentication ----------------------------------------------------------------------------

/// `Authentication::SessionLookup#find_session_by_cookie`
pub async fn find_session_by_cookie(c: &Ctx) -> Result<Option<Session>> {
    let Some(token) = c.cookies.signed("session_token") else { return Ok(None) };
    c.app().read(move |conn| Session::find_by_token(conn, &token)).await
}

/// `require_authentication`: `restore_authentication || bot_authentication || request_authentication`.
pub async fn require_authentication(c: &mut Ctx) -> Result<()> {
    if restore_authentication(c).await? || bot_authentication(c).await? {
        return Ok(());
    }
    request_authentication(c)
}

/// `restore_authentication`: resume the session named by the `session_token` cookie.
pub async fn restore_authentication(c: &mut Ctx) -> Result<bool> {
    match find_session_by_cookie(c).await? {
        Some(session) => {
            resume_session(c, session).await?;
            Ok(true)
        }
        None => Ok(false),
    }
}

/// `bot_authentication`: `params[:bot_key].present?` and a matching active bot.
pub async fn bot_authentication(c: &mut Ctx) -> Result<bool> {
    let Some(param) = c.params.get("bot_key").filter(|p| p.is_present()) else { return Ok(false) };
    // `params[:bot_key].strip` raises NoMethodError for a hash or array.
    let Some(bot_key) = param.as_str().map(|key| ruby_compat::strip(key).to_string()) else {
        return Err(Error::internal(anyhow::anyhow!("undefined method 'strip' for bot_key")));
    };
    let bot = c.app().read(move |conn| User::authenticate_bot(conn, &bot_key)).await?;
    match bot {
        Some(bot) => {
            c.set_current(CurrentUser(bot));
            set_authenticated_by(c, AuthenticatedBy::BotKey);
            Ok(true)
        }
        None => Ok(false),
    }
}

/// `request_authentication`: remember where we were, then off to sign in.
pub fn request_authentication(c: &mut Ctx) -> Result<()> {
    let url = c.request.url();
    c.session().insert("return_to_after_authenticating", url);
    let location = c.url_for(&campfire_routes::new_session());
    halt(c.redirect_to(&location)?)
}

/// `redirect_signed_in_user_to_root`
pub fn redirect_signed_in_user_to_root(c: &mut Ctx) -> Result<()> {
    if signed_in(c) {
        let root = c.url_for(&campfire_routes::root());
        return halt(c.redirect_to(&root)?);
    }
    Ok(())
}

/// `has_secure_password`'s `password=`, hashed on the blocking pool ahead of the write that saves
/// it, so bcrypt (about 250 ms) holds neither an async thread nor the database writer.
pub async fn password_digest(c: &Ctx, password: Option<String>) -> Result<Option<PasswordDigest>> {
    let Some(password) = password else { return Ok(None) };
    PasswordDigest::hash(password, c.app().db.env().bcrypt_cost).await.map(Some).map_err(Error::internal)
}

/// `User.active.authenticate_by(email_address:, password:)`: the user is looked up on a reader,
/// and the password checked once the reader is released.
pub async fn authenticate_by(c: &Ctx, email_address: String, password: String) -> Result<Option<User>> {
    // `authenticate_by` returns nil for a blank password before looking anything up.
    if password.is_empty() {
        return Ok(None);
    }
    let candidate = c.app().read(move |conn| User::find_active_by_email_address(conn, &email_address)).await?;
    tokio::task::spawn_blocking(move || User::authenticated(candidate, &password)).await.map_err(Error::internal)
}

/// `start_new_session_for(user)`
pub async fn start_new_session_for(c: &mut Ctx, user: User) -> Result<Session> {
    let (user_agent, ip) = (c.request.user_agent().map(str::to_string), c.request.remote_ip()?.to_string());
    let user_id = user.id;
    let session =
        c.app().db.write(move |tx| Session::start(tx, user_id, user_agent.as_deref(), Some(&ip))).await.map_err(|error| match error {
            campfire_db::Error::RecordNotFound("User") => Error::Status(StatusCode::UNAUTHORIZED),
            error => Error::internal(error),
        })?;
    authenticated_as(c, session.clone(), Some(user), true).await?;
    Ok(session)
}

/// `resume_session(session)`: refresh its activity (at most hourly), then authenticate as it.
///
/// Only a session due for its refresh goes to the database writer, so other requests don't queue
/// behind every write for nothing. The `session_token` cookie is re-signed on the same schedule
/// rather than on every request as Rails does, which keeps its 20-year expiry rolling without a
/// cookie on every response.
pub async fn resume_session(c: &mut Ctx, session: Session) -> Result<()> {
    let refresh = session.needs_resume(campfire_db::Timestamp::from_jiff(c.now()));
    let session = if refresh {
        let (user_agent, ip) = (c.request.user_agent().map(str::to_string), c.request.remote_ip()?.to_string());
        c.app()
            .write(move |tx| {
                let mut session = session;
                session.resume(tx, user_agent.as_deref(), Some(&ip))?;
                Ok(session)
            })
            .await?
    } else {
        session
    };
    authenticated_as(c, session, None, refresh).await
}

/// `authenticated_as(session)`: `Current.session = session` (which sets `Current.user` to
/// `session.user`), `authenticated_by` session, and, with `set_cookie`, a fresh `session_token` cookie.
async fn authenticated_as(c: &mut Ctx, session: Session, user: Option<User>, set_cookie: bool) -> Result<()> {
    let user = match user {
        Some(user) => Some(user),
        None => {
            let user_id = session.user_id;
            c.app().read(move |conn| User::find_by_id(conn, user_id)).await?
        }
    };
    if set_cookie {
        set_authentication_cookie(c, &session)?;
    }
    c.set_current(CurrentSession(session));
    if let Some(user) = user {
        c.set_current(CurrentUser(user));
    }
    set_authenticated_by(c, AuthenticatedBy::Session);
    Ok(())
}

/// `cookies.signed.permanent[:session_token] = { value: session.token, httponly: true, same_site: :lax }`
fn set_authentication_cookie(c: &mut Ctx, session: &Session) -> Result<()> {
    let cookie = Cookie::new(session.token.clone()).permanent().httponly().same_site(Some(SameSite::Lax));
    c.cookies.set_signed("session_token", cookie)
}

/// `terminate_current_session`: destroy the session, reset the Rails session, drop the cookie,
/// and disconnect the user's sockets (`reset_remote_connections`, errors only logged).
pub async fn terminate_current_session(c: &mut Ctx) -> Result<()> {
    if let Some(session) = current_session(c).cloned() {
        c.app().write(move |tx| session.destroy(tx)).await?;
    }
    c.reset_session();
    c.cookies.delete("session_token");
    if let Some(user) = current_user(c).cloned()
        && let Err(error) = c
            .app()
            .db
            .write(move |tx| {
                user.reset_remote_connections(tx);
                Ok(())
            })
            .await
    {
        tracing::warn!("Could not disconnect remote connections on sign out: {error}");
    }
    Ok(())
}

/// `post_authenticating_url`: `session.delete(:return_to_after_authenticating) || root_url`.
pub fn post_authenticating_url(c: &mut Ctx) -> String {
    let stored = c.session().remove("return_to_after_authenticating");
    match stored {
        Some(serde_json::Value::String(url)) => url,
        Some(serde_json::Value::Null) | None => c.url_for(&campfire_routes::root()),
        Some(other) => other.to_string(),
    }
}

/// `deny_bots`: 403 for bot-key requests.
pub fn deny_bots(c: &mut Ctx) -> Result<()> {
    if authenticated_by(c) == AuthenticatedBy::BotKey {
        return halt(head(StatusCode::FORBIDDEN));
    }
    Ok(())
}

// --- Authorization -----------------------------------------------------------------------------

/// `ensure_can_administer`: 403 unless `Current.user.can_administer?` (no record).
pub fn ensure_can_administer(c: &mut Ctx) -> Result<()> {
    let allowed = current_user(c).is_some_and(|user| user.can_administer(None, false));
    if !allowed {
        return halt(head(StatusCode::FORBIDDEN));
    }
    Ok(())
}

// --- AllowBrowser --------------------------------------------------------------------------------

/// `allow_browser versions: VERSIONS, block: -> { render template: "sessions/incompatible_browser" }`
///
/// The platform parsed for the check is kept for the layout's [`platform`], so a page parses its
/// User-Agent once. A missing or blank one isn't checked, and is parsed only if a page renders.
pub async fn allow_browser(c: &mut Ctx) -> Result<()> {
    let Some(header) = c.request.user_agent().filter(|header| user_agent::is_present(header)) else {
        return Ok(());
    };
    let platform = platform::ApplicationPlatform::new(Some(header));
    let blocked = platform.browser_blocked();
    c.set_current(platform);
    if blocked {
        return halt(render_incompatible_browser(c).await?);
    }
    Ok(())
}

/// `render template: "sessions/incompatible_browser"` (200). Rendered from a before-action, so
/// it's HTML whatever the request format. The layout is the controller's: turbo-rails' frame
/// layout for a Turbo-Frame request, except in controllers that declare their own layout
/// (`MessagesController` and its `Messages::ByBotsController`), which always use the application
/// layout.
async fn render_incompatible_browser(c: &mut Ctx) -> Result {
    use askama::Template;
    use campfire_views::sessions::IncompatibleBrowser;

    let own_layout = c
        .current::<crate::controllers::MatchedRoute>()
        .is_some_and(|route| route.endpoint.starts_with("messages#") || route.endpoint.starts_with("messages/by_bots#"));
    use crate::controllers::presenters::view_context::{page_in_any_format, page_or_frame_in_any_format};

    // An explicit `render template:`, so no format lookup: a blocked browser gets this page for
    // /webmanifest.json, /service-worker.js or `Accept: application/json` alike (verified against
    // the reference), never a 406.
    let response = if own_layout {
        page_in_any_format(c, StatusCode::OK, |ctx| IncompatibleBrowser { ctx }.render()).await?
    } else {
        page_or_frame_in_any_format(
            c,
            StatusCode::OK,
            |ctx| IncompatibleBrowser { ctx }.render(),
            |ctx| {
                let page = IncompatibleBrowser { ctx };
                campfire_views::layouts::frame(ctx, page.as_head(), page.as_content())
            },
        )
        .await?
    };
    Ok(response.content_type(campfire_kit::response::HTML_UTF8))
}

// --- SetPlatform -------------------------------------------------------------------------------

/// `platform` (`helper_method`): `@platform ||= ApplicationPlatform.new(request.user_agent)`, the
/// one [`allow_browser`] kept, or a new one when it didn't parse the header.
pub fn platform(c: &Ctx) -> Cow<'_, platform::ApplicationPlatform> {
    match c.current::<platform::ApplicationPlatform>() {
        Some(platform) => Cow::Borrowed(platform),
        None => Cow::Owned(platform::ApplicationPlatform::new(c.request.user_agent())),
    }
}

// --- TrackedRoomVisit ----------------------------------------------------------------------------

/// `remember_last_room_visited`: `cookies.permanent[:last_room] = @room.id`.
///
/// Only when it changes: Rails sets it on every room page.
pub fn remember_last_room_visited(c: &mut Ctx, room_id: i64) {
    let room_id = room_id.to_string();
    if c.cookies.get("last_room") != Some(room_id.as_str()) {
        c.cookies.set("last_room", Cookie::new(room_id).permanent());
    }
}

/// `last_room_visited`: the `last_room` cookie's room if the user is in it, else
/// `Current.user.rooms.original`.
pub async fn last_room_visited(c: &Ctx) -> Result<Option<Room>> {
    let Some(user_id) = current_user(c).map(|user| user.id) else { return Ok(None) };
    let last_room = last_room_cookie(c);
    c.app().read(move |conn| last_room_visited_in(conn, user_id, last_room)).await
}

/// The `last_room` cookie, as `find_by(id: cookies[:last_room])` casts it (like an integer column).
pub fn last_room_cookie(c: &Ctx) -> Option<i64> {
    c.cookies.get("last_room").and_then(integer_cast)
}

/// [`last_room_visited`] inside a read that's already on a reader, for the user and the
/// [`last_room_cookie`].
pub fn last_room_visited_in(conn: &campfire_db::Connection, user_id: i64, last_room: Option<i64>) -> campfire_db::Result<Option<Room>> {
    if let Some(room_id) = last_room
        && let Some(room) = Room::find_for_user(conn, user_id, room_id)?
    {
        return Ok(Some(room));
    }
    Room::original_for_user(conn, user_id)
}

// --- RoomScoped ----------------------------------------------------------------------------------

/// `RoomScoped#set_room`: `Current.user.memberships.find_by!(room_id: params[:room_id])`, 404
/// otherwise. Returns the membership and its room.
pub async fn set_room(c: &mut Ctx) -> Result<(Membership, Room)> {
    let user_id = require_current_user(c)?.id;
    let Some(room_id) = c.param_str("room_id").and_then(integer_cast) else { return Err(Error::NotFound) };
    c.app()
        .read(move |conn| {
            let membership =
                Membership::find_by_room_and_user(conn, room_id, user_id)?.ok_or(campfire_db::Error::RecordNotFound("Membership"))?;
            // `@membership.room` is nil when the room is gone (memberships have no foreign key to
            // rooms), and the action fails on it: a 500, where `Membership#room`'s
            // `RecordNotFound` would be a 404.
            let room =
                Room::find_by_id(conn, membership.room_id)?.ok_or_else(|| campfire_db::Error::other("the membership's room is gone"))?;
            Ok((membership, room))
        })
        .await
}

// --- Helpers -------------------------------------------------------------------------------------

/// `head status` from a before-action. Rails sets the controller's `formats` only after the
/// before-callbacks have run (`AbstractController::Callbacks#process_action` wraps
/// `ActionController::Rendering#process_action`), so a `head` there falls back to `Mime[:html]`
/// whatever the request format: `text/html`, no charset. (A `head` inside the action itself uses
/// the request format: that's `c.head`.)
pub fn head(status: StatusCode) -> campfire_kit::Response {
    let response = campfire_kit::Response::new(status);
    if matches!(status.as_u16(), 100..=199 | 204 | 205 | 304) { response } else { response.content_type("text/html") }
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Request, header};
    use campfire_db::NewUser;
    use campfire_kit::{Kit, KitConfig};
    use tower::ServiceExt;

    use super::*;

    #[test]
    fn before_builders() {
        let before = Before::default().allow_bot_access().skip_forgery_protection();
        assert_eq!(before.authentication, Authentication::Required);
        assert!(!before.deny_bots);
        assert!(!before.forgery_protection);
        assert_eq!(Before::default().require_unauthenticated_access().authentication, Authentication::RequireUnauthenticated);
    }

    // Revoke between password verification and session creation.
    async fn login_with_revocation(c: &mut Ctx) -> Result {
        let user = authenticate_by(c, "user@example.com".into(), "secret".into()).await?.unwrap();
        let id = user.id;
        let revoke = c.param_str("revoke").unwrap_or_default().to_string();
        c.app()
            .db
            .write(move |tx| {
                let mut user = User::find(tx.conn(), id)?;
                match revoke.as_str() {
                    "ban" => user.ban(tx),
                    "deactivate" => user.deactivate(tx),
                    _ => Ok(()),
                }
            })
            .await
            .map_err(Error::internal)?;
        start_new_session_for(c, user).await?;
        Ok(c.head(StatusCode::OK))
    }

    #[tokio::test]
    async fn revoked_login_does_not_issue_an_authentication_cookie() {
        for revoke in ["ban", "deactivate", "none"] {
            let dir = tempfile::tempdir().unwrap();
            let config = crate::config::Config::from_lookup(|key| match key {
                "SECRET_KEY_BASE" => Some("session-revocation-test-secret".into()),
                "DISABLE_SSL" => Some("true".into()),
                "CAMPFIRE_STORAGE_PATH" => Some(dir.path().to_string_lossy().into_owned()),
                _ => None,
            })
            .unwrap();
            let booted = crate::app::boot(config).await.unwrap();
            let digest = PasswordDigest::hash("secret".into(), 4).await.unwrap();
            let user = booted
                .app
                .db
                .write(move |tx| {
                    User::create(
                        tx,
                        NewUser {
                            name: "User".into(),
                            email_address: Some("user@example.com".into()),
                            password_digest: Some(digest),
                            ..NewUser::default()
                        },
                    )
                })
                .await
                .unwrap();
            let app = booted.app.clone();
            let kit = Kit::new(KitConfig::production(true), app.secrets.clone(), app.clock.clone(), app.clone());
            let router = campfire_kit::app(axum::Router::new().route("/login", campfire_kit::post(login_with_revocation)), kit);
            let request =
                Request::post(format!("/login?revoke={revoke}")).header(header::HOST, "campfire.test").body(Body::empty()).unwrap();
            let response = router.oneshot(request).await.unwrap();
            let allowed = revoke == "none";
            assert_eq!(response.status(), if allowed { StatusCode::OK } else { StatusCode::UNAUTHORIZED });
            let has_cookie =
                response.headers().get_all(header::SET_COOKIE).iter().any(|v| v.to_str().unwrap().starts_with("session_token="));
            assert_eq!(has_cookie, allowed, "{revoke}");
            let count = app.db.read(move |conn| Session::count_for_user(conn, user.id)).await.unwrap();
            assert_eq!(count, i64::from(allowed), "{revoke}");
            booted.jobs.shutdown(std::time::Duration::from_secs(5)).await;
        }
    }
}
