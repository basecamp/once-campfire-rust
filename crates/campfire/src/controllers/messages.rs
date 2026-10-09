//! `MessagesController` (reference/app/controllers/messages_controller.rb), including the
//! multipart attachment upload the composer's `FileUploader` posts here, plus what
//! `Messages::ByBotsController` reuses: creating a message (with `process_attachment`),
//! delivering webhooks and the broadcasts.

pub mod boosts;
pub mod by_bots;

use askama::Template;
use campfire_db::{Message, NewMessage, Role, Room, Status, User};
use campfire_kit::format;
use campfire_kit::{Ctx, Error, Freshness, Param, Result, StatusCode, halt, permit_keys};
use campfire_richtext::Content;
use campfire_storage::{Blob, Staged, Variation};
use campfire_views::messages as views;
use ruby_compat::integer_cast;
use sha2::{Digest, Sha256};

use crate::active_storage::{self, keep_after_commit, storage_error};
use crate::app::{App, AppCtx};
use crate::concerns::{self, Before, before_actions, require_current_user};
use crate::controllers::presenters::attachments::Assignment;
use crate::controllers::presenters::page::{self, Rendered};
use crate::controllers::presenters::{DbResolver, Presenter, room_kind};

// --- Actions ------------------------------------------------------------------------------------

/// `index` (`layout false`): the page before/after a message, or the last page; 204 when empty.
pub async fn index(c: &mut Ctx) -> Result {
    before_actions(c, Before::default()).await?;
    let (_, room) = concerns::set_room(c).await?;
    if let Some(response) = crate::response_cache::lookup(c) {
        return Ok(response);
    }
    let messages = find_paged_messages(c, &room).await?;
    if messages.is_empty() {
        return Ok(c.head(StatusCode::NO_CONTENT));
    }
    c.respond_to(&[&format::HTML])?;
    let views = present(c, move |presenter| presenter.messages(&messages)).await?;
    let response =
        page::bare(c, StatusCode::OK, &format::HTML, |ctx| campfire_views::render_sized!(views::Index { ctx, messages: &views })).await?;
    // Presentation also depends on creators, boosts and rich text. Direct SQL may change them
    // without touching messages.updated_at, so record versions alone cannot validate this body.
    let etag = match &response.body {
        campfire_kit::Body::Parts(parts) => parts.etag(),
        campfire_kit::Body::Bytes(body) => hex::encode(Sha256::digest(body)),
        _ => return Ok(response),
    };
    // These records cannot supply a reliable Last-Modified for timestamp-free foreign edits.
    // Use the content validator; an IMS-only request conservatively receives the current body.
    let freshness = Freshness { etag: Some(etag), template: Some(TEMPLATE_DIGEST_INDEX.into()), ..Freshness::default() };
    Ok(c.fresh_when(freshness).unwrap_or(response))
}

/// Stands in for the digest `ETagWithTemplateDigest` adds for `messages/index` (only the ETag's
/// shape has to match the reference).
const TEMPLATE_DIGEST_INDEX: &str = "messages/index";

/// `create`: `set_room` runs inside the action, and a room that's gone renders `room_not_found`.
pub async fn create(c: &mut Ctx) -> Result {
    before_actions(c, Before::default()).await?;
    let room = match concerns::set_room(c).await {
        Ok((_, room)) => room,
        Err(Error::NotFound) => return render_room_not_found(c).await,
        Err(error) => return Err(error),
    };
    let attributes = message_params(c)?;
    let message = create_message(c, &room, attributes).await?;
    broadcast_create(c, &room, &message).await?;
    deliver_webhooks_to_bots(c, &room, &message).await?;

    // The message partial comes out of the fragment cache `broadcast_create` just filled
    // (`cache [ message, "presentation-v3" ]`), so it's the request-less rendering: no CSRF
    // tokens in its forms.
    c.respond_to(&[&format::TURBO_STREAM])?;
    let kind = room_kind(room.room_type);
    let app = c.app().clone();
    let base_url = c.url_for("");
    let renderer = page::DetachedRenderer::new(&app);
    let html = c
        .app()
        .read(move |conn| {
            let presenter = Presenter::new(conn, &app, None);
            let item = renderer.with_fragments(|| presenter.message_item(&message))?;
            let account = campfire_db::Account::first(conn)?;
            renderer
                .render_at(&app, account.as_ref(), &base_url, |ctx| views::CreateStream { ctx, message: &item, room_kind: kind }.render())
                .map_err(campfire_db::Error::other)
        })
        .await?;
    Ok(c.render(StatusCode::OK, &format::TURBO_STREAM, html))
}

pub async fn show(c: &mut Ctx) -> Result {
    before_actions(c, Before::default()).await?;
    let (_, room) = concerns::set_room(c).await?;
    let message = set_message(c, &room).await?;
    c.respond_to(&[&format::HTML])?;
    let view = present(c, move |presenter| presenter.message(&message)).await?;
    page::content_in_application_layout(c, StatusCode::OK, |ctx| views::Show { ctx, message: &view }.render()).await
}

pub async fn edit(c: &mut Ctx) -> Result {
    before_actions(c, Before::default()).await?;
    let (_, room) = concerns::set_room(c).await?;
    let message = set_message(c, &room).await?;
    ensure_can_administer(c, &message)?;
    c.respond_to(&[&format::HTML])?;
    let edit = present(c, move |presenter| {
        Ok(views::EditView { editable_body_html: presenter.editable_body(&message)?, message: presenter.message(&message)? })
    })
    .await?;
    page::content_in_application_layout(c, StatusCode::OK, |ctx| views::Edit { ctx, edit: &edit }.render()).await
}

pub async fn update(c: &mut Ctx) -> Result {
    before_actions(c, Before::default()).await?;
    let (_, room) = concerns::set_room(c).await?;
    let message = set_message(c, &room).await?;
    ensure_can_administer(c, &message)?;
    let attributes = message_params(c)?;
    let message = update_message(c, message, attributes).await?;
    broadcast_replace(c, &room, &message).await?;

    // respond_to html: redirect; json: `render :show`, which has no JSON template here.
    match c.respond_to(&[&format::HTML, &format::JSON])? {
        f if *f == format::JSON => Err(Error::internal(anyhow::anyhow!("Missing template messages/show"))),
        _ => {
            let url = c.url_for(&campfire_routes::room_message(room.id, message.id));
            c.redirect_to(&url)
        }
    }
}

pub async fn destroy(c: &mut Ctx) -> Result {
    before_actions(c, Before::default()).await?;
    let (_, room) = concerns::set_room(c).await?;
    let message = set_message(c, &room).await?;
    ensure_can_administer(c, &message)?;
    destroy_message(c, &room, &message).await?;

    c.respond_to(&[&format::TURBO_STREAM])?;
    let view = present(c, move |presenter| presenter.message(&message)).await?;
    page::bare(c, StatusCode::OK, &format::TURBO_STREAM, |_| views::DestroyStream { message: &view }.render()).await
}

// --- Before-actions and params --------------------------------------------------------------------

/// `@room.messages.find(params[:id])`
pub(crate) async fn set_message(c: &mut Ctx, room: &Room) -> Result<Message> {
    let Some(id) = c.param_str("id").and_then(integer_cast) else { return Err(Error::NotFound) };
    let room_id = room.id;
    c.app().read(move |conn| Message::find_in_room(conn, room_id, id)).await
}

/// `head :forbidden unless Current.user.can_administer?(@message)`
pub(crate) fn ensure_can_administer(c: &mut Ctx, message: &Message) -> Result<()> {
    if !require_current_user(c)?.can_administer(Some(message.creator_id), false) {
        return halt(concerns::head(StatusCode::FORBIDDEN));
    }
    Ok(())
}

/// What `create_with_attachment!`/`update!` receive.
#[derive(Debug, Default, Clone)]
pub(crate) struct MessageParams {
    pub body: Option<String>,
    /// `attachment=`: `None` when the key wasn't given.
    pub attachment: Option<Assignment>,
    pub client_message_id: Option<String>,
}

/// `params.require(:message).permit(:body, :attachment, :client_message_id)`
fn message_params(c: &Ctx) -> Result<MessageParams> {
    let message = c.params.require("message")?;
    let permitted = message.permit(&permit_keys(&["body", "attachment", "client_message_id"]));
    let text = |key: &str| permitted.get(key).and_then(Param::as_str).map(str::to_string);
    Ok(MessageParams { body: text("body"), attachment: attachment_assignment(&permitted)?, client_message_id: text("client_message_id") })
}

/// What assigning the permitted `attachment` does: an upload replaces the attachment, nil or ""
/// removes it (`Attached::Changes::DeleteOne`), anything else raises.
pub(crate) fn attachment_assignment(permitted: &campfire_kit::ParamMap) -> Result<Option<Assignment>> {
    match Assignment::from_params(permitted, "attachment")? {
        Assignment::Unchanged => Ok(None),
        assignment => Ok(Some(assignment)),
    }
}

/// `@room.messages.find(params[:before])` and friends (`find_paged_messages`).
pub(crate) async fn find_paged_messages(c: &Ctx, room: &Room) -> Result<Vec<Message>> {
    let present = |key: &str| c.params.get(key).filter(|p| p.is_present()).map(|p| p.as_str().and_then(integer_cast));
    let (before, after) = (present("before"), present("after"));
    let room_id = room.id;
    c.app()
        .read(move |conn| match (before, after) {
            (Some(before), _) => {
                let message = Message::find_in_room(conn, room_id, before.ok_or(campfire_db::Error::RecordNotFound("Message"))?)?;
                Message::page_before(conn, room_id, &message)
            }
            (None, Some(after)) => {
                let message = Message::find_in_room(conn, room_id, after.ok_or(campfire_db::Error::RecordNotFound("Message"))?)?;
                Message::page_after(conn, room_id, &message)
            }
            (None, None) => Message::last_page(conn, room_id),
        })
        .await
}

// --- Creating, updating, destroying ---------------------------------------------------------------

/// `@room.messages.create_with_attachment!(attributes)`: the message (with its uploaded blob, in
/// one transaction), then `process_attachment`. The upload's file is copied into storage and the
/// body canonicalized before the transaction, so the writer only inserts rows.
pub(crate) async fn create_message(c: &Ctx, room: &Room, attributes: MessageParams) -> Result<Message> {
    let creator_id = require_current_user(c)?.id;
    let room_id = room.id;
    let attachment = match attributes.attachment {
        Some(Assignment::Create(upload)) => Some(upload.stage(c.app()).await?),
        Some(Assignment::Invalid) => return Err(invalid_attachment()),
        _ => None,
    };
    let (body, plain_text) = match attributes.body {
        Some(body) => {
            let (body, plain_text) = canonicalize_body_with_plain_text(c.app(), body, Some(c.request.host())).await?;
            (Some(body), plain_text)
        }
        None => (None, None),
    };
    let (message, blob) = c
        .app()
        .write(move |tx| {
            let blob = attachment.map(|staged| save_staged(tx, staged)).transpose()?;
            let message = Message::create_prepared(
                tx,
                NewMessage {
                    room_id,
                    creator_id,
                    client_message_id: attributes.client_message_id,
                    body,
                    attachment_blob_id: blob.as_ref().map(|blob| blob.id),
                },
                plain_text,
            )?;
            Ok((message, blob))
        })
        .await?;
    // Without an attachment, `message` is the row as stored: nothing after the commit writes to it,
    // and Rails answers with the same record (`create!(attributes).tap(&:process_attachment)`,
    // reference/app/models/message/attachment.rb). Analyzing an attachment touches the message, so
    // then it's read back.
    let Some(blob) = blob else { return Ok(message) };
    let id = message.id;
    process_attachment(c.app(), blob).await?;
    c.app().read(move |conn| Message::find(conn, id)).await
}

/// Inserts a staged blob's row, keeping its file once the transaction commits.
pub(crate) fn save_staged(tx: &mut campfire_db::Tx<'_>, staged: Staged) -> campfire_db::Result<Blob> {
    let blob = staged.insert(tx.conn(), tx.now().jiff()).map_err(storage_error)?;
    keep_after_commit(tx, staged);
    Ok(blob)
}

/// [`canonical_body`] on a reader, ahead of the write that stores it.
pub(crate) async fn canonicalize_body(app: &App, body: String, request_host: Option<String>) -> Result<String> {
    let app2 = app.clone();
    app.read(move |conn| Ok(canonical_body(conn, &app2, &body, request_host))).await
}

/// [`canonicalize_body`], with the canonical body's plain text when it has no attachments. Only
/// attachments read records (mentions their users), so such a body is canonicalized and its plain
/// text worked out right here, holding no reader connection and leaving the write only its
/// statements. A body with attachments is canonicalized on a reader and gets no plain text: the
/// write reads its records, where they're current.
pub(crate) async fn canonicalize_body_with_plain_text(
    app: &App,
    body: String,
    request_host: Option<String>,
) -> Result<(String, Option<String>)> {
    if let Some(prepared) = prepare_without_records(&body, request_host.clone()) {
        return Ok(prepared);
    }
    Ok((canonicalize_body(app, body, request_host).await?, None))
}

/// [`canonical_body`] and `to_plain_text` of a body that names no records, or `None` if it may.
/// Rendering asks its resolver only about attachments, so a body without any never asks: if this
/// one did, the markup check missed something, and the result is set aside for the reader's.
fn prepare_without_records(body: &str, request_host: Option<String>) -> Option<(String, Option<String>)> {
    if may_have_attachments(body) {
        return None;
    }
    let resolver = NoRecords::default();
    let ctx = campfire_richtext::RenderContext { resolver: &resolver, request_host };
    let canonical = Content::load(body, &ctx).map(|content| content.to_html()).unwrap_or_else(|_| body.to_string());
    // As `AppRichText::to_plain_text`, which renders without a request host.
    let ctx = campfire_richtext::RenderContext { resolver: &resolver, request_host: None };
    let plain_text = campfire_richtext::to_plain_text(&canonical, &ctx).unwrap_or_else(|error| {
        tracing::error!(%error, "to_plain_text raised");
        String::new()
    });
    (!resolver.asked.get()).then_some((canonical, Some(plain_text)))
}

/// A resolver with no records to find, which notes whether it was asked.
#[derive(Default)]
struct NoRecords {
    asked: std::cell::Cell<bool>,
}

impl campfire_richtext::AttachableResolver for NoRecords {
    fn locate_signed(&self, _: &str) -> campfire_richtext::SignedLookup {
        self.asked.set(true);
        campfire_richtext::SignedLookup::Invalid
    }

    fn find_gid(&self, _: &str) -> campfire_richtext::GidLookup {
        self.asked.set(true);
        campfire_richtext::GidLookup::NotFound
    }
}

/// Whether `html` may hold an attachment, in Action Text's markup or Trix's, in any case.
fn may_have_attachments(html: &str) -> bool {
    let html = html.to_ascii_lowercase();
    html.contains("action-text-attachment") || html.contains("data-trix-attachment")
}

/// Assigning a String to a rich text attribute stores the canonicalized content
/// (`ActionText::Content.new(body, canonicalize: true).to_html`).
pub(crate) fn canonical_body(conn: &campfire_db::Connection, app: &App, body: &str, request_host: Option<String>) -> String {
    let resolver = DbResolver { conn, secrets: &app.secrets, now: app.clock.now() };
    let ctx = resolver.render_context(request_host);
    Content::load(body, &ctx).map(|content| content.to_html()).unwrap_or_else(|_| body.to_string())
}

/// Assigning something that isn't an upload, a signed blob id, nil or "".
fn invalid_attachment() -> Error {
    Error::internal(anyhow::anyhow!("Could not find or build blob: expected attachable"))
}

/// `Message#process_attachment`: analyze the blob now (its `after_update` touches the message),
/// then generate the video preview or the `:thumb` representation.
pub(crate) async fn process_attachment(app: &App, blob: Blob) -> Result<()> {
    let blob = analyze_attachment(app, blob).await?;
    if blob.is_video() {
        // attachment.preview(format: :webp).processed
        active_storage::processed_preview(app, blob, Variation::format_only("webp")).await?;
    } else if blob.is_representable() {
        // attachment.representation(:thumb).processed
        let thumb = Variation::resize_to_limit(1200, 800, None);
        active_storage::processed_representation(app, blob, thumb).await?;
    }
    Ok(())
}

/// `blob.analyze`: its `after_update` touches the attached records. The file is analyzed off the
/// writer.
async fn analyze_attachment(app: &App, blob: Blob) -> Result<Blob> {
    let metadata = active_storage::analyzed_metadata(app, &blob).await?;
    app.write(move |tx| {
        let mut blob = blob;
        blob.update_metadata(tx.conn(), metadata).map_err(storage_error)?;
        touch_attachment_records(tx, blob.id)?;
        Ok(blob)
    })
    .await
}

/// `Blob#touch_attachments`: each attached record is touched (a message also touches its room).
fn touch_attachment_records(tx: &mut campfire_db::Tx<'_>, blob_id: i64) -> campfire_db::Result<()> {
    for (record_type, record_id) in campfire_storage::blob::attachment_records(tx.conn(), blob_id).map_err(storage_error)? {
        if record_type == "Message" {
            Message::find(tx.conn(), record_id)?.touch(tx)?;
        }
    }
    Ok(())
}

/// `@message.update!(message_params)`. A new attachment replaces the old one (whose blob is purged
/// later) without `process_attachment`: the blob is only analyzed, by `ActiveStorage::AnalyzeJob`
/// after commit (verified against the reference with a bot's `PUT` and `attachment`).
pub(crate) async fn update_message(c: &Ctx, message: Message, attributes: MessageParams) -> Result<Message> {
    let attachment = match attributes.attachment {
        Some(Assignment::Invalid) => return Err(invalid_attachment()),
        Some(Assignment::Create(upload)) => Some(Some(upload.stage(c.app()).await?)),
        Some(_) => Some(None),
        None => None,
    };
    let body = match attributes.body {
        Some(body) => Some(canonicalize_body(c.app(), body, Some(c.request.host())).await?),
        None => None,
    };
    let (id, blob) = c
        .app()
        .write(move |tx| {
            let mut message = message;
            if let Some(body) = body {
                message.update_body(tx, &body)?;
            }
            let attachment_given = attachment.is_some();
            let blob = attachment.flatten().map(|staged| save_staged(tx, staged)).transpose()?;
            if attachment_given {
                message.replace_attachment(tx, blob.as_ref().map(|blob| blob.id))?;
            }
            Ok((message.id, blob))
        })
        .await?;
    if let Some(blob) = blob.filter(|blob| !blob.is_analyzed()) {
        let job_app = c.app().clone();
        c.app().jobs.perform_later("ActiveStorage::AnalyzeJob", async move {
            analyze_attachment(&job_app, blob).await.map(drop).map_err(|e| anyhow::anyhow!("{e:?}"))
        });
    }
    c.app().read(move |conn| Message::find(conn, id)).await
}

/// `@message.destroy` then `@message.broadcast_remove`.
pub(crate) async fn destroy_message(c: &Ctx, room: &Room, message: &Message) -> Result<()> {
    let destroyed = message.clone();
    c.app().write(move |tx| destroyed.destroy(tx)).await?;
    c.app().broadcasts.message_remove(room, message);
    Ok(())
}

// --- Broadcasts and webhooks -----------------------------------------------------------------------

/// `@message.broadcast_create`: the message partial appended to the room, then the unread pings.
pub(crate) async fn broadcast_create(c: &Ctx, room: &Room, message: &Message) -> Result<()> {
    let (app, room, message) = (c.app().clone(), room.clone(), message.clone());
    let base_url = page::renderer_base_url(c);
    let renderer = page::DetachedRenderer::new(&app);
    c.app()
        .read(move |conn| {
            let presenter = Presenter::new(conn, &app, None);
            let view = presenter.message(&message)?;
            let account = campfire_db::Account::first(conn)?;
            let html = renderer.render_at(&app, account.as_ref(), &base_url, |ctx| views::message(ctx, &view));
            let partials = Rendered { message: Some(html), ..Rendered::default() };
            app.broadcasts.message_create(conn, &room, &message, &partials)
        })
        .await
}

/// `broadcast_replace_to @room, :messages, target: [ @message, :presentation ], partial:
/// "messages/presentation", attributes: { maintain_scroll: true }`
pub(crate) async fn broadcast_replace(c: &Ctx, room: &Room, message: &Message) -> Result<()> {
    let (app, room, message) = (c.app().clone(), room.clone(), message.clone());
    let base_url = page::renderer_base_url(c);
    let renderer = page::DetachedRenderer::new(&app);
    c.app()
        .read(move |conn| {
            let presenter = Presenter::new(conn, &app, None);
            let view = presenter.message(&message)?;
            let account = campfire_db::Account::first(conn)?;
            let html = renderer
                .render_at(&app, account.as_ref(), &base_url, |ctx| views::PresentationPartial { ctx, message: &view }.render())
                .map_err(campfire_db::Error::other)?;
            let partials = Rendered { message_presentation: Some(html), ..Rendered::default() };
            app.broadcasts.message_replace(&room, &message, &partials);
            Ok(())
        })
        .await
}

/// `deliver_webhooks_to_bots`: every active bot in a direct room, else every mentioned active
/// bot, except the message's creator.
pub(crate) async fn deliver_webhooks_to_bots(c: &Ctx, room: &Room, message: &Message) -> Result<()> {
    let (app, room, eligible) = (c.app().clone(), room.clone(), message.clone());
    let bots: Vec<User> = c
        .app()
        .read(move |conn| {
            let candidates = if room.direct() { room.active_bots(conn)? } else { eligible.mentionees(conn, &*app.db.env().rich_text)? };
            Ok(candidates
                .into_iter()
                .filter(|user| user.role == Role::Bot && user.status == Status::Active && user.id != eligible.creator_id)
                .collect())
        })
        .await?;
    if bots.is_empty() {
        return Ok(());
    }
    // bot.deliver_webhook_later(@message)
    let message_id = message.id;
    c.app().write(move |tx| bots.iter().try_for_each(|bot| bot.deliver_webhook_later(tx, message_id))).await
}

// --- Rendering ------------------------------------------------------------------------------------

/// Runs `f` with a presenter on a reader connection.
pub(crate) async fn present<T: Send + 'static>(
    c: &Ctx,
    f: impl FnOnce(&Presenter) -> campfire_db::Result<T> + Send + 'static,
) -> Result<T> {
    let app = c.app().clone();
    let fragments = crate::response_cache::fragments(c);
    let request_host = Some(c.request.host());
    c.app()
        .read(move |conn| {
            let presenter = Presenter::new(conn, &app, request_host);
            // The Jbuilder partials (`json.cache!`) read the fragment cache on this thread.
            campfire_views::fragment_cache::with(&fragments, || f(&presenter))
        })
        .await
}

/// `render action: :room_not_found` (inside the layout).
async fn render_room_not_found(c: &mut Ctx) -> Result {
    c.respond_to(&[&format::HTML])?;
    page::content_in_application_layout(c, StatusCode::OK, |_| views::RoomNotFound.render()).await
}

#[cfg(test)]
mod tests;
