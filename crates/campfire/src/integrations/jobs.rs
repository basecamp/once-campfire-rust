//! The jobs integrations perform for the app's job runner: `Room::PushMessageJob`
//! (reference/app/jobs/room/push_message_job.rb) and `Bot::WebhookJob`
//! (reference/app/jobs/bot/webhook_job.rb), including what `Webhook#deliver` does with a reply
//! (create the bot's message, process an attachment, `broadcast_create`).

use anyhow::{Context as _, anyhow};
use campfire_db::{Database, Event, Message, NewMessage, PushSubscription, Room, User, Webhook};
use campfire_views::messages as views;

use super::net::Network;
use super::web_push::{self, VapidConfig, VapidError};
use super::webhook::{self, WebhookReply};
use crate::app::App;
use crate::config::Config;
use crate::controllers::messages::{canonicalize_body, process_attachment, save_staged};
use crate::controllers::presenters::Presenter;
use crate::controllers::presenters::page::{self, Rendered};
use crate::jobs::{JobKind, Registry};

/// Registers the handlers for `Event::PushMessage` and `Event::DeliverWebhook`.
pub fn register_jobs(registry: &mut Registry) {
    registry.handle(JobKind::PushMessage, push_message);
    registry.handle(JobKind::DeliverWebhook, deliver_webhook);
}

/// `Room::PushMessageJob#perform(room, message)`: `Room::MessagePusher.new(room:, message:).push`,
/// unless Web Push is off.
async fn push_message(app: App, event: Event) -> anyhow::Result<()> {
    let Event::PushMessage { message_id, .. } = event else { return Ok(()) };
    let Some(pool) = app.web_push.clone() else { return Ok(()) };
    let db = app.db.clone();
    // Offloaded: it encrypts a notification for every subscriber.
    app.db
        .read_offloaded(move |conn| {
            let message = Message::find(conn, message_id)?;
            web_push::push_message(&pool, conn, &*db.env().rich_text, &message, db.env().now()).map(|_| ())
        })
        .await?;
    Ok(())
}

/// config/initializers/web_push.rb (`config.x.web_push_pool`): the pool, whose invalid
/// subscription handler destroys the subscription (`Push::Subscription.find_by(id:)&.destroy`).
/// `None`, and Web Push is off, when the VAPID keys are missing or invalid. Call from inside the
/// runtime.
pub fn web_push_pool(config: &Config, db: &Database, backlog: std::sync::Arc<campfire_jobs::Backlog>) -> Option<web_push::Pool> {
    let vapid = match VapidConfig::from_config(config) {
        Ok(vapid) => vapid,
        Err(error @ VapidError::Missing) => {
            tracing::warn!("Web Push is off: {error}");
            return None;
        }
        Err(error) => {
            tracing::error!("Web Push is off: {error}");
            return None;
        }
    };
    let db = db.clone();
    let destroy = move |id| {
        db.write_blocking(move |tx| match PushSubscription::find(tx.conn(), id) {
            Ok(subscription) => subscription.destroy(tx),
            Err(campfire_db::Error::RecordNotFound(_)) => Ok(()),
            Err(error) => Err(error),
        })
    };
    Some(web_push::Pool::counting(Network::system(), vapid, destroy, backlog))
}

/// `Bot::WebhookJob#perform(bot, message)`: `bot.deliver_webhook(message)`, i.e.
/// `webhook.deliver(message)`, then the reply.
async fn deliver_webhook(app: App, event: Event) -> anyhow::Result<()> {
    let Event::DeliverWebhook { bot_id, message_id } = event else { return Ok(()) };
    let db = app.db.clone();
    let (bot, room, url, payload) = app
        .db
        .read(move |conn| {
            let bot = User::find(conn, bot_id)?;
            let message = Message::find(conn, message_id)?;
            let room = Room::find(conn, message.room_id)?;
            let Some(webhook) = Webhook::find_by_user(conn, bot_id)? else { return Ok(None) };
            let payload = webhook.payload(
                conn,
                &*db.env().rich_text,
                &message,
                &campfire_routes::room_bot_messages(room.id, bot.bot_key()),
                &campfire_routes::room_at_message(room.id, message.id),
            )?;
            Ok(Some((bot, room, webhook.url, payload)))
        })
        .await?
        .context("undefined method 'deliver' for nil (the bot has no webhook)")?;

    let delivery = webhook::deliver(&Network::system(), url.as_deref().unwrap_or(""), payload).await?;
    let message = match delivery.reply {
        WebhookReply::None => return Ok(()),
        WebhookReply::Text(text) => create_text_reply(&app, &room, &bot, text).await?,
        WebhookReply::Attachment(attachment) => create_attachment_reply(&app, &room, &bot, attachment).await?,
    };
    broadcast_create(&app, &room, &message).await
}

/// `room.messages.create!(body: text, creator: user)`: the text is assigned to the rich text
/// body, which stores it canonicalized (as `MessagesController` does; there's no request host
/// in a job).
async fn create_text_reply(app: &App, room: &Room, bot: &User, text: String) -> anyhow::Result<Message> {
    let (room_id, creator_id) = (room.id, bot.id);
    let body = canonicalize_body(app, text, None).await.map_err(|e| anyhow!("{e:?}"))?;
    let message = app
        .db
        .write(move |tx| {
            Message::create(tx, NewMessage { room_id, creator_id, client_message_id: None, body: Some(body), attachment_blob_id: None })
        })
        .await?;
    Ok(message)
}

/// `ActiveStorage::Blob.create_and_upload!` (its own save), then
/// `room.messages.create_with_attachment!(attachment:, creator: user)`, which processes the
/// attachment.
async fn create_attachment_reply(app: &App, room: &Room, bot: &User, attachment: webhook::Attachment) -> anyhow::Result<Message> {
    let storage = app.storage.clone();
    let staged = tokio::task::spawn_blocking(move || attachment.stage_blob(&storage)).await??;
    let blob = app.db.write(move |tx| save_staged(tx, staged)).await?;

    let (room_id, creator_id, blob_id) = (room.id, bot.id, blob.id);
    let message = app
        .db
        .write(move |tx| {
            Message::create(tx, NewMessage { room_id, creator_id, client_message_id: None, body: None, attachment_blob_id: Some(blob_id) })
        })
        .await?;
    process_attachment(app, blob).await.map_err(|e| anyhow!("{e:?}"))?;
    let id = message.id;
    Ok(app.db.read(move |conn| Message::find(conn, id)).await?)
}

/// `message.broadcast_create`, rendered without a request (`ApplicationController.renderer`).
async fn broadcast_create(app: &App, room: &Room, message: &Message) -> anyhow::Result<()> {
    let (app, room, message) = (app.clone(), room.clone(), message.clone());
    let db = app.db.clone();
    let renderer = page::DetachedRenderer::new(&app);
    db.read(move |conn| {
        let presenter = Presenter::new(conn, &app, None);
        let view = presenter.message(&message)?;
        let account = campfire_db::Account::first(conn)?;
        let html = renderer.render(&app, account.as_ref(), |ctx| views::message(ctx, &view));
        let partials = Rendered { message: Some(html), ..Rendered::default() };
        app.broadcasts.message_create(conn, &room, &message, &partials)
    })
    .await?;
    Ok(())
}
