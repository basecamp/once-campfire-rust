# Campfire in Rust

A Rust implementation of [ONCE Campfire](https://github.com/basecamp/once-campfire). It uses the
existing SQLite database, storage layout and signed/encrypted cookies, so existing installs can
upgrade without migrating data or signing everyone out.

One `campfire` executable replaces Ruby, Puma, Redis, Resque and Thruster, with libvips and ffmpeg
for media. The Rails frontend ships with a few [port-owned overrides](crates/assets/OVERRIDES.md).
The app includes TLS, HTTP/2, Web Push, bot webhooks, search and Action Cable-compatible WebSockets.

## Running it

With [ONCE](https://github.com/basecamp/once), on a server with Docker:

```sh
once deploy ghcr.io/basecamp/once-campfire-rust --host chat.example.com
```

ONCE manages secrets, TLS, backups and upgrades. Or run Docker directly:

```sh
docker run -d -p 80:80 -p 443:443 \
  -e SECRET_KEY_BASE=... -e VAPID_PUBLIC_KEY=... -e VAPID_PRIVATE_KEY=... \
  -e TLS_DOMAIN=chat.example.com \
  -v campfire:/rails/storage \
  ghcr.io/basecamp/once-campfire-rust
```

- `TLS_DOMAIN` enables automatic Let's Encrypt certificates; `DISABLE_SSL` enables plain HTTP.
- `/rails/storage` holds the database, uploads, backups and certificates. Existing installs must
  keep their storage and secrets.
- Web Push needs a valid P-256 VAPID key pair in URL-safe Base64. `VAPID_SUBJECT` sets the contact
  URL; its default is `https://` plus the first `TLS_DOMAIN`, or the project's URL.
- The app listener on `TARGET_PORT` (3000) binds loopback. `TARGET_BIND` overrides this; that listener
  trusts `X-Forwarded-*` from whoever reaches it. Other settings are in
  [`config.rs`](crates/campfire/src/config.rs).
- Images support amd64 and arm64. `:latest` and version tags track
  [releases](https://github.com/basecamp/once-campfire-rust/releases); `:main` tracks the main branch.

## Performance

Measured with 16 concurrent clients on an AMD Ryzen AI MAX+ 395 with 32 GB RAM,
with four hardware cores allocated to each app.

| HTTP workload (requests/sec) | Rails | [Django](https://github.com/basecamp/once-campfire-django) | [Laravel](https://github.com/basecamp/once-campfire-laravel) | [Express](https://github.com/basecamp/once-campfire-express) | [Elixir](https://github.com/basecamp/once-campfire-elixir) | [Go](https://github.com/basecamp/once-campfire-go) | [Rust](https://github.com/basecamp/once-campfire-rust) | [C](https://github.com/basecamp/once-campfire-c) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Room page | 4,101 | 1,507 | 3,872 | 42,636 | 5,350 | 53,060 | 106,494 | 137,524 |
| Messages page | 4,115 | 1,596 | 3,995 | 74,362 | 5,712 | 54,800 | 102,697 | 144,642 |
| Sidebar | 4,333 | 1,873 | 4,493 | 94,329 | 5,949 | 59,144 | 120,294 | 152,002 |
| Search | 4,282 | 1,862 | 4,172 | 84,665 | 5,848 | 60,509 | 121,378 | 149,487 |
| Post a message | 330 | 262 | 794 | 2,155 | 1,278 | 9,021 | 8,037 | 7,530 |
| Backend KLOC | 5.5 | 5.3 | 4.1 | 6.8 | 11.6 | 23.2 | 30.7 | 94.8 |

KLOC counts backend code plus executable code in templates; excludes plain HTML, frontend, tests, dependencies and generated files.

[Shared verification](https://github.com/basecamp/once-campfire-verification) · [Detailed results](https://github.com/basecamp/once-campfire-verification/blob/main/docs/performance-review.md).

Database scheduling, rich text rendering and cached-page gzip improvements contributed by
Daniel Collin ([emoon](https://github.com/emoon)) in
[#43](https://github.com/basecamp/once-campfire-rust/pull/43).

## Development

Check out the `reference/` submodule before building. Rust 1.98.1 is available through mise;
native media dependencies are specified in the [`Dockerfile`](Dockerfile).

```sh
git submodule update --init
parity/bin/reference build
parity/bin/seed build
CAMPFIRE_REQUIRE_SEED=1 cargo test --workspace --exclude html5ever
cargo clippy --workspace --exclude html5ever --all-targets
parity/bin/candidate build
parity/bin/candidate compare
bench/run
```

Seed generation and parity checks need Docker. Tests without the seed skip app integration tests.
For local development, run `cargo run -p campfire -- server` with `SECRET_KEY_BASE` set
(or `SECRET_KEY_BASE_DUMMY=1`). Build an image with `docker build -t campfire-rust .`.

The parity harness compares HTML, DOM, accessibility trees, assets, Cable frames and screenshots
against Rails. See [`parity/SCREENS.md`](parity/SCREENS.md) for coverage and masks,
[`AGENTS.md`](AGENTS.md) for repository layout and working rules,
[`CONTRIBUTING.md`](CONTRIBUTING.md) for contributions, and [`SECURITY.md`](SECURITY.md) for security reports.

## Known differences

The app keeps the Rails database, storage and current cookie formats compatible. Deliberate
behavior changes and compatibility limits are listed below.

<details>
<summary>Differences from Rails</summary>

- Broadcast rendering captures its fragment generation before reading the view, and
  uses a separate namespace from request-host-dependent fragments.
- Session-transfer auto-submit forms explicitly close their form tag; the pinned Rails
  reference omitted it.
- Background sidebar refreshes preserve an open New Ping form and selected recipients.

- Sidebar connection refresh waits for the current Turbo frame to finish loading,
  preventing an aborted response on startup or reconnect. Obsolete connections and removed frames do not reload.

- **WebSockets:** `permessage-deflate` without context takeover compresses each broadcast once
  for all subscribers. Decoded messages remain identical.
- **CSRF:** `Sec-Fetch-Site` replaces tokens. Writes accept `same-origin` and `same-site`, reject
  `cross-site` and missing headers over HTTPS with 422, and retain the `Origin` check. Plain HTTP
  accepts missing headers with `SameSite=Lax` cookies. Pages omit CSRF tags and fields; old tabs
  still work, but HTTPS forms require a browser that sends the header (Safari 16.4 or newer).
- **Jobs:** Redis and Resque are replaced by in-process queues with `JOB_CONCURRENCY` workers per
  job kind. Queued pushes and webhooks are lost on a crash; slow webhooks don't block pushes.
- **Push:** invalid VAPID keys disable push at boot. Subscriptions survive TLS/configuration
  failures and are deleted only on 404/410 or an invalid subscription P-256 key. Notification
  bodies are truncated with an ellipsis at 3 KB and titles at 256 bytes. `VAPID_SUBJECT` is configurable.
  Delivery timeouts are 10 seconds per connect/read and 30 seconds overall.
- **Cookies:** sessions are written only on change and deleted when empty; `last_room` only on
  change. `session_token` is re-signed on the hourly activity refresh, retaining its rolling
  20-year expiry. Other authenticated reads avoid the database writer.
- **Caching:** room, messages and search ETags hash cached page parts rather than the body.
  Copy-link buttons cache paths and resolve them against the page URL; bot JSON is cached per
  base URL, preventing a request's Host from changing other users' links. Authenticated room,
  messages, sidebar and search responses also retain their completed identity/gzip representation
  for up to 15 seconds in a bounded 64 MiB store (`CAMPFIRE_RESPONSE_CACHE_MB=0` disables it).
  Authentication and permissions are checked on every request; any SQLite commit, including
  another process's writes, invalidates previous responses. Cookies, flash, request variants,
  conditional requests and HEAD retain their normal behavior. Message fragments share the
  existing 32 MiB budget across origins and database generations, so direct SQL edits remain visible even
  without timestamp updates. Message ETags include rendered dependencies; messages omit
  `Last-Modified` because row timestamps cannot validate foreign edits. An `If-Modified-Since`
  request receives the current body.
- **SQLite:** boot adds `index_messages_on_room_id_and_created_at` and
  `index_messages_on_room_id_and_updated_at` if missing. They remain compatible with Rails.
  Memory mapping is disabled; reads use SQLite's page cache.
- **Message creation:** the search index and unread memberships are written in the message's own
  transaction, as current Rails does, rather than in commits of their own after it, so a message is
  never stored without them.
- **Media formats:** libvips 8.16.1 and ffmpeg 7.1.5 use the Rails image's Debian sources, with
  byte-identical thumbnails, posters and metadata for supported formats. libvips omits loaders
  Rails already blocks. ffmpeg omits external-library-only formats: tracker modules, game-console
  music, JPEG XL/SVG frames, codec2, teletext and DASH/IMF. Tracker/game-console uploads lack
  duration and bit rate. Unused encoders, muxers, hardware and network support are omitted.
- **Media processing:** message uploads are copied and checksummed before saving their rows, then
  deleted if saving fails. The redundant MD5 reread is skipped; analysis, variants, posters and
  client direct-upload checksums still validate files. At most four media jobs run off the database
  writer. Variants/posters are saved already analyzed; concurrent transforms keep the first saved
  result and delete duplicates. ffmpeg posters time out at 60 seconds, ffprobe at 30.
- **Request limits:** non-file-upload bodies and direct uploads are capped at 16 MiB (413).
  Nonnumeric direct-upload sizes and oversized QR codes return 422. Page numbers cap at a billion.
- **Cable limits:** 64 subscriptions per connection, 4 KiB identifiers and 1 MiB messages.
  Clients that don't read for 30 seconds disconnect. Banning/deactivating a user closes their
  connections after commit.
- **Unfurling:** 10 seconds overall, 5 per connect/read, at most 16 concurrent unfurls, and only
  the first 256 attributes of a `meta` tag are read. Timed-out pages unfurl nothing.
- **Webhooks:** 60 seconds overall, 7 per connect/read. Replies over 100 MB after decompression
  fail delivery without posting a response.
- **Front server:** `TARGET_PORT` binds loopback and enforces front-server timeouts and
  `MAX_REQUEST_BODY`. Cache keys count toward `CACHE_SIZE`, preserve raw paths/queries, skip URIs
  over 2 KB and forward range requests. Idle HTTP/1 connections close at the shorter of
  `HTTP_IDLE_TIMEOUT` and `HTTP_READ_TIMEOUT` until request headers arrive (30 seconds with defaults,
  60 with image settings); HTTP/2 uses the idle timeout. Response header lines containing DEL are omitted.
- **Passwords:** bcrypt runs outside database connections/transactions. Unknown emails still
  perform one bcrypt check.
- **JSON:** floats use the shortest equivalent digits. The web app manifest properly JSON-escapes
  account names and URLs.
- **Search:** words are literal full-text terms, including `NOT`, `AND`, `OR` and `NEAR`.
  The newest 100 matches are selected by message id, as in current Rails. Imported messages
  with creation times out of id order follow id order in search. A bounded global scan
  falls back to a membership-scoped query when most recent matches are inaccessible.
- **Routes and UI:** `/rooms/directs/:id` redirects to the room; infinite `Accept` q-values sort
  first or last by sign; EdgeHTML install instructions include the missing image; the new-ping
  picker requests JSON so suggestions appear.
- **Rich text attributes:** autolinking escapes `<`/`>` in attributes to prevent stored XSS.
  Sanitization drops `name` attributes to prevent DOM clobbering. Styles retain only `color` and
  `background-color` with plain keyword/hex/RGB/HSL values or CSS variables in bot/webhook HTML;
  message pages drop styles.
- **Rich text attachments:** content attachments nest at most eight levels; deeper content is
  empty. Deleted-user mentions render ☒ and are omitted in the editor. Active Storage attachments
  embedded in message bodies, which the composer can't create, render ☒.
- **Malformed rich text:** plain-text extraction failures are logged and use empty text or an
  attachment filename; messages are still indexed, pushed, broadcast and sent to bots. Bodies
  beyond 400 nesting levels or 400 attributes per element are stored unchanged with empty plain
  text. These messages render as unrenderable.
- **Not ported:** Active Storage streaming's duplicate `session_token` cookie or legacy AES-CBC
  cookies; Campfire uses AES-GCM.

HTTP-01 ACME validation is only unit-tested; TLS-ALPN-01 is tested end to end against a local ACME
server. Rich text is checked against Rails on 658 cases, including 400 fuzzed cases.

</details>

## License

MIT. See [`MIT-LICENSE`](MIT-LICENSE).
