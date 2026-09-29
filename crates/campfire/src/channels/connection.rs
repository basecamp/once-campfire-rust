//! `ApplicationCable::Connection` (reference/app/channels/application_cable/connection.rb):
//! `current_user` from the signed `session_token` cookie (`Authentication::SessionLookup`), or
//! `reject_unauthorized_connection`.
use campfire_cable::{Authenticate, ConnectRequest};
use campfire_db::{Database, Session, User};
use campfire_kit::{CookieJar, SharedClock, SharedCrypto};

use super::CableUser;

pub struct SessionAuthenticator {
    db: Database,
    crypto: SharedCrypto,
    clock: SharedClock,
}

impl SessionAuthenticator {
    pub fn new(db: Database, crypto: SharedCrypto, clock: SharedClock) -> Self {
        Self { db, crypto, clock }
    }

    /// `cookies.signed[:session_token]`.
    fn session_token(&self, request: &ConnectRequest) -> Option<String> {
        let headers = request.headers.get_all("cookie").iter().filter_map(|value| value.to_str().ok());
        CookieJar::from_headers(headers, self.crypto.clone(), self.clock.clone()).signed("session_token")
    }
}

impl Authenticate<CableUser> for SessionAuthenticator {
    async fn connect(&self, request: &ConnectRequest) -> Option<CableUser> {
        let token = self.session_token(request)?;
        let user = self
            .db
            .read(move |conn| match Session::find_by_token(conn, &token)? {
                Some(session) => User::find_by_id(conn, session.user_id),
                None => Ok(None),
            })
            .await;
        match user {
            Ok(user) => user.map(|user| CableUser { id: user.id, name: user.name }),
            Err(error) => {
                tracing::error!(%error, "Could not look up the cable session");
                None
            }
        }
    }
}
