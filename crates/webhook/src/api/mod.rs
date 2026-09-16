//! The `/v0` mailbox HTTP API.
//!
//! Mounted on the same Function URL as the `/webhooks/...` paths. Every route
//! is behind bearer auth, and every response body — success or failure —
//! follows the published mailbox API contract, so an existing client works
//! against this service with only the base URL and key changed.
//!
//! Routes the contract defines but this service does not implement answer
//! `501` with a parseable body rather than a bare status line, and unknown
//! `/v0` paths answer the contract's `404`. Paths outside `/v0` keep axum's
//! default `404`.

pub mod auth;
pub mod error;
pub mod keys;
pub mod labels;
pub mod pagination;
pub mod read;
pub mod unimplemented;

use std::sync::Arc;

use axum::Router;
use axum::routing::{any, delete, get, post};

use crate::api::unimplemented::{method_not_allowed, not_implemented, unknown_route};
use crate::state::{AppState, Services};

/// The `/v0` router, ready to nest. Auth wraps every route including the
/// fallbacks, so an unauthenticated caller learns nothing about which paths
/// exist.
pub fn router<T: Services>(state: Arc<AppState<T>>) -> Router {
    Router::new()
        // Reads.
        .route("/inboxes", get(read::list_inboxes::<T>))
        .route("/inboxes/{inbox_id}", get(read::get_inbox::<T>))
        .route(
            "/inboxes/{inbox_id}/messages",
            get(read::list_messages::<T>),
        )
        .route(
            "/inboxes/{inbox_id}/messages/{message_id}",
            get(read::get_message::<T>).patch(labels::update_labels::<T>),
        )
        .route(
            "/inboxes/{inbox_id}/messages/{message_id}/raw",
            get(read::get_raw::<T>),
        )
        .route(
            "/inboxes/{inbox_id}/messages/{message_id}/attachments/{attachment_id}",
            get(read::get_attachment::<T>),
        )
        .route("/inboxes/{inbox_id}/threads", get(read::list_threads::<T>))
        .route(
            "/inboxes/{inbox_id}/threads/{thread_id}",
            get(read::get_thread::<T>),
        )
        // Resource groups this service does not implement.
        .route("/inboxes/{inbox_id}/drafts", any(not_implemented))
        .route("/inboxes/{inbox_id}/drafts/{*rest}", any(not_implemented))
        .route("/inboxes/{inbox_id}/labels", any(not_implemented))
        .route("/inboxes/{inbox_id}/labels/{*rest}", any(not_implemented))
        .route("/pods", any(not_implemented))
        .route("/pods/{*rest}", any(not_implemented))
        .route("/domains", any(not_implemented))
        .route("/domains/{*rest}", any(not_implemented))
        .route("/webhooks", any(not_implemented))
        .route("/webhooks/{*rest}", any(not_implemented))
        // Phase-4 method/path pairs: the same paths serve real reads later,
        // so only these methods answer 501.
        .route("/inboxes", post(not_implemented))
        .route("/inboxes/{inbox_id}", delete(not_implemented))
        .route(
            "/inboxes/{inbox_id}/threads/{thread_id}",
            delete(not_implemented),
        )
        .route(
            "/inboxes/{inbox_id}/messages/{message_id}",
            delete(not_implemented),
        )
        // Send and reply are not implemented yet.
        .route("/inboxes/{inbox_id}/messages/send", post(not_implemented))
        .route(
            "/inboxes/{inbox_id}/messages/{message_id}/reply",
            post(not_implemented),
        )
        .fallback(unknown_route)
        .method_not_allowed_fallback(method_not_allowed)
        .layer(axum::middleware::from_fn_with_state(
            Arc::clone(&state),
            auth::require_bearer::<T>,
        ))
        .with_state(state)
}
