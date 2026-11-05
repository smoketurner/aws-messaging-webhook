//! Storing and loading a message's content document in the mail bucket.

use axum::body::Bytes;
use mailbox::content::{MAX_CONTENT_BYTES, MessageContent, serialize_within};
use mailbox::{InboxId, MailMessage, MessageId};

use crate::mail::object_keys;
use crate::mail::objects::{ObjectError, ObjectStore};

/// Stores `content` for a message. A document already there is left alone:
/// message ids are deterministic for inbound mail, so a redelivery writes
/// the same bytes.
///
/// A document that would exceed [`MAX_CONTENT_BYTES`] is stored without its
/// bodies (and, if still too large, further fields; see
/// `serialize_within`), so every stored document can be read back; what
/// is dropped stays reachable in the raw message, as attachments past the
/// cap do.
///
/// # Errors
///
/// [`ObjectError`] when the document cannot be serialized or written.
pub async fn store<S: ObjectStore>(
    objects: &S,
    inbox_id: &InboxId,
    message_id: &MessageId,
    content: &MessageContent,
) -> Result<(), ObjectError> {
    let serialize_error =
        |e| ObjectError::Permanent(anyhow::anyhow!("serializing message content: {e}"));
    let (body, fields_dropped) =
        serialize_within(content, MAX_CONTENT_BYTES).map_err(serialize_error)?;
    if fields_dropped {
        tracing::warn!(
            inbox_id = inbox_id.as_str(),
            %message_id,
            max_bytes = MAX_CONTENT_BYTES,
            event = "message_content_bodies_dropped",
            "message content exceeds the document cap once serialized; storing it without its bodies, and any further fields needed to fit, all of which remain in the raw message"
        );
    }
    objects
        .put_object_if_absent(
            &object_keys::content(inbox_id, message_id),
            Bytes::from(body),
            "application/json",
        )
        .await
        .map(|_outcome| ())
}

/// Loads a message's document. A missing document reads as empty content:
/// it only happens once retention has removed it, and the item's own fields
/// are still worth returning.
///
/// # Errors
///
/// [`ObjectError`] when the document cannot be read or does not parse.
pub async fn load<S: ObjectStore>(
    objects: &S,
    msg: &MailMessage,
) -> Result<MessageContent, ObjectError> {
    let key = object_keys::content(&msg.inbox_id, &msg.message_id);
    let body = match objects.get_object(&key, MAX_CONTENT_BYTES).await {
        Ok(body) => body,
        Err(ObjectError::NotFound) => {
            tracing::warn!(
                inbox_id = msg.inbox_id.as_str(),
                message_id = %msg.message_id,
                event = "message_content_missing",
                "message content document is missing; returning the item alone"
            );
            return Ok(MessageContent::default());
        }
        Err(error) => return Err(error),
    };
    serde_json::from_slice(&body)
        .map_err(|e| ObjectError::Permanent(anyhow::anyhow!("parsing {key}: {e}")))
}
