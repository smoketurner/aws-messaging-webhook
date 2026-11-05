//! Mail bucket object keys. The bucket's lifecycle rules expire objects by
//! these prefixes, so the shapes are a storage format.

use crate::mail::InboxId;

/// Where a message's content document lives: per inbox, because one inbound
/// message delivered to two inboxes is two independent messages.
#[must_use]
pub fn content(inbox_id: &InboxId, message_id: &str) -> String {
    format!("messages/{}/{message_id}.json", inbox_id.as_str())
}

/// Where one kept inbound attachment lives.
#[must_use]
pub fn attachment(message_id: &str, attachment_id: &str) -> String {
    format!("attachments/{message_id}/{attachment_id}")
}

/// Where the send spec for `message_id` lives.
#[must_use]
pub fn spec(message_id: &str) -> String {
    format!("outbox/{message_id}/spec.json")
}

/// Where one outbound attachment's bytes live before the message is sent.
#[must_use]
pub fn part(message_id: &str, attachment_id: &str) -> String {
    format!("outbox/{message_id}/parts/{attachment_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_is_scoped_to_the_inbox() {
        assert_eq!(
            content(&InboxId::new("support@example.com"), "01a0-msg"),
            "messages/support@example.com/01a0-msg.json"
        );
    }

    #[test]
    fn attachment_has_the_stored_shape() {
        assert_eq!(attachment("mid-1", "att-1"), "attachments/mid-1/att-1");
    }

    #[test]
    fn outbox_keys_all_sit_under_one_prefix() {
        // Retention and cleanup both work by prefix, so every object for a
        // message must be under it.
        let prefix = "outbox/mid-1/";
        assert!(spec("mid-1").starts_with(prefix));
        assert!(part("mid-1", "att_1").starts_with(prefix));
        assert_eq!(spec("mid-1"), "outbox/mid-1/spec.json");
        assert_eq!(part("mid-1", "att_1"), "outbox/mid-1/parts/att_1");
    }
}
