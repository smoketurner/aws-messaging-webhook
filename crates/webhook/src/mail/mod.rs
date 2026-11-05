//! The binary's side of the mailbox: inbound ingest, the sender, the
//! relay's mail events, attachment fetching, and the mail bucket.

pub mod content;
pub(crate) mod events;
pub mod fetch;
pub mod ingest;
pub mod object_keys;
pub mod objects;
pub mod sender;
