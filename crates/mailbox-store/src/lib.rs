//! The mail table: its keys, the transaction planner and its cancellation
//! decoding, the [`table::Table`] seam with its `DynamoDB` implementation, and
//! [`mailbox::Mailbox`], every mailbox read and write written once on top of
//! it. The `memory` feature adds an in-memory `Table` for tests.

pub mod dynamo_table;
pub mod keys;
pub mod mailbox;
#[cfg(feature = "memory")]
pub mod memory;
pub mod plan;
pub mod sdk_error;
pub mod store;
pub mod table;
pub mod txn;
