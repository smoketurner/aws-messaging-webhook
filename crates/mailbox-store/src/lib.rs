//! The mail table: its keys, the transaction planner and its cancellation
//! decoding, the [`Table`] seam with its `DynamoDB` implementation, and
//! [`Mailbox`], every mailbox read and write written once on top of it. The
//! `memory` feature adds an in-memory `Table` for tests.

#[cfg(test)]
mod alias_tests;
mod dynamo_table;
pub mod keys;
mod mailbox;
#[cfg(feature = "memory")]
mod memory;
mod plan;
mod sdk_error;
mod store;
mod table;
#[cfg(test)]
mod thread_race_tests;
mod txn;

pub use dynamo_table::DynamoTable;
pub use mailbox::Mailbox;
#[cfg(feature = "memory")]
pub use memory::{Injected, MailMemoryStore, ReadFailure, sample_message};
pub use plan::PlannedOp;
pub use sdk_error::FromSdkFailure;
pub use store::{
    DEFAULT_LIMIT, EnqueueOutcome, ListQuery, MAX_LIMIT, MailStoreError, Page, ThreadView,
};
pub use table::{MailIndex, PageQuery, Read, Table, TxnOutcome};
pub use txn::TxnDecision;
