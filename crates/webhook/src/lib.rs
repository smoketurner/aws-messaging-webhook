//! Webhook receiver for AWS End User Messaging and SES events delivered over
//! SNS: verify → persist → lifecycle actions → publish to EventBridge.

pub mod actions;
pub mod allowlist;
pub mod app;
pub mod aws;
pub mod config;
pub mod entry;
pub mod error;
pub mod model;
pub mod publish;
pub mod sns;
pub mod state;
pub mod store;
