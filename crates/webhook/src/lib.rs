//! Webhook receiver for AWS End User Messaging and SES events delivered over
//! SNS: verify → persist → lifecycle actions → publish to EventBridge.

pub mod actions;
pub mod allowlist;
pub mod api;
pub mod app;
pub mod aws;
pub mod config;
pub mod entry;
mod error;
mod events_keys;
pub mod logging;
pub mod mail;
pub mod metrics;
pub mod model;
pub mod publish;
mod sns;
pub mod state;
pub mod store;
mod stream;
