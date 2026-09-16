//! An in-memory [`ApiKeySource`] for handler tests.
//!
//! Tests set the key document the parameter store would return, can make the
//! fetch fail to exercise the cold-cache and outage paths, and can count
//! fetches to prove the miss-refresh floor holds.

use std::future::Future;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use aws_messaging_webhook::api::keys::{ApiKeyError, ApiKeySource};
use sha2::{Digest, Sha256};

/// What the next fetch should do.
enum Outcome {
    Document(String),
    Unavailable,
}

pub struct FakeApiKeys {
    outcome: Mutex<Outcome>,
    fetches: AtomicUsize,
}

impl FakeApiKeys {
    /// A source holding one key with the given id, as an operator would store
    /// it: the SHA-256 hash, never the key.
    #[must_use]
    pub fn with_key(key: &str, id: &str) -> Self {
        let source = Self {
            outcome: Mutex::new(Outcome::Unavailable),
            fetches: AtomicUsize::new(0),
        };
        source.set_keys(&[(key, id)]);
        source
    }

    /// A source whose parameter cannot be read.
    #[must_use]
    pub fn unavailable() -> Self {
        Self {
            outcome: Mutex::new(Outcome::Unavailable),
            fetches: AtomicUsize::new(0),
        }
    }

    /// Replaces the stored document with hashes of `keys`.
    pub fn set_keys(&self, keys: &[(&str, &str)]) {
        let entries: Vec<String> = keys
            .iter()
            .map(|(key, id)| {
                let digest = Sha256::digest(key.as_bytes());
                format!(r#"{{"id":"{id}","sha256":"{digest:x}"}}"#)
            })
            .collect();
        let document = format!(r#"{{"keys":[{}]}}"#, entries.join(","));
        *self.outcome.lock().unwrap() = Outcome::Document(document);
    }

    /// Makes every later fetch fail, as an SSM outage would.
    pub fn make_unavailable(&self) {
        *self.outcome.lock().unwrap() = Outcome::Unavailable;
    }

    /// How many times the cache has read the parameter.
    #[must_use]
    pub fn fetches(&self) -> usize {
        self.fetches.load(Ordering::SeqCst)
    }
}

/// Defaults to unavailable: a test that never configures keys gets 503
/// rather than silently authenticating.
impl Default for FakeApiKeys {
    fn default() -> Self {
        Self::unavailable()
    }
}

impl ApiKeySource for FakeApiKeys {
    fn fetch(&self) -> impl Future<Output = Result<String, ApiKeyError>> + Send {
        self.fetches.fetch_add(1, Ordering::SeqCst);
        let result = match &*self.outcome.lock().unwrap() {
            Outcome::Document(document) => Ok(document.clone()),
            Outcome::Unavailable => Err(ApiKeyError::Fetch(anyhow::anyhow!(
                "simulated parameter store outage"
            ))),
        };
        std::future::ready(result)
    }
}
