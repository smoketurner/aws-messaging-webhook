//! Fake `ObjectStore` test double: an in-memory object map with per-key
//! error injection, including a never-completing mode for the D48
//! paused-time deadline tests.

use std::collections::HashMap;
use std::future;
use std::sync::Mutex;

use anyhow::anyhow;
use aws_messaging_webhook::mail::ObjectMeta;
use aws_messaging_webhook::mail::PutOutcome;
use aws_messaging_webhook::mail::objects::{ObjectError, ObjectStore};
use axum::body::Bytes;

#[derive(Debug, Clone)]
struct StoredObject {
    body: Bytes,
    content_type: String,
}

/// A failure to inject for every call against one key, until
/// [`FakeObjectStore::clear`] or [`FakeObjectStore::inject`] replaces it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectFailure {
    NotFound,
    Permanent,
    Transient,
    /// The call's future never resolves — for D48's paused-time deadline
    /// tests, where the surrounding `tokio::time::timeout` is what's under
    /// test, not this store.
    Hang,
}

/// The ordered sequence of calls made against each operation (N23): a
/// per-key call count is `iter().filter(|k| k == key).count()`, and the
/// D48 "no put still pending after the deadline" test relies on a call
/// being recorded synchronously at entry, before any injected failure
/// (including [`ObjectFailure::Hang`]) is even consulted — so a hung call
/// still shows up here exactly once, and never again once its future is
/// dropped.
#[derive(Default)]
struct CallLog {
    get_object: Vec<String>,
    head_object: Vec<String>,
    put_object_if_absent: Vec<String>,
}

#[derive(Default)]
pub struct FakeObjectStore {
    objects: Mutex<HashMap<String, StoredObject>>,
    failures: Mutex<HashMap<String, ObjectFailure>>,
    calls: Mutex<CallLog>,
}

impl FakeObjectStore {
    /// Seeds `key` with a body and content type, as if `put_object_if_absent`
    /// had already succeeded — for `get_object`/`head_object` tests that
    /// don't need to exercise the put path first.
    pub fn seed(&self, key: impl Into<String>, body: Bytes, content_type: impl Into<String>) {
        self.objects.lock().unwrap().insert(
            key.into(),
            StoredObject {
                body,
                content_type: content_type.into(),
            },
        );
    }

    /// Makes every call against `key` fail with `failure` until the
    /// injection is replaced or [`Self::clear`] is called.
    pub fn inject(&self, key: impl Into<String>, failure: ObjectFailure) {
        self.failures.lock().unwrap().insert(key.into(), failure);
    }

    /// Removes any injected failure for `key`.
    pub fn clear(&self, key: &str) {
        self.failures.lock().unwrap().remove(key);
    }

    /// The body currently stored under `key` (put or seeded), for
    /// assertions on what an ingest/send flow actually wrote.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<Bytes> {
        self.objects
            .lock()
            .unwrap()
            .get(key)
            .map(|o| o.body.clone())
    }

    /// The content type currently stored under `key`.
    #[must_use]
    pub fn content_type(&self, key: &str) -> Option<String> {
        self.objects
            .lock()
            .unwrap()
            .get(key)
            .map(|o| o.content_type.clone())
    }

    #[must_use]
    pub fn contains(&self, key: &str) -> bool {
        self.objects.lock().unwrap().contains_key(key)
    }

    fn injected(&self, key: &str) -> Option<ObjectFailure> {
        self.failures.lock().unwrap().get(key).copied()
    }

    /// The number of `get_object` calls made against `key` so far.
    #[must_use]
    pub fn get_object_call_count(&self, key: &str) -> usize {
        count(&self.calls.lock().unwrap().get_object, key)
    }

    /// The number of `head_object` calls made against `key` so far.
    #[must_use]
    pub fn head_object_call_count(&self, key: &str) -> usize {
        count(&self.calls.lock().unwrap().head_object, key)
    }

    /// The number of `put_object_if_absent` calls made against `key` so far.
    #[must_use]
    pub fn put_object_call_count(&self, key: &str) -> usize {
        count(&self.calls.lock().unwrap().put_object_if_absent, key)
    }

    /// The ordered sequence of every key `get_object` was called with
    /// (repeats included).
    #[must_use]
    pub fn get_object_calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().get_object.clone()
    }

    /// The ordered sequence of every key `head_object` was called with
    /// (repeats included).
    #[must_use]
    pub fn head_object_calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().head_object.clone()
    }

    /// The ordered sequence of every key `put_object_if_absent` was called
    /// with (repeats included).
    #[must_use]
    pub fn put_object_calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().put_object_if_absent.clone()
    }
}

fn count(calls: &[String], key: &str) -> usize {
    calls.iter().filter(|k| k.as_str() == key).count()
}

impl ObjectStore for FakeObjectStore {
    async fn get_object(&self, key: &str, max_bytes: u64) -> Result<Bytes, ObjectError> {
        self.calls.lock().unwrap().get_object.push(key.to_owned());
        match self.injected(key) {
            Some(ObjectFailure::Hang) => future::pending().await,
            Some(ObjectFailure::NotFound) => return Err(ObjectError::NotFound),
            Some(ObjectFailure::Permanent) => {
                return Err(ObjectError::Permanent(anyhow!(
                    "injected permanent get_object failure for {key}"
                )));
            }
            Some(ObjectFailure::Transient) => {
                return Err(ObjectError::Transient(anyhow!(
                    "injected transient get_object failure for {key}"
                )));
            }
            None => {}
        }
        let body = self
            .objects
            .lock()
            .unwrap()
            .get(key)
            .map(|o| o.body.clone())
            .ok_or(ObjectError::NotFound)?;
        let size = u64::try_from(body.len()).unwrap_or(u64::MAX);
        if size > max_bytes {
            return Err(ObjectError::TooLarge { size });
        }
        Ok(body)
    }

    async fn head_object(&self, key: &str) -> Result<Option<ObjectMeta>, ObjectError> {
        self.calls.lock().unwrap().head_object.push(key.to_owned());
        match self.injected(key) {
            Some(ObjectFailure::Hang) => future::pending().await,
            Some(ObjectFailure::NotFound) => return Ok(None),
            Some(ObjectFailure::Permanent) => {
                return Err(ObjectError::Permanent(anyhow!(
                    "injected permanent head_object failure for {key}"
                )));
            }
            Some(ObjectFailure::Transient) => {
                return Err(ObjectError::Transient(anyhow!(
                    "injected transient head_object failure for {key}"
                )));
            }
            None => {}
        }
        Ok(self.objects.lock().unwrap().get(key).map(|o| ObjectMeta {
            size: u64::try_from(o.body.len()).unwrap_or(u64::MAX),
        }))
    }

    async fn put_object_if_absent(
        &self,
        key: &str,
        body: Bytes,
        content_type: &str,
    ) -> Result<PutOutcome, ObjectError> {
        self.calls
            .lock()
            .unwrap()
            .put_object_if_absent
            .push(key.to_owned());
        match self.injected(key) {
            Some(ObjectFailure::Hang) => future::pending().await,
            Some(ObjectFailure::Permanent) => {
                return Err(ObjectError::Permanent(anyhow!(
                    "injected permanent put_object_if_absent failure for {key}"
                )));
            }
            Some(ObjectFailure::Transient) => {
                return Err(ObjectError::Transient(anyhow!(
                    "injected transient put_object_if_absent failure for {key}"
                )));
            }
            // `NotFound` isn't meaningful for a put; ignored.
            Some(ObjectFailure::NotFound) | None => {}
        }
        let mut objects = self.objects.lock().unwrap();
        if objects.contains_key(key) {
            return Ok(PutOutcome::AlreadyExists);
        }
        objects.insert(
            key.to_owned(),
            StoredObject {
                body,
                content_type: content_type.to_owned(),
            },
        );
        Ok(PutOutcome::Created)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn put_then_get_roundtrips_the_body_and_content_type() {
        let store = FakeObjectStore::default();
        let outcome = store
            .put_object_if_absent("k", Bytes::from_static(b"hello"), "text/plain")
            .await
            .unwrap();
        assert_eq!(outcome, PutOutcome::Created);
        assert_eq!(store.get("k").unwrap(), Bytes::from_static(b"hello"));
        assert_eq!(store.content_type("k").unwrap(), "text/plain");
        assert!(store.contains("k"));

        let fetched = store.get_object("k", 1024).await.unwrap();
        assert_eq!(fetched, Bytes::from_static(b"hello"));

        let meta = store.head_object("k").await.unwrap().unwrap();
        assert_eq!(meta.size, 5);
    }

    #[tokio::test]
    async fn put_is_idempotent_on_a_conflicting_key() {
        let store = FakeObjectStore::default();
        store
            .put_object_if_absent("k", Bytes::from_static(b"first"), "text/plain")
            .await
            .unwrap();
        let second = store
            .put_object_if_absent("k", Bytes::from_static(b"second"), "text/plain")
            .await
            .unwrap();
        assert_eq!(second, PutOutcome::AlreadyExists);
        // The first write wins; a losing racer's body never lands.
        assert_eq!(store.get("k").unwrap(), Bytes::from_static(b"first"));
    }

    #[tokio::test]
    async fn get_and_head_on_a_missing_key() {
        let store = FakeObjectStore::default();
        assert!(matches!(
            store.get_object("missing", 1024).await,
            Err(ObjectError::NotFound)
        ));
        assert!(store.head_object("missing").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn get_object_rejects_a_body_over_max_bytes() {
        let store = FakeObjectStore::default();
        store.seed("k", Bytes::from_static(b"0123456789"), "text/plain");
        let error = store.get_object("k", 5).await.unwrap_err();
        assert!(matches!(error, ObjectError::TooLarge { size: 10 }));
    }

    #[tokio::test]
    async fn injected_failures_apply_to_every_operation() {
        let store = FakeObjectStore::default();
        store.seed("k", Bytes::from_static(b"body"), "text/plain");

        store.inject("k", ObjectFailure::NotFound);
        assert!(matches!(
            store.get_object("k", 1024).await,
            Err(ObjectError::NotFound)
        ));
        assert!(store.head_object("k").await.unwrap().is_none());

        store.inject("k", ObjectFailure::Permanent);
        assert!(matches!(
            store.get_object("k", 1024).await,
            Err(ObjectError::Permanent(_))
        ));

        store.inject("k", ObjectFailure::Transient);
        assert!(matches!(
            store
                .put_object_if_absent("k", Bytes::new(), "text/plain")
                .await,
            Err(ObjectError::Transient(_))
        ));

        store.clear("k");
        assert!(store.get_object("k", 1024).await.is_ok());
    }

    #[tokio::test]
    async fn call_counters_track_every_operation_per_key() {
        let store = FakeObjectStore::default();
        store.seed("k", Bytes::from_static(b"body"), "text/plain");

        store.get_object("k", 1024).await.unwrap();
        store.get_object("k", 1024).await.unwrap();
        store.head_object("k").await.unwrap();
        store
            .put_object_if_absent("other", Bytes::new(), "text/plain")
            .await
            .unwrap();

        assert_eq!(store.get_object_call_count("k"), 2);
        assert_eq!(store.head_object_call_count("k"), 1);
        assert_eq!(store.put_object_call_count("other"), 1);
        assert_eq!(store.put_object_call_count("k"), 0);
        assert_eq!(
            store.get_object_calls(),
            vec!["k".to_owned(), "k".to_owned()]
        );
    }

    #[tokio::test]
    async fn a_call_is_counted_even_when_the_injected_failure_hangs() {
        let store = std::sync::Arc::new(FakeObjectStore::default());
        store.inject("k", ObjectFailure::Hang);

        // The call is recorded synchronously at entry, before the future
        // ever resolves (N23) — spawn it, let it start, then abort it
        // without ever completing, mirroring what a D48 timeout does to a
        // hung put.
        let task_store = std::sync::Arc::clone(&store);
        let hung = tokio::spawn(async move {
            task_store
                .put_object_if_absent("k", Bytes::new(), "text/plain")
                .await
        });
        tokio::task::yield_now().await;
        hung.abort();
        let _ = hung.await;

        assert_eq!(
            store.put_object_call_count("k"),
            1,
            "the call was recorded even though it never returned"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn hang_never_resolves_and_is_caught_by_an_external_timeout() {
        let store = FakeObjectStore::default();
        store.inject("k", ObjectFailure::Hang);
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            store.put_object_if_absent("k", Bytes::new(), "text/plain"),
        )
        .await;
        assert!(result.is_err(), "the hang must outlast a 30s timeout");
    }
}
