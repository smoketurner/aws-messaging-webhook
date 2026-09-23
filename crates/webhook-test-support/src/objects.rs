//! Fake `ObjectStore` test double: an in-memory object map with per-key
//! error injection, including a never-completing mode for the
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
    /// The call's future never resolves — for the paused-time deadline
    /// tests, where the surrounding `tokio::time::timeout` is what's under
    /// test, not this store.
    Hang,
}

/// The ordered sequence of calls made against each operation: a
/// per-key call count is `iter().filter(|k| k == key).count()`, and the
/// "no put still pending after the deadline" test relies on a call
/// being recorded synchronously at entry, before any injected failure
/// (including [`ObjectFailure::Hang`]) is even consulted — so a hung call
/// still shows up here exactly once, and never again once its future is
/// dropped.
#[derive(Default)]
struct CallLog {
    get_object: Vec<String>,
    head_object: Vec<String>,
    put_object_if_absent: Vec<String>,
    delete_object: Vec<String>,
}

#[derive(Default)]
pub struct FakeObjectStore {
    objects: Mutex<HashMap<String, StoredObject>>,
    failures: Mutex<HashMap<String, ObjectFailure>>,
    calls: Mutex<CallLog>,
    /// A 1-based countdown that fails the Nth `put_object_if_absent` call with
    /// `ObjectError::Transient`, regardless of key, then clears itself.
    /// `outbound_message_id` is random, so the spec key is unknowable ahead of
    /// time; only call-order counting can target a particular put.
    nth_put_failure: Mutex<Option<usize>>,
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

    /// Makes the Nth `put_object_if_absent` call (1-based, across all keys)
    /// fail with `ObjectError::Transient`, then clears the injection. Use this
    /// to fail a particular put when the key is unknowable ahead of time (as
    /// `outbound_message_id` is random). A put that the Nth countdown fails is
    /// still recorded in [`Self::put_object_calls`]. A per-key injection by
    /// [`Self::inject`] pre-empts the countdown entirely for the call it fails,
    /// so a per-key fixture never consumes the countdown.
    pub fn inject_nth_put_if_absent(&self, n: usize) {
        *self.nth_put_failure.lock().unwrap() = Some(n);
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

    /// The ordered sequence of every key `delete_object` was called with.
    #[must_use]
    pub fn delete_object_calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().delete_object.clone()
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
        // A per-key failure pre-empts the nth-call countdown: the countdown
        // targets a particular put by call order, and a per-key fixture (a
        // `Hang` that never resolves, or a `Permanent`/`Transient` error) is a
        // different setup that should not consume it.
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
        let fail = {
            let mut guard = self.nth_put_failure.lock().unwrap();
            match *guard {
                Some(1) => {
                    *guard = None;
                    true
                }
                Some(n) => {
                    *guard = Some(n - 1);
                    false
                }
                None => false,
            }
        };
        if fail {
            return Err(ObjectError::Transient(anyhow!(
                "injected transient put_object_if_absent failure on the countdown call for {key}"
            )));
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

    async fn delete_object(&self, key: &str) -> Result<(), ObjectError> {
        self.calls
            .lock()
            .unwrap()
            .delete_object
            .push(key.to_owned());
        match self.injected(key) {
            Some(ObjectFailure::Hang) => future::pending().await,
            Some(ObjectFailure::Permanent) => {
                return Err(ObjectError::Permanent(anyhow!(
                    "injected permanent delete_object failure for {key}"
                )));
            }
            Some(ObjectFailure::Transient) => {
                return Err(ObjectError::Transient(anyhow!(
                    "injected transient delete_object failure for {key}"
                )));
            }
            // Deleting something already absent is the outcome the caller
            // wanted.
            Some(ObjectFailure::NotFound) | None => {}
        }
        self.objects.lock().unwrap().remove(key);
        Ok(())
    }

    /// A recognizable stand-in for a presigned URL, carrying the key and the
    /// signed response overrides so a test can assert on all three. The real
    /// implementation signs locally and never checks that the object exists,
    /// so this does not either — only an injected failure makes it fail.
    async fn presign_get(
        &self,
        key: &str,
        disposition: Option<&str>,
        content_type: Option<&str>,
    ) -> Result<String, ObjectError> {
        match self.injected(key) {
            Some(ObjectFailure::Hang) => future::pending().await,
            Some(ObjectFailure::NotFound) => return Err(ObjectError::NotFound),
            Some(ObjectFailure::Permanent) => {
                return Err(ObjectError::Permanent(anyhow!(
                    "injected permanent presign_get failure for {key}"
                )));
            }
            Some(ObjectFailure::Transient) => {
                return Err(ObjectError::Transient(anyhow!(
                    "injected transient presign_get failure for {key}"
                )));
            }
            None => {}
        }
        let mut url =
            format!("https://example-bucket.s3.amazonaws.test/{key}?X-Amz-Signature=fake");
        if let Some(disposition) = disposition {
            url.push_str("&response-content-disposition=");
            url.push_str(&urlencode(disposition));
        }
        if let Some(content_type) = content_type {
            url.push_str("&response-content-type=");
            url.push_str(&urlencode(content_type));
        }
        Ok(url)
    }
}

/// Percent-encodes everything outside the unreserved set, so an assertion on
/// the fake's URL sees the same escaping a real signer would apply.
fn urlencode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.as_bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(char::from(*byte));
        } else {
            const HEX: &[u8; 16] = b"0123456789ABCDEF";
            out.push('%');
            out.push(char::from(HEX[usize::from(byte >> 4)]));
            out.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
    }
    out
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
        // ever resolves — spawn it, let it start, then abort it
        // without ever completing, mirroring what a timeout does to a
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

    #[tokio::test]
    async fn inject_nth_put_fails_only_the_nth_call_then_clears_itself() {
        let store = FakeObjectStore::default();
        store.inject_nth_put_if_absent(2);

        let first = store
            .put_object_if_absent("a", Bytes::from_static(b"first"), "text/plain")
            .await;
        assert_eq!(first.unwrap(), PutOutcome::Created);
        assert!(store.contains("a"));

        // The second call fails with Transient and is still recorded.
        let second = store
            .put_object_if_absent("b", Bytes::from_static(b"second"), "text/plain")
            .await;
        assert!(matches!(second, Err(ObjectError::Transient(_))));
        assert!(!store.contains("b"));

        // The countdown has cleared itself: a later put proceeds normally.
        let third = store
            .put_object_if_absent("c", Bytes::from_static(b"third"), "text/plain")
            .await;
        assert_eq!(third.unwrap(), PutOutcome::Created);
        assert!(store.contains("c"));

        assert_eq!(
            store.put_object_calls(),
            vec!["a".to_owned(), "b".to_owned(), "c".to_owned()]
        );
    }

    #[tokio::test]
    async fn inject_nth_put_countdown_is_key_agnostic_and_one_based() {
        let store = FakeObjectStore::default();
        // 1 fails the very first call regardless of key.
        store.inject_nth_put_if_absent(1);
        let failed = store
            .put_object_if_absent("whatever", Bytes::new(), "text/plain")
            .await;
        assert!(matches!(failed, Err(ObjectError::Transient(_))));
        assert!(!store.contains("whatever"));
        // The countdown cleared: a follow-up put succeeds.
        store
            .put_object_if_absent("whatever", Bytes::new(), "text/plain")
            .await
            .unwrap();
        assert!(store.contains("whatever"));
    }

    #[tokio::test]
    async fn inject_nth_put_countdown_does_not_block_other_operations() {
        let store = FakeObjectStore::default();
        store.seed("seeded", Bytes::from_static(b"body"), "text/plain");
        store.inject_nth_put_if_absent(1);
        // The countdown scopes only to put_object_if_absent; reads/deletes run.
        assert!(store.get_object("seeded", 1024).await.is_ok());
        assert!(store.delete_object("seeded").await.is_ok());
    }
}
