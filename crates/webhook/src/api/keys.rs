//! Bearer API keys: the key document, its cache, and constant-time matching.
//!
//! The operator stores SHA-256 hashes of the keys in an SSM `SecureString`,
//! never the keys themselves:
//!
//! ```json
//! {"keys": [{"id": "key_1", "sha256": "<64 hex chars>"}]}
//! ```
//!
//! Three properties matter here, and each has a test:
//!
//! - **Constant-time matching.** Every candidate is compared, with no early
//!   exit, so response timing says nothing about how much of a key matched.
//! - **A miss cannot drive unlimited fetches.** An unauthenticated caller
//!   presenting junk would otherwise trigger a parameter read per request; a
//!   miss refreshes at most once per [`MISS_REFRESH_FLOOR`], and the stamp is
//!   taken *before* the fetch so concurrent misses collapse into one.
//! - **A cold cache is retryable, not a rejection.** If the cache has never
//!   loaded and the fetch fails, callers get 503; a valid key must never be
//!   answered with 401 because the parameter store blinked.
//! - **An outage cannot drive unlimited scheduled fetches.** A failed refresh
//!   keeps the last good cache in use but re-arms the scheduled-refresh gate
//!   for [`FAILURE_BACKOFF`], so a sustained outage yields at most one
//!   scheduled fetch per window — not one per request — until SSM recovers.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Deserialize;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tokio::time::Instant;

/// How often a loaded cache refreshes on its own.
const REFRESH_INTERVAL: Duration = Duration::from_secs(300);

/// Minimum spacing between refreshes triggered by an unrecognized key.
const MISS_REFRESH_FLOOR: Duration = Duration::from_secs(30);

/// How long a loaded cache is treated as fresh after a refresh failure, so a
/// sustained parameter-store outage drives at most one *scheduled* fetch per
/// window instead of one per request. Matches [`MISS_REFRESH_FLOOR`] so the
/// two outage throttles share a cadence.
const FAILURE_BACKOFF: Duration = Duration::from_secs(30);

/// Length of a SHA-256 digest in hex characters.
const HEX_DIGEST_LEN: usize = 64;

/// The parsed `SecureString` document.
#[derive(Debug, Deserialize)]
struct KeyDocument {
    keys: Vec<KeyEntry>,
}

#[derive(Debug, Deserialize)]
struct KeyEntry {
    id: String,
    sha256: String,
}

/// One usable key: its operator-facing id and the digest to match against.
#[derive(Debug, Clone)]
struct KeyHash {
    id: String,
    digest: [u8; 32],
}

/// Reads the raw key document. The production implementation calls SSM with
/// decryption; tests use an in-memory fake.
pub trait ApiKeySource: Send + Sync {
    fn fetch(&self) -> impl Future<Output = Result<String, ApiKeyError>> + Send;
}

#[derive(Debug, thiserror::Error)]
pub enum ApiKeyError {
    #[error("api key parameter could not be read")]
    Fetch(#[source] anyhow::Error),
    #[error("api key parameter is malformed")]
    Malformed(#[source] anyhow::Error),
}

/// The outcome of checking a presented key.
#[derive(Debug, PartialEq, Eq)]
pub enum Verdict {
    /// Matched the key with this id.
    Allowed(String),
    /// No configured key matched.
    Denied,
    /// The cache has never loaded and the parameter store is unreachable, so
    /// whether the key is valid is unknown. Callers map this to 503.
    Unavailable,
}

#[derive(Default)]
struct CacheState {
    /// `None` until the first successful load.
    hashes: Option<Arc<Vec<KeyHash>>>,
    /// When the scheduled-refresh gate was last armed: advanced to `now` on a
    /// successful load, or rewound on a failed one so a failing source does not
    /// re-enter `refresh` on every request. `None` until the first successful
    /// load — a cold cache is never re-armed on failure, so it stays `Unavailable`.
    loaded_at: Option<Instant>,
    last_miss_refresh: Option<Instant>,
}

/// In-memory key cache shared by every request in one execution environment.
#[derive(Default)]
pub struct KeyCache {
    state: Mutex<CacheState>,
}

impl KeyCache {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Checks `presented` against the configured keys, refreshing the cache
    /// when it is stale, never loaded, or (rate-limited) on a miss.
    pub async fn verify<S: ApiKeySource>(&self, source: &S, presented: &str) -> Verdict {
        let digest = Sha256::digest(presented.as_bytes());

        let (mut hashes, stale) = self.snapshot();
        if hashes.is_none() || stale {
            hashes = self.refresh(source, hashes).await;
        }

        let Some(current) = hashes else {
            // Never loaded and the fetch failed: unknown, not denied.
            return Verdict::Unavailable;
        };

        if let Some(id) = match_key(&current, &digest) {
            return Verdict::Allowed(id);
        }

        // A miss may mean a key was just added. Refresh at most once per
        // MISS_REFRESH_FLOOR so junk keys can't drive parameter reads.
        if self.claim_miss_refresh()
            && let Some(refreshed) = self.refresh(source, Some(current)).await
            && let Some(id) = match_key(&refreshed, &digest)
        {
            return Verdict::Allowed(id);
        }
        Verdict::Denied
    }

    /// Current hashes plus whether the scheduled refresh is due.
    fn snapshot(&self) -> (Option<Arc<Vec<KeyHash>>>, bool) {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let stale = state
            .loaded_at
            .is_none_or(|at| at.elapsed() >= REFRESH_INTERVAL);
        (state.hashes.clone(), stale)
    }

    /// Takes the miss-refresh slot if the floor has elapsed. The stamp is set
    /// here, before the fetch, so concurrent misses collapse into one fetch.
    fn claim_miss_refresh(&self) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let allowed = state
            .last_miss_refresh
            .is_none_or(|at| at.elapsed() >= MISS_REFRESH_FLOOR);
        if allowed {
            state.last_miss_refresh = Some(Instant::now());
        }
        allowed
    }

    /// Fetches and replaces the cache. On failure the previous value is kept
    /// and returned, so a loaded cache survives a parameter-store outage, and
    /// the scheduled-refresh gate is re-armed for [`FAILURE_BACKOFF`] so the
    /// outage drives at most one scheduled fetch per window.
    async fn refresh<S: ApiKeySource>(
        &self,
        source: &S,
        previous: Option<Arc<Vec<KeyHash>>>,
    ) -> Option<Arc<Vec<KeyHash>>> {
        // The lock is never held across this await (clippy::await_holding_lock).
        let fetched = source.fetch().await;
        match fetched.and_then(|raw| parse_document(&raw)) {
            Ok(keys) => {
                let keys = Arc::new(keys);
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.hashes = Some(Arc::clone(&keys));
                state.loaded_at = Some(Instant::now());
                Some(keys)
            }
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    event = "api_keys_refresh_failed",
                    had_cache = previous.is_some(),
                    "could not refresh API keys"
                );
                // Re-arm the gate so a failing source drives one scheduled
                // fetch per `FAILURE_BACKOFF` rather than one per request: by
                // rewinding `loaded_at` the cache is treated as fresh for the
                // backoff, then becomes stale again. Only a cache that already
                // holds a value is worth serving through an outage, so a cold
                // cache is left stale. The `checked_sub`s never move the gate
                // earlier: when this failure fires before the window would have
                // reopened — notably a miss-driven refresh just after a
                // successful load — the existing, longer deferral is kept, so
                // the scheduled refresh still opens at the original five-minute
                // mark. (The report's literal `now() - REFRESH_INTERVAL +
                // FAILURE_BACKOFF` form, by contrast, back-dates the gate to
                // `now() - 270 s` whenever it fires, so an early miss-refresh
                // failure reopens the scheduled gate far earlier than five
                // minutes — verified to fail the early-miss test.)
                if previous.is_some()
                    && let Some(rewind) = REFRESH_INTERVAL.checked_sub(FAILURE_BACKOFF)
                    && let Some(rewound) = Instant::now().checked_sub(rewind)
                {
                    let mut state = self
                        .state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if !matches!(state.loaded_at, Some(existing) if existing > rewound) {
                        state.loaded_at = Some(rewound);
                    }
                }
                previous
            }
        }
    }
}

/// Compares `digest` against every candidate without an early exit.
fn match_key(keys: &[KeyHash], digest: &[u8]) -> Option<String> {
    let mut matched: Option<&KeyHash> = None;
    for key in keys {
        if bool::from(key.digest.ct_eq(digest)) {
            matched = Some(key);
        }
    }
    matched.map(|key| key.id.clone())
}

fn parse_document(raw: &str) -> Result<Vec<KeyHash>, ApiKeyError> {
    let document: KeyDocument =
        serde_json::from_str(raw).map_err(|e| ApiKeyError::Malformed(e.into()))?;
    document
        .keys
        .into_iter()
        .map(|entry| {
            let digest = decode_hex_digest(&entry.sha256).ok_or_else(|| {
                ApiKeyError::Malformed(anyhow::anyhow!(
                    "key {} has a malformed sha256 value",
                    entry.id
                ))
            })?;
            Ok(KeyHash {
                id: entry.id,
                digest,
            })
        })
        .collect()
}

fn decode_hex_digest(hex: &str) -> Option<[u8; 32]> {
    if hex.len() != HEX_DIGEST_LEN {
        return None;
    }
    let mut out = [0u8; 32];
    let (pairs, rest) = hex.as_bytes().as_chunks::<2>();
    if !rest.is_empty() {
        return None;
    }
    for (slot, [high, low]) in out.iter_mut().zip(pairs) {
        let high = char::from(*high).to_digit(16)?;
        let low = char::from(*low).to_digit(16)?;
        *slot = u8::try_from(high * 16 + low).ok()?;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    const KEY: &str = "am_test_key";
    /// `sha256("am_test_key")`, the value an operator would store.
    fn document_for(key: &str, id: &str) -> String {
        let digest = Sha256::digest(key.as_bytes());
        format!(r#"{{"keys":[{{"id":"{id}","sha256":"{digest:x}"}}]}}"#)
    }

    struct FakeSource {
        document: Mutex<Result<String, ()>>,
        fetches: AtomicUsize,
    }

    impl FakeSource {
        fn ok(document: String) -> Self {
            Self {
                document: Mutex::new(Ok(document)),
                fetches: AtomicUsize::new(0),
            }
        }

        fn failing() -> Self {
            Self {
                document: Mutex::new(Err(())),
                fetches: AtomicUsize::new(0),
            }
        }

        fn set(&self, document: Result<String, ()>) {
            *self.document.lock().unwrap() = document;
        }

        fn fetches(&self) -> usize {
            self.fetches.load(Ordering::SeqCst)
        }
    }

    impl ApiKeySource for FakeSource {
        fn fetch(&self) -> impl Future<Output = Result<String, ApiKeyError>> + Send {
            self.fetches.fetch_add(1, Ordering::SeqCst);
            let result = match &*self.document.lock().unwrap() {
                Ok(document) => Ok(document.clone()),
                Err(()) => Err(ApiKeyError::Fetch(anyhow::anyhow!("ssm unavailable"))),
            };
            std::future::ready(result)
        }
    }

    #[tokio::test]
    async fn a_configured_key_is_allowed_and_names_its_id() {
        let source = FakeSource::ok(document_for(KEY, "key_1"));
        let cache = KeyCache::new();
        assert_eq!(
            cache.verify(&source, KEY).await,
            Verdict::Allowed("key_1".to_owned())
        );
    }

    #[tokio::test]
    async fn an_unknown_key_is_denied() {
        let source = FakeSource::ok(document_for(KEY, "key_1"));
        let cache = KeyCache::new();
        assert_eq!(cache.verify(&source, "am_wrong").await, Verdict::Denied);
    }

    #[tokio::test]
    async fn a_cold_cache_that_cannot_load_is_unavailable_not_denied() {
        let source = FakeSource::failing();
        let cache = KeyCache::new();
        assert_eq!(cache.verify(&source, KEY).await, Verdict::Unavailable);
    }

    #[tokio::test]
    async fn a_malformed_document_leaves_a_cold_cache_unavailable() {
        let source = FakeSource::ok(r#"{"keys":[{"id":"k","sha256":"nope"}]}"#.to_owned());
        let cache = KeyCache::new();
        assert_eq!(cache.verify(&source, KEY).await, Verdict::Unavailable);
    }

    #[tokio::test(start_paused = true)]
    async fn a_loaded_cache_survives_a_later_fetch_failure() {
        let source = FakeSource::ok(document_for(KEY, "key_1"));
        let cache = KeyCache::new();
        assert!(matches!(
            cache.verify(&source, KEY).await,
            Verdict::Allowed(_)
        ));

        source.set(Err(()));
        tokio::time::advance(REFRESH_INTERVAL + Duration::from_secs(1)).await;

        assert!(
            matches!(cache.verify(&source, KEY).await, Verdict::Allowed(_)),
            "a valid key must keep working while the parameter store is down"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn repeated_misses_do_not_refetch_within_the_floor() {
        let source = FakeSource::ok(document_for(KEY, "key_1"));
        let cache = KeyCache::new();

        // First miss: one load plus one miss-triggered refresh.
        assert_eq!(cache.verify(&source, "am_junk").await, Verdict::Denied);
        let after_first = source.fetches();

        for _ in 0..20 {
            assert_eq!(cache.verify(&source, "am_junk").await, Verdict::Denied);
        }
        assert_eq!(
            source.fetches(),
            after_first,
            "misses inside the floor must not reach the parameter store"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_key_added_after_the_floor_is_picked_up_on_the_next_miss() {
        let source = FakeSource::ok(document_for("am_old", "key_1"));
        let cache = KeyCache::new();
        assert_eq!(cache.verify(&source, KEY).await, Verdict::Denied);

        source.set(Ok(document_for(KEY, "key_2")));
        tokio::time::advance(MISS_REFRESH_FLOOR + Duration::from_secs(1)).await;

        assert_eq!(
            cache.verify(&source, KEY).await,
            Verdict::Allowed("key_2".to_owned()),
            "a freshly added key should work without waiting for the scheduled refresh"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn repro_outage_valid_key_fetches_unbounded() {
        let source = FakeSource::ok(document_for(KEY, "key_1"));
        let cache = KeyCache::new();
        assert!(matches!(
            cache.verify(&source, KEY).await,
            Verdict::Allowed(_)
        ));

        source.set(Err(()));
        tokio::time::advance(REFRESH_INTERVAL + Duration::from_secs(1)).await;

        let before = source.fetches();
        for _ in 0..5 {
            assert!(matches!(
                cache.verify(&source, KEY).await,
                Verdict::Allowed(_)
            ));
        }
        let fetches_during_outage = source.fetches() - before;
        assert!(
            fetches_during_outage <= 1,
            "REFRESH_INTERVAL should bound fetches to <= 1 per window, got {fetches_during_outage}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn repro_outage_junk_key_fetches_unbounded() {
        let source = FakeSource::ok(document_for(KEY, "key_1"));
        let cache = KeyCache::new();
        assert!(matches!(
            cache.verify(&source, KEY).await,
            Verdict::Allowed(_)
        ));

        source.set(Err(()));
        tokio::time::advance(REFRESH_INTERVAL + Duration::from_secs(1)).await;

        let before = source.fetches();
        for _ in 0..5 {
            assert_eq!(cache.verify(&source, "am_junk").await, Verdict::Denied);
        }
        let fetches_during_outage = source.fetches() - before;
        assert!(
            fetches_during_outage <= 2,
            "junk keys during outage should be throttled to at most 2 fetches \
             (first scheduled + first miss), got {fetches_during_outage}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_refresh_rearms_the_gate_for_the_backoff_not_forever() {
        let source = FakeSource::ok(document_for(KEY, "key_1"));
        let cache = KeyCache::new();
        assert!(matches!(
            cache.verify(&source, KEY).await,
            Verdict::Allowed(_)
        ));

        source.set(Err(()));
        tokio::time::advance(REFRESH_INTERVAL + Duration::from_secs(1)).await;

        // The first request past the window triggers one failed scheduled
        // refresh; re-arming keeps the gate closed for FAILURE_BACKOFF.
        let after = source.fetches();
        assert!(matches!(
            cache.verify(&source, KEY).await,
            Verdict::Allowed(_)
        ));
        assert_eq!(
            source.fetches() - after,
            1,
            "one scheduled fetch when the window opens"
        );

        // Subsequent requests inside the backoff must not refetch.
        let after = source.fetches();
        for _ in 0..5 {
            assert!(matches!(
                cache.verify(&source, KEY).await,
                Verdict::Allowed(_)
            ));
        }
        assert_eq!(
            source.fetches() - after,
            0,
            "no refetches within FAILURE_BACKOFF"
        );

        // Once FAILURE_BACKOFF elapses the gate reopens: one more fetch is due.
        tokio::time::advance(FAILURE_BACKOFF + Duration::from_secs(1)).await;
        let after = source.fetches();
        assert!(matches!(
            cache.verify(&source, KEY).await,
            Verdict::Allowed(_)
        ));
        assert_eq!(
            source.fetches() - after,
            1,
            "the gate reopens after exactly FAILURE_BACKOFF"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_miss_refresh_failure_before_the_window_keeps_the_scheduled_gate() {
        let source = FakeSource::ok(document_for(KEY, "key_1"));
        let cache = KeyCache::new();
        assert!(matches!(
            cache.verify(&source, KEY).await,
            Verdict::Allowed(_)
        ));
        assert_eq!(source.fetches(), 1);

        // A miss-driven refresh fails at the very start, long before the
        // five-minute window would reopen. This must leave the scheduled-
        // refresh gate armed at the original five-minute mark — neither moved
        // earlier (a naive back-date would prematurely reopen the gate) nor
        // stuck closed — and must not panic on the rewind.
        source.set(Err(()));
        let after_miss = source.fetches();
        assert_eq!(cache.verify(&source, "am_junk").await, Verdict::Denied);
        assert_eq!(
            source.fetches() - after_miss,
            1,
            "the miss drives one failed fetch"
        );

        // A request well inside the window is served from the cache with no
        // fetch, so the early miss did not move the gate earlier.
        tokio::time::advance(MISS_REFRESH_FLOOR + Duration::from_secs(1)).await;
        let before_mid = source.fetches();
        assert!(matches!(
            cache.verify(&source, KEY).await,
            Verdict::Allowed(_)
        ));
        assert_eq!(
            source.fetches() - before_mid,
            0,
            "no scheduled fetch before the window opens"
        );

        // Once the window opens the scheduled refresh fires once, then
        // re-arms. No subtraction needed: a full REFRESH_INTERVAL crosses the
        // original mark from here.
        tokio::time::advance(REFRESH_INTERVAL).await;
        let before_window = source.fetches();
        assert!(matches!(
            cache.verify(&source, KEY).await,
            Verdict::Allowed(_)
        ));
        assert_eq!(
            source.fetches() - before_window,
            1,
            "exactly one scheduled fetch once the window opens"
        );

        // That failure re-arms the gate; further requests in the backoff do
        // not refetch.
        let after_rearm = source.fetches();
        for _ in 0..5 {
            assert!(matches!(
                cache.verify(&source, KEY).await,
                Verdict::Allowed(_)
            ));
        }
        assert_eq!(
            source.fetches() - after_rearm,
            0,
            "no refetches within FAILURE_BACKOFF"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn failure_backoff_bounds_revocation_latency_after_a_failed_refresh() {
        let source = FakeSource::ok(document_for(KEY, "key_1"));
        let cache = KeyCache::new();
        assert!(matches!(
            cache.verify(&source, KEY).await,
            Verdict::Allowed(_)
        ));

        // The outage begins past the window; the scheduled refresh fails and
        // re-arms the gate.
        source.set(Err(()));
        tokio::time::advance(REFRESH_INTERVAL + Duration::from_secs(1)).await;
        assert!(matches!(
            cache.verify(&source, KEY).await,
            Verdict::Allowed(_)
        ));

        // SSM recovers immediately with the key revoked (only key_2 remains).
        source.set(Ok(document_for("am_other_key", "key_2")));

        // Inside FAILURE_BACKOFF the scheduled gate stays closed, so the
        // still-cached (now revoked) valid key keeps working right after the
        // failure — revocation latency for a valid key is bounded by
        // FAILURE_BACKOFF, well inside the healthy regime's REFRESH_INTERVAL.
        assert!(
            matches!(cache.verify(&source, KEY).await, Verdict::Allowed(_)),
            "revocation is not picked up before FAILURE_BACKOFF elapses"
        );

        // Once FAILURE_BACKOFF elapses the gate reopens, the next fetch sees
        // the new document, and the revoked key is now denied.
        tokio::time::advance(FAILURE_BACKOFF).await;
        assert_eq!(
            cache.verify(&source, KEY).await,
            Verdict::Denied,
            "the revoked key is denied once the gate reopens"
        );
    }

    #[test]
    fn hex_digests_are_validated() {
        assert!(decode_hex_digest("ab").is_none(), "wrong length");
        assert!(
            decode_hex_digest(&"z".repeat(HEX_DIGEST_LEN)).is_none(),
            "non-hex characters"
        );
        assert!(decode_hex_digest(&"a".repeat(HEX_DIGEST_LEN)).is_some());
    }

    #[test]
    fn every_candidate_is_compared() {
        let keys: Vec<KeyHash> = ["key_1", "key_2", "key_3"]
            .into_iter()
            .map(|id| KeyHash {
                id: id.to_owned(),
                digest: Sha256::digest(id.as_bytes()).into(),
            })
            .collect();
        let last = Sha256::digest(b"key_3");
        assert_eq!(match_key(&keys, &last), Some("key_3".to_owned()));
        assert_eq!(match_key(&keys, &Sha256::digest(b"absent")), None);
    }
}
