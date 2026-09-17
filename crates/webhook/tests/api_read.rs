#![expect(clippy::unwrap_used, reason = "test code panics on setup failure")]

//! The `/v0` read endpoints, driven through the real router and the real
//! insert path: every fixture goes in through `insert_message`, so the items
//! these reads return are the ones ingest would actually write.

use aws_messaging_webhook::mail::store::MailStore as _;
use aws_messaging_webhook::mail::{InboxId, ids, time};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt as _;
use webhook_test_support::mail_memory::sample_message;
use webhook_test_support::{Harness, harness};

const KEY: &str = "am_live_key";
const INBOX: &str = "support";

async fn get(h: &Harness, path: &str) -> (StatusCode, Value) {
    let request = Request::get(path)
        .header("authorization", format!("Bearer {KEY}"))
        .body(Body::empty())
        .unwrap();
    let response = aws_messaging_webhook::app::app(h.state.clone())
        .oneshot(request)
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

/// A harness with an authenticated key and an existing inbox.
async fn seeded() -> Harness {
    let h = harness().await;
    h.state.services.api_keys.set_keys(&[(KEY, "key_1")]);
    h.state
        .services
        .ensure_inbox(
            &InboxId(INBOX.to_owned()),
            &format!("{INBOX}@example.com"),
            "2026-01-01T00:00:00.000Z",
        )
        .await
        .unwrap();
    h
}

/// Inserts a message whose id encodes `at`, so index order is real time order.
async fn insert(h: &Harness, at: &str, thread: &str, adjust: impl FnOnce(&mut MailMessageOwned)) {
    let ms = time::parse(at).unwrap();
    let id = ids::inbound_message_id(&format!("ses-{at}-{thread}"), ms).to_string();
    let mut msg = sample_message(INBOX, &id, thread);
    msg.timestamp = at.to_owned();
    msg.created_at = at.to_owned();
    msg.updated_at = at.to_owned();
    adjust(&mut msg);
    h.state.services.insert_message(&msg).await.unwrap();
}

type MailMessageOwned = aws_messaging_webhook::mail::MailMessage;

#[tokio::test]
async fn listing_messages_returns_newest_first_by_default() {
    let h = seeded().await;
    insert(&h, "2026-01-01T09:00:00.000Z", "t1", |_| {}).await;
    insert(&h, "2026-01-01T10:00:00.000Z", "t2", |m| {
        m.subject = "Second".to_owned();
    })
    .await;

    let (status, body) = get(&h, &format!("/v0/inboxes/{INBOX}/messages")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["count"], 2);
    assert_eq!(body["limit"], 20);
    assert_eq!(body["messages"][0]["subject"], "Second");
    assert!(body["next_page_token"].is_null());
}

#[tokio::test]
async fn ascending_reverses_the_order() {
    let h = seeded().await;
    insert(&h, "2026-01-01T09:00:00.000Z", "t1", |m| {
        m.subject = "First".to_owned();
    })
    .await;
    insert(&h, "2026-01-01T10:00:00.000Z", "t2", |_| {}).await;

    let (_, body) = get(&h, &format!("/v0/inboxes/{INBOX}/messages?ascending=true")).await;

    assert_eq!(body["messages"][0]["subject"], "First");
}

#[tokio::test]
async fn a_page_token_walks_the_whole_list_without_gaps_or_repeats() {
    let h = seeded().await;
    for hour in 9..14 {
        insert(
            &h,
            &format!("2026-01-01T{hour:02}:00:00.000Z"),
            "t1",
            |_| {},
        )
        .await;
    }

    let mut seen: Vec<String> = Vec::new();
    let mut path = format!("/v0/inboxes/{INBOX}/messages?limit=2");
    loop {
        let (status, body) = get(&h, &path).await;
        assert_eq!(status, StatusCode::OK);
        for message in body["messages"].as_array().unwrap() {
            seen.push(message["message_id"].as_str().unwrap().to_owned());
        }
        let Some(token) = body["next_page_token"].as_str() else {
            break;
        };
        path = format!("/v0/inboxes/{INBOX}/messages?limit=2&page_token={token}");
    }

    assert_eq!(seen.len(), 5, "every message appears exactly once");
    let mut unique = seen.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), 5, "no message is repeated across pages");
}

#[tokio::test]
async fn a_page_token_from_another_inbox_is_rejected() {
    // The token's partition is re-checked against the path, so a token can
    // never be replayed against an inbox it was not issued for.
    let h = seeded().await;
    h.state
        .services
        .ensure_inbox(
            &InboxId("billing".to_owned()),
            "billing@example.com",
            "2026-01-01T00:00:00.000Z",
        )
        .await
        .unwrap();
    for hour in 9..12 {
        insert(
            &h,
            &format!("2026-01-01T{hour:02}:00:00.000Z"),
            "t1",
            |_| {},
        )
        .await;
    }

    let (_, body) = get(&h, &format!("/v0/inboxes/{INBOX}/messages?limit=1")).await;
    let token = body["next_page_token"].as_str().unwrap();

    let (status, body) = get(
        &h,
        &format!("/v0/inboxes/billing/messages?page_token={token}"),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["name"], "ValidationError");
    assert_eq!(body["errors"][0]["path"], "page_token");
}

#[tokio::test]
async fn spam_and_trash_are_hidden_unless_asked_for() {
    let h = seeded().await;
    insert(&h, "2026-01-01T09:00:00.000Z", "t1", |m| {
        m.labels = vec!["received".to_owned(), "spam".to_owned()];
    })
    .await;
    insert(&h, "2026-01-01T10:00:00.000Z", "t2", |m| {
        m.subject = "Clean".to_owned();
    })
    .await;

    let (_, body) = get(&h, &format!("/v0/inboxes/{INBOX}/messages")).await;
    assert_eq!(body["count"], 1);
    assert_eq!(body["messages"][0]["subject"], "Clean");

    let (_, body) = get(
        &h,
        &format!("/v0/inboxes/{INBOX}/messages?include_spam=true"),
    )
    .await;
    assert_eq!(body["count"], 2);
}

#[tokio::test]
async fn asking_for_a_label_overrides_the_flag_that_would_hide_it() {
    let h = seeded().await;
    insert(&h, "2026-01-01T09:00:00.000Z", "t1", |m| {
        m.labels = vec!["received".to_owned(), "trash".to_owned()];
    })
    .await;

    let (_, body) = get(&h, &format!("/v0/inboxes/{INBOX}/messages?labels=trash")).await;

    assert_eq!(body["count"], 1);
}

#[tokio::test]
async fn substring_filters_are_case_insensitive_and_combine_across_fields() {
    let h = seeded().await;
    insert(&h, "2026-01-01T09:00:00.000Z", "t1", |m| {
        m.from = "Alice@Example.com".to_owned();
        m.subject = "Invoice 42".to_owned();
    })
    .await;
    insert(&h, "2026-01-01T10:00:00.000Z", "t2", |m| {
        m.from = "bob@example.com".to_owned();
        m.subject = "Invoice 43".to_owned();
    })
    .await;

    let (_, body) = get(
        &h,
        &format!("/v0/inboxes/{INBOX}/messages?from=alice&subject=invoice"),
    )
    .await;
    assert_eq!(body["count"], 1);
    assert_eq!(body["messages"][0]["subject"], "Invoice 42");

    // Both fields must match: a sender that matches with a subject that does
    // not returns nothing.
    let (_, body) = get(
        &h,
        &format!("/v0/inboxes/{INBOX}/messages?from=alice&subject=receipt"),
    )
    .await;
    assert_eq!(body["count"], 0);
}

#[tokio::test]
async fn time_bounds_select_a_half_open_window() {
    let h = seeded().await;
    for hour in 9..13 {
        insert(&h, &format!("2026-01-01T{hour:02}:00:00.000Z"), "t1", |m| {
            m.subject = format!("hour {hour}");
        })
        .await;
    }

    let (_, body) = get(
        &h,
        &format!(
            "/v0/inboxes/{INBOX}/messages?after=2026-01-01T10:00:00.000Z&before=2026-01-01T12:00:00.000Z&ascending=true"
        ),
    )
    .await;

    // `after` includes its own instant, `before` excludes it.
    assert_eq!(body["count"], 2);
    assert_eq!(body["messages"][0]["subject"], "hour 10");
    assert_eq!(body["messages"][1]["subject"], "hour 11");
}

#[tokio::test]
async fn a_bad_query_parameter_reports_every_problem_at_once() {
    let h = seeded().await;

    let (status, body) = get(
        &h,
        &format!("/v0/inboxes/{INBOX}/messages?limit=500&ascending=maybe"),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["name"], "ValidationError");
    let paths: Vec<&str> = body["errors"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["path"].as_str().unwrap())
        .collect();
    assert_eq!(paths, vec!["limit", "ascending"]);
}

#[tokio::test]
async fn fetching_one_message_returns_the_body_a_list_omits() {
    let h = seeded().await;
    insert(&h, "2026-01-01T09:00:00.000Z", "t1", |m| {
        m.text = Some("the full body".to_owned());
        m.headers.insert("X-Custom".to_owned(), "yes".to_owned());
    })
    .await;

    let (_, list) = get(&h, &format!("/v0/inboxes/{INBOX}/messages")).await;
    let id = list["messages"][0]["message_id"].as_str().unwrap();
    assert!(
        list["messages"][0]["text"].is_null(),
        "list view omits text"
    );

    let (status, body) = get(&h, &format!("/v0/inboxes/{INBOX}/messages/{id}")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["text"], "the full body");
    assert_eq!(body["headers"]["X-Custom"], "yes");
}

#[tokio::test]
async fn an_unknown_message_is_not_found() {
    let h = seeded().await;

    let (status, body) = get(&h, &format!("/v0/inboxes/{INBOX}/messages/nope")).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["name"], "NotFoundError");
}

#[tokio::test]
async fn a_thread_embeds_its_messages_oldest_first() {
    let h = seeded().await;
    insert(&h, "2026-01-01T09:00:00.000Z", "t1", |m| {
        m.subject = "First".to_owned();
    })
    .await;
    insert(&h, "2026-01-01T10:00:00.000Z", "t1", |m| {
        m.subject = "Second".to_owned();
    })
    .await;

    let (status, body) = get(&h, &format!("/v0/inboxes/{INBOX}/threads/t1")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["count"], 2);
    assert_eq!(body["message_count"], 2);
    assert_eq!(body["messages"][0]["subject"], "First");
    assert_eq!(body["messages"][1]["subject"], "Second");
}

#[tokio::test]
async fn listing_threads_orders_by_last_activity() {
    let h = seeded().await;
    insert(&h, "2026-01-01T09:00:00.000Z", "t1", |_| {}).await;
    insert(&h, "2026-01-01T10:00:00.000Z", "t2", |_| {}).await;
    // A reply lifts the older thread back to the top.
    insert(&h, "2026-01-01T11:00:00.000Z", "t1", |_| {}).await;

    let (status, body) = get(&h, &format!("/v0/inboxes/{INBOX}/threads")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["count"], 2);
    assert_eq!(body["threads"][0]["thread_id"], "t1");
    assert_eq!(body["threads"][1]["thread_id"], "t2");
}

#[tokio::test]
async fn an_unknown_thread_is_not_found() {
    let h = seeded().await;

    let (status, _) = get(&h, &format!("/v0/inboxes/{INBOX}/threads/nope")).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn inboxes_list_and_fetch() {
    let h = seeded().await;

    let (status, body) = get(&h, "/v0/inboxes").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["count"], 1);
    assert_eq!(body["inboxes"][0]["inbox_id"], INBOX);
    assert_eq!(body["inboxes"][0]["pod_id"], "pod_default");

    let (status, body) = get(&h, &format!("/v0/inboxes/{INBOX}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["inbox_id"], INBOX);
}

#[tokio::test]
async fn an_unknown_inbox_is_not_found() {
    let h = seeded().await;

    let (status, _) = get(&h, "/v0/inboxes/nope").await;

    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn reads_still_require_a_key() {
    let h = seeded().await;
    let request = Request::get(format!("/v0/inboxes/{INBOX}/messages"))
        .body(Body::empty())
        .unwrap();
    let response = aws_messaging_webhook::app::app(h.state.clone())
        .oneshot(request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}
