//! `PATCH /v0/inboxes/{inbox_id}/messages/{message_id}` — changing a
//! message's labels.
//!
//! There is no "mark as read" endpoint in the contract: a client marks mail
//! read by removing the `unread` label here.
//!
//! Both fields accept either a single label or an array of them, which is
//! what the published schema says, so a client sending `"add_labels": "read"`
//! and one sending `["read"]` behave identically.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};
use serde::Deserialize;

use crate::api::error::{ApiError, FieldError};
use crate::api::json::{ApiJson, OneOrMany};
use crate::state::{AppState, Backend};
use mailbox::MessageId;
use mailbox::{InboxId, LABEL_MAX_BYTES, MESSAGE_USER_LABEL_CAP, labels, time, wire};

/// One label or a list of them.
type Labels = OneOrMany<String>;

#[derive(Debug, Deserialize)]
pub(crate) struct UpdateRequest {
    #[serde(default)]
    add_labels: Option<Labels>,
    #[serde(default)]
    remove_labels: Option<Labels>,
}

/// Normalizes and validates a list of caller-supplied labels.
///
/// Labels are trimmed, lowercased and deduplicated so `Unread` and `unread`
/// cannot both land on a message and so a removal matches what a list filter
/// would match. Every problem is reported against `field`, and a label that
/// has one is dropped rather than carried forward.
///
/// Shared with the send path, so a label the API accepts on a send is a
/// label it accepts on a patch.
pub(crate) fn normalize_labels(
    labels_in: Vec<String>,
    field: &'static str,
    errors: &mut Vec<FieldError>,
) -> Vec<String> {
    let mut cleaned: Vec<String> = Vec::new();
    for label in labels_in {
        let label = label.trim().to_lowercase();
        if label.is_empty() {
            errors.push(FieldError::new(field, "a label cannot be empty".to_owned()));
            continue;
        }
        if label.len() > LABEL_MAX_BYTES {
            errors.push(FieldError::new(
                field,
                format!("`{label}` is longer than {LABEL_MAX_BYTES} bytes"),
            ));
            continue;
        }
        if labels::is_reserved(&label) {
            errors.push(FieldError::new(
                field,
                format!(
                    "`{label}` is set by the service and cannot be changed; \
                 only `unread`, `spam`, `trash` and your own labels can"
                ),
            ));
            continue;
        }
        if !cleaned.contains(&label) {
            cleaned.push(label);
        }
    }
    // A request cannot name more user labels than a message may carry; the
    // toggleable system labels `unread`/`spam`/`trash` are caller-owned state
    // and, like every other system label, do not count toward the cap. This
    // mirrors the storage cap in `mail::plan::check_user_labels`, which uses
    // `labels::user_label_count`, so a request the store accepts is not
    // rejected here for naming a state toggle alongside a full set of user
    // labels.
    if labels::user_label_count(&cleaned) > MESSAGE_USER_LABEL_CAP {
        errors.push(FieldError::new(
            field,
            format!("at most {MESSAGE_USER_LABEL_CAP} labels per request"),
        ));
    }
    cleaned.sort();
    cleaned
}

/// `PATCH /v0/inboxes/{inbox_id}/messages/{message_id}`
///
/// # Errors
///
/// [`ApiError::Validation`] for an empty, oversized, reserved or contradictory
/// label, or one that would leave the message or its thread over its label
/// cap; [`ApiError::NotFound`] when the inbox holds no such message; a
/// store failure otherwise.
pub(crate) async fn update_labels<B: Backend>(
    State(state): State<Arc<AppState<B>>>,
    Path((inbox, message_id)): Path<(InboxId, MessageId)>,
    ApiJson(request): ApiJson<UpdateRequest>,
) -> Result<Json<wire::MessageLabels>, ApiError> {
    let mut errors = Vec::new();
    let add = normalize_labels(
        request.add_labels.map_or_default(Labels::into_vec),
        "add_labels",
        &mut errors,
    );
    let remove = normalize_labels(
        request.remove_labels.map_or_default(Labels::into_vec),
        "remove_labels",
        &mut errors,
    );

    // Adding and removing the same label has no defensible outcome, so it is
    // rejected rather than silently resolved one way.
    for label in &add {
        if remove.contains(label) {
            errors.push(FieldError::new(
                "add_labels",
                format!("`{label}` is in both add_labels and remove_labels"),
            ));
        }
    }

    if !errors.is_empty() {
        return Err(ApiError::Validation(errors));
    }

    let now = time::format(time::now_ms());
    let labels = state
        .mailbox
        .update_labels(&inbox, &message_id, &add, &remove, &now)
        .await?
        .ok_or(ApiError::NotFound)?;

    Ok(Json(wire::MessageLabels { message_id, labels }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn clean_all(values: &[&str]) -> (Vec<String>, Vec<String>) {
        let mut errors = Vec::new();
        let values: Vec<String> = values.iter().map(|v| (*v).to_owned()).collect();
        let cleaned = normalize_labels(values, "add_labels", &mut errors);
        (
            cleaned,
            errors.into_iter().map(|error| error.message).collect(),
        )
    }

    #[test]
    fn labels_are_lowercased_trimmed_sorted_and_deduplicated() {
        let (cleaned, errors) = clean_all(&["  Urgent ", "urgent", "Billing"]);
        assert_eq!(cleaned, vec!["billing", "urgent"]);
        assert_eq!(errors, [] as [String; 0]);
    }

    #[test]
    fn reserved_labels_are_rejected_but_the_toggleable_ones_are_not() {
        let (cleaned, errors) = clean_all(&["received"]);
        assert_eq!(cleaned, [] as [String; 0]);
        assert_eq!(errors.len(), 1);

        let (cleaned, errors) = clean_all(&["unread", "spam", "trash"]);
        assert_eq!(cleaned, vec!["spam", "trash", "unread"]);
        assert_eq!(errors, [] as [String; 0]);
    }

    #[test]
    fn empty_and_oversized_labels_are_rejected() {
        let (cleaned, errors) = clean_all(&["   "]);
        assert_eq!(cleaned, [] as [String; 0]);
        assert_eq!(errors.len(), 1);

        let long = "x".repeat(LABEL_MAX_BYTES + 1);
        let (cleaned, errors) = clean_all(&[&long]);
        assert_eq!(cleaned, [] as [String; 0]);
        assert_eq!(errors.len(), 1);
    }

    #[test]
    fn a_single_label_and_a_list_of_one_are_the_same_request() {
        let mut errors = Vec::new();
        let one = Labels::One("Urgent".to_owned()).into_vec();
        let many = Labels::Many(vec!["Urgent".to_owned()]).into_vec();
        let one = normalize_labels(one, "add", &mut errors);
        let many = normalize_labels(many, "add", &mut errors);
        assert_eq!(one, many);
        assert!(errors.is_empty());
    }

    #[test]
    fn an_absent_field_is_an_empty_list() {
        let mut errors = Vec::new();
        let absent: Option<Labels> = None;
        let labels = absent.map_or_default(Labels::into_vec);
        assert_eq!(
            normalize_labels(labels, "add_labels", &mut errors),
            [] as [String; 0]
        );
        assert!(errors.is_empty());
    }

    #[test]
    fn the_string_form_deserializes() {
        let request: UpdateRequest =
            serde_json::from_str(r#"{"add_labels":"read","remove_labels":["unread"]}"#).unwrap();
        let mut errors = Vec::new();
        assert_eq!(
            normalize_labels(
                request.add_labels.map_or_default(Labels::into_vec),
                "add_labels",
                &mut errors
            ),
            vec!["read"]
        );
        assert_eq!(
            normalize_labels(
                request.remove_labels.map_or_default(Labels::into_vec),
                "remove_labels",
                &mut errors
            ),
            vec!["unread"]
        );
        assert!(errors.is_empty());
    }
}
