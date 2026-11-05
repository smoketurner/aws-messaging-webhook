//! [`SmsVoiceApi`] over End User Messaging SMS.

use aws_sdk_pinpointsmsvoicev2::error::SdkError;
use aws_sdk_pinpointsmsvoicev2::types::MessageFeedbackStatus;

use crate::actions::{ActionError, FeedbackStatus, SmsVoiceApi};
use crate::aws::FromSdkFailure as _;

/// The End User Messaging SMS client.
pub struct SmsClient {
    client: aws_sdk_pinpointsmsvoicev2::Client,
}

impl SmsClient {
    #[must_use]
    pub(crate) fn new(client: aws_sdk_pinpointsmsvoicev2::Client) -> Self {
        Self { client }
    }
}

impl SmsVoiceApi for SmsClient {
    async fn put_message_feedback(
        &self,
        message_id: &str,
        status: FeedbackStatus,
    ) -> Result<(), ActionError> {
        let status = match status {
            FeedbackStatus::Received => MessageFeedbackStatus::Received,
            FeedbackStatus::Failed => MessageFeedbackStatus::Failed,
        };
        let result = self
            .client
            .put_message_feedback()
            .message_id(message_id)
            .message_feedback_status(status)
            .send()
            .await;
        match result {
            Ok(_) => Ok(()),
            // DLRs carry no flag saying whether the message was sent with
            // feedback enabled, so this call is made for every terminal
            // event; "no such feedback record" is the expected no-op.
            Err(SdkError::ServiceError(ctx)) if ctx.err().is_resource_not_found_exception() => {
                tracing::debug!(message_id, "message has no feedback record; skipping");
                Ok(())
            }
            Err(error) => Err(ActionError::from_sdk("PutMessageFeedback", &error)),
        }
    }

    async fn put_opted_out_number(
        &self,
        opt_out_list_name: &str,
        phone_number: &str,
    ) -> Result<(), ActionError> {
        self.client
            .put_opted_out_number()
            .opt_out_list_name(opt_out_list_name)
            .opted_out_number(phone_number)
            .send()
            .await
            .map_err(|e| ActionError::from_sdk("PutOptedOutNumber", &e))?;
        Ok(())
    }

    async fn delete_opted_out_number(
        &self,
        opt_out_list_name: &str,
        phone_number: &str,
    ) -> Result<(), ActionError> {
        let result = self
            .client
            .delete_opted_out_number()
            .opt_out_list_name(opt_out_list_name)
            .opted_out_number(phone_number)
            .send()
            .await;
        match result {
            Ok(_) => Ok(()),
            // Not opted out = the desired end state; treat as success.
            Err(SdkError::ServiceError(ctx)) if ctx.err().is_resource_not_found_exception() => {
                Ok(())
            }
            Err(error) => Err(ActionError::from_sdk("DeleteOptedOutNumber", &error)),
        }
    }
}
