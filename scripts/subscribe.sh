#!/usr/bin/env bash
set -euo pipefail

# Subscribe an SNS topic to a deployed webhook path and verify auto-confirm.
#
# Usage: subscribe.sh <stack-name> <sms/inbound|sms/events|ses/events|ses/inbound> <topic-arn>
#
# Sets the topic's SignatureVersion to 2 (SHA256), subscribes the Function URL
# over HTTPS, then polls until the webhook's auto-confirmation lands.

if [[ $# -ne 3 ]]; then
  echo "usage: $0 <stack-name> <webhook-path> <topic-arn>" >&2
  exit 64
fi

stack_name=$1
webhook_path=$2
topic_arn=$3

case "${webhook_path}" in
sms/inbound | sms/events | ses/events | ses/inbound) ;;
*)
  echo "webhook-path must be one of: sms/inbound sms/events ses/events ses/inbound" >&2
  exit 64
  ;;
esac

base_url=$(aws cloudformation describe-stacks \
  --stack-name "${stack_name}" \
  --query "Stacks[0].Outputs[?OutputKey=='WebhookUrl'].OutputValue" \
  --output text)
endpoint="${base_url%/}/webhooks/${webhook_path}"

echo "Setting SignatureVersion=2 on ${topic_arn}"
aws sns set-topic-attributes \
  --topic-arn "${topic_arn}" \
  --attribute-name SignatureVersion \
  --attribute-value 2

echo "Subscribing ${endpoint}"
subscription_arn=$(aws sns subscribe \
  --topic-arn "${topic_arn}" \
  --protocol https \
  --notification-endpoint "${endpoint}" \
  --return-subscription-arn \
  --query SubscriptionArn \
  --output text)

for _ in $(seq 1 15); do
  status=$(aws sns get-subscription-attributes \
    --subscription-arn "${subscription_arn}" \
    --query "Attributes.PendingConfirmation" \
    --output text 2>/dev/null || echo "unknown")
  if [[ ${status} == "false" ]]; then
    echo "Subscription confirmed: ${subscription_arn}"
    exit 0
  fi
  sleep 2
done

echo "Subscription still pending after 30s — check the function logs" >&2
echo "(is the topic in ALLOWED_TOPICS? is raw message delivery disabled?)" >&2
exit 1
