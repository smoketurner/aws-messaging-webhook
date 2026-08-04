#!/usr/bin/env bash
set -euo pipefail

# End-to-end check against a deployed dev stack using the SES mailbox
# simulator: sends a mail to bounce@simulator.amazonses.com, then verifies
# (1) a ses.bounce event reaches the EventBridge bus (via a temporary
# rule -> SQS queue), (2) the DynamoDB event record is PUBLISHED, and
# (3) the address landed on the SES account suppression list.
#
# Usage: e2e-ses-bounce.sh <stack-name> <verified-sender-email>
#
# Prerequisites: the sender identity is SES-verified, and the stack's
# ses/events webhook path is subscribed to the configuration set's SNS topic.

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <stack-name> <verified-sender-email>" >&2
  exit 64
fi

stack_name=$1
sender=$2
bounce_recipient="bounce@simulator.amazonses.com"
probe="webhook-e2e-$(date +%s)"

bus_name=$(aws cloudformation describe-stacks --stack-name "${stack_name}" \
  --query "Stacks[0].Outputs[?OutputKey=='EventBusName'].OutputValue" --output text)
table_name=$(aws cloudformation describe-stacks --stack-name "${stack_name}" \
  --query "Stacks[0].Outputs[?OutputKey=='TableName'].OutputValue" --output text)

queue_url=$(aws sqs create-queue --queue-name "${probe}" --query QueueUrl --output text)
queue_arn=$(aws sqs get-queue-attributes --queue-url "${queue_url}" \
  --attribute-names QueueArn --query "Attributes.QueueArn" --output text)

cleanup() {
  aws events remove-targets --event-bus-name "${bus_name}" --rule "${probe}" --ids probe >/dev/null 2>&1 || true
  aws events delete-rule --event-bus-name "${bus_name}" --name "${probe}" >/dev/null 2>&1 || true
  aws sqs delete-queue --queue-url "${queue_url}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

aws events put-rule --event-bus-name "${bus_name}" --name "${probe}" \
  --event-pattern '{"detail-type":["ses.bounce"]}' >/dev/null
aws sqs set-queue-attributes --queue-url "${queue_url}" --attributes "{
  \"Policy\": \"{\\\"Version\\\":\\\"2012-10-17\\\",\\\"Statement\\\":[{\\\"Effect\\\":\\\"Allow\\\",\\\"Principal\\\":{\\\"Service\\\":\\\"events.amazonaws.com\\\"},\\\"Action\\\":\\\"sqs:SendMessage\\\",\\\"Resource\\\":\\\"${queue_arn}\\\"}]}\"
}"
aws events put-targets --event-bus-name "${bus_name}" --rule "${probe}" \
  --targets "Id=probe,Arn=${queue_arn}" >/dev/null

echo "Sending bounce-simulator mail from ${sender}"
aws sesv2 send-email --from-email-address "${sender}" \
  --destination "ToAddresses=${bounce_recipient}" \
  --content "Simple={Subject={Data=${probe}},Body={Text={Data=e2e bounce probe}}}" >/dev/null

echo "Waiting for ses.bounce on bus ${bus_name}..."
detail=""
for _ in $(seq 1 30); do
  message=$(aws sqs receive-message --queue-url "${queue_url}" \
    --wait-time-seconds 5 --query "Messages[0].Body" --output text)
  if [[ ${message} != "None" && -n ${message} ]]; then
    detail=${message}
    break
  fi
done
if [[ -z ${detail} ]]; then
  echo "FAIL: no ses.bounce event arrived within ~150s" >&2
  exit 1
fi
echo "PASS: ses.bounce event received"

message_id=$(python3 -c "import json,sys; print(json.loads(sys.argv[1])['detail']['meta']['messageId'])" "${detail}")
status=$(aws dynamodb query --table-name "${table_name}" \
  --key-condition-expression "pk = :pk AND begins_with(sk, :evt)" \
  --expression-attribute-values "{\":pk\":{\"S\":\"MSG#${message_id}\"},\":evt\":{\"S\":\"EVT#\"}}" \
  --query "Items[0].status.S" --output text)
echo "DynamoDB record status for MSG#${message_id}: ${status}"
if [[ ${status} != "PUBLISHED" ]]; then
  echo "FAIL: expected PUBLISHED, got ${status}" >&2
  exit 1
fi

if aws sesv2 get-suppressed-destination --email-address "${bounce_recipient}" \
  --query "SuppressedDestination.Reason" --output text; then
  echo "PASS: ${bounce_recipient} is on the account suppression list"
else
  echo "WARN: ${bounce_recipient} not found on the suppression list" >&2
fi
