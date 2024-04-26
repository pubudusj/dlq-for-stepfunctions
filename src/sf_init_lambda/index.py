import boto3
import json
import os

sfn = boto3.client("stepfunctions")

STATE_MACHINE_ARN = os.environ["STATE_MACHINE_ARN"]
DLQ_URL = os.environ["DLQ_URL"]


def handler(event, context):
    for sqs_record in event["Records"]:
        data = _format_data(json.loads(sqs_record.get("body", "{}")))
        _intialize_step_functions_execution(
            state_machine_arn=STATE_MACHINE_ARN,
            data=data,
        )


def _format_data(sqs_record: dict):
    metadata = sqs_record.get("metadata", {})

    if metadata.get("attempt", None) is None:
        metadata["attempt"] = 1
    else:
        metadata["attempt"] += 1

    if metadata.get("sqs_dlq_url", None) is None:
        metadata["sqs_dlq_url"] = DLQ_URL

    return {
        "metadata": metadata,
        "data": sqs_record.get("data", {}),
    }


def _intialize_step_functions_execution(
    state_machine_arn: str,
    data: dict,
):
    sfn.start_execution(
        stateMachineArn=state_machine_arn,
        input=json.dumps(data),
    )
