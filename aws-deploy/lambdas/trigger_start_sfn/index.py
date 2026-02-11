import json
import os
from urllib.parse import unquote_plus
import boto3

sfn = boto3.client("stepfunctions")

def parse_input(event: dict) -> tuple[str, str, str | None]:
    # S3 notification event
    if isinstance(event, dict) and event.get("Records"):
        rec = event["Records"][0]
        bucket = rec["s3"]["bucket"]["name"]
        key = unquote_plus(rec["s3"]["object"]["key"])
        return bucket, key, None

    # Manual invoke: { "bucket": "...", "key": "...", "model_id": "..." }
    if isinstance(event, dict) and event.get("bucket") and event.get("key"):
        return event["bucket"], event["key"], event.get("model_id")

    raise ValueError("Unsupported event shape. Provide S3 event or {bucket,key,model_id?}.")

def handler(event, context):
    sm_arn = os.environ["STATE_MACHINE_ARN"]
    default_model_id = os.environ.get("DEFAULT_MODEL_ID")

    bucket, key, model_id = parse_input(event)
    model_id = model_id or default_model_id

    inp = {"bucket": bucket, "key": key, "model_id": model_id}

    resp = sfn.start_execution(
        stateMachineArn=sm_arn,
        input=json.dumps(inp)
    )

    return {"ok": True, "executionArn": resp["executionArn"], "input": inp}
