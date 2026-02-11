import json
import boto3
import os
import logging
from datetime import datetime

# ----------------------------
# Configuration
# ----------------------------
REGION = os.environ.get("AWS_REGION", "us-east-1")
MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "amazon.titan-text-lite-v1")
BUCKET = os.environ.get("UPGRADE_BUCKET")

s3 = boto3.client("s3", region_name=REGION)
bedrock = boto3.client("bedrock-runtime", region_name=REGION)

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# ----------------------------
# Bedrock Call
# ----------------------------
def call_bedrock(prompt_text):

    body = {
        "inputText": prompt_text
    }

    try:
        response = bedrock.invoke_model(
            modelId=MODEL_ID,
            body=json.dumps(body),
            contentType="application/json",
            accept="application/json"
        )

        response_body = json.loads(response["body"].read())
        output_text = response_body.get("results", [{}])[0].get("outputText", "")

        return output_text

    except Exception as e:
        logger.error(f"Bedrock invocation failed: {str(e)}")
        return f"""
# Bedrock Invocation Failed

Error:
{str(e)}

AI layer is model-agnostic and will function once model access is enabled.
"""


# ----------------------------
# Report Builder
# ----------------------------
def build_prompt(summary_json):

    return f"""
You are an enterprise Oracle Database upgrade reviewer.

Generate:

1. Executive Summary (non-technical)
2. Technical Validation Summary
3. Risk Assessment
4. Remediation Steps
5. Preventive Controls
6. LinkedIn Portfolio Narrative (short)

Upgrade Summary:
{json.dumps(summary_json, indent=2)}
"""


# ----------------------------
# Lambda Handler
# ----------------------------
def lambda_handler(event, context):

    logger.info(f"Received event: {json.dumps(event)}")

    run_id = event.get("run_id")
    if not run_id:
        return {"status": "error", "message": "run_id missing"}

    try:
        metrics_key = f"runs/{run_id}/metrics/metrics.json"

        response = s3.get_object(Bucket=BUCKET, Key=metrics_key)
        metrics_data = json.loads(response["Body"].read())

        logger.info("metrics.json loaded successfully")

    except Exception as e:
        logger.error(f"Error loading metrics.json: {str(e)}")
        return {"status": "error", "message": f"Failed to load metrics.json: {str(e)}"}

    prompt = build_prompt(metrics_data)

    ai_report = call_bedrock(prompt)

    timestamp = datetime.utcnow().isoformat()

    final_report = f"""
# Oracle Upgrade AI Report
Run ID: {run_id}
Generated: {timestamp}

---

{ai_report}
"""

    report_key = f"runs/{run_id}/reports/ai_report.md"

    try:
        s3.put_object(
            Bucket=BUCKET,
            Key=report_key,
            Body=final_report.encode("utf-8"),
            ContentType="text/markdown"
        )

        logger.info("Report saved successfully")

    except Exception as e:
        logger.error(f"Error saving report: {str(e)}")
        return {"status": "error", "message": f"Failed to save report: {str(e)}"}

    return {
        "status": "success",
        "run_id": run_id,
        "report_location": f"s3://{BUCKET}/{report_key}"
    }
