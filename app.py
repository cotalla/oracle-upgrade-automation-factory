import json
import os
import logging
from datetime import datetime
from typing import Optional, Tuple

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REGION = os.environ.get("AWS_REGION", "us-east-1")
BUCKET = os.environ.get("UPGRADE_BUCKET")  # required
MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "amazon.titan-text-lite-v1")  # model-agnostic placeholder

s3 = boto3.client("s3", region_name=REGION)
brt = boto3.client("bedrock-runtime", region_name=REGION)


# ----------------------------
# Utilities
# ----------------------------
def s3_get_json(bucket: str, key: str) -> Optional[dict]:
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read())
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
            return None
        raise


def s3_put_text(bucket: str, key: str, text: str) -> None:
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=text.encode("utf-8"),
        ContentType="text/markdown",
    )


def extract_run_id_from_key(key: str) -> Optional[str]:
    """
    Supports keys like:
      runs/<run_id>/metrics.json
      runs/<run_id>/metrics/metrics.json
      <run_id>/metrics.json
    """
    parts = key.strip("/").split("/")
    if len(parts) >= 2 and parts[0] == "runs":
        return parts[1]
    if len(parts) >= 1 and parts[0].startswith("run-"):
        return parts[0]
    return None


def build_prompt(metrics: dict, sanitized: Optional[dict], s3_key_triggered: str) -> str:
    # Keep prompt compact and structured
    context = {
        "s3_key_triggered": s3_key_triggered,
        "metrics": metrics,
    }
    if sanitized:
        context["sanitized_summary"] = sanitized

    return (
        "You are an enterprise Oracle DBA upgrade reviewer.\n"
        "Generate a concise report with:\n"
        "1) Executive summary (non-technical)\n"
        "2) Technical summary (what happened, key checks)\n"
        "3) Risks / impact (enterprise perspective)\n"
        "4) Root cause (based only on provided data)\n"
        "5) Recommended next steps and preventive controls\n"
        "6) 5-7 sentence LinkedIn portfolio narrative\n\n"
        "IMPORTANT: Do not invent facts. Only use provided data.\n\n"
        f"DATA:\n{json.dumps(context, indent=2)}\n"
    )


def call_bedrock(prompt: str) -> str:
    """
    Titan-style request body. If you swap models later, adjust here only.
    """
    body = {"inputText": prompt}
    resp = brt.invoke_model(
        modelId=MODEL_ID,
        body=json.dumps(body),
        contentType="application/json",
        accept="application/json",
    )
    payload = json.loads(resp["body"].read())

    # Titan response format
    text = payload.get("results", [{}])[0].get("outputText", "")
    return (text or "").strip()


def deterministic_fallback(metrics: dict, sanitized: Optional[dict]) -> str:
    run_id = metrics.get("run_id") or "unknown"
    before_ = metrics.get("invalid_objects_before")
    after_ = metrics.get("invalid_objects_after")
    ora_errors = metrics.get("ora_errors", []) or metrics.get("errors", [])
    invalids = metrics.get("invalid_objects_sample", []) or metrics.get("invalid_objects", [])

    lines = []
    lines.append("## Executive Summary")
    lines.append(
        f"This run captured an Oracle migration/upgrade validation report for **{run_id}**. "
        f"Invalid objects before: **{before_}**; after: **{after_}**. "
        "This report was generated using structured artifacts; AI generation is currently blocked by account authorization."
    )

    lines.append("\n## Technical Summary")
    lines.append(f"- Invalid objects before: {before_}")
    lines.append(f"- Invalid objects after: {after_}")
    lines.append(f"- ORA errors detected (sample): {len(ora_errors) if ora_errors else 0}")

    if ora_errors:
        lines.append("\n### ORA Errors (sample)")
        for e in ora_errors[:10]:
            lines.append(f"- {e}")

    if invalids:
        lines.append("\n### Invalid Objects (sample)")
        for o in invalids[:25]:
            if isinstance(o, dict):
                lines.append(f"- {o.get('owner','?')}.{o.get('name','?')} ({o.get('type','?')})")
            else:
                lines.append(f"- {str(o)}")

    if sanitized:
        lines.append("\n## Sanitized Summary (input)")
        lines.append("```json")
        lines.append(json.dumps(sanitized, indent=2)[:4000])
        lines.append("```")

    lines.append("\n## Recommended Next Steps / Preventive Controls")
    lines.append("- Keep a deterministic pre/post validation checklist (DBA_OBJECTS invalids, DBA_ERRORS, dependencies).")
    lines.append("- Add a dependency check step for views/packages prior to cutover.")
    lines.append("- Standardize artifact storage in S3 per run_id (logs, metrics.json, final report).")
    lines.append("- Use AI only for summarization and documentation; never for executing DB changes automatically.")

    lines.append("\n## LinkedIn Narrative")
    lines.append(
        "I built an Oracle upgrade/migration lab using Docker and automated pre/post validation. "
        "Artifacts (logs + structured metrics) are published to S3 per run, and a Lambda function produces a standardized executive report. "
        "The design is model-agnostic: the AI layer can be enabled via Amazon Bedrock once account authorization is granted, "
        "while deterministic reporting ensures repeatability and auditability today."
    )

    return "\n".join(lines).strip()


# ----------------------------
# Event parsing
# ----------------------------
def get_bucket_and_key_from_event(event: dict) -> Tuple[Optional[str], Optional[str]]:
    """
    Supports:
      - S3 event notifications
      - Manual invocation with {"bucket": "...", "key": "..."} or {"run_id":"..."}
    """
    # S3 event
    records = event.get("Records")
    if isinstance(records, list) and records:
        r0 = records[0]
        if r0.get("eventSource") == "aws:s3":
            b = r0.get("s3", {}).get("bucket", {}).get("name")
            k = r0.get("s3", {}).get("object", {}).get("key")
            return b, k

    # Manual bucket/key
    if "bucket" in event and "key" in event:
        return event["bucket"], event["key"]

    return None, None


# ----------------------------
# Lambda handler
# ----------------------------
def lambda_handler(event, context):
    if not BUCKET:
        return {"status": "error", "message": "Missing env var UPGRADE_BUCKET"}

    logger.info("Event received: %s", json.dumps(event)[:2000])

    event_bucket, event_key = get_bucket_and_key_from_event(event)

    # Determine run_id
    run_id = event.get("run_id")
    if not run_id and event_key:
        run_id = extract_run_id_from_key(event_key)

    if not run_id:
        return {"status": "error", "message": "Could not determine run_id from event. Provide run_id or use S3 key under runs/<run_id>/..."}

    # Build keys (match your existing patterns)
    # Prefer metrics.json in a run folder; support either metrics.json at root or under metrics/metrics.json.
    candidate_metrics_keys = [
        f"runs/{run_id}/metrics/metrics.json",
        f"runs/{run_id}/metrics.json",
        f"{run_id}/metrics.json",
    ]

    metrics = None
    used_metrics_key = None
    for k in candidate_metrics_keys:
        metrics = s3_get_json(BUCKET, k)
        if metrics:
            used_metrics_key = k
            break

    if not metrics:
        return {
            "status": "error",
            "run_id": run_id,
            "message": f"metrics.json not found. Tried: {candidate_metrics_keys}",
        }

    # Optional sanitized summary (you already have sanitized_summary.json locally; if you upload it per run, we’ll read it)
    candidate_sanitized_keys = [
        f"runs/{run_id}/sanitized_summary.json",
        f"runs/{run_id}/sanitized/sanitized_summary.json",
    ]
    sanitized = None
    for k in candidate_sanitized_keys:
        sanitized = s3_get_json(BUCKET, k)
        if sanitized:
            break

    # Generate report
    header = (
        f"# Oracle Upgrade / Migration Report\n"
        f"**Run ID:** {run_id}\n"
        f"**Generated (UTC):** {datetime.utcnow().isoformat()}Z\n"
        f"**Metrics Source:** s3://{BUCKET}/{used_metrics_key}\n"
        + (f"**Trigger Object:** s3://{event_bucket}/{event_key}\n" if event_bucket and event_key else "")
        + "\n---\n"
    )

    engine = "fallback"
    try:
        prompt = build_prompt(metrics, sanitized, event_key or "")
        body_text = call_bedrock(prompt)
        report_body = body_text if body_text else "Bedrock returned empty output."
        engine = f"bedrock:{MODEL_ID}"
    except Exception as e:
        logger.error("Bedrock invoke failed: %s", str(e))
        report_body = deterministic_fallback(metrics, sanitized) + f"\n\n---\n**Bedrock error:** {str(e)}\n"

    final_report = header + report_body + "\n"

    # Write report back to S3 under the same run
    report_key = f"runs/{run_id}/executive_report.md"
    s3_put_text(BUCKET, report_key, final_report)

    return {
        "status": "success",
        "run_id": run_id,
        "engine": engine,
        "report_s3": f"s3://{BUCKET}/{report_key}",
    }
