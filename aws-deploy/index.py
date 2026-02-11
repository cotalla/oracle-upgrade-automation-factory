import json
import os
import re
import logging
from datetime import datetime
from typing import Optional, Tuple, List

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REGION = os.environ.get("AWS_REGION", "us-east-1")
BUCKET = os.environ.get("BUCKET_NAME")
DEFAULT_MODEL_ID = os.environ.get("DEFAULT_MODEL_ID", "amazon.titan-text-lite-v1")

s3 = boto3.client("s3", region_name=REGION)
brt = boto3.client("bedrock-runtime", region_name=REGION)


# --------------------------
# S3 helpers
# --------------------------
def s3_get_text(bucket: str, key: str) -> Optional[str]:
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return obj["Body"].read().decode("utf-8", errors="ignore")
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("NoSuchKey", "404"):
            return None
        raise

def s3_put_json(bucket: str, key: str, data: dict) -> None:
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data, indent=2).encode("utf-8"),
        ContentType="application/json",
    )

def s3_put_md(bucket: str, key: str, text: str) -> None:
    s3.put_object(Bucket=bucket, Key=key, Body=text.encode("utf-8"), ContentType="text/markdown")

def s3_list_keys(bucket: str, prefix: str) -> List[str]:
    keys = []
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        for item in resp.get("Contents", []):
            keys.append(item["Key"])
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return keys


# --------------------------
# Event helpers
# --------------------------
def extract_run_id_from_key(key: str) -> Optional[str]:
    parts = key.strip("/").split("/")
    if len(parts) >= 2 and parts[0] == "runs":
        return parts[1]
    return None

def get_bucket_key_from_event(event: dict) -> Tuple[Optional[str], Optional[str]]:
    recs = event.get("Records")
    if isinstance(recs, list) and recs:
        r0 = recs[0]
        if r0.get("eventSource") == "aws:s3":
            b = r0.get("s3", {}).get("bucket", {}).get("name")
            k = r0.get("s3", {}).get("object", {}).get("key")
            return b, k
    if "bucket" in event and "key" in event:
        return event["bucket"], event["key"]
    return None, None


# --------------------------
# Parsers (deterministic)
# --------------------------
def parse_invalid_object_proof(txt: Optional[str]) -> dict:
    """
    Your invalid_object_proof.txt is a proof artifact. We extract:
      - invalid object names if present
      - ORA errors if present
    """
    if not txt:
        return {"invalid_objects": [], "ora_errors": []}
    invalids = []
    ora = []
    for line in txt.splitlines():
        line = line.strip()
        if not line:
            continue
        if "ORA-" in line:
            ora.append(line)
        # detect object-like patterns OWNER.OBJECT or just VIEW name
        m = re.search(r"([A-Z0-9_]+)\.([A-Z0-9_]+)", line.upper())
        if m:
            invalids.append(f"{m.group(1)}.{m.group(2)}")
    # unique
    invalids = list(dict.fromkeys(invalids))
    ora = list(dict.fromkeys(ora))
    return {"invalid_objects": invalids[:50], "ora_errors": ora[:25]}

def parse_orders_count_proof(txt: Optional[str]) -> Optional[int]:
    if not txt:
        return None
    # extract last integer in file
    nums = re.findall(r"\b\d+\b", txt)
    return int(nums[-1]) if nums else None

def extract_ora_errors_from_log(txt: Optional[str]) -> List[str]:
    if not txt:
        return []
    errs = []
    for line in txt.splitlines():
        if "ORA-" in line:
            errs.append(line.strip())
    # unique
    out = []
    seen = set()
    for e in errs:
        if e not in seen:
            out.append(e)
            seen.add(e)
    return out[:50]


def build_metrics(run_id: str) -> dict:
    base = f"runs/{run_id}/"

    invalid_txt = s3_get_text(BUCKET, base + "04-validation/invalid_object_proof.txt")
    orders_txt = s3_get_text(BUCKET, base + "04-validation/orders_count_proof.txt")
    validation_log = s3_get_text(BUCKET, base + "04-validation/validation_23c.log")

    # find expdp/impdp logs dynamically
    keys = s3_list_keys(BUCKET, base + "03-migration/")
    expdp_log_key = next((k for k in keys if k.endswith(".log") and "expdp" in k.lower()), None)
    impdp_log_key = next((k for k in keys if k.endswith(".log") and "impdp" in k.lower()), None)

    expdp_log = s3_get_text(BUCKET, expdp_log_key) if expdp_log_key else None
    impdp_log = s3_get_text(BUCKET, impdp_log_key) if impdp_log_key else None

    inv = parse_invalid_object_proof(invalid_txt)
    orders = parse_orders_count_proof(orders_txt)

    ora_errors = []
    ora_errors += inv.get("ora_errors", [])
    ora_errors += extract_ora_errors_from_log(validation_log)
    ora_errors += extract_ora_errors_from_log(expdp_log)
    ora_errors += extract_ora_errors_from_log(impdp_log)

    # unique
    uniq = []
    seen = set()
    for e in ora_errors:
        if e not in seen:
            uniq.append(e)
            seen.add(e)

    metrics = {
        "run_id": run_id,
        "generated_utc": datetime.utcnow().isoformat() + "Z",
        "bucket": BUCKET,
        "paths": {
            "invalid_object_proof": base + "04-validation/invalid_object_proof.txt",
            "orders_count_proof": base + "04-validation/orders_count_proof.txt",
            "validation_log": base + "04-validation/validation_23c.log",
            "expdp_log": expdp_log_key,
            "impdp_log": impdp_log_key,
        },
        "upgrade": {
            "source": "Oracle 18c (Docker)",
            "target": "Oracle 23c/23ai (Docker)",
            "method": "Data Pump expdp/impdp"
        },
        "signals": {
            "invalid_objects_detected": len(inv.get("invalid_objects", [])),
            "invalid_objects_sample": inv.get("invalid_objects", []),
            "orders_count": orders,
            "ora_errors_sample": uniq[:25],
        },
        "notes": [
            "metrics.json was generated deterministically from S3 artifacts.",
            "AI (Bedrock) is used only for summarization/documentation; no automated DB command execution."
        ]
    }
    return metrics


# --------------------------
# Bedrock
# --------------------------
def call_bedrock(prompt: str) -> str:
    body = {"inputText": prompt}
    resp = brt.invoke_model(
        modelId=DEFAULT_MODEL_ID,
        body=json.dumps(body),
        contentType="application/json",
        accept="application/json",
    )
    payload = json.loads(resp["body"].read())
    text = payload.get("results", [{}])[0].get("outputText", "")
    return (text or "").strip()

def build_prompt(metrics: dict) -> str:
    return (
        "You are an enterprise Oracle DBA upgrade reviewer.\n"
        "Write a report with:\n"
        "1) Executive summary\n"
        "2) Technical validation summary\n"
        "3) Risks/impact\n"
        "4) Root cause hypothesis (based ONLY on provided errors/signals)\n"
        "5) Remediation + preventive controls\n"
        "6) LinkedIn portfolio narrative (5-7 sentences)\n\n"
        "Do not invent facts.\n\n"
        f"DATA:\n{json.dumps(metrics, indent=2)}\n"
    )

def fallback_report(metrics: dict, bedrock_error: str) -> str:
    sig = metrics.get("signals", {})
    invalid_cnt = sig.get("invalid_objects_detected")
    orders = sig.get("orders_count")
    errors = sig.get("ora_errors_sample", [])

    lines = []
    lines.append("## Executive Summary")
    lines.append(
        f"This run generated an Oracle migration/upgrade validation summary. "
        f"Invalid objects detected: **{invalid_cnt}**. "
        f"Orders count proof: **{orders}**."
    )

    lines.append("\n## Technical Summary")
    lines.append(f"- Invalid objects sample: {sig.get('invalid_objects_sample')}")
    lines.append(f"- ORA errors (sample): {len(errors)}")
    if errors:
        lines.append("\n### ORA errors (sample)")
        for e in errors[:15]:
            lines.append(f"- {e}")

    lines.append("\n## Preventive Controls")
    lines.append("- Add dependency checks to detect views broken by dropped/renamed columns.")
    lines.append("- Standardize pre/post validation and keep artifacts in S3 per run_id.")
    lines.append("- Keep AI optional and non-destructive (documentation only).")

    lines.append("\n## LinkedIn Narrative")
    lines.append(
        "I built a Docker-based Oracle migration/upgrade lab using Data Pump (expdp/impdp) and automated validation. "
        "Artifacts are stored per run in S3, and Lambda generates standardized executive reports. "
        "The architecture supports GenAI summarization via Amazon Bedrock when authorized, while deterministic parsing ensures auditability."
    )

    lines.append("\n---")
    lines.append(f"**Bedrock error:** {bedrock_error}")

    return "\n".join(lines)


def handler(event, context):
    if not BUCKET:
        return {"status": "error", "message": "Missing BUCKET_NAME env var"}

    trig_bucket, trig_key = get_bucket_key_from_event(event)

    run_id = event.get("run_id")
    if not run_id and trig_key:
        run_id = extract_run_id_from_key(trig_key)

    if not run_id:
        return {"status": "error", "message": "Could not determine run_id"}

    metrics = build_metrics(run_id)

    # Write metrics where your structure expects genai artifacts
    metrics_key = f"runs/{run_id}/07-genai/metrics.json"
    s3_put_json(BUCKET, metrics_key, metrics)

    # Output report path matches your existing structure
    report_key = f"runs/{run_id}/05-reports/executive_report.md"

    header = (
        f"# Oracle Upgrade / Migration Executive Report\n"
        f"**Run ID:** {run_id}\n"
        f"**Generated (UTC):** {datetime.utcnow().isoformat()}Z\n"
        + (f"**Trigger:** s3://{trig_bucket}/{trig_key}\n" if trig_bucket and trig_key else "")
        + f"**Metrics:** s3://{BUCKET}/{metrics_key}\n\n---\n"
    )

    engine = "fallback"
    try:
        prompt = build_prompt(metrics)
        body = call_bedrock(prompt)
        engine = f"bedrock:{DEFAULT_MODEL_ID}"
        if not body:
            body = "Bedrock returned empty output."
    except Exception as e:
        body = fallback_report(metrics, str(e))

    final = header + body + "\n"
    s3_put_md(BUCKET, report_key, final)

    return {"status": "success", "run_id": run_id, "engine": engine, "report_s3": f"s3://{BUCKET}/{report_key}", "metrics_s3": f"s3://{BUCKET}/{metrics_key}"}
