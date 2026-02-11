import json
import os
from pathlib import Path
import boto3

s3 = boto3.client("s3")
brt = boto3.client("bedrock-runtime", region_name=os.environ.get("AWS_REGION", "us-east-1"))

PROMPTS_DIR = Path(__file__).parent / "prompts"

def load_prompt(name: str) -> str:
    p = PROMPTS_DIR / f"{name}.prompt.txt"
    if not p.exists():
        raise ValueError(f"Missing prompt: {p}")
    return p.read_text(encoding="utf-8")

def invoke_anthropic(model_id: str, user_text: str) -> str:
    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 1800,
        "temperature": 0.2,
        "messages": [{"role": "user", "content": user_text}],
    }
    resp = brt.invoke_model(
        modelId=model_id,
        contentType="application/json",
        accept="application/json",
        body=json.dumps(body).encode("utf-8"),
    )
    payload = json.loads(resp["body"].read())
    return "".join(block.get("text", "") for block in payload.get("content", []))

def handler(event, context):
    bucket = event.get("bucket") or os.environ["BUCKET_NAME"]
    evidence_key = event["evidence_key"]
    output_key = event["output_key"]
    prompt_name = event["prompt_name"]

    model_id = event.get("model_id") or os.environ.get("DEFAULT_MODEL_ID")
    if not model_id:
        raise ValueError("model_id not provided and DEFAULT_MODEL_ID not set")

    evidence_obj = s3.get_object(Bucket=bucket, Key=evidence_key)
    evidence_text = evidence_obj["Body"].read().decode("utf-8", errors="replace")

    prompt_template = load_prompt(prompt_name)

    composed = (
        prompt_template.strip()
        + "\n\nSTRICT RULES:\n"
          "- Use ONLY the EVIDENCE_JSON below.\n"
          "- Do not invent object names, counts, statuses, or execution claims.\n"
          "- If missing, write 'Unknown'.\n"
          "- Cite the JSON keys you used.\n"
        + "\n\nEVIDENCE_JSON:\n"
        + evidence_text
    )

    out = invoke_anthropic(model_id, composed)

    content_type = "text/plain"
    if output_key.endswith(".md"):
        content_type = "text/markdown"

    s3.put_object(
        Bucket=bucket,
        Key=output_key,
        Body=out.encode("utf-8"),
        ContentType=content_type,
    )

    return {
        "ok": True,
        "bucket": bucket,
        "model_id": model_id,
        "prompt_name": prompt_name,
        "evidence_key": evidence_key,
        "output_key": output_key,
        "bytes_written": len(out.encode("utf-8"))
    }
