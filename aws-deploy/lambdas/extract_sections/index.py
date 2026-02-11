import os
import re
import boto3

s3 = boto3.client("s3")

def get_text(bucket: str, key: str, max_bytes: int = 800_000) -> str:
    obj = s3.get_object(Bucket=bucket, Key=key)
    data = obj["Body"].read(max_bytes)
    return data.decode("utf-8-sig", errors="replace")

def put_text(bucket: str, key: str, text: str):
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=text.encode("utf-8"),
        ContentType="text/markdown"
    )

def extract_section(md: str, heading: str):
    pattern = rf"(?ms)^\s*##\s+{re.escape(heading)}\s*\r?\n(.*?)(?=^\s*##\s|\Z)"
    m = re.search(pattern, md)
    if not m:
        return None
    body = m.group(1).rstrip()
    return f"## {heading}\n{body}\n"

def handler(event, context):
    bucket = event.get("bucket") or os.environ.get("BUCKET_NAME")
    run_prefix = event["run_prefix"].rstrip("/")
    exec_key = event["executive_report_key"]

    md = get_text(bucket, exec_key)

    validation = extract_section(md, "Validation (from proof artifacts)")
    ora39082 = extract_section(md, "Compilation Warnings (ORA-39082)")

    out_prefix = f"{run_prefix}/07-genai/sections"
    wrote = []

    if validation:
        put_text(bucket, f"{out_prefix}/validation.md", validation)
        wrote.append(f"{out_prefix}/validation.md")

    if ora39082:
        put_text(bucket, f"{out_prefix}/ora39082.md", ora39082)
        wrote.append(f"{out_prefix}/ora39082.md")

    return {"ok": True, "wrote": wrote}
