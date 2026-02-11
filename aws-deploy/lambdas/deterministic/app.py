# GenAI-Assisted Oracle Upgrade Automation Framework (AWS Lambda)
#
# Trigger:
#   S3:ObjectCreated on runs/*/00-metadata/metrics.json
#
# Outputs (deterministic, non-AI):
#   - runs/<date>/00-metadata/sanitized_summary.json
#   - runs/<date>/05-reports/executive_report.md
#
# This version:
#   - Selects FINAL impdp attempt deterministically (retry# > LastModified)
#   - Extracts ORA-* counts + Data Pump completion state + error count
#   - Extracts ORA-39082 compilation warning objects
#   - Parses validation proof artifacts in YOUR formats:
#       * invalid_object_proof.txt (table rows with INVALID)
#       * orders_count_proof.txt (integer-only line: 50000)
#   - Produces PASS/WARN/FAIL and risk score/factors deterministically
#
# Guardrails:
#   - No DB connectivity from AWS
#   - No SQL/OS command execution
#   - Bounded S3 reads
#   - Allowlisted parsing only
#   - Writes only summaries/excerpts back to S3

import json
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import unquote_plus

import boto3
from botocore.exceptions import ClientError

s3 = boto3.client("s3")

# --------------------------
# Limits / Allowlist
# --------------------------

MAX_BYTES_METRICS = 800_000
MAX_BYTES_LOG = 600_000
MAX_BYTES_PROOF = 120_000

ALLOWLIST_RELATIVE_KEYS = [
    "02-precheck/precheck.log",
    "03-migration/expdp_legacy_18c.log",
    "04-validation/validation_23c.log",
]

MIGRATION_PREFIX_REL = "03-migration/"

INVALID_OBJECT_PROOF_REL = "04-validation/invalid_object_proof.txt"
ORDERS_COUNT_PROOF_REL = "04-validation/orders_count_proof.txt"

# --------------------------
# Regex patterns
# --------------------------

ORA_RE = re.compile(r"\b(ORA-\d{5})\b", re.IGNORECASE)

DP_SUCCESS_RE = re.compile(r"\bsuccessfully completed\b", re.IGNORECASE)
DP_COMPLETED_WITH_ERRORS_RE = re.compile(r"\bcompleted with\s+(\d+)\s+error", re.IGNORECASE)
DP_COMPLETED_WITH_ERRORS_RE2 = re.compile(r"\bcompleted with\s+error", re.IGNORECASE)
DP_COMPLETED_RE = re.compile(r"\bcompleted\b", re.IGNORECASE)

IMPDP_RETRY_RE = re.compile(r"retry(\d+)?", re.IGNORECASE)

# ORA-39082 line example:
# ORA-39082: Object type VIEW:"LEGACY_APP"."BAD_VIEW" created with compilation warnings
ORA_39082_OBJ_RE = re.compile(
    r'ORA-39082:\s+Object type\s+(\w+):"([^"]+)"\."([^"]+)"\s+created with compilation warnings',
    re.IGNORECASE,
)

# used for orders_count_proof parsing
INTEGER_LINE_RE = re.compile(r"^\s*(\d{1,12})\s*$")

# --------------------------
# Severity taxonomy
# --------------------------

FATAL_ORA = {"ORA-39000", "ORA-31640", "ORA-27037"}
WARN_ORA = {
    "ORA-31642", "ORA-39127", "ORA-44002", "ORA-06550",
    "ORA-39082",  # created with compilation warnings
}
INFO_ORA = {"ORA-06512"}

# --------------------------
# Risk weights (deterministic)
# --------------------------

RISK_WEIGHTS = {
    "missing_required_log": 15,
    "missing_impdp_log": 25,
    "impdp_retry_present": 10,
    "fatal_ora_present": 50,
    "warn_ora_present": 15,
    "dp_completion_marker_missing": 10,    # only when no fatal ORAs (de-dup)
    "expdp_completed_with_errors": 10,
    "validation_invalid_objects_present": 25,
    "validation_orders_count_missing": 5,
}

# --------------------------
# Data structures
# --------------------------

@dataclass
class LogResult:
    key_rel: str
    found: bool
    text: str | None
    ora_counts: dict[str, int]
    dp_state: str
    dp_error_count: int | None


# --------------------------
# Event and S3 helpers
# --------------------------

def _get_bucket_key_from_event(event: dict) -> tuple[str, str]:
    """
    Supports:
      - S3 event notification shape
      - manual shape: { "bucket": "...", "key": "..." }
    """
    if isinstance(event, dict) and event.get("Records"):
        rec = event["Records"][0]
        bucket = rec["s3"]["bucket"]["name"]
        key = unquote_plus(rec["s3"]["object"]["key"])
        return bucket, key

    if isinstance(event, dict) and event.get("bucket") and event.get("key"):
        return event["bucket"], event["key"]

    raise ValueError("Unsupported event shape (expected S3 event or {bucket,key}).")


def _derive_run_prefix(metrics_key: str) -> str:
    suffix = "/00-metadata/metrics.json"
    if not metrics_key.endswith(suffix):
        raise ValueError(f"Unexpected trigger key. Expected suffix '{suffix}', got: {metrics_key}")
    # Intentionally removes without leading slash to keep trailing slash in run_prefix
    return metrics_key[: -len("00-metadata/metrics.json")]


def _s3_get_text(bucket: str, key: str, max_bytes: int) -> str:
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read(max_bytes)
    # utf-8-sig strips BOM if present
    try:
        return body.decode("utf-8-sig", errors="replace")
    except Exception:
        return body.decode("utf-8", errors="replace")


def _s3_try_get_text(bucket: str, key: str, max_bytes: int) -> str | None:
    try:
        return _s3_get_text(bucket, key, max_bytes=max_bytes)
    except ClientError as e:
        code = (e.response.get("Error") or {}).get("Code", "")
        if code in ("NoSuchKey", "404", "NotFound"):
            return None
        raise


# --------------------------
# Parsing helpers
# --------------------------

def _parse_ora_counts(text: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for code in ORA_RE.findall(text or ""):
        cu = code.upper()
        counts[cu] = counts.get(cu, 0) + 1
    return counts


def _dp_completion_state_and_errors(text: str) -> tuple[str, int | None]:
    if DP_SUCCESS_RE.search(text or ""):
        return "SUCCESS", 0

    m = DP_COMPLETED_WITH_ERRORS_RE.search(text or "")
    if m:
        try:
            return "COMPLETED_WITH_ERRORS", int(m.group(1))
        except Exception:
            return "COMPLETED_WITH_ERRORS", None

    if DP_COMPLETED_WITH_ERRORS_RE2.search(text or ""):
        return "COMPLETED_WITH_ERRORS", None

    if DP_COMPLETED_RE.search(text or ""):
        return "COMPLETED", None

    return "NONE", None


def _impdp_retry_number(filename: str) -> int:
    m = IMPDP_RETRY_RE.search(filename or "")
    if not m:
        return 0
    if not m.group(1):
        return 1
    try:
        return int(m.group(1))
    except Exception:
        return 1


def _analyze_log(bucket: str, run_prefix: str, rel_key: str) -> LogResult:
    abs_key = run_prefix + rel_key
    text = _s3_try_get_text(bucket, abs_key, max_bytes=MAX_BYTES_LOG)
    if text is None:
        return LogResult(rel_key, False, None, {}, "NONE", None)

    dp_state, dp_errs = _dp_completion_state_and_errors(text)
    return LogResult(rel_key, True, text, _parse_ora_counts(text), dp_state, dp_errs)


def _pick_best_impdp_log(bucket: str, run_prefix: str) -> tuple[str | None, int, list[dict[str, Any]], str]:
    prefix = run_prefix + MIGRATION_PREFIX_REL
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1000)
    objs = resp.get("Contents") or []

    candidates = []
    for o in objs:
        key = o.get("Key", "")
        base = key.split("/")[-1]
        if base.startswith("impdp_") and base.endswith(".log"):
            rn = _impdp_retry_number(base)
            candidates.append((o, rn))

    meta: list[dict[str, Any]] = []
    for o, rn in candidates:
        meta.append({
            "key": o["Key"].replace(run_prefix, ""),
            "base_name": o["Key"].split("/")[-1],
            "retry_number": rn,
            "last_modified": o["LastModified"].isoformat() if hasattr(o["LastModified"], "isoformat") else str(o["LastModified"]),
            "size": o.get("Size"),
        })

    if not candidates:
        return None, 0, meta, "no_candidates"

    # select by retry_number then LastModified
    candidates.sort(key=lambda x: (x[1], x[0]["LastModified"]))
    selected_obj, _selected_rn = candidates[-1]

    max_rn = max(rn for _, rn in candidates)
    selection_reason = "filename_retry_number_then_lastmodified" if max_rn > 0 else "lastmodified"

    return selected_obj["Key"], len(candidates), meta, selection_reason


def _extract_excerpts(text: str, codes: list[str], context_lines: int = 2, max_total_lines: int = 20) -> dict[str, list[str]]:
    excerpts: dict[str, list[str]] = {}
    if not text or not codes:
        return excerpts

    lines = (text or "").splitlines()
    used = 0

    for code in codes:
        code_u = code.upper()
        hit_idx = None
        for i, ln in enumerate(lines):
            if code_u in ln.upper():
                hit_idx = i
                break
        if hit_idx is None:
            continue

        start = max(0, hit_idx - context_lines)
        end = min(len(lines), hit_idx + context_lines + 1)
        chunk = lines[start:end]

        remaining = max_total_lines - used
        if remaining <= 0:
            break
        if len(chunk) > remaining:
            chunk = chunk[:remaining]

        excerpts[code_u] = chunk
        used += len(chunk)

    return excerpts


def _extract_compile_warnings_39082(text: str) -> list[dict[str, str]]:
    findings: list[dict[str, str]] = []
    for m in ORA_39082_OBJ_RE.finditer(text or ""):
        findings.append({
            "ora": "ORA-39082",
            "object_type": m.group(1).upper(),
            "schema": m.group(2),
            "object_name": m.group(3),
        })
    return findings


# --------------------------
# Validation proof parsing (YOUR formats)
# --------------------------

def _parse_invalid_object_proof(text: str) -> dict[str, Any]:
    """
    Your file format example:

    OWNER       OBJECT_NAME   OBJECT_TYPE   STATUS
    ----------  ------------  ------------  -------
    LEGACY_APP  BAD_VIEW      VIEW          INVALID

    We count data rows where STATUS column contains INVALID.
    """
    if not text:
        return {"count": None, "objects": []}

    objects: list[dict[str, str]] = []
    count = 0

    for ln in text.splitlines():
        s = ln.strip()
        if not s:
            continue
        # skip header/separator lines
        if s.upper().startswith("OWNER") or set(s) <= set("- "):
            continue

        # split on 2+ spaces (SQL*Plus table style)
        parts = re.split(r"\s{2,}", s)
        if len(parts) >= 4:
            owner, obj_name, obj_type, status = parts[0], parts[1], parts[2], parts[3]
            if status.upper() == "INVALID":
                count += 1
                objects.append({
                    "owner": owner,
                    "object_name": obj_name,
                    "object_type": obj_type,
                    "status": status,
                })

    return {"count": count, "objects": objects}


def _parse_orders_count_proof(text: str) -> int | None:
    """
    Your file format example:

    ORDERS_COUNT
    ------------
    50000

    We take the first integer-only line.
    """
    if not text:
        return None

    for ln in text.splitlines():
        m = INTEGER_LINE_RE.match(ln)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                return None
    return None


def _parse_validation(bucket: str, run_prefix: str) -> dict[str, Any]:
    invalid_txt = _s3_try_get_text(bucket, run_prefix + INVALID_OBJECT_PROOF_REL, max_bytes=MAX_BYTES_PROOF)
    orders_txt = _s3_try_get_text(bucket, run_prefix + ORDERS_COUNT_PROOF_REL, max_bytes=MAX_BYTES_PROOF)

    invalid_parsed = _parse_invalid_object_proof(invalid_txt or "")
    orders_count = _parse_orders_count_proof(orders_txt or "")

    invalid_count = invalid_parsed["count"]

    status = "UNKNOWN"
    if invalid_count is not None:
        if invalid_count > 0:
            status = "WARN"
        else:
            status = "PASS" if orders_count is not None else "WARN"

    return {
        "status": status,
        "invalid_objects_count": invalid_count,
        "invalid_objects_sample": invalid_parsed["objects"][:20],
        "orders_count": orders_count,
        "evidence": {
            "invalid_object_proof": invalid_txt is not None,
            "orders_count_proof": orders_txt is not None,
        },
    }


# --------------------------
# Risk + status
# --------------------------

def _risk_level(score: int) -> str:
    if score >= 70:
        return "HIGH"
    if score >= 35:
        return "MEDIUM"
    return "LOW"


def _risk_score(required_logs: list[LogResult], impdp: LogResult | None, impdp_count: int, expdp: LogResult | None, validation: dict[str, Any]) -> tuple[int, list[dict[str, Any]]]:
    score = 0
    factors: list[dict[str, Any]] = []

    for lr in required_logs:
        if not lr.found:
            score += RISK_WEIGHTS["missing_required_log"]
            factors.append({"factor": "missing_required_log", "weight": RISK_WEIGHTS["missing_required_log"], "evidence": lr.key_rel})

    if impdp is None or not impdp.found:
        score += RISK_WEIGHTS["missing_impdp_log"]
        factors.append({"factor": "missing_impdp_log", "weight": RISK_WEIGHTS["missing_impdp_log"], "evidence": "No impdp log selected/found under 03-migration/"})

    if impdp_count > 1:
        score += RISK_WEIGHTS["impdp_retry_present"]
        factors.append({"factor": "impdp_retry_present", "weight": RISK_WEIGHTS["impdp_retry_present"], "evidence": f"impdp_log_count={impdp_count}"})

    imp_ora = (impdp.ora_counts if (impdp and impdp.found) else {}) or {}
    fatal_codes = sorted([c for c in imp_ora.keys() if c in FATAL_ORA])
    warn_codes = sorted([c for c in imp_ora.keys() if c in WARN_ORA])

    fatal_present = bool(fatal_codes)
    if fatal_present:
        score += RISK_WEIGHTS["fatal_ora_present"]
        factors.append({"factor": "fatal_ora_present", "weight": RISK_WEIGHTS["fatal_ora_present"], "evidence": fatal_codes})
    elif warn_codes:
        score += RISK_WEIGHTS["warn_ora_present"]
        factors.append({"factor": "warn_ora_present", "weight": RISK_WEIGHTS["warn_ora_present"], "evidence": warn_codes})

    # de-dup: only add completion marker missing when no fatal ORAs
    if (impdp and impdp.found) and (impdp.dp_state == "NONE") and (not fatal_present):
        score += RISK_WEIGHTS["dp_completion_marker_missing"]
        factors.append({"factor": "dp_completion_marker_missing", "weight": RISK_WEIGHTS["dp_completion_marker_missing"], "evidence": impdp.key_rel})

    if expdp and expdp.found and expdp.dp_state == "COMPLETED_WITH_ERRORS":
        score += RISK_WEIGHTS["expdp_completed_with_errors"]
        factors.append({"factor": "expdp_completed_with_errors", "weight": RISK_WEIGHTS["expdp_completed_with_errors"], "evidence": {"log": expdp.key_rel, "error_count": expdp.dp_error_count}})

    inv = validation.get("invalid_objects_count")
    if inv is not None and inv > 0:
        score += RISK_WEIGHTS["validation_invalid_objects_present"]
        factors.append({"factor": "validation_invalid_objects_present", "weight": RISK_WEIGHTS["validation_invalid_objects_present"], "evidence": inv})

    if validation.get("orders_count") is None:
        score += RISK_WEIGHTS["validation_orders_count_missing"]
        factors.append({"factor": "validation_orders_count_missing", "weight": RISK_WEIGHTS["validation_orders_count_missing"], "evidence": "orders_count_proof missing or unparseable"})

    return min(100, score), factors


def _classify_status(expdp: LogResult | None, impdp: LogResult | None, validation: dict[str, Any], impdp_count: int) -> tuple[str, list[str]]:
    reasons: list[str] = []

    if impdp_count > 1:
        reasons.append(f"Multiple impdp attempts detected (count={impdp_count}).")

    imp_ora = (impdp.ora_counts if (impdp and impdp.found) else {}) or {}
    if any(c in FATAL_ORA for c in imp_ora.keys()):
        reasons.append("Fatal ORA codes detected in impdp.")
        return "FAIL", reasons

    # validation contributes a reason (does not force PASS alone)
    if validation.get("status") == "WARN":
        reasons.append("Post-validation indicates WARN conditions (invalid objects present).")

    if impdp and impdp.found and impdp.dp_state == "COMPLETED_WITH_ERRORS":
        reasons.append("impdp completed with errors.")
        return "WARN", reasons

    if expdp and expdp.found and expdp.dp_state == "COMPLETED_WITH_ERRORS":
        reasons.append("expdp completed with errors.")
        return "WARN", reasons

    # PASS only when both datapump SUCCESS and validation PASS
    if (impdp and impdp.found and impdp.dp_state == "SUCCESS") and (expdp and expdp.found and expdp.dp_state == "SUCCESS") and validation.get("status") == "PASS":
        return "PASS", reasons

    reasons.append("Evidence present but not definitive SUCCESS for all phases.")
    return "WARN", reasons


# --------------------------
# Executive report
# --------------------------

def _top_ora(ora_counts_by_file: dict[str, dict[str, int]], top_n: int = 10) -> list[tuple[str, int]]:
    agg: dict[str, int] = {}
    for _, counts in (ora_counts_by_file or {}).items():
        for code, cnt in (counts or {}).items():
            agg[code] = agg.get(code, 0) + int(cnt)
    return sorted(agg.items(), key=lambda x: x[1], reverse=True)[:top_n]


def _render_executive_md(summary: dict) -> str:
    run = summary.get("run", {}) or {}
    inv = summary.get("artifact_inventory", {}) or {}
    derived = summary.get("derived", {}) or {}
    dp = summary.get("datapump", {}) or {}
    risk = summary.get("risk", {}) or {}
    validation = summary.get("validation", {}) or {}
    compile_warnings = summary.get("compile_warnings", []) or []

    top_ora = _top_ora(summary.get("ora_counts_by_file", {}) or {}, top_n=10)

    lines: list[str] = []
    lines.append("# Oracle Upgrade/Migration Executive Summary")
    lines.append("")
    lines.append("## Run Overview")
    lines.append(f"- **Run ID:** `{run.get('run_id', '')}`")
    lines.append(f"- **Run Prefix:** `{run.get('run_prefix', '')}`")
    lines.append(f"- **Environment:** `{run.get('environment', '')}`")
    lines.append(f"- **AWS Region:** `{run.get('aws_region', '')}`")
    lines.append(f"- **S3 Bucket:** `{run.get('s3_bucket', '')}`")
    lines.append(f"- **Created UTC:** `{run.get('created_utc', '')}`")
    lines.append(f"- **Overall Status:** `{summary.get('overall_status', 'UNKNOWN')}`")
    lines.append("")

    lines.append("## Deterministic Risk Assessment")
    lines.append(f"- **Risk score (0-100):** {risk.get('score')}")
    lines.append(f"- **Risk level:** `{risk.get('level')}`")
    if risk.get("factors"):
        lines.append("- **Top factors:**")
        for f in (risk.get("factors") or [])[:10]:
            lines.append(f"  - `{f.get('factor')}` (+{f.get('weight')}): {f.get('evidence')}")
    lines.append("")

    lines.append("## Evidence Inventory (S3)")
    lines.append(f"- **Object count:** {inv.get('object_count')}")
    lines.append(f"- **Total bytes:** {inv.get('total_bytes')}")
    lines.append(f"- **Selected IMPDP log (final attempt):** `{derived.get('selected_impdp_log')}`")
    lines.append(f"- **Selection reason:** `{dp.get('selection_reason')}`")
    lines.append("")

    lines.append("## Data Pump Status (heuristic)")
    lines.append(
        f"- **Export log:** `{dp.get('export', {}).get('log')}` → `{dp.get('export', {}).get('status')}` "
        f"(state={dp.get('export', {}).get('completion_state')}, errors={dp.get('export', {}).get('completed_with_error_count')})"
    )
    lines.append(
        f"- **Import log:** `{dp.get('import', {}).get('log')}` → `{dp.get('import', {}).get('status')}` "
        f"(state={dp.get('import', {}).get('completion_state')}, attempts={dp.get('import', {}).get('attempt_count')})"
    )
    lines.append("")

    lines.append("## Validation (from proof artifacts)")
    lines.append(f"- **Validation status:** `{validation.get('status')}`")
    lines.append(f"- **Invalid objects (count):** {validation.get('invalid_objects_count')}")
    lines.append(f"- **Orders count proof:** {validation.get('orders_count')}")
    if validation.get("invalid_objects_sample"):
        lines.append("- **Invalid objects (sample):**")
        for o in validation["invalid_objects_sample"][:10]:
            lines.append(f"  - {o.get('owner')}.{o.get('object_name')} ({o.get('object_type')}) = {o.get('status')}")
    lines.append("")

    lines.append("## Key Findings (ORA-* taxonomy)")
    if not top_ora:
        lines.append("- No ORA-* patterns detected in parsed logs.")
    else:
        for code, cnt in top_ora:
            lines.append(f"- {code}: {cnt}")
    lines.append("")

    lines.append("## Compilation Warnings (ORA-39082)")
    if not compile_warnings:
        lines.append("- None detected.")
    else:
        for item in compile_warnings[:20]:
            lines.append(f"- {item.get('object_type')}: {item.get('schema')}.{item.get('object_name')}")
    lines.append("")

    excerpts = summary.get("evidence_excerpts", {}) or {}
    if excerpts:
        lines.append("## Evidence Excerpts (bounded)")
        for src, by_code in excerpts.items():
            lines.append(f"### {src}")
            for code, chunk in (by_code or {}).items():
                lines.append(f"- **{code}**")
                lines.append("```")
                lines.extend(chunk)
                lines.append("```")
        lines.append("")

    lines.append("## Governance / Guardrails")
    for g in summary.get("guardrails", []) or []:
        lines.append(f"- {g}")
    lines.append("")
    lines.append("---")
    lines.append("**Note:** Generated from S3 artifacts only. No DB commands executed by AWS components.")
    lines.append("")
    return "\n".join(lines)


# --------------------------
# Lambda handler
# --------------------------

def lambda_handler(event, context):
    if isinstance(event, dict):
        print("event_top_keys=", list(event.keys()))

    bucket, key = _get_bucket_key_from_event(event)
    run_prefix = _derive_run_prefix(key)

    print(f"trigger_bucket={bucket}")
    print(f"trigger_key={key}")
    print(f"derived_run_prefix={run_prefix}")

    metrics = json.loads(_s3_get_text(bucket, key, max_bytes=MAX_BYTES_METRICS))

    # allowlisted logs
    log_results: list[LogResult] = []
    log_presence: dict[str, Any] = {}
    ora_counts_by_file: dict[str, dict[str, int]] = {}

    for rel in ALLOWLIST_RELATIVE_KEYS:
        lr = _analyze_log(bucket, run_prefix, rel)
        log_results.append(lr)
        log_presence[rel] = lr.found
        ora_counts_by_file[rel] = lr.ora_counts

    expdp_lr = next((x for x in log_results if x.key_rel.endswith("expdp_legacy_18c.log")), None)

    # select final impdp
    selected_impdp_abs, impdp_log_count, impdp_candidates, selection_reason = _pick_best_impdp_log(bucket, run_prefix)

    impdp_lr: LogResult | None = None
    selected_impdp_rel: str | None = None
    evidence_excerpts: dict[str, dict[str, list[str]]] = {}
    compile_warnings: list[dict[str, str]] = []

    if selected_impdp_abs:
        selected_impdp_rel = selected_impdp_abs.replace(run_prefix, "")
        impdp_text = _s3_get_text(bucket, selected_impdp_abs, max_bytes=MAX_BYTES_LOG)
        dp_state, dp_errs = _dp_completion_state_and_errors(impdp_text)
        impdp_lr = LogResult(selected_impdp_rel, True, impdp_text, _parse_ora_counts(impdp_text), dp_state, dp_errs)

        log_presence[selected_impdp_rel] = True
        ora_counts_by_file[selected_impdp_rel] = impdp_lr.ora_counts

        fatal_in_impdp = sorted([c for c in impdp_lr.ora_counts.keys() if c in FATAL_ORA])
        if fatal_in_impdp:
            evidence_excerpts[selected_impdp_rel] = _extract_excerpts(impdp_text, fatal_in_impdp)

        compile_warnings = _extract_compile_warnings_39082(impdp_text)

        print(f"selected_impdp_log={selected_impdp_abs} selection_reason={selection_reason}")
    else:
        print("selected_impdp_log=None")

    # validation (proof parsing)
    validation = _parse_validation(bucket, run_prefix)

    # status + risk
    overall_status, status_reasons = _classify_status(expdp_lr, impdp_lr, validation, impdp_log_count)
    risk_score_value, risk_factors = _risk_score(log_results, impdp_lr, impdp_log_count, expdp_lr, validation)

    def dp_status(lr: LogResult | None) -> str:
        if lr is None or not lr.found:
            return "MISSING"
        if any(c in FATAL_ORA for c in (lr.ora_counts or {}).keys()):
            return "FAIL"
        if lr.dp_state == "SUCCESS":
            return "PASS"
        if lr.dp_state == "COMPLETED_WITH_ERRORS":
            return "WARN"
        return "WARN"

    datapump = {
        "export": {
            "log": expdp_lr.key_rel if expdp_lr else None,
            "status": dp_status(expdp_lr),
            "completion_state": expdp_lr.dp_state if expdp_lr else "NONE",
            "completed_with_error_count": expdp_lr.dp_error_count if expdp_lr else None,
        },
        "import": {
            "log": selected_impdp_rel,
            "status": dp_status(impdp_lr),
            "completion_state": impdp_lr.dp_state if impdp_lr else "NONE",
            "attempt_count": impdp_log_count,
        },
        "impdp_candidates": impdp_candidates,
        "selection_reason": selection_reason,
    }

    summary = {
        "schema_version": "1.5.0",
        "run": metrics.get("run", {}) or {},
        "trigger": {"bucket": bucket, "key": key},
        "derived": {"run_prefix": run_prefix, "selected_impdp_log": selected_impdp_rel},
        "artifact_inventory": metrics.get("artifacts_summary", {}) or {},
        "log_presence": log_presence,
        "ora_counts_by_file": ora_counts_by_file,
        "datapump": datapump,
        "compile_warnings": compile_warnings,
        "validation": validation,
        "overall_status": overall_status,
        "status_reasons": status_reasons,
        "risk": {"score": risk_score_value, "level": _risk_level(risk_score_value), "factors": risk_factors},
        "evidence_excerpts": evidence_excerpts,
        "guardrails": [
            "No DB connections from AWS.",
            "No execution of SQL/OS commands.",
            "Allowlisted log parsing only.",
            "Bounded reads from S3 objects.",
        ],
    }

    # write sanitized summary
    summary_key = run_prefix + "00-metadata/sanitized_summary.json"
    s3.put_object(
        Bucket=bucket,
        Key=summary_key,
        Body=(json.dumps(summary, indent=2)).encode("utf-8"),
        ContentType="application/json",
    )

    # write executive report
    report_key = run_prefix + "05-reports/executive_report.md"
    s3.put_object(
        Bucket=bucket,
        Key=report_key,
        Body=_render_executive_md(summary).encode("utf-8"),
        ContentType="text/markdown",
    )

    print(f"wrote_sanitized_summary={summary_key}")
    print(f"wrote_executive_report={report_key}")

    # IMPORTANT: return bucket + run_prefix for Step Functions orchestration
    return {
        "ok": True,
        "bucket": bucket,
        "run_prefix": run_prefix.rstrip("/"),
        "overall_status": overall_status,
        "risk_score": risk_score_value,
        "selected_impdp_log": selected_impdp_rel,
        "sanitized_summary_key": summary_key,
        "executive_report_key": report_key,
    }