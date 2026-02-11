# Oracle Upgrade/Migration Executive Summary

## Run Overview
- **Run ID:** `2026-02-10T083050Z_18c_to_23ai`
- **Run Prefix:** `runs/2026-02-10/`
- **Environment:** `local-docker-windows`
- **AWS Region:** `us-east-1`
- **S3 Bucket:** `oracle-migration-artifacts-448792658038`
- **Created UTC:** `2026-02-11T08:30:52.3753723Z`
- **Overall Status:** `WARN`

## Deterministic Risk Assessment
- **Risk score (0-100):** 60
- **Risk level:** `MEDIUM`
- **Top factors:**
  - `impdp_retry_present` (+10): impdp_log_count=2
  - `warn_ora_present` (+15): ['ORA-39082']
  - `expdp_completed_with_errors` (+10): {'log': '03-migration/expdp_legacy_18c.log', 'error_count': 2}
  - `validation_invalid_objects_present` (+25): 1

## Evidence Inventory (S3)
- **Object count:** 22
- **Total bytes:** 1204602
- **Selected IMPDP log (final attempt):** `03-migration/impdp_legacy_23c_retry2.log`
- **Selection reason:** `filename_retry_number_then_lastmodified`

## Data Pump Status (heuristic)
- **Export log:** `03-migration/expdp_legacy_18c.log` → `WARN` (state=COMPLETED_WITH_ERRORS, errors=2)
- **Import log:** `03-migration/impdp_legacy_23c_retry2.log` → `WARN` (state=COMPLETED_WITH_ERRORS, attempts=2)

## Validation (from proof artifacts)
- **Validation status:** `WARN`
- **Invalid objects (count):** 1
- **Orders count proof:** 50000
- **Invalid objects (sample):**
  - LEGACY_APP.BAD_VIEW (VIEW) = INVALID

## Key Findings (ORA-* taxonomy)
- ORA-06512: 14
- ORA-06550: 4
- ORA-31642: 2
- ORA-39127: 2
- ORA-44002: 2
- ORA-39082: 1

## Compilation Warnings (ORA-39082)
- VIEW: LEGACY_APP.BAD_VIEW

## Governance / Guardrails
- No DB connections from AWS.
- No execution of SQL/OS commands.
- Allowlisted log parsing only.
- Bounded reads from S3 objects.

---
**Note:** Generated from S3 artifacts only. No DB commands executed by AWS components.

