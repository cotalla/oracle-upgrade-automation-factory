# Oracle Upgrade / Migration Executive Report
**Run ID:** 2026-02-10
**Generated (UTC):** 2026-02-11T21:00:44.804519Z
**Metrics:** s3://oracle-migration-artifacts-448792658038/runs/2026-02-10/07-genai/metrics.json

---
## Executive Summary
This run generated an Oracle migration/upgrade validation summary. Invalid objects detected: **0**. Orders count proof: **50000**.

## Technical Summary
- Invalid objects sample: []
- ORA errors (sample): 13

### ORA errors (sample)
- >>> ORA-31642: the following SQL statement fails:
- ORA-06512: at "SYS.DBMS_SYS_ERROR", line 86
- ORA-06550: line 1, column 8:
- ORA-39127: unexpected error from call to GET_ACTION_SCHEMA TAG: OLAPC Calling: SYS.DBMS_CUBE_EXP.SCHEMA_INFO_EXP schema: LEGACY_APP prepost: 0 isdba: 1
- ORA-44002: invalid object name
- ORA-06512: at "SYS.DBMS_ASSERT", line 417
- ORA-06512: at "SYS.DBMS_ASSERT", line 412
- ORA-06512: at "SYS.DBMS_METADATA", line 11602
- ORA-06512: at "SYS.DBMS_SYS_ERROR", line 95
- ORA-39127: unexpected error from call to GET_ACTION_SCHEMA TAG: OLAPC Calling: SYS.DBMS_CUBE_EXP.SCHEMA_INFO_EXP schema: LEGACY_APP prepost: 1 isdba: 1
- ORA-39000: bad dump file specification
- ORA-31640: unable to open dump file "/opt/oracle/admin/FREE/dpdump/4A3FF8819B29119AE0636402000A28B5/legacy_app_18c.dmp" for read
- ORA-27037: unable to obtain file status

## Preventive Controls
- Add dependency checks to detect views broken by dropped/renamed columns.
- Standardize pre/post validation and keep artifacts in S3 per run_id.
- Keep AI optional and non-destructive (documentation only).

## LinkedIn Narrative
I built a Docker-based Oracle migration/upgrade lab using Data Pump (expdp/impdp) and automated validation. Artifacts are stored per run in S3, and Lambda generates standardized executive reports. The architecture supports GenAI summarization via Amazon Bedrock when authorized, while deterministic parsing ensures auditability.

---
**Bedrock error:** An error occurred (ValidationException) when calling the InvokeModel operation: Operation not allowed
