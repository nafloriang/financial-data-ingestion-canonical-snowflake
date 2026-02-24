# Financial Data Ingestion & Canonical Modeling (Snowflake SQL)

This submission provides a single executable SQL script: `main.sql`.

## How to run
1. Open a Snowflake worksheet connected to the account/environment where `FIN_INGEST` exists.
2. Run `main.sql` top-to-bottom in one session.

## What `main.sql` creates
- Canonical tables in `FIN_INGEST.CANON`:
  - `CAN_TXN`
  - `CAN_TXN_LINE`
  - `CAN_TXN_ANOMALY`
- Operational views in `FIN_INGEST.OPS`:
  - `VW_LOAD_AUDIT_SUMMARY`
  - `VW_CANON_COUNTS`
  - `VW_ANOMALY_COUNTS`

## Notes
- Script is idempotent (`CREATE OR REPLACE` + `MERGE`).
- Includes optional/raw ingestion reference blocks for `COPY INTO` from the existing GCS stage, with audit capture via `RESULT_SCAN(LAST_QUERY_ID())`.
- Canonicalization keeps schema drift data in `attributes VARIANT` and records data quality issues in `CAN_TXN_ANOMALY`.
