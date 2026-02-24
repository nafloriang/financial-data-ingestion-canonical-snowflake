# Architecture & Data Quality Design

## 1) Problem Context
Different clients send transaction data in incompatible shapes (CSV/XML/JSON), with known quality defects:
- duplicate transactions,
- missing/null required fields,
- extra/unexpected fields,
- negative quantity/amount values,
- inconsistent nesting.

The solution standardizes ingestion into a canonical model using **Snowflake SQL only**.

---

## 2) Layered Architecture

### RAW Layer (`FIN_INGEST.RAW`)
Purpose: immutable landing zone with minimal transformation.
- `RAW_TXN_JSON`
- `RAW_TXN_XML`
- `RAW_CSV_GENERIC`
- `RAW_LOAD_AUDIT`

Key behavior:
- `COPY INTO ... ON_ERROR='CONTINUE'` prevents load-job abortion on malformed records.
- `RESULT_SCAN(LAST_QUERY_ID())` captures load telemetry to `RAW_LOAD_AUDIT`.

### STAGING Layer (TEMP tables)
Purpose: in-session canonical shaping before durable upsert.
- `STG_CAN_TXN_HEADER`
- `STG_CAN_TXN_LINE`
- `STG_ANOMALY`

Key behavior:
- wide `COALESCE` pathing for schema variance,
- `TRY_TO_*` casting for resilient parsing,
- flattening for nested arrays/objects.

### CANON Layer (`FIN_INGEST.CANON`)
Purpose: analytics-ready unified model.
- `CAN_TXN` (header grain)
- `CAN_TXN_LINE` (line-item grain)
- `CAN_TXN_ANOMALY` (quality event grain)

### OPS Layer (`FIN_INGEST.OPS`)
Purpose: operational observability.
- `VW_LOAD_AUDIT_SUMMARY`
- `VW_CANON_COUNTS`
- `VW_ANOMALY_COUNTS`

---

## 3) Canonical Data Model

## Header Grain (`CAN_TXN`)
One record per canonical transaction ID.
Includes business fields plus:
- `is_valid` boolean quality state,
- `anomaly_codes` array snapshot,
- `attributes VARIANT` for schema-drift retention.

## Line Grain (`CAN_TXN_LINE`)
One record per canonical transaction + line number.
Supports normalized line economics while preserving raw fragments in `attributes`.

## Anomaly Grain (`CAN_TXN_ANOMALY`)
One record per detected rule-hit for audit and downstream triage.

---

## 4) Transformation Strategy

### Header normalization
1. Parse each source format independently.
2. Harmonize key names with `COALESCE`.
3. Convert values with `TRY_TO_TIMESTAMP_NTZ` / `TRY_TO_NUMBER`.
4. Build dedupe key:
   - primary: `(client_id, source_system, source_txn_id)`
   - fallback: payload hash.
5. Generate deterministic `canonical_txn_id` using SHA2.
6. Mark first-class header anomalies in `anomaly_codes`.

### Line normalization
1. `LATERAL FLATTEN` for JSON/XML nested line arrays.
2. positional mapping for CSV line fields.
3. retain uncertain mappings in `attributes` metadata.

### Canonical persistence
`MERGE` is used for idempotent upserts to support reruns and late-arriving corrections.

---

## 5) Anomaly Handling Rules

Rules implemented:
- `DUPLICATE_TXN` → duplicate business key in same client/source domain.
- `MISSING_REQUIRED` → missing `txn_timestamp` or `total_amount`.
- `NEGATIVE_AMOUNT` → header `total_amount < 0`.
- `NEGATIVE_QTY` → line `quantity < 0`.
- `NEGATIVE_AMOUNT_LINE` → line `line_amount < 0`.

Handling approach:
- Keep records (do not discard) to preserve forensic lineage.
- Flag validity (`is_valid=false`) and persist detailed anomalies.
- Expose anomaly aggregates via `VW_ANOMALY_COUNTS`.

---

## 6) Unexpected/Extra Fields and Nesting Variance

- Extra fields are retained in `attributes VARIANT` instead of being dropped.
- Inconsistent source keys are resolved by precedence ordering in `COALESCE` chains.
- Nested structures are normalized with `FLATTEN` and fallback path candidates.

This provides extensibility for new client shapes without immediate DDL redesign.

---

## 7) Operational Runbook (Recommended)

1. Run `00_bootstrap` once per environment.
2. Execute raw `COPY INTO` loads per source folder.
3. Immediately append audit rows from `RESULT_SCAN`.
4. Run transformation + merges.
5. Run anomaly detection merge.
6. Validate with smoke tests and OPS views.
7. Promote same scripts across environments (dev/test/prod) using parameterized DB/schema wrappers if needed.

---

## 8) Senior-Architect Notes / Improvement Backlog

- Add task orchestration using Snowflake Tasks/Streams for incremental micro-batch ingestion.
- Add conformed dimensions (merchant/customer/account) for star-schema analytics.
- Add quarantine tables for hard parse failures if strict controls are required.
- Add SLA alerting from OPS views (e.g., error thresholds, anomaly spikes).
- Add versioned mapping metadata table to externalize `COALESCE` key precedence per client.

