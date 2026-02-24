# Financial Data Ingestion & Canonical Modeling (Snowflake SQL Only)

This repository implements a **modular SQL-only ingestion app** for onboarding financial transactions from **CSV, XML, and JSON** sources into a single canonical model in Snowflake.

It is designed to satisfy the offline coding exercise requirements:
1. Raw ingestion by file type.
2. Canonical model definition.
3. Transformation into canonical tables.
4. Data quality anomaly detection and handling.
5. Production-style documentation and modular deployment.

---

## Repository Structure

- `main.sql`
  Orchestrator/manifest that defines execution order.
- `sql/00_bootstrap.sql`
  Database and schema bootstrap.
- `sql/01_raw_ingestion.sql`
  Raw ingestion templates (COPY INTO + audit insert).
- `sql/02_canonical_ddl.sql`
  Canonical target model DDL.
- `sql/03_transform_headers.sql`
  Header-level normalization and dedup-ready staging.
- `sql/04_transform_lines.sql`
  Line-level normalization for nested/semi-structured payloads.
- `sql/05_merge_canonical.sql`
  MERGE-based upserts into canonical header/line tables.
- `sql/06_anomaly_detection.sql`
  Persistent anomaly capture and categorization.
- `sql/07_ops_views.sql`
  Operational visibility views.
- `sql/08_smoke_tests.sql`
  Post-load verification queries.
- `docs/architecture.md`
  Detailed architecture, anomaly strategy, and operational notes.

---

## How to Run

### Option A — Snowflake Worksheet
Execute the SQL scripts in this exact order:
1. `sql/00_bootstrap.sql`
2. `sql/01_raw_ingestion.sql`
3. `sql/02_canonical_ddl.sql`
4. `sql/03_transform_headers.sql`
5. `sql/04_transform_lines.sql`
6. `sql/05_merge_canonical.sql`
7. `sql/06_anomaly_detection.sql`
8. `sql/07_ops_views.sql`
9. `sql/08_smoke_tests.sql`

### Option B — SnowSQL CLI
Use the `!source` commands shown in `main.sql`.

---

## Deliverables Mapping to the Exercise

- **Raw ingestion DDL / patterns**: `sql/01_raw_ingestion.sql`
- **Canonical DDL**: `sql/02_canonical_ddl.sql`
- **Transformation SQL**: `sql/03_transform_headers.sql`, `sql/04_transform_lines.sql`, `sql/05_merge_canonical.sql`
- **Anomaly handling notes**: `sql/06_anomaly_detection.sql` + `docs/architecture.md`

---

## Design Principles

- SQL-only approach (no Python / no external ETL).
- Idempotent canonical loads via `MERGE`.
- Schema drift tolerance via `VARIANT` and `TRY_TO_*` conversion.
- Canonical survivorship through deterministic dedupe keys.
- Explicit anomaly persistence for auditability.

For full rationale, see `docs/architecture.md`.
