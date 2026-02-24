/* 01_raw_ingestion.sql
   Raw ingestion DDL/patterns aligned to FIN_INGEST real environment.

   Why ON_ERROR='CONTINUE'?
   - This exercise includes intentionally malformed/anomalous records.
   - We continue loading valid rows to preserve maximum data for canonical QA handling.

   Why RAW_LOAD_AUDIT?
   - COPY history needs durable traceability (rows parsed/loaded/errors + first error).
   - RESULT_SCAN(LAST_QUERY_ID()) is captured immediately after each COPY in-session.

   Why we do NOT force-fix structurally invalid files here:
   - RAW layer is an immutable landing zone.
   - Structural correction belongs to downstream canonical validation/anomaly logic,
     not destructive mutation during ingestion.
*/

USE DATABASE FIN_INGEST;
USE SCHEMA RAW;

/* ---------------------------------------------------------------------------
   External access objects (idempotent setup)
   NOTE: In some Snowflake editions, CREATE STORAGE INTEGRATION IF NOT EXISTS
         may require elevated privileges. Keep as declarative environment guard.
--------------------------------------------------------------------------- */
CREATE STORAGE INTEGRATION IF NOT EXISTS gcs_int_fin
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = GCS
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://nestor-florian-project/');

CREATE STAGE IF NOT EXISTS STG_FIN_GCS
  URL = 'gcs://nestor-florian-project/'
  STORAGE_INTEGRATION = gcs_int_fin;

CREATE FILE FORMAT IF NOT EXISTS FF_JSON
  TYPE = JSON
  STRIP_OUTER_ARRAY = TRUE;

CREATE FILE FORMAT IF NOT EXISTS FF_XML
  TYPE = XML;

CREATE FILE FORMAT IF NOT EXISTS FF_CSV
  TYPE = CSV
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"';

/* RAW_LOAD_AUDIT physical schema already exists in environment.
   Keep hardening DDL aligned to exact column names. */
CREATE TABLE IF NOT EXISTS FIN_INGEST.RAW.RAW_LOAD_AUDIT (
  src_file STRING,
  file_type STRING,
  load_status STRING,
  rows_parsed NUMBER,
  rows_loaded NUMBER,
  errors_seen NUMBER,
  first_error STRING,
  load_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- XML COPY (ClientA)
COPY INTO FIN_INGEST.RAW.RAW_TXN_XML (client_id, src_file, src_row_number, payload)
FROM (
    SELECT
        'ClientA'::STRING                          AS client_id,
        METADATA$FILENAME::STRING                  AS src_file,
        METADATA$FILE_ROW_NUMBER::NUMBER           AS src_row_number,
        $1                                          AS payload
    FROM @FIN_INGEST.RAW.STG_FIN_GCS/client_a/xml/
)
FILE_FORMAT = (FORMAT_NAME = FIN_INGEST.RAW.FF_XML)
ON_ERROR = 'CONTINUE';

INSERT INTO FIN_INGEST.RAW.RAW_LOAD_AUDIT (
    src_file, file_type, load_status, rows_parsed, rows_loaded, errors_seen, first_error, load_ts
)
SELECT
    "file"::STRING          AS src_file,
    'XML'::STRING            AS file_type,
    "status"::STRING        AS load_status,
    "rows_parsed"::NUMBER   AS rows_parsed,
    "rows_loaded"::NUMBER   AS rows_loaded,
    "errors_seen"::NUMBER   AS errors_seen,
    "first_error"::STRING   AS first_error,
    CURRENT_TIMESTAMP()      AS load_ts
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- JSON COPY (ClientC)
COPY INTO FIN_INGEST.RAW.RAW_TXN_JSON (client_id, src_file, src_row_number, payload)
FROM (
    SELECT
        'ClientC'::STRING                          AS client_id,
        METADATA$FILENAME::STRING                  AS src_file,
        METADATA$FILE_ROW_NUMBER::NUMBER           AS src_row_number,
        $1                                          AS payload
    FROM @FIN_INGEST.RAW.STG_FIN_GCS/client_c/json/
)
FILE_FORMAT = (FORMAT_NAME = FIN_INGEST.RAW.FF_JSON)
ON_ERROR = 'CONTINUE';

INSERT INTO FIN_INGEST.RAW.RAW_LOAD_AUDIT (
    src_file, file_type, load_status, rows_parsed, rows_loaded, errors_seen, first_error, load_ts
)
SELECT
    "file"::STRING          AS src_file,
    'JSON'::STRING           AS file_type,
    "status"::STRING        AS load_status,
    "rows_parsed"::NUMBER   AS rows_parsed,
    "rows_loaded"::NUMBER   AS rows_loaded,
    "errors_seen"::NUMBER   AS errors_seen,
    "first_error"::STRING   AS first_error,
    CURRENT_TIMESTAMP()      AS load_ts
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- CSV COPY (client_id derived from folder path)
COPY INTO FIN_INGEST.RAW.RAW_CSV_GENERIC (client_id, src_file, src_row_number, payload)
FROM (
    SELECT
        CASE
            WHEN METADATA$FILENAME ILIKE 'client_a/%' THEN 'ClientA'
            WHEN METADATA$FILENAME ILIKE 'client_c/%' THEN 'ClientC'
            ELSE SPLIT_PART(METADATA$FILENAME, '/', 1)
        END::STRING                                  AS client_id,
        METADATA$FILENAME::STRING                    AS src_file,
        METADATA$FILE_ROW_NUMBER::NUMBER             AS src_row_number,
        ARRAY_CONSTRUCT(*)                           AS payload
    FROM @FIN_INGEST.RAW.STG_FIN_GCS/
)
FILES = ('client_a/csv/transactions.csv','client_c/csv/transactions.csv')
FILE_FORMAT = (FORMAT_NAME = FIN_INGEST.RAW.FF_CSV)
ON_ERROR = 'CONTINUE';

INSERT INTO FIN_INGEST.RAW.RAW_LOAD_AUDIT (
    src_file, file_type, load_status, rows_parsed, rows_loaded, errors_seen, first_error, load_ts
)
SELECT
    "file"::STRING          AS src_file,
    'CSV'::STRING            AS file_type,
    "status"::STRING        AS load_status,
    "rows_parsed"::NUMBER   AS rows_parsed,
    "rows_loaded"::NUMBER   AS rows_loaded,
    "errors_seen"::NUMBER   AS errors_seen,
    "first_error"::STRING   AS first_error,
    CURRENT_TIMESTAMP()      AS load_ts
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
