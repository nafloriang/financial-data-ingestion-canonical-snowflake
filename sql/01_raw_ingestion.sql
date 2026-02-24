/* 01_raw_ingestion.sql
   Raw ingestion DDL and COPY templates (SQL only).
   Execute COPY + audit insert pairs in same session.
*/

USE DATABASE FIN_INGEST;
USE SCHEMA RAW;

-- Optional hardening DDL for raw landing tables (if not already present in environment).
CREATE TABLE IF NOT EXISTS FIN_INGEST.RAW.RAW_LOAD_AUDIT (
  file STRING,
  file_type STRING,
  load_status STRING,
  rows_parsed NUMBER,
  rows_loaded NUMBER,
  errors_seen NUMBER,
  first_error STRING,
  load_ts TIMESTAMP_NTZ
);

-- JSON CLIENT LOAD PATTERN
COPY INTO FIN_INGEST.RAW.RAW_TXN_JSON (client_id, src_file, src_row_number, payload)
FROM (
    SELECT
        'CLIENT_C'::STRING                                                AS client_id,
        METADATA$FILENAME::STRING                                         AS src_file,
        METADATA$FILE_ROW_NUMBER::NUMBER                                  AS src_row_number,
        $1                                                                 AS payload
    FROM @FIN_INGEST.RAW.STG_FIN_GCS/client_c/json/
)
FILE_FORMAT = (FORMAT_NAME = FIN_INGEST.RAW.FF_JSON)
ON_ERROR = 'CONTINUE';

INSERT INTO FIN_INGEST.RAW.RAW_LOAD_AUDIT
SELECT file, 'JSON', status, rows_parsed, rows_loaded, errors_seen, first_error, CURRENT_TIMESTAMP()
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- XML CLIENT LOAD PATTERN
COPY INTO FIN_INGEST.RAW.RAW_TXN_XML (client_id, src_file, src_row_number, payload)
FROM (
    SELECT
        'CLIENT_A'::STRING                                                AS client_id,
        METADATA$FILENAME::STRING                                         AS src_file,
        METADATA$FILE_ROW_NUMBER::NUMBER                                  AS src_row_number,
        $1                                                                 AS payload
    FROM @FIN_INGEST.RAW.STG_FIN_GCS/client_a/xml/
)
FILE_FORMAT = (FORMAT_NAME = FIN_INGEST.RAW.FF_XML)
ON_ERROR = 'CONTINUE';

INSERT INTO FIN_INGEST.RAW.RAW_LOAD_AUDIT
SELECT file, 'XML', status, rows_parsed, rows_loaded, errors_seen, first_error, CURRENT_TIMESTAMP()
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- CSV CLIENT LOAD PATTERN
COPY INTO FIN_INGEST.RAW.RAW_CSV_GENERIC (client_id, src_file, src_row_number, payload)
FROM (
    SELECT
        IFF(METADATA$FILENAME ILIKE '%client_a/%', 'CLIENT_A', 'CLIENT_C') AS client_id,
        METADATA$FILENAME::STRING                                           AS src_file,
        METADATA$FILE_ROW_NUMBER::NUMBER                                    AS src_row_number,
        ARRAY_CONSTRUCT(*)                                                  AS payload
    FROM @FIN_INGEST.RAW.STG_FIN_GCS/
)
FILES = ('client_a/csv/transactions.csv','client_c/csv/transactions.csv')
FILE_FORMAT = (FORMAT_NAME = FIN_INGEST.RAW.FF_CSV)
ON_ERROR = 'CONTINUE';

INSERT INTO FIN_INGEST.RAW.RAW_LOAD_AUDIT
SELECT file, 'CSV', status, rows_parsed, rows_loaded, errors_seen, first_error, CURRENT_TIMESTAMP()
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
