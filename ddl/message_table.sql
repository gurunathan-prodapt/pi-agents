-- BigQuery DDL for message_table
-- Replaces Oracle table used by vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
-- This table stores job status, error details, and additional information.

CREATE TABLE IF NOT EXISTS dw_is_error_management.message_table (
    eintragsnr INT64 NOT NULL OPTIONS(description="Unique entry number for a job execution"),
    job_kennung STRING OPTIONS(description="Identifier for the job"),
    programmname STRING OPTIONS(description="Name of the program/script executed"),
    log_datei STRING OPTIONS(description="Path to the log file for this execution"),
    status STRING NOT NULL OPTIONS(description="Current status of the job ('LAEUFT', 'OK', 'ABBRUCH')"),
    created_at TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the entry was created"),
    updated_at TIMESTAMP OPTIONS(description="Timestamp of the last update to the entry"),
    last_error_type STRING OPTIONS(description="Type of the last error reported"),
    last_error_nr INT64 OPTIONS(description="Number of the last error reported"),
    last_error_zusatz1 STRING OPTIONS(description="Additional info 1 for the last error"),
    last_error_zusatz2 STRING OPTIONS(description="Additional info 2 for the last error"),
    zusatzinfos_date DATE OPTIONS(description="Date-specific additional information (Stichtag)"),
    zusatzinfos_text STRING OPTIONS(description="General timing or other additional text information")
)
PARTITION BY RANGE_BUCKET(eintragsnr, GENERATE_ARRAY(0, 1000000000, 1000000)) -- Example partitioning by eintragsnr
CLUSTER BY job_kennung, status
OPTIONS(
    description="Table to store job execution messages, status, and error information."
);

-- Note: BigQuery does not enforce primary keys as constraints, but 'eintragsnr' is
-- intended to be a unique identifier. Uniqueness must be managed by the application logic.