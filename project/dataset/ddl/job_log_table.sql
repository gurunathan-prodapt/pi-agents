-- Optional: DDL for a job logging table, if `FOSJobErzeugeEintrag` functionality is activated.
-- This table is not directly part of the original script's output but provides a BigQuery equivalent for logging.
-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_cntrct_dist.ksh

CREATE OR REPLACE TABLE `project.dataset.job_log_table`
(
    job_kennung STRING NOT NULL OPTIONS(description="Identifier for the job"),
    entry_nr STRING OPTIONS(description="Entry number or instance identifier for the job run"),
    stichtag_date DATE OPTIONS(description="Business date for which the job was run"),
    status STRING OPTIONS(description="Status of the job run (e.g., 'RUNNING', 'SUCCESS', 'FAILED')"),
    message STRING OPTIONS(description="Descriptive message about the job status or event"),
    processed_records INT64 OPTIONS(description="Number of records processed or affected by the job"),
    created_at TIMESTAMP OPTIONS(description="Timestamp when the log entry was created")
)
-- Partitioning by date and clustering by job_kennung can be beneficial for large tables
-- PARTITION BY DATE(created_at)
-- CLUSTER BY job_kennung
;