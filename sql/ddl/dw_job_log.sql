-- Target: BigQuery
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount.ksh
-- Description: DDL for BigQuery logging table to track job execution status and metadata.

CREATE TABLE IF NOT EXISTS project.dataset.dw_job_log (
    job_kennung STRING NOT NULL OPTIONS(description="Identifier for the specific job type (e.g., r_ausd_v_ta_p_discount)"),
    dw_eintrags_nr INT64 NOT NULL OPTIONS(description="Unique entry number for each run of a job_kennung"),
    prog_name STRING OPTIONS(description="Name of the program/script that initiated the job"),
    prog_version STRING OPTIONS(description="Version of the program/script"),
    log_file_path STRING OPTIONS(description="Logical path or identifier for associated logs (e.g., entry number)"),
    status STRING NOT NULL OPTIONS(description="Current status of the job run (e.g., 'RUNNING', 'OK', 'FAILED')"),
    start_timestamp TIMESTAMP OPTIONS(description="Timestamp when the job run started"),
    end_timestamp TIMESTAMP OPTIONS(description="Timestamp when the job run ended"),
    message STRING OPTIONS(description="General message or last significant event of the job run")
)
OPTIONS(
    description="Logs for tracking job execution status and metadata."
);