--
-- Target BigQuery DDL for table project.dataset.job_log
-- Replaces custom shell/SQL*Plus logging mechanisms.
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_assign.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_assign.ksh
--
CREATE TABLE IF NOT EXISTS project.dataset.job_log (
    log_id STRING NOT NULL OPTIONS(description="Unique identifier for each log entry"),
    job_name STRING NOT NULL OPTIONS(description="Name of the BigQuery job or stored procedure"),
    job_kennung STRING OPTIONS(description="Job identifier passed as parameter (from legacy system)"),
    eintrags_nr INT64 OPTIONS(description="Entry number passed as parameter (from legacy system)"),
    start_time TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the job execution started"),
    end_time TIMESTAMP OPTIONS(description="Timestamp when the job execution ended"),
    status STRING NOT NULL OPTIONS(description="Status of the job execution (e.g., 'RUNNING', 'SUCCESS', 'FAILED')"),
    record_count INT64 OPTIONS(description="Number of records processed/inserted by the job"),
    error_message STRING OPTIONS(description="Detailed error message if the job failed"),
    log_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the log entry was created/updated")
)
PARTITION BY
    DATE(start_time)
CLUSTER BY
    job_name, status;