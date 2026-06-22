--
-- DDL for BigQuery table: your_gcp_project.your_bq_dataset.job_table
-- Replaces implicit job tracking in vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_bp_ref.ksh
--
CREATE TABLE your_gcp_project.your_bq_dataset.job_table (
    job_kennung STRING NOT NULL OPTIONS(description="Unique identifier for the job"),
    eintrags_nr STRING NOT NULL OPTIONS(description="Entry number for the job instance"),
    active_flag BOOL NOT NULL OPTIONS(description="Indicates if the job is currently active"),
    start_time DATETIME OPTIONS(description="Timestamp when the job started"),
    end_time DATETIME OPTIONS(description="Timestamp when the job ended"),
    status STRING OPTIONS(description="Current status of the job (e.g., 'RUNNING', 'COMPLETED', 'FAILED')"),
    message STRING OPTIONS(description="Additional status message or error details")
);