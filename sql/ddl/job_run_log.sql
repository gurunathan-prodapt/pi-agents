--
-- BigQuery DDL for the job_run_log table
-- Replaces general logging and record count capture aspects of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_c_bfc.ksh
--

CREATE TABLE IF NOT EXISTS `your_project.your_dataset.job_run_log` (
    run_id STRING NOT NULL OPTIONS(description="Unique identifier for the job run"),
    job_kenn_ung STRING NOT NULL OPTIONS(description="Job identifier from the legacy system (p_JobKennung)"),
    eintrags_nr STRING NOT NULL OPTIONS(description="Entry number from the legacy system (p_EintragsNr)"),
    event_type STRING NOT NULL OPTIONS(description="Type of event (e.g., 'START', 'END', 'RECORDS_PROCESSED')"),
    event_message STRING OPTIONS(description="Descriptive message for the event"),
    record_count INT64 OPTIONS(description="Number of records processed, if applicable"),
    event_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the event occurred"),
    source_script STRING OPTIONS(description="Name of the BigQuery stored procedure or script that generated the log")
)
OPTIONS(
    description="Logs significant events and metrics during job execution"
);