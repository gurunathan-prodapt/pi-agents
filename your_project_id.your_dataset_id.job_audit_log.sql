-- BigQuery DDL for job_audit_log table
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_c_bfc.ksh
-- This table replaces shell-based logging mechanisms.

CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.job_audit_log` (
    entry_nr INT64 NOT NULL,
    job_kennung STRING NOT NULL,
    script_name STRING NOT NULL,
    log_file STRING, -- Not directly used in BQ, but kept for historical context/mapping
    stichtag DATE,
    status STRING NOT NULL,
    created_ts TIMESTAMP NOT NULL,
    end_ts TIMESTAMP,
    message STRING
)
OPTIONS(
    description="Audit log for job executions, replacing shell script logging."
);