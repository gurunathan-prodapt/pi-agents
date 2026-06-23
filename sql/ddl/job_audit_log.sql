-- DDL for project.dataset.job_audit_log
-- Legacy Source: r_ausd_bp_ta_bpr_apn.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_apn.ksh

CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.job_audit_log` (
    job_nr INT64 NOT NULL OPTIONS(description="Unique job run number for a given job_kennung"),
    job_kennung STRING NOT NULL OPTIONS(description="Identifier for the job (e.g., ausd_bp_ta_bpr_apn)"),
    prog_name STRING OPTIONS(description="Program name as defined in the source script"),
    prog_version STRING OPTIONS(description="Program version"),
    log_datei STRING OPTIONS(description="Simulated log file name"),
    stichtag STRING OPTIONS(description="Reference date for the job run (DDMMYYYY)"),
    status STRING NOT NULL OPTIONS(description="Execution status (e.g., STARTED, OK, ERROR)"),
    message STRING OPTIONS(description="Detailed message about the status or error"),
    created_at TIMESTAMP NOT NULL OPTIONS(description="Timestamp of the log entry")
);