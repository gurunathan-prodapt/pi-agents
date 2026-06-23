-- BigQuery DDL for job_status table
-- Replaces aspects of DWMSG_SetzeStatusOK in vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_aurd_rechstan.ksh
CREATE TABLE IF NOT EXISTS `project.dataset.job_status` (
    job_id STRING NOT NULL PRIMARY KEY OPTIONS(description="Unique identifier for the job"),
    last_run_timestamp TIMESTAMP OPTIONS(description="Timestamp of the last job run"),
    overall_status STRING OPTIONS(description="Overall status of the job (e.g., OK, FAILED, RUNNING)"),
    last_stichtag DATE OPTIONS(description="Cutoff date used in the last successful run"),
    last_wiederanlauf_wert INT64 OPTIONS(description="Restart value used in the last run")
)
OPTIONS(
    description="Control table to track the overall status of a job."
);