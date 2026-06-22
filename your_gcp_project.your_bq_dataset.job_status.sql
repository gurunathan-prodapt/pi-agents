-- DDL for job_status table
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_einzeln.ksh
-- This table tracks the latest status of the job.

CREATE TABLE IF NOT EXISTS `your_gcp_project.your_bq_dataset.job_status` (
    job_name STRING NOT NULL PRIMARY KEY OPTIONS(description="Name of the job"),
    last_run_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp of the last update to this status"),
    status STRING NOT NULL OPTIONS(description="Current status of the job (RUNNING, SUCCEEDED, FAILED)"),
    last_error_message STRING OPTIONS(description="Last error message if the job failed"),
    last_stichtag STRING OPTIONS(description="Stichtag (snapshot date) parameter of the last run"),
    last_wiederanlaufwert STRING OPTIONS(description="Wiederanlaufwert (restart value) parameter of the last run")
)
OPTIONS(
    description="Table to store the current status of jobs."
);