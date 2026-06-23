-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_upgrade.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_upgrade.ksh

CREATE TABLE IF NOT EXISTS `PROJECT_ID.DATASET_ID.job_control` (
    job_name STRING NOT NULL,
    job_entry_no INT64 NOT NULL, -- The job_entry_no of the last successful or attempted run for this job
    job_status STRING NOT NULL, -- e.g., 'RUNNING', 'OK', 'FAILED'
    stichtag STRING,
    stichtag_format STRING,
    updated_ts TIMESTAMP NOT NULL,
    status_ts TIMESTAMP NOT NULL -- Timestamp when the status was last updated
)
OPTIONS(
    description="Control table for job statuses and parameters. Stores the latest state of each unique job."
);