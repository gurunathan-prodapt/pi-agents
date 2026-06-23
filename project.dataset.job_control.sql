-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_period.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_period.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.job_control` (
    job_entry_nr INT64 NOT NULL OPTIONS(description="Unique identifier for each job execution"),
    job_name STRING NOT NULL OPTIONS(description="Name of the job (e.g., r_ausd_v_ta_period)"),
    script_name STRING OPTIONS(description="Name of the executing script/procedure"),
    log_file_name STRING OPTIONS(description="Logical name for the log file (for compatibility, actual logs are in job_log table)"),
    stichtag DATE OPTIONS(description="Key date for the job run"),
    stichtag_format STRING OPTIONS(description="Format of the stichtag (e.g., YYYYMMDD)"),
    status STRING NOT NULL OPTIONS(description="Current status of the job (STARTED, OK, ERROR)"),
    created_ts TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the job entry was created"),
    finished_ts TIMESTAMP OPTIONS(description="Timestamp when the job finished (successfully or with error)")
);