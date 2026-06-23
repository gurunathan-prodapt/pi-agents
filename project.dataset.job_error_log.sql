-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_period.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_period.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.job_error_log` (
    job_name STRING NOT NULL OPTIONS(description="Name of the job"),
    job_entry_nr INT64 NOT NULL OPTIONS(description="Associated job control entry number"),
    error_nr INT64 OPTIONS(description="Numeric error code"),
    error_arg STRING OPTIONS(description="Argument associated with the error"),
    error_message STRING NOT NULL OPTIONS(description="Detailed error message"),
    created_ts TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the error entry was created")
);