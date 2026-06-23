-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_period.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_period.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.job_log` (
    job_name STRING NOT NULL OPTIONS(description="Name of the job"),
    job_entry_nr INT64 NOT NULL OPTIONS(description="Associated job control entry number"),
    log_message STRING NOT NULL OPTIONS(description="Detailed log message"),
    created_ts TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the log entry was created")
);