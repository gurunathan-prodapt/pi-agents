-- DDL for job_log table
-- Legacy Source: Part of the custom DWMSG_ logging framework in r_ausd_v_ta_period.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_period.ksh

CREATE TABLE IF NOT EXISTS `your_gcp_project.your_bq_dataset.job_log` (
    job_name STRING NOT NULL OPTIONS(description="Identifier for the ETL job"),
    job_entry_nr INT64 NOT NULL OPTIONS(description="Unique entry number for a specific job run, similar to DW_EintragsNr"),
    log_level STRING NOT NULL OPTIONS(description="Level of the log entry (e.g., I=Info, S=Success, E=Error, W=Warning)"),
    error_nr INT64 OPTIONS(description="Error number, if applicable (corresponds to ErrNr from ksh)"),
    error_arg STRING OPTIONS(description="Argument associated with the error, if applicable (corresponds to ErrArg from ksh)"),
    message STRING NOT NULL OPTIONS(description="Log message details"),
    log_file_name STRING OPTIONS(description="Simulated log file name, or reference to a BigQuery job ID/log stream"),
    business_date DATE OPTIONS(description="Business date associated with the job run (corresponds to v_sysdate)"),
    created_at TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the log entry was created"),
    updated_at TIMESTAMP OPTIONS(description="Timestamp when the log entry was last updated (e.g., for status changes)")
);