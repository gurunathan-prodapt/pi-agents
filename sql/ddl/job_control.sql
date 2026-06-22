-- DDL for job_control table
-- Legacy Source: Part of the custom DWMSG_ logging framework in r_ausd_v_ta_period.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_period.ksh

CREATE TABLE IF NOT EXISTS `your_gcp_project.your_bq_dataset.job_control` (
    job_name STRING NOT NULL OPTIONS(description="Identifier for the ETL job"),
    job_entry_nr INT64 NOT NULL OPTIONS(description="Unique entry number for a specific job run"),
    stichtag DATE NOT NULL OPTIONS(description="The 'business date' or key date for the job run"),
    stichtag_format STRING OPTIONS(description="Format of the stichtag, if relevant (e.g., 'YYYYMMDD')"),
    created_at TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the control entry was created")
);