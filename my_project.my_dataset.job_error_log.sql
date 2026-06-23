-- DDL for job_error_log table
-- Replaces error reporting functionality from legacy job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_da_vda_tk.ksh

CREATE TABLE IF NOT EXISTS my_project.my_dataset.job_error_log (
    job_name STRING NOT NULL OPTIONS(description="Name of the job that failed"),
    entry_nr STRING OPTIONS(description="Entry number associated with the job run"),
    stichtag STRING OPTIONS(description="Processing date (DDMMYYYY) during the failure"),
    error_message STRING NOT NULL OPTIONS(description="Detailed error message"),
    created_at TIMESTAMP OPTIONS(description="Timestamp of the error entry")
);