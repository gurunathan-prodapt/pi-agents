-- DDL for job_run_log table
-- Replaces logging functionality from legacy job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_da_vda_tk.ksh

CREATE TABLE IF NOT EXISTS my_project.my_dataset.job_run_log (
    tab_name STRING NOT NULL OPTIONS(description="Name of the job or table processed"),
    job_kennung STRING OPTIONS(description="Job identifier from input parameters"),
    eintrags_nr STRING OPTIONS(description="Entry number from input parameters"),
    stichtag STRING OPTIONS(description="Processing date (DDMMYYYY) from input parameters"),
    wiederanlauf_wert STRING OPTIONS(description="Restart value from input parameters"),
    records_processed INT64 OPTIONS(description="Number of records processed or inserted"),
    created_at TIMESTAMP OPTIONS(description="Timestamp of the log entry")
);