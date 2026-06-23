-- Legacy Source: N/A (New logging table)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_upgrade.ksh
--
-- DDL for the job registration and status tracking table in BigQuery.
-- This replaces the shell script's implicit job tracking.

CREATE TABLE IF NOT EXISTS `project.dataset.job_table` (
    job_id STRING OPTIONS(description="Unique identifier for each job run"),
    job_kennung STRING OPTIONS(description="Job identifier from input parameters"),
    eintrags_nr STRING OPTIONS(description="Entry number from input parameters"),
    tab_name STRING OPTIONS(description="Target table name (e.g., ta_vvl_upgrade)"),
    status STRING OPTIONS(description="Current status of the job (e.g., 'RUNNING', 'COMPLETED', 'FAILED')"),
    created_ts TIMESTAMP OPTIONS(description="Timestamp when the job record was created"),
    updated_ts TIMESTAMP OPTIONS(description="Timestamp when the job record was last updated")
);