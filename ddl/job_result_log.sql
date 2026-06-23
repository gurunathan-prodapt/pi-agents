-- Legacy Source: Temporary file ($DW_DIR_UTL/bert_k_ausd_v_ta_vvl_upgrade_$$.tmp)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_upgrade.ksh
--
-- DDL for the job result logging table in BigQuery.
-- This replaces the temporary file mechanism for capturing processed records.

CREATE TABLE IF NOT EXISTS `project.dataset.job_result_log` (
    job_kennung STRING OPTIONS(description="Job identifier from input parameters"),
    eintrags_nr STRING OPTIONS(description="Entry number from input parameters"),
    tab_name STRING OPTIONS(description="Target table name"),
    records_processed INT64 OPTIONS(description="Number of records processed by the SQL script"),
    finished_ts TIMESTAMP OPTIONS(description="Timestamp when the job result was logged")
);