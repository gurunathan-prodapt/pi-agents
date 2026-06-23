-- Legacy Source: f_alis_msgerr.ksh (Custom error handling)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_upgrade.ksh
--
-- DDL for the error logging table in BigQuery.
-- This replaces the custom shell error framework and logging.

CREATE TABLE IF NOT EXISTS `project.dataset.job_error_log` (
    error_ts TIMESTAMP OPTIONS(description="Timestamp of the error"),
    procedure_name STRING OPTIONS(description="Name of the stored procedure where the error occurred"),
    error_code STRING OPTIONS(description="SQLSTATE or custom error code"),
    error_message STRING OPTIONS(description="Detailed error message"),
    job_kennung STRING OPTIONS(description="Job identifier from input parameters"),
    eintrags_nr STRING OPTIONS(description="Entry number from input parameters")
);