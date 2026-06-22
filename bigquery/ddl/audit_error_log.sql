-- DDL for error_log table
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis_his.ksh

CREATE OR REPLACE TABLE `project.audit.error_log`
(
    log_timestamp TIMESTAMP,
    job_name STRING,
    job_id STRING,
    error_code INT64,
    error_message STRING,
    error_detail STRING,
    parameters JSON,
    logged_by STRING
);