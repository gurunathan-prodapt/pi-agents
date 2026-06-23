-- BigQuery DDL for the error_log table
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_instance.ksh
-- This table replaces error messaging from f_alis_msgerr.ksh and shell script's error handling.
--
-- Please replace `project.dataset` with your actual GCP Project ID and BigQuery Dataset ID.

CREATE TABLE IF NOT EXISTS `project.dataset.error_log` (
    process_name STRING,
    error_nr INT64,
    error_arg STRING,
    created_at TIMESTAMP
);