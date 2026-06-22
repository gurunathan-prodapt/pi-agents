--
-- DDL for BigQuery table: your_gcp_project.your_bq_dataset.error_log
-- Replaces functionality of f_alis_msgerr.ksh and error handling in vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_bp_ref.ksh
--
CREATE TABLE your_gcp_project.your_bq_dataset.error_log (
    error_ts DATETIME NOT NULL OPTIONS(description="Timestamp of the error"),
    error_nr INT64 OPTIONS(description="Error number or code"),
    error_arg STRING OPTIONS(description="Error message or argument"),
    procedure_name STRING NOT NULL OPTIONS(description="Name of the procedure where the error occurred")
);