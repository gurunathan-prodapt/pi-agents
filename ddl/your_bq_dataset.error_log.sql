-- BigQuery DDL for error_log table
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_vertrag.ksh
-- This table is used for logging errors encountered during job execution.

CREATE TABLE IF NOT EXISTS `your_gcp_project.your_bq_dataset.error_log`
(
  timestamp      TIMESTAMP,
  job_name       STRING,
  error_code     INT664,
  error_argument STRING,
  job_kennung    STRING,
  eintrags_nr    STRING,
  stichtag       DATE,
  error_message  STRING
);