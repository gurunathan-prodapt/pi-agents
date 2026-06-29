-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh
-- Purpose: Table definition to capture parameter, date, and validation failures.

CREATE TABLE IF NOT EXISTS `project.dataset.job_error_log` (
  tab_name STRING OPTIONS(description="Name of the target table being processed"),
  error_code INT64 OPTIONS(description="Alis-compatible error identification number"),
  error_arg STRING OPTIONS(description="Descriptive message detailing the validation error or missing argument"),
  created_at TIMESTAMP OPTIONS(description="Timestamp when the error log was recorded")
);