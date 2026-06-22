-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_da_vda_tk.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_da_vda_tk.ksh
-- Description: DDL for the process log table used by the migrated BigQuery stored procedure.

CREATE TABLE IF NOT EXISTS `project.dataset.process_log` (
  log_timestamp TIMESTAMP,
  table_name STRING,
  job_kennung STRING,
  entry_number STRING,
  business_date_param STRING, -- Original input string for stichtag
  message STRING
);