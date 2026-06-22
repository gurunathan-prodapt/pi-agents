-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_da_vda_tk.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_da_vda_tk.ksh
-- Description: DDL for the job status table used by the migrated BigQuery stored procedure, replacing FOSJobErzeugeEintrag functionality.

CREATE TABLE IF NOT EXISTS `project.dataset.job_table` (
  log_timestamp TIMESTAMP,
  table_name STRING,
  job_status_code_1 STRING, -- e.g., 'A'
  job_status_code_2 STRING, -- e.g., 'I'
  business_date_start DATE,
  business_date_end DATE,
  process_flag_1 STRING, -- e.g., 'J'
  process_flag_2 STRING, -- e.g., 'N'
  records_processed INT64,
  description STRING
);