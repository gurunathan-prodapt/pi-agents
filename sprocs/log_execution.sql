-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs3.ksh
-- Helper procedure for logging execution details into the `execution_log` table.
-- Please replace `your_project_id` and `your_dataset_id` with your actual BigQuery project and dataset.
CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.log_execution`(
  p_procedure_name STRING,
  p_job_name STRING,
  p_entry_nr STRING,
  p_tab_name STRING,
  p_records_processed INT64,
  p_status STRING -- e.g., 'SUCCESS', 'FAILED', 'SKIPPED_ALREADY_ACTIVE'
)
BEGIN
  INSERT INTO `your_project_id.your_dataset_id.execution_log` (log_ts, procedure_name, job_name, entry_nr, tab_name, records_processed, status)
  VALUES (CURRENT_TIMESTAMP(), p_procedure_name, p_job_name, p_entry_nr, p_tab_name, p_records_processed, p_status);
END;