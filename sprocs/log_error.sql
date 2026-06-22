-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs3.ksh
-- Helper procedure for logging errors into the `error_log` table.
-- Please replace `your_project_id` and `your_dataset_id` with your actual BigQuery project and dataset.
CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.log_error`(
  p_procedure_name STRING,
  p_err_nr INT64,
  p_err_arg STRING,
  p_message STRING
)
BEGIN
  INSERT INTO `your_project_id.your_dataset_id.error_log` (log_ts, procedure_name, err_nr, err_arg, message)
  VALUES (CURRENT_TIMESTAMP(), p_procedure_name, p_err_nr, p_err_arg, p_message);
END;