-- BigQuery Stored Procedure for r_ausd_v_ta_vertrag_tmp
-- Main orchestrator, replacing the original KornShell wrapper vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vertrag_tmp.ksh
CREATE OR REPLACE PROCEDURE `my_project.my_target_dataset.r_ausd_v_ta_vertrag_tmp`(
  IN p_execution_date STRING -- Expected format 'YYYYMMDD', corresponds to v_datum
)
BEGIN
  DECLARE job_name STRING DEFAULT 'r_ausd_v_ta_vertrag_tmp';
  DECLARE start_ts TIMESTAMP;
  DECLARE stichtag_date DATE;
  DECLARE error_msg STRING;
  DECLARE exit_code INT64 DEFAULT 0;

  SET start_ts = CURRENT_TIMESTAMP();

  BEGIN TRANSACTION;

  -- 1. Log job start
  CALL `my_project.my_utils_dataset.DWMSG_ErzeugeEintrag`(
    job_name,
    'STARTED',
    'Job started for execution date: ' || p_execution_date,
    start_ts,
    NULL,
    NULL
  );

  -- 2. Set the key date (Stichtag) - for validation/use if needed
  -- Although not directly used for assignment in this script, it demonstrates the utility call.
  CALL `my_project.my_utils_dataset.DWMSG_SetzeStichtagInfo`(p_execution_date, stichtag_date);

  -- 3. Execute the core transformation logic
  CALL `my_project.my_target_dataset.k_ausd_v_ta_vertrag_tmp`(p_execution_date);

  -- 4. Log successful completion
  CALL `my_project.my_utils_dataset.DWMSG_SetzeStatusOK`(job_name, start_ts);
  CALL `my_project.my_utils_dataset.DWPA_UTIL_SKRIPT_runstatement`(0, 'Processing finished successfully.');

  COMMIT TRANSACTION;

EXCEPTION WHEN ERROR THEN
  -- 5. Log error and roll back
  SET error_msg = @@error.message;
  ROLLBACK TRANSACTION;
  CALL `my_project.my_utils_dataset.DWMSG_Fehlerbehandlung`(job_name, 'Error during execution of k_ausd_v_ta_vertrag_tmp for date: ' || p_execution_date, error_msg);
  CALL `my_project.my_utils_dataset.DWPA_UTIL_SKRIPT_runstatement`(1, 'Processing failed due to an error.'); -- Log final failure
END;