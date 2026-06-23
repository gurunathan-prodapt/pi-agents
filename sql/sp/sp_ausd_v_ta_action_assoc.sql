-- BigQuery Stored Procedure replacing vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_action_assoc.ksh
-- This procedure orchestrates data preparation related to the ta_action_assoc entity.
-- It handles parameter parsing, basic error checking, job management, and calls the core SQL logic.
CREATE OR REPLACE PROCEDURE `my_project.my_dataset.sp_ausd_v_ta_action_assoc`(
  p_JobKennung STRING,
  p_EintragsNr STRING
)
BEGIN
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';
  DECLARE v_TabName STRING DEFAULT 'ta_action_assoc';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_procedure_name STRING DEFAULT 'sp_ausd_v_ta_action_assoc';

  -- Parameter validation (based on pruefeParameterGesetzt from h_alis_parameter.ksh)
  IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
    SET ErrNr = 1;
    SET ErrArg = 'Jobkennung';
  END IF;

  IF ErrNr = 0 AND (p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '') THEN
    SET ErrNr = 1;
    SET ErrArg = 'EintragsNr';
  END IF;

  IF ErrNr != 0 THEN
    -- Log error (based on DWMSG_MeldeFehler from f_alis_msgerr.ksh)
    INSERT INTO `my_project.my_dataset.error_log`
      (error_ts, error_code, error_arg, procedure_name, message)
    VALUES
      (CURRENT_TIMESTAMP(), ErrNr, ErrArg, v_procedure_name, 'Bitte ueber Rahmenscript aufrufen');

    RAISE USING MESSAGE = CONCAT('FEHLER: 0 E ', CAST(ErrNr AS STRING), ' ', ErrArg);
  END IF;

  -- Job start logging
  INSERT INTO `my_project.my_dataset.job_log`
    (job_kennung, eintrags_nr, tab_name, status, start_ts)
  VALUES
    (p_JobKennung, p_EintragsNr, v_TabName, 'STARTED', CURRENT_TIMESTAMP());

  -- Deactivate old active jobs (based on script's implied purpose)
  -- This assumes 'active_flag' exists in job_table and tracks active jobs.
  UPDATE `my_project.my_dataset.job_table`
  SET active_flag = FALSE,
      end_ts = CURRENT_TIMESTAMP()
  WHERE job_kennung = p_JobKennung
    AND active_flag = TRUE
    AND eintrags_nr != p_EintragsNr;

  -- Insert/update current job into job_table to mark it as active
  MERGE `my_project.my_dataset.job_table` AS T
  USING (SELECT p_JobKennung AS job_kennung, p_EintragsNr AS eintrags_nr) AS S
  ON T.job_kennung = S.job_kennung AND T.eintrags_nr = S.eintrags_nr
  WHEN MATCHED THEN
    UPDATE SET active_flag = TRUE, start_ts = CURRENT_TIMESTAMP(), end_ts = NULL
  WHEN NOT MATCHED THEN
    INSERT (job_kennung, eintrags_nr, active_flag, start_ts, end_ts)
    VALUES (p_JobKennung, p_EintragsNr, TRUE, CURRENT_TIMESTAMP(), NULL);

  -- Execute migrated SQL logic from d_ausd_v_ta_action_assoc.sql
  -- This is a placeholder call. The actual SQL logic from d_ausd_v_ta_action_assoc.sql
  -- should be migrated into a separate BigQuery Stored Procedure, e.g.,
  -- `sp_d_ausd_v_ta_action_assoc`, and called here.
  -- Alternatively, the SQL logic can be inlined here if it's not too complex.
  -- For this migration, we assume `sp_d_ausd_v_ta_action_assoc` will be created.
  CALL `my_project.my_dataset.sp_d_ausd_v_ta_action_assoc`(p_JobKennung, p_EintragsNr);

  -- Example record count - replace with actual logic to get records processed by sp_d_ausd_v_ta_action_assoc
  -- This count would typically be returned by the called procedure or derived from target tables.
  -- For demonstration, a dummy count is used.
  SET v_records = (SELECT COUNT(*) FROM `my_project.my_dataset.some_target_table_after_sql_execution` WHERE job_kennung = p_JobKennung);
  -- If sp_d_ausd_v_ta_action_assoc returns the count:
  -- CALL `my_project.my_dataset.sp_d_ausd_v_ta_action_assoc`(p_JobKennung, p_EintragsNr, OUT v_records);

  -- Job end logging
  UPDATE `my_project.my_dataset.job_log`
  SET status = 'FINISHED',
      record_count = v_records,
      end_ts = CURRENT_TIMESTAMP()
  WHERE job_kennung = p_JobKennung
    AND eintrags_nr = p_EintragsNr
    AND status = 'STARTED';

  -- Deactivate current job in job_table
  UPDATE `my_project.my_dataset.job_table`
  SET active_flag = FALSE,
      end_ts = CURRENT_TIMESTAMP()
  WHERE job_kennung = p_JobKennung
    AND eintrags_nr = p_EintragsNr
    AND active_flag = TRUE;

  SELECT ' ---------- ENDE Datenverarbeitung ---------- ' AS message;
  SELECT v_records AS processed_records;

EXCEPTION WHEN ERROR THEN
  -- Log error for any unhandled exceptions during execution
  INSERT INTO `my_project.my_dataset.error_log`
    (error_ts, error_code, error_arg, procedure_name, message)
  VALUES
    (CURRENT_TIMESTAMP(), -1, '', v_procedure_name, ERROR_MESSAGE());

  -- Update job_log with FAILED status
  UPDATE `my_project.my_dataset.job_log`
  SET status = 'FAILED',
      end_ts = CURRENT_TIMESTAMP(),
      record_count = 0 -- or NULL, depending on preference for failed jobs
  WHERE job_kennung = p_JobKennung
    AND eintrags_nr = p_EintragsNr
    AND status = 'STARTED';

  -- Deactivate current job in job_table if it failed
  UPDATE `my_project.my_dataset.job_table`
  SET active_flag = FALSE,
      end_ts = CURRENT_TIMESTAMP()
  WHERE job_kennung = p_JobKennung
    AND eintrags_nr = p_EintragsNr
    AND active_flag = TRUE;

  RAISE USING MESSAGE = CONCAT('Stored Procedure Failed: ', ERROR_MESSAGE());
END;