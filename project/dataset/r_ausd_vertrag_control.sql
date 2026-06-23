-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_acc_ref.ksh
-- Description: Main control stored procedure, migrated from k_ausd_v_ta_acc_ref.ksh.
CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_vertrag_control`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING
)
BEGIN
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';
  DECLARE v_TabName STRING DEFAULT 'ta_acc_ref';
  DECLARE v_records INT64 DEFAULT 0;

  -- Parameter validation
  IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
    SET ErrNr = 193;
    SET ErrArg = 'Jobkennung';
  END IF;
  IF ErrNr = 0 AND (p_EintragsNr IS NULL OR p_EintragsNr = '') THEN
    SET ErrNr = 193;
    SET ErrArg = 'EintragsNr';
  END IF;

  -- Error handling for parameter validation
  IF ErrNr != 0 THEN
    INSERT INTO `project.dataset.error_log` (error_ts, error_source, error_nr, error_arg, message_text)
    VALUES (CURRENT_TIMESTAMP(), 'r_ausd_vertrag_control', ErrNr, ErrArg, CONCAT('Parameter validation failed for: ', ErrArg));
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = CONCAT('Parameter validation failed: ', ErrArg);
  END IF;

  -- Deactivate older active jobs for the same job_kennung but different eintrags_nr
  UPDATE `project.dataset.job_table`
  SET active_flag = FALSE, updated_at = CURRENT_TIMESTAMP()
  WHERE job_kennung = p_JobKennung
    AND active_flag = TRUE
    AND eintrags_nr <> p_EintragsNr;

  -- Register current job as active
  INSERT INTO `project.dataset.job_table` (job_kennung, eintrags_nr, tab_name, active_flag, created_at)
  VALUES (p_JobKennung, p_EintragsNr, v_TabName, TRUE, CURRENT_TIMESTAMP());

  -- Execute migrated SQL logic from d_ausd_v_ta_acc_ref.sql
  -- This call populates v_records with the number of processed records.
  CALL `project.dataset.d_ausd_v_ta_acc_ref`(p_EintragsNr, p_JobKennung, v_TabName, v_records);

  -- Log processed record count and job details
  INSERT INTO `project.dataset.job_run_log` (job_kennung, eintrags_nr, tab_name, records_processed, logged_at)
  VALUES (p_JobKennung, p_EintragsNr, v_TabName, v_records, CURRENT_TIMESTAMP());

  -- Deactivate the current job after completion
  UPDATE `project.dataset.job_table`
  SET active_flag = FALSE, updated_at = CURRENT_TIMESTAMP()
  WHERE job_kennung = p_JobKennung
    AND eintrags_nr = p_EintragsNr
    AND tab_name = v_TabName;

EXCEPTION WHEN ERROR THEN
  -- Log error in case the procedure fails
  INSERT INTO `project.dataset.error_log` (error_ts, error_source, error_nr, error_arg, message_text)
  VALUES (CURRENT_TIMESTAMP(), 'r_ausd_vertrag_control', 999, 'Execution Error', ERROR_MESSAGE());
  -- Re-raise the error to signal failure
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = ERROR_MESSAGE();
END;