--
-- BigQuery Stored Procedure for orchestration and control logic
-- Replaces: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_disc_zusgf.ksh
--
CREATE OR REPLACE PROCEDURE `my_project.my_dataset.r_ausd_vertrag_control`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING,
  OUT p_records_processed INT64
)
BEGIN
  DECLARE v_TabName STRING DEFAULT 'ta_disc_zusgf';
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';
  DECLARE error_message STRING;
  DECLARE records_count INT64;

  -- Parameter validation
  IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
    SET ErrNr = 193;
    SET ErrArg = 'Jobkennung';
  END IF;

  IF ErrNr = 0 AND (p_EintragsNr IS NULL OR p_EintragsNr = '') THEN
    SET ErrNr = 193;
    SET ErrArg = 'EintragsNr';
  END IF;

  IF ErrNr != 0 THEN
    -- Log error to a BigQuery table
    INSERT INTO `my_project.my_dataset.job_error_log` (job_kennung, eintrags_nr, err_nr, err_arg, created_ts)
    VALUES (p_JobKennung, p_EintragsNr, ErrNr, ErrArg, CURRENT_TIMESTAMP());
    SET error_message = FORMAT('FEHLER: 0 E %d %s', ErrNr, ErrArg);
    RAISE SCRIPT_EXCEPTION(error_message);
  END IF;

  -- Error handling block for the core SQL logic call
  BEGIN
    -- Call the migrated SQL logic stored procedure
    CALL `my_project.my_dataset.d_ausd_v_ta_disc_zusgf_sp`(p_EintragsNr, p_JobKennung, v_TabName, records_count);
    SET p_records_processed = records_count;

    -- Log successful completion
    INSERT INTO `my_project.my_dataset.job_run_log` (job_kennung, eintrags_nr, records_processed, created_ts)
    VALUES (p_JobKennung, p_EintragsNr, p_records_processed, CURRENT_TIMESTAMP());

  EXCEPTION WHEN ERROR THEN
    -- Capture error details
    SET error_message = @@error.message;
    SET ErrNr = 999; -- Generic error code for SQL exceptions
    SET ErrArg = 'SQL_EXCEPTION';

    -- Log error to a BigQuery table
    INSERT INTO `my_project.my_dataset.job_error_log` (job_kennung, eintrags_nr, err_nr, err_arg, created_ts)
    VALUES (p_JobKennung, p_EintragsNr, ErrNr, error_message, CURRENT_TIMESTAMP());
    RAISE; -- Re-raise the exception to propagate it up
  END;

END;