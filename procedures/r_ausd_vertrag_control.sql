--
-- BigQuery Stored Procedure for job control flow.
-- Replaces: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_discount.ksh
--
CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_vertrag_control`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING
)
BEGIN
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';
  DECLARE v_TabName STRING DEFAULT 'ta_p_discount';
  DECLARE v_records INT64 DEFAULT 0;

  -- Parameter validation (replaces getopts and pruefeParameterGesetzt)
  IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
    SET ErrNr = 193;
    SET ErrArg = 'Jobkennung';
  END IF;

  IF ErrNr = 0 AND (p_EintragsNr IS NULL OR p_EintragsNr = '') THEN
    SET ErrNr = 193;
    SET ErrArg = 'EintragsNr';
  END IF;

  -- Error handling (replaces DWMSG_MeldeFehler and shell exit)
  IF ErrNr != 0 THEN
    INSERT INTO `project.dataset.error_log`
      (error_number, error_argument, job_kennung, eintrags_nr, created_at, error_message)
    VALUES
      (ErrNr, ErrArg, p_JobKennung, p_EintragsNr, CURRENT_TIMESTAMP(),
       FORMAT('FEHLER: 0 E %d %s. Bitte ueber Rahmenscript aufrufen', ErrNr, ErrArg));

    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = FORMAT('FEHLER: 0 E %d %s. Bitte ueber Rahmenscript aufrufen', ErrNr, ErrArg);
  END IF;

  -- Log job start
  INSERT INTO `project.dataset.job_log`
    (job_kennung, eintrags_nr, tab_name, records_processed, status, created_at)
  VALUES
    (p_JobKennung, p_EintragsNr, v_TabName, NULL, 'STARTED', CURRENT_TIMESTAMP());

  -- Main processing block
  BEGIN
    -- Call the BigQuery Stored Procedure equivalent of d_ausd_v_ta_p_discount.sql
    -- This procedure would contain the actual data transformation logic.
    CALL `project.dataset.d_ausd_v_ta_p_discount`(
      p_EintragsNr,
      p_JobKennung
    );

    -- Record count replacement for temporary file (cat $tmpFile)
    -- Assuming eintrags_nr is a relevant filter for this job's processed records
    -- This count should reflect the records affected by the d_ausd_v_ta_p_discount procedure.
    -- Adjust the WHERE clause as needed based on the actual logic of d_ausd_v_ta_p_discount.
    SET v_records = (
      SELECT COUNT(*) FROM `project.dataset.ta_p_discount` WHERE eintrags_nr = p_EintragsNr
    );

    -- Log job completion (replaces implicit job table updates)
    UPDATE `project.dataset.job_log`
    SET
        records_processed = v_records,
        status = 'DONE'
    WHERE
        job_kennung = p_JobKennung AND eintrags_nr = p_EintragsNr AND status = 'STARTED';

  EXCEPTION WHEN ERROR THEN
    -- Generic error handling for SQL execution failures
    INSERT INTO `project.dataset.error_log`
      (error_number, error_argument, job_kennung, eintrags_nr, created_at, error_message)
    VALUES
      (999, 'SQL_EXECUTION_FAILURE', p_JobKennung, p_EintragsNr, CURRENT_TIMESTAMP(), @@error.message);

    -- Update job log to FAILED
    UPDATE `project.dataset.job_log`
    SET
        status = 'FAILED',
        records_processed = v_records -- Or NULL, depending on desired behavior for failed jobs
    WHERE
        job_kennung = p_JobKennung AND eintrags_nr = p_EintragsNr AND status = 'STARTED';

    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = FORMAT('SQL execution failed within r_ausd_vertrag_control: %s', @@error.message);
  END;
END;