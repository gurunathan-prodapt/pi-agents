CREATE OR REPLACE PROCEDURE dwh_bert_dataset.r_ausd_bp_ta_msisdn_wrapper(
  p_stichtag_in STRING,
  p_wiederanlaufWert_in STRING
)
BEGIN
  -- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn.ksh
  -- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn.ksh

  DECLARE v_stichtag STRING;
  DECLARE v_wiederanlaufWert STRING;
  DECLARE v_job_kennung STRING DEFAULT 'ausd_bp_ta_msisdn';
  DECLARE v_job_entry_nr STRING DEFAULT GENERATE_UUID(); -- Unique identifier for this job run
  DECLARE v_prog_name STRING DEFAULT 'Bereitstellung Basisprodukte BERT';
  DECLARE v_prog_version STRING DEFAULT 'V2.0.0';

  -- Defaulting p_wiederanlaufWert: if input is NULL or empty, default to '0'.
  SET v_wiederanlaufWert = COALESCE(NULLIF(p_wiederanlaufWert_in, ''), '0');

  -- Determine Stichtag: if input is NULL or empty, default to current system date.
  IF p_stichtag_in IS NULL OR p_stichtag_in = '' THEN
    SET v_stichtag = FORMAT_DATE('%d%m%Y', CURRENT_DATE()); -- Format as DDMMYYYY
  ELSE
    SET v_stichtag = p_stichtag_in;
  END IF;

  -- Log Job Start
  INSERT INTO dwh_bert_dataset.job_log (
    job_name, job_entry_nr, log_level, log_message, created_at, status, job_version, stichtag, wiederanlaufwert
  )
  VALUES (
    v_job_kennung, v_job_entry_nr, 'INFO', CONCAT('Job started: ', v_prog_name, ' ', v_prog_version), CURRENT_TIMESTAMP(), 'STARTED', v_prog_version, SAFE.PARSE_DATE('%d%m%Y', v_stichtag), v_wiederanlaufWert
  );

  BEGIN
    -- Validate Stichtag format (DDMMYYYY) after initial logging but before calling controller
    IF SAFE.PARSE_DATE('%d%m%Y', v_stichtag) IS NULL THEN
      -- Log validation failure
      INSERT INTO dwh_bert_dataset.job_log (job_name, job_entry_nr, log_level, log_message, created_at, status, error_code)
      VALUES (v_job_kennung, v_job_entry_nr, 'ERROR', CONCAT('Invalid Stichtag format provided or derived: ', v_stichtag, '. Expected DDMMYYYY.'), CURRENT_TIMESTAMP(), 'FAILED', 'VALIDATION_ERROR');
      SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = CONCAT('Invalid Stichtag format: ', v_stichtag, '. Expected DDMMYYYY.');
    END IF;

    -- Call the controller procedure
    CALL dwh_bert_dataset.k_ausd_bp_ta_msisdn_controller(
      v_job_kennung,
      v_stichtag,
      v_job_entry_nr,
      v_wiederanlaufWert
    );

    -- Log Job Success
    INSERT INTO dwh_bert_dataset.job_log (
      job_name, job_entry_nr, log_level, log_message, created_at, status, finished_at
    )
    VALUES (
      v_job_kennung, v_job_entry_nr, 'INFO', 'Job completed successfully.', CURRENT_TIMESTAMP(), 'COMPLETED', CURRENT_TIMESTAMP()
    );

  EXCEPTION WHEN ERROR THEN
    -- Log Job Failure
    INSERT INTO dwh_bert_dataset.job_log (
      job_name, job_entry_nr, log_level, log_message, created_at, status, error_code, error_argument, finished_at
    )
    VALUES (
      v_job_kennung, v_job_entry_nr, 'ERROR', CONCAT('Job failed with error: ', @@error.message), CURRENT_TIMESTAMP(), 'FAILED', @@error.code, @@error.message, CURRENT_TIMESTAMP()
    );
    RAISE; -- Re-raise the error for external orchestration systems to catch
  END;

END;