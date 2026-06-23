-- BigQuery Stored Procedure for r_ausd_rechempf.ksh
-- Replaces: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_rechempf.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_rechempf.ksh

CREATE OR REPLACE PROCEDURE project.dataset.sp_initial_befuellung_vertrags_cache_fos(
  p_stichtag STRING, -- Optional: DDMMYYYY. If NULL, defaults to current date.
  p_wiederanlauf_wert INT64 -- Optional: If NULL, defaults to 0.
)
BEGIN
  DECLARE v_job_id STRING;
  DECLARE v_job_kennung STRING DEFAULT 'BERT_P_RECH_EMPF';
  DECLARE v_stichtag_actual STRING;
  DECLARE v_wiederanlauf_wert_actual INT64;
  DECLARE v_message STRING;

  SET v_job_id = GENERATE_UUID();

  -- Parameter defaulting
  SET v_wiederanlauf_wert_actual = IFNULL(p_wiederanlauf_wert, 0);
  SET v_stichtag_actual = IFNULL(p_stichtag, FORMAT_DATE('%d%m%Y', CURRENT_DATE()));

  -- Log job start
  INSERT INTO project.dataset.job_log (job_id, start_time, status, message, parameters, run_date)
  VALUES (v_job_id, CURRENT_TIMESTAMP(), 'RUNNING', 'Job started', TO_JSON(STRUCT(p_stichtag, p_wiederanlauf_wert)) , CURRENT_DATE());

  -- Main logic block with error handling
  BEGIN
    -- Log parameters
    SET v_message = FORMAT('Stichtag: %s, Wiederanlaufwert: %d', v_stichtag_actual, v_wiederanlauf_wert_actual);
    INSERT INTO project.dataset.job_log_messages (job_id, log_time, message_type, message)
    VALUES (v_job_id, CURRENT_TIMESTAMP(), 'INFO', 'Parameters: ' || v_message);

    -- Call the core processing stored procedure
    CALL project.dataset.sp_k_ausd_rechempf(
      v_job_id,
      v_job_kennung,
      v_stichtag_actual,
      v_wiederanlauf_wert_actual
    );

    -- If successful, log completion
    INSERT INTO project.dataset.job_log_messages (job_id, log_time, message_type, message)
    VALUES (v_job_id, CURRENT_TIMESTAMP(), 'INFO', 'The processing completed without detectable errors.');

    UPDATE project.dataset.job_log
    SET end_time = CURRENT_TIMESTAMP(), status = 'OK'
    WHERE job_id = v_job_id;

  EXCEPTION WHEN ERROR THEN
    -- Log error
    INSERT INTO project.dataset.job_error_log (job_id, error_time, error_message, stack_trace, parameters)
    VALUES (v_job_id, CURRENT_TIMESTAMP(), @@error.message, @@error.stack_trace, TO_JSON(STRUCT(p_stichtag, p_wiederanlauf_wert)));

    UPDATE project.dataset.job_log
    SET end_time = CURRENT_TIMESTAMP(), status = 'FAILED', message = @@error.message
    WHERE job_id = v_job_id;

    RAISE; -- Re-raise the error
  END;

END;