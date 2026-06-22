-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs.ksh

CREATE OR REPLACE PROCEDURE `project.dataset.sp_vertragsdatenabgleich`(
  IN p_s STRING, -- Corresponds to -s (Stichtag)
  IN p_l STRING  -- Corresponds to -l (Laufnummer)
)
BEGIN
  DECLARE v_job_id STRING DEFAULT GENERATE_UUID();
  DECLARE v_job_name STRING DEFAULT 'r_ausd_v_ta_cntrct_crs';
  DECLARE v_script_name STRING DEFAULT 'sp_vertragsdatenabgleich';
  DECLARE v_status STRING DEFAULT 'RUNNING';
  DECLARE v_message STRING;
  DECLARE v_start_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  DECLARE v_end_ts TIMESTAMP;
  DECLARE v_run_date DATE;
  DECLARE v_error_code INT64;
  DECLARE v_error_detail STRING;

  -- Attempt to parse p_s as a DATE. If invalid, this will be caught later.
  SET v_run_date = SAFE.PARSE_DATE('%Y-%m-%d', p_s);

  -- Parameter validation equivalent to getopts handling for missing arguments
  IF p_s IS NULL OR p_l IS NULL THEN
    SET v_status = 'FAILED';
    SET v_error_code = 193;
    SET v_message = 'ERROR: Missing required argument (-s or -l).';

    -- Insert error into job_audit_log
    INSERT INTO `project.dataset.job_audit_log` (job_id, job_name, script_name, status, message, start_ts, end_ts, run_date, error_code, error_detail)
    VALUES (v_job_id, v_job_name, v_script_name, v_status, v_message, v_start_ts, CURRENT_TIMESTAMP(), v_run_date, v_error_code, v_message);

    RAISE USING MESSAGE = v_message;
  END IF;

  -- Validate if p_s could be parsed as a valid date
  IF v_run_date IS NULL THEN
    SET v_status = 'FAILED';
    SET v_error_code = 192; -- Using 192 for 'unknown parameter' equivalent to invalid format
    SET v_message = 'ERROR: Invalid date format for -s parameter. Expected YYYY-MM-DD.';

    -- Insert error into job_audit_log
    INSERT INTO `project.dataset.job_audit_log` (job_id, job_name, script_name, status, message, start_ts, end_ts, run_date, error_code, error_detail)
    VALUES (v_job_id, v_job_name, v_script_name, v_status, v_message, v_start_ts, CURRENT_TIMESTAMP(), NULL, v_error_code, v_message);

    RAISE USING MESSAGE = v_message;
  END IF;


  -- Initial job start log
  INSERT INTO `project.dataset.job_audit_log` (job_id, job_name, script_name, status, message, start_ts, run_date)
  VALUES (v_job_id, v_job_name, v_script_name, v_status, 'Job started.', v_start_ts, v_run_date);

  BEGIN
    -- Call the core reconciliation stored procedure
    -- This replaces the shell script's call to k_ausd_v_ta_cntrct_crs.ksh
    CALL `project.dataset.sp_ausd_v_ta_cntrct_crs`(v_job_id, p_s, p_l);

    -- If core procedure completes successfully
    SET v_status = 'SUCCESS';
    SET v_message = 'Die Abarbeitung wurde ohne erkennbare Fehler beendet.';
    SET v_end_ts = CURRENT_TIMESTAMP();

    UPDATE `project.dataset.job_audit_log`
    SET
      status = v_status,
      message = v_message,
      end_ts = v_end_ts
    WHERE job_id = v_job_id;

  EXCEPTION WHEN ERROR THEN
    SET v_status = 'FAILED';
    SET v_error_detail = @@error.message;
    SET v_message = 'AppError: Abbruch. Core procedure failed.';
    SET v_end_ts = CURRENT_TIMESTAMP();

    UPDATE `project.dataset.job_audit_log`
    SET
      status = v_status,
      message = v_message,
      end_ts = v_end_ts,
      error_code = 1, -- Generic error code for unhandled exceptions in core proc
      error_detail = v_error_detail
    WHERE job_id = v_job_id;

    RAISE USING MESSAGE = CONCAT(v_message, ' Details: ', v_error_detail);
  END;
END;