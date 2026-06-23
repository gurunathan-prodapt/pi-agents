-- BigQuery Stored Procedure
-- Replaces: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount.ksh
CREATE OR REPLACE PROCEDURE project.dataset.sp_vertragsdatenabgleich_ta_discount(
  IN p_stichtag STRING,       -- Corresponds to -s <Stichtag>
  IN p_logfile_suffix STRING, -- Corresponds to -t <Logfile-suffix>
  IN p_job_identifier STRING, -- Corresponds to -j <JobKennung>
  IN p_help BOOLEAN DEFAULT FALSE -- For -h (help)
)
BEGIN
  -- Declare variables
  DECLARE v_script_name STRING DEFAULT 'r_ausd_v_ta_discount';
  DECLARE v_job_kennung STRING;
  DECLARE v_entry_nr INT64;
  DECLARE v_logfile_name STRING;
  DECLARE v_stichtag_info STRING;
  DECLARE v_start_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  DECLARE v_end_ts TIMESTAMP;
  DECLARE v_error_message STRING;
  DECLARE v_error_stack STRING;

  -- Help option: if p_help is true, print usage and exit
  IF p_help THEN
    SELECT 'Usage: CALL project.dataset.sp_vertragsdatenabgleich_ta_discount(p_stichtag => <stichtag>, p_logfile_suffix => <suffix>, p_job_identifier => <jobkennung>, p_help => TRUE/FALSE)' AS Usage;
    SELECT '  p_stichtag: Reference date (e.g., YYYYMMDD).' AS Option_s;
    SELECT '  p_logfile_suffix: Suffix for log file (optional).' AS Option_t;
    SELECT '  p_job_identifier: Unique job identifier.' AS Option_j;
    SELECT '  p_help: Display this help message.' AS Option_h;
    RETURN;
  END IF;

  -- Validate mandatory parameters
  IF p_job_identifier IS NULL OR TRIM(p_job_identifier) = '' THEN
    SET v_error_message = 'Job identifier (-j) is a mandatory parameter.';
    CALL project.dataset.sp_log_error(v_job_kennung, v_entry_nr, 1, 'Missing Parameter', v_error_message); -- Log error to table
    RAISE; -- Re-raise to signal failure
  END IF;

  -- Set JobKennung (uppercase)
  SET v_job_kennung = UPPER(p_job_identifier);

  -- Set LogFileName (based on original shell script's behavior)
  SET v_logfile_name = CONCAT(v_script_name, '_', COALESCE(p_logfile_suffix, ''), '.log');

  -- Set StichtagInfo
  SET v_stichtag_info = COALESCE(p_stichtag, FORMAT_DATE('%Y%m%d', CURRENT_DATE())); -- Default to current date if not provided

  BEGIN TRANSACTION;

  -- Determine the next entry number for the job
  SELECT COALESCE(MAX(entry_nr), 0) + 1 INTO v_entry_nr
  FROM project.dataset.job_control
  WHERE job_kennung = v_job_kennung;

  -- Log job start into job_control table
  INSERT INTO project.dataset.job_control (
    entry_nr,
    job_kennung,
    script_name,
    log_file,
    status,
    stichtag_info,
    created_ts,
    finished_ts
  )
  VALUES (
    v_entry_nr,
    v_job_kennung,
    v_script_name,
    v_logfile_name,
    'RUNNING',
    v_stichtag_info,
    v_start_ts,
    NULL
  );

  -- Call the core processing stored procedure
  CALL project.dataset.sp_k_ausd_v_ta_discount(v_job_kennung, v_entry_nr);

  -- Update job_control table for successful completion
  SET v_end_ts = CURRENT_TIMESTAMP();
  UPDATE project.dataset.job_control
  SET
    status = 'OK',
    finished_ts = v_end_ts
  WHERE
    job_kennung = v_job_kennung AND entry_nr = v_entry_nr;

  COMMIT TRANSACTION;

EXCEPTION WHEN ERROR THEN
  -- Rollback transaction in case of error
  ROLLBACK TRANSACTION;

  -- Capture error details
  SET v_error_message = @@error.message;
  SET v_error_stack = @@error.stack_trace;
  SET v_end_ts = CURRENT_TIMESTAMP();

  -- Log error to job_error_log table
  INSERT INTO project.dataset.job_error_log (
    job_kennung,
    entry_nr,
    error_nr,
    error_arg,
    error_message,
    created_ts
  )
  VALUES (
    v_job_kennung,
    v_entry_nr,
    2, -- Generic error number for SQL errors
    'BigQuery Error',
    v_error_message,
    v_end_ts
  );

  -- Update job_control table for error status
  UPDATE project.dataset.job_control
  SET
    status = 'ERROR',
    finished_ts = v_end_ts
  WHERE
    job_kennung = v_job_kennung AND entry_nr = v_entry_nr;

  -- Re-raise the error for external orchestration systems to catch
  RAISE;
END;
-- Helper procedure to log errors (replaces f_alis_msgerr.ksh partially)
-- This is a simplified version; a more robust solution might have more parameters
-- to mirror the original f_alis_msgerr.ksh functionality.
CREATE OR REPLACE PROCEDURE project.dataset.sp_log_error(
  IN job_kennung STRING,
  IN entry_nr INT64,
  IN error_nr INT64,
  IN error_arg STRING,
  IN error_message STRING
)
BEGIN
  INSERT INTO project.dataset.job_error_log (
    job_kennung,
    entry_nr,
    error_nr,
    error_arg,
    error_message,
    created_ts
  )
  VALUES (
    job_kennung,
    entry_nr,
    error_nr,
    error_arg,
    error_message,
    CURRENT_TIMESTAMP()
  );
END;