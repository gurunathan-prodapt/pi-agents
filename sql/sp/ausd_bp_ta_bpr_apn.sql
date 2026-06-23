-- Stored Procedure for the wrapper logic r_ausd_bp_ta_bpr_apn.ksh
-- Legacy Source: r_ausd_bp_ta_bpr_apn.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_apn.ksh

CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.ausd_bp_ta_bpr_apn`(
  IN p_stichtag STRING,
  IN p_wiederanlaufWert INT64
)
BEGIN
  -- Declare variables
  DECLARE v_prog_name STRING DEFAULT 'Bereitstellung Basisprodukte BERT';
  DECLARE v_prog_version STRING DEFAULT 'V2.0.0';
  DECLARE v_job_kennung STRING DEFAULT 'ausd_bp_ta_bpr_apn';
  DECLARE v_job_nr INT64;
  DECLARE v_log_datei STRING;
  DECLARE v_sysdate STRING;
  DECLARE v_final_stichtag STRING;
  DECLARE v_final_wiederanlaufWert INT66;
  DECLARE v_err_msg STRING DEFAULT NULL;

  -- 1. Initialize defaults and parse parameters
  SET v_final_wiederanlaufWert = IFNULL(p_wiederanlaufWert, 0);

  -- Derive system date in DDMMYYYY format
  SET v_sysdate = FORMAT_DATE('%d%m%Y', CURRENT_DATE());

  -- Default stichtag if not provided or empty
  SET v_final_stichtag = IFNULL(NULLIF(TRIM(p_stichtag), ''), v_sysdate);

  -- 2. Validate required parameters
  ASSERT v_final_stichtag IS NOT NULL AND v_final_stichtag != '' AS 'Stichtag must be set and not empty.';

  -- Optional: validate date format DDMMYYYY
  ASSERT SAFE.PARSE_DATE('%d%m%Y', v_final_stichtag) IS NOT NULL AS 'Invalid Stichtag format. Expected DDMMYYYY.';

  -- 3. Create job number and log reference (placeholder logic for audit table)
  -- This assumes job_nr is sequential per job_kennung
  SET v_job_nr = (
    SELECT IFNULL(MAX(job_nr), 0) + 1
    FROM `your_project_id.your_dataset_id.job_audit_log`
    WHERE job_kennung = v_job_kennung
  );

  SET v_log_datei = CONCAT('job_', v_job_kennung, '_', CAST(v_job_nr AS STRING), '.log');

  -- 4. Insert audit start record
  INSERT INTO `your_project_id.your_dataset_id.job_audit_log` (
    job_nr,
    job_kennung,
    prog_name,
    prog_version,
    log_datei,
    stichtag,
    status,
    message,
    created_at
  )
  VALUES (
    v_job_nr,
    v_job_kennung,
    v_prog_name,
    v_prog_version,
    v_log_datei,
    v_final_stichtag,
    'STARTED',
    'Job started',
    CURRENT_TIMESTAMP()
  );

  -- 5. Begin error trapping block
  BEGIN
    -- 6. Invoke the core kernel script (migrated k_ausd_bp_ta_bpr_apn.ksh)
    CALL `your_project_id.your_dataset_id.k_ausd_bp_ta_bpr_apn`(
      v_job_kennung,
      v_final_stichtag,
      v_job_nr,
      v_final_wiederanlaufWert
    );

    -- 7. Mark success if kernel procedure completes without error
    INSERT INTO `your_project_id.your_dataset_id.job_audit_log` (
      job_nr,
      job_kennung,
      prog_name,
      prog_version,
      log_datei,
      stichtag,
      status,
      message,
      created_at
    )
    VALUES (
      v_job_nr,
      v_job_kennung,
      v_prog_name,
      v_prog_version,
      v_log_datei,
      v_final_stichtag,
      'OK',
      'Die Abarbeitung wurde ohne erkennbare Fehler beendet',
      CURRENT_TIMESTAMP()
    );

  EXCEPTION WHEN ERROR THEN
    SET v_err_msg = @@error.message;

    -- 8. Log error message
    INSERT INTO `your_project_id.your_dataset_id.job_audit_log` (
      job_nr,
      job_kennung,
      prog_name,
      prog_version,
      log_datei,
      stichtag,
      status,
      message,
      created_at
    )
    VALUES (
      v_job_nr,
      v_job_kennung,
      v_prog_name,
      v_prog_version,
      v_log_datei,
      v_final_stichtag,
      'ERROR',
      CONCAT('AppError: Abbruch - ', v_err_msg),
      CURRENT_TIMESTAMP()
    );

    -- Re-raise the error to signal job failure to the orchestrator
    RAISE USING MESSAGE = CONCAT('AppError: Abbruch - ', v_err_msg);
  END;
END;