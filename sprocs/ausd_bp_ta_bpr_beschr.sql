-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_beschr.ksh
-- Target BigQuery Stored Procedure for orchestration.

CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_bpr_beschr`(
  IN p_stichtag_in STRING,
  IN p_wiederanlaufWert_in INT64
)
BEGIN
  DECLARE v_sysdate STRING;
  DECLARE v_stichtag STRING;
  DECLARE v_restart_value INT64 DEFAULT 0;
  DECLARE v_job_kennung STRING DEFAULT 'AUSD_BP_TA_BPR_BESCHR';
  DECLARE v_job_nr INT64;

  -- 1. Initialize restart value
  SET v_restart_value = IFNULL(p_wiederanlaufWert_in, 0);

  -- 2. Current system date in DDMMYYYY
  SET v_sysdate = FORMAT_DATE('%d%m%Y', CURRENT_DATE());

  -- 3. Default processing date
  SET v_stichtag = IFNULL(NULLIF(p_stichtag_in, ''), v_sysdate);

  -- 4. Parameter validation
  IF v_stichtag IS NULL OR v_stichtag = '' THEN
    INSERT INTO `project.dataset.job_log`
    (job_kennung, status, message, created_at)
    VALUES
    (v_job_kennung, 'ERROR', 'Stichtag missing', CURRENT_TIMESTAMP());
    RAISE USING MESSAGE = 'Parameter validation failed: Stichtag missing';
  END IF;

  -- 5. Create job entry / audit record
  -- In a real scenario, v_job_nr would be generated, and a more robust audit table used.
  INSERT INTO `project.dataset.job_audit`
  (job_kennung, stichtag, restart_value, status, created_at)
  VALUES
  (v_job_kennung, v_stichtag, v_restart_value, 'STARTED', CURRENT_TIMESTAMP());

  -- Get a job_nr for logging further steps (placeholder implementation, often a sequence or hash)
  -- For simplicity, deriving from audit table count here.
  SET v_job_nr = (SELECT COUNT(*) FROM `project.dataset.job_audit` WHERE job_kennung = v_job_kennung AND stichtag = v_stichtag AND restart_value = v_restart_value);

  -- 6. Call core data processing stored procedure
  CALL `project.dataset.process_contract_cache_data`(v_stichtag, v_restart_value, v_job_nr);

  -- 7. Mark success
  UPDATE `project.dataset.job_audit`
  SET status = 'OK',
      finished_at = CURRENT_TIMESTAMP()
  WHERE job_kennung = v_job_kennung
    AND stichtag = v_stichtag
    AND restart_value = v_restart_value
    AND status = 'STARTED';

EXCEPTION WHEN ERROR THEN
  -- 8. Error handling: Log error and re-raise as per design document.
  -- This will insert a new error record. A more sophisticated approach might update the existing 'STARTED' record.
  INSERT INTO `project.dataset.job_audit`
  (job_kennung, stichtag, restart_value, status, message, created_at)
  VALUES
  (v_job_kennung, v_stichtag, v_restart_value, 'ERROR', @@error.message, CURRENT_TIMESTAMP());
  RAISE;
END;