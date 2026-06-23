-- BigQuery Stored Procedure for r_ausd_bp_ta_bpr_basis.ksh
-- Replaces vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_basis.ksh
CREATE OR REPLACE PROCEDURE `project.dataset.Bereitstellung_Basisprodukte_BERT`(
  IN p_stichtag_input STRING,
  IN p_wiederanlaufWert_input INT64
)
BEGIN
  -- Variable declarations and initializations
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';
  DECLARE DW_EintragsNr INT64;
  DECLARE v_sysdate STRING DEFAULT FORMAT_DATE('%d%m%Y', CURRENT_DATE());
  DECLARE v_stichtag STRING;
  DECLARE v_wiederanlaufWert INT64;
  DECLARE JobKennung STRING DEFAULT 'ausd_bp_ta_bpr_basis';
  DECLARE LogDatei STRING;

  -- Defaulting logic
  SET v_wiederanlaufWert = COALESCE(p_wiederanlaufWert_input, 0);
  SET v_stichtag = COALESCE(p_stichtag_input, v_sysdate);

  -- Parameter validation
  IF v_stichtag IS NULL OR v_stichtag = '' THEN
    SET ErrNr = 193;
    SET ErrArg = 'Stichtag';
  END IF;

  -- Error handling (logging and signaling)
  IF ErrNr != 0 THEN
    INSERT INTO `project.dataset.job_error_log` (job_name, error_nr, error_arg, created_at)
    VALUES (JobKennung, ErrNr, ErrArg, CURRENT_TIMESTAMP());
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Parameter validation failed';
  END IF;

  -- Derive DW_EintragsNr for run logging
  SET DW_EintragsNr = (SELECT IFNULL(MAX(job_id), 0) + 1 FROM `project.dataset.job_audit_log`);
  SET LogDatei = CONCAT('job_', CAST(DW_EintragsNr AS STRING), '_', JobKennung, '.log'); -- For reference, not actively used for file logging

  -- Job audit logging (STARTED)
  INSERT INTO `project.dataset.job_audit_log` (job_name, stichtag, wiederanlaufwert, sysdate_value, status, created_at)
  VALUES (JobKennung, v_stichtag, v_wiederanlaufWert, v_sysdate, 'STARTED', CURRENT_TIMESTAMP());

  -- Job run logging (RUNNING)
  INSERT INTO `project.dataset.job_run_log` (job_id, job_name, log_file, stichtag, sysdate_value, status, created_at)
  VALUES (DW_EintragsNr, JobKennung, LogDatei, v_stichtag, v_sysdate, 'RUNNING', CURRENT_TIMESTAMP());

  -- Call to the migrated kernel script's stored procedure
  CALL `project.dataset.k_ausd_bp_ta_bpr_basis`(JobKennung, v_stichtag, DW_EintragsNr, v_wiederanlaufWert);

  -- Update job run and audit logs (OK/SUCCESS)
  UPDATE `project.dataset.job_run_log`
  SET status = 'OK', finished_at = CURRENT_TIMESTAMP()
  WHERE job_id = DW_EintragsNr;

  -- Note: job_audit_log is typically updated with final status for the *specific run* via an update,
  -- but the design shows a new insert. For simplicity, we'll insert a final record.
  INSERT INTO `project.dataset.job_audit_log` (job_name, stichtag, wiederanlaufwert, sysdate_value, status, created_at)
  VALUES (JobKennung, v_stichtag, v_wiederanlaufWert, v_sysdate, 'SUCCESS', CURRENT_TIMESTAMP());

EXCEPTION WHEN ERROR THEN
  -- Error handling for any unhandled exceptions during execution
  INSERT INTO `project.dataset.job_error_log` (job_name, error_nr, error_arg, created_at)
  VALUES (JobKennung, 999, 'Unhandled exception', CURRENT_TIMESTAMP()); -- Use a generic error number for unhandled exceptions

  UPDATE `project.dataset.job_run_log`
  SET status = 'FAILED', finished_at = CURRENT_TIMESTAMP()
  WHERE job_id = DW_EintragsNr;

  INSERT INTO `project.dataset.job_audit_log` (job_name, stichtag, wiederanlaufwert, sysdate_value, status, created_at)
  VALUES (JobKennung, v_stichtag, v_wiederanlaufWert, v_sysdate, 'FAILED', CURRENT_TIMESTAMP());

  -- Re-raise the error to propagate it to the caller (e.g., orchestrator)
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = FORMAT("Execution failed for job '%s' with error: %s", JobKennung, @@error.message);
END;