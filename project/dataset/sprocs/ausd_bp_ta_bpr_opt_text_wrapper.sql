--
-- Target: BigQuery Stored Procedure for wrapper logic
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_opt_text.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_opt_text.ksh
--

CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_bpr_opt_text_wrapper`(
  IN p_stichtag STRING,
  IN p_wiederanlaufWert INT64
)
BEGIN
  DECLARE v_sysdate STRING;
  DECLARE v_stichtag STRING;
  DECLARE v_wiederanlaufWert INT64;
  DECLARE v_jobkennung STRING DEFAULT 'ausd_bp_ta_bpr_opt_text';
  DECLARE v_eintragsnr INT64;
  DECLARE v_logdatei STRING;
  DECLARE v_errnr INT64 DEFAULT 0;
  DECLARE v_errarg STRING DEFAULT '';

  BEGIN
    SET v_wiederanlaufWert = IFNULL(p_wiederanlaufWert, 0);
    SET v_sysdate = FORMAT_DATE('%d%m%Y', CURRENT_DATE());
    SET v_stichtag = IFNULL(p_stichtag, v_sysdate);

    ASSERT v_stichtag IS NOT NULL
      AS 'Stichtag must be provided or derivable';

    -- Initial log entry for job start
    INSERT INTO `project.dataset.job_log_audit`
      (job_name, status, stichtag, restart_value, created_at)
    VALUES
      (v_jobkennung, 'STARTED', v_stichtag, v_wiederanlaufWert, CURRENT_TIMESTAMP());

    -- Determine next entry number for logging
    SET v_eintragsnr = (
      SELECT IFNULL(MAX(entry_nr), 0) + 1
      FROM `project.dataset.job_log_audit`
      WHERE job_name = v_jobkennung
    );

    SET v_logdatei = CONCAT('log_', v_jobkennung, '_', CAST(v_eintragsnr AS STRING));

    -- Log entry for job running with details
    INSERT INTO `project.dataset.job_log_audit`
      (entry_nr, job_name, script_name, log_name, stichtag, status, created_at)
    VALUES
      (v_eintragsnr, v_jobkennung, 'ausd_bp_ta_bpr_opt_text_wrapper', v_logdatei, v_stichtag, 'RUNNING', CURRENT_TIMESTAMP());

    -- Call core business logic procedure
    CALL `project.dataset.k_ausd_bp_ta_bpr_opt_text`(
      v_jobkennung,
      v_stichtag,
      v_eintragsnr,
      v_wiederanlaufWert
    );

    -- Log success
    INSERT INTO `project.dataset.job_log_audit`
      (entry_nr, job_name, status, message, created_at)
    VALUES
      (v_eintragsnr, v_jobkennung, 'OK', 'Die Abarbeitung wurde ohne erkennbare Fehler beendet', CURRENT_TIMESTAMP());

  EXCEPTION WHEN ERROR THEN
    -- Log error
    INSERT INTO `project.dataset.job_log_audit`
      (entry_nr, job_name, status, message, created_at)
    VALUES
      (v_eintragsnr, v_jobkennung, 'ERROR', @@error.message, CURRENT_TIMESTAMP());
    RAISE USING MESSAGE = 'AppError: Abbruch'; -- Re-raise for external orchestration if needed
  END;
END;