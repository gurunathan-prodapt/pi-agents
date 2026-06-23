-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_dist.ksh
-- Purpose: Create BigQuery Stored Procedure for orchestration logic.
CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_cntrct_dist_wrapper`(
  IN p_stichtag STRING,
  IN p_wiederanlaufwert INT64
)
BEGIN
  DECLARE v_progname STRING DEFAULT 'Bereitstellung Basisprodukte BERT';
  DECLARE v_progversion STRING DEFAULT 'V2.0.0';
  DECLARE v_jobkennung STRING DEFAULT 'ausd_bp_ta_cntrct_dist';
  DECLARE v_sysdate STRING;
  DECLARE v_stichtag STRING;
  DECLARE v_wiederanlaufwert INT64;
  DECLARE v_eintragsnr INT64;

  SET v_wiederanlaufwert = IFNULL(p_wiederanlaufwert, 0);
  SET v_sysdate = FORMAT_DATE('%d%m%Y', CURRENT_DATE());
  SET v_stichtag = IFNULL(NULLIF(p_stichtag, ''), v_sysdate);

  IF v_stichtag IS NULL OR v_stichtag = '' THEN
    INSERT INTO `project.dataset.job_log`
    (job_name, log_level, message, created_at)
    VALUES
    (v_jobkennung, 'ERROR', 'Stichtag parameter missing', CURRENT_TIMESTAMP());

    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'Stichtag parameter missing';
  END IF;

  SET v_eintragsnr = (
    SELECT IFNULL(MAX(entry_no), 0) + 1
    FROM `project.dataset.job_log`
    WHERE job_name = v_jobkennung
  );

  INSERT INTO `project.dataset.job_log`
  (entry_no, job_name, log_level, message, stichtag, sysdate, created_at)
  VALUES
  (v_eintragsnr, v_jobkennung, 'INFO',
   CONCAT('Job started: ', v_progname, ' ', v_progversion),
   v_stichtag, v_sysdate, CURRENT_TIMESTAMP());

  BEGIN
    CALL `project.dataset.ausd_bp_ta_cntrct_dist_core`(
      v_jobkennung,
      v_stichtag,
      v_eintragsnr,
      v_wiederanlaufwert
    );

    INSERT INTO `project.dataset.job_log`
    (entry_no, job_name, log_level, message, stichtag, created_at)
    VALUES
    (v_eintragsnr, v_jobkennung, 'INFO',
     'Die Abarbeitung wurde ohne erkennbare Fehler beendet',
     v_stichtag,
     CURRENT_TIMESTAMP());

    INSERT INTO `project.dataset.job_status`
    (entry_no, job_name, status, updated_at)
    VALUES
    (v_eintragsnr, v_jobkennung, 'OK', CURRENT_TIMESTAMP());

  EXCEPTION WHEN ERROR THEN
    INSERT INTO `project.dataset.job_log`
    (entry_no, job_name, log_level, message, stichtag, created_at)
    VALUES
    (v_eintragsnr, v_jobkennung, 'ERROR',
     CONCAT('AppError: Abbruch - ', @@error.message),
     v_stichtag,
     CURRENT_TIMESTAMP());

    INSERT INTO `project.dataset.job_status`
    (entry_no, job_name, status, updated_at)
    VALUES
    (v_eintragsnr, v_jobkennung, 'FAILED', CURRENT_TIMESTAMP());

    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'Job failed';
  END;
END;