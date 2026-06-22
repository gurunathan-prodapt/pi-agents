-- Target BigQuery Stored Procedure for wrapper script
-- Replaces vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount_rr.ksh
-- Generated for job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount_rr.ksh
CREATE OR REPLACE PROCEDURE `project.dataset.sp_vertragsdatenabgleich_ta_discount_rr`(
  IN p_help BOOL,
  IN p_s STRING,
  IN p_l STRING
)
BEGIN
  DECLARE v_prog_name STRING DEFAULT 'Vertragsdatenabgleich';
  DECLARE v_job_kennung STRING DEFAULT 'BERT_V_TA_DISCOUNT_RR';
  DECLARE v_sysdate STRING DEFAULT FORMAT_DATE('%d%m%Y', CURRENT_DATE());
  DECLARE v_err_nr INT64 DEFAULT 0;
  DECLARE v_err_arg STRING DEFAULT '';
  DECLARE v_dw_eintragsnr INT64 DEFAULT 0;
  DECLARE v_logdatei STRING DEFAULT '';
  DECLARE v_status STRING DEFAULT 'INIT';

  -- Usage/help handling
  IF p_help THEN
    SELECT v_prog_name AS Programm, 'V1.0.0' AS Version, 'Aufruf: Parameter -h zeigt diese Seite an' AS Beschreibung;
    RETURN;
  END IF;

  -- Parameter validation placeholder (replaces getopts logic)
  IF p_s IS NULL OR p_l IS NULL THEN
    SET v_err_nr = 193;
    SET v_err_arg = IF(p_s IS NULL, 's', 'l');
  END IF;

  IF v_err_nr != 0 THEN
    -- Simulate DWMSG_MeldeFehler
    INSERT INTO `project.dataset.job_log` (job_kennung, job_entry_nr, log_level, message, log_file_name, created_at)
    VALUES (v_job_kennung, v_dw_eintragsnr, 'E', CONCAT('Parameterfehler: ', v_err_arg), v_logdatei, CURRENT_TIMESTAMP());
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = CONCAT('Parameter error: ', v_err_arg);
  END IF;

  -- Simulate DWMSG_ErmittleNr
  SET v_dw_eintragsnr = (SELECT IFNULL(MAX(job_entry_nr), 0) + 1 FROM `project.dataset.job_log` WHERE job_kennung = v_job_kennung);
  -- Simulate DWMSG_Logdateiname
  SET v_logdatei = CONCAT(v_job_kennung, '_', CAST(v_dw_eintragsnr AS STRING), '.log');
  -- Simulate DWMSG_ErzeugeEintrag
  INSERT INTO `project.dataset.job_log` (job_kennung, job_entry_nr, log_level, message, log_file_name, created_at)
  VALUES (v_job_kennung, v_dw_eintragsnr, 'I', CONCAT('Job start: ', v_prog_name), v_logdatei, CURRENT_TIMESTAMP());
  -- Simulate DWMSG_SetzeStichtagInfo
  INSERT INTO `project.dataset.job_control` (job_kennung, job_entry_nr, stichtag, stichtag_format, status, created_at, updated_at)
  VALUES (v_job_kennung, v_dw_eintragsnr, v_sysdate, 'DDMMYYYY', 'INIT', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

  BEGIN
    -- Print job banner
    SELECT 'Job' AS section, v_dw_eintragsnr AS job_nr, v_job_kennung AS job_kennung, v_logdatei AS logdatei;

    -- Invoke core script (replace with actual BigQuery SP for k_ausd_v_ta_discount_rr.ksh)
    CALL `project.dataset.sp_k_ausd_v_ta_discount_rr`(v_job_kennung, v_dw_eintragsnr);

    -- Simulate success message
    INSERT INTO `project.dataset.job_log` (job_kennung, job_entry_nr, log_level, message, log_file_name, created_at)
    VALUES (v_job_kennung, v_dw_eintragsnr, 'I', 'Die Abarbeitung wurde ohne erkennbare Fehler beendet', v_logdatei, CURRENT_TIMESTAMP());
    -- Simulate DWMSG_SetzeStatusOK
    UPDATE `project.dataset.job_control` SET status = 'OK', updated_at = CURRENT_TIMESTAMP() WHERE job_kennung = v_job_kennung AND job_entry_nr = v_dw_eintragsnr;
    SET v_status = 'OK';

  EXCEPTION WHEN ERROR THEN
    -- Simulate DWMSG_Fehlerbehandlung
    INSERT INTO `project.dataset.job_log` (job_kennung, job_entry_nr, log_level, message, log_file_name, created_at)
    VALUES (v_job_kennung, v_dw_eintragsnr, 'E', CONCAT('AppError: Abbruch - ', @@error.message), v_logdatei, CURRENT_TIMESTAMP());
    UPDATE `project.dataset.job_control` SET status = 'ERROR', updated_at = CURRENT_TIMESTAMP() WHERE job_kennung = v_job_kennung AND job_entry_nr = v_dw_eintragsnr;
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = CONCAT('AppError: Abbruch - ', @@error.message);
  END;
END;