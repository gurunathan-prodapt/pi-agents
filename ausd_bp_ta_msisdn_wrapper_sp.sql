-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn.ksh
-- Description: BigQuery Stored Procedure for orchestrating the basic products data preparation for BERT (r_ausd_bp_ta_msisdn.ksh).
CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_msisdn_wrapper`(
  IN p_stichtag_string STRING,
  IN p_wiederanlaufWert INT64
)
BEGIN
  -- Variable declarations and initializations
  DECLARE v_sysdate DATE;
  DECLARE v_stichtag DATE;
  DECLARE v_eff_wiederanlaufWert INT64 DEFAULT 0;
  DECLARE v_job_kennung STRING DEFAULT 'ausd_bp_ta_msisdn';
  DECLARE v_job_nr INT64;
  DECLARE v_log_dateiname STRING;
  DECLARE v_err_nr INT64 DEFAULT 0;
  DECLARE v_err_arg STRING DEFAULT '';
  DECLARE v_status STRING DEFAULT 'INIT';

  -- Date determination and defaulting
  SET v_sysdate = CURRENT_DATE();
  IF p_wiederanlaufWert IS NULL THEN SET v_eff_wiederanlaufWert = 0; ELSE SET v_eff_wiederanlaufWert = p_wiederanlaufWert; END IF;
  IF p_stichtag_string IS NULL OR TRIM(p_stichtag_string) = '' THEN SET v_stichtag = v_sysdate; ELSE SET v_stichtag = PARSE_DATE('%d%m%Y', p_stichtag_string); END IF;

  -- Parameter validation
  IF v_stichtag IS NULL THEN SET v_err_nr = 193; SET v_err_arg = 'Stichtag'; END IF;
  IF v_err_nr <> 0 THEN
    INSERT INTO `project.dataset.job_audit_log` (job_kennung, job_nr, log_ts, status, err_nr, err_arg, message)
    VALUES (v_job_kennung, NULL, CURRENT_TIMESTAMP(), 'ERROR', v_err_nr, v_err_arg, 'Required parameter missing or invalid');
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Parameter validation failed';
  END IF;

  -- Job initialization and logging (DWMSG_ErmittleNr, DWMSG_Logdateiname, DWMSG_ErzeugeEintrag, DWMSG_SetzeStichtagInfo equivalents)
  SET v_job_nr = (SELECT IFNULL(MAX(job_nr), 0) + 1 FROM `project.dataset.job_audit_log` WHERE job_kennung = v_job_kennung);
  SET v_log_dateiname = CONCAT(v_job_kennung, '_', CAST(v_job_nr AS STRING), '.log');
  INSERT INTO `project.dataset.job_audit_log` (job_kennung, job_nr, log_ts, status, stichtag, sysdate, log_file, message)
  VALUES (v_job_kennung, v_job_nr, CURRENT_TIMESTAMP(), 'STARTED', v_stichtag, v_sysdate, v_log_dateiname, 'Job started');
  INSERT INTO `project.dataset.job_run_info` (job_kennung, job_nr, stichtag, sysdate, created_ts)
  VALUES (v_job_kennung, v_job_nr, v_stichtag, v_sysdate, CURRENT_TIMESTAMP());

  -- Core business logic placeholder: Call the downstream procedure
  CALL `project.dataset.k_ausd_bp_ta_msisdn`(v_job_kennung, v_stichtag, v_job_nr, v_eff_wiederanlaufWert);

  -- Success handling
  INSERT INTO `project.dataset.job_audit_log` (job_kennung, job_nr, log_ts, status, message)
  VALUES (v_job_kennung, v_job_nr, CURRENT_TIMESTAMP(), 'OK', 'Die Abarbeitung wurde ohne erkennbare Fehler beendet');
  SET v_status = 'OK';

EXCEPTION WHEN ERROR THEN
  INSERT INTO `project.dataset.job_audit_log` (job_kennung, job_nr, log_ts, status, message)
  VALUES (v_job_kennung, v_job_nr, CURRENT_TIMESTAMP(), 'ERROR', 'AppError: Abbruch');
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Job failed';
END;