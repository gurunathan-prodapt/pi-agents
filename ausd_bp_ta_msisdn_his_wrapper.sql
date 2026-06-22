-- Target for: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn_his.ksh
-- Description: BigQuery Stored Procedure replacing the KornShell wrapper script, handling parameter parsing, logging, and invoking the core logic.

CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_msisdn_his_wrapper`(
  IN p_stichtag_in STRING,         -- Input Stichtag (DDMMYYYY)
  IN p_wiederanlaufWert_in INT64   -- Input Wiederanlaufwert
)
BEGIN
  DECLARE v_sysdate STRING;
  DECLARE v_stichtag STRING;
  DECLARE v_wiederanlaufWert INT64;
  DECLARE v_jobkennung STRING DEFAULT 'AUSD_BP_TA_MSISDN_HIS';
  DECLARE v_eintragsnr INT64;
  DECLARE v_logdatei STRING;
  DECLARE v_status STRING DEFAULT 'STARTED';

  -- Initialize Wiederanlaufwert
  SET v_wiederanlaufWert = IFNULL(p_wiederanlaufWert_in, 0);

  -- Determine system date (replaces DWDate_Gib_Zeitraum / system date)
  SET v_sysdate = FORMAT_DATE('%d%m%Y', CURRENT_DATE());

  -- Determine Stichtag (defaults to system date if not provided)
  SET v_stichtag = IFNULL(p_stichtag_in, v_sysdate);

  -- Parameter validation (replaces pruefeParameterGesetzt)
  IF v_stichtag IS NULL OR v_stichtag = '' THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Required parameter Stichtag is missing or empty.';
  END IF;

  -- Determine job entry number (replaces DWMSG_ErmittleNr)
  -- Assumes dwmsg_job_audit table exists and stores job_id and job_name
  SET v_eintragsnr = (
    SELECT IFNULL(MAX(job_id), 0) + 1
    FROM `project.dataset.dwmsg_job_audit`
    WHERE job_name = v_jobkennung
  );

  -- Generate log file name (replaces DWMSG_Logdateiname)
  SET v_logdatei = CONCAT('job_', CAST(v_eintragsnr AS STRING), '_', v_jobkennung, '.log');

  -- Log job start (replaces DWMSG_ErzeugeEintrag)
  INSERT INTO `project.dataset.dwmsg_job_audit` (job_id, job_name, script_name, log_file, stichtag, status, created_at)
  VALUES (v_eintragsnr, v_jobkennung, 'ausd_bp_ta_msisdn_his_wrapper', v_logdatei, v_stichtag, 'STARTED', CURRENT_TIMESTAMP());

  -- Main logic block with exception handling (replaces 'trap' and error functions like DWMSG_Fehlerbehandlung)
  BEGIN
    -- Call the core processing stored procedure
    -- This procedure would contain the logic of 'k_ausd_bp_ta_msisdn_his.ksh'
    CALL `project.dataset.k_ausd_bp_ta_msisdn_his`(
      v_jobkennung,
      v_stichtag,
      v_eintragsnr,
      v_wiederanlaufWert
    );

    -- If successful, update status (replaces DWMSG_SetzeStatusOK)
    SET v_status = 'COMPLETED';
    INSERT INTO `project.dataset.dwmsg_job_audit` (job_id, job_name, script_name, log_file, stichtag, status, created_at)
    VALUES (v_eintragsnr, v_jobkennung, 'ausd_bp_ta_msisdn_his_wrapper', v_logdatei, v_stichtag, 'COMPLETED', CURRENT_TIMESTAMP());

  EXCEPTION WHEN ERROR THEN
    -- Handle error (replaces DWMSG_MeldeFehler)
    SET v_status = 'FAILED';
    INSERT INTO `project.dataset.dwmsg_job_audit` (job_id, job_name, script_name, log_file, stichtag, status, error_message, created_at)
    VALUES (v_eintragsnr, v_jobkennung, 'ausd_bp_ta_msisdn_his_wrapper', v_logdatei, v_stichtag, 'FAILED', @@error.message, CURRENT_TIMESTAMP());
    RAISE; -- Re-raise the error to propagate it to the caller
  END;

END;