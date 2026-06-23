-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_drop_temp_table.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_drop_temp_table.ksh

-- This stored procedure is the BigQuery equivalent of the r_drop_temp_table.ksh wrapper script.
-- It handles parameter parsing, logging, and orchestrates the call to the core cleanup procedure.
CREATE OR REPLACE PROCEDURE `project.dataset.BERT_DROP_TEMP_TABLE`(
  IN p_stichtag_in STRING,    -- Stichtag (DDMMYYYY) from command line, optional
  IN p_wiederanlaufWert_in STRING -- Wiederanlaufwert from command line, optional
)
BEGIN
  -- Declare variables
  DECLARE v_job_kennung STRING DEFAULT 'BERT_DROP_TEMP_TABLE';
  DECLARE v_eintragsnr INT64;
  DECLARE v_log_message STRING;
  DECLARE v_stichtag_sysdate STRING;
  DECLARE v_stichtag STRING;
  DECLARE v_wiederanlaufWert INT64;
  DECLARE v_err_nr INT64 DEFAULT 0;
  DECLARE v_err_arg STRING DEFAULT '';
  DECLARE v_status STRING DEFAULT 'OK';

  -- Simulate DWMSG_ErmittleNr by getting a unique entry number (e.g., max_id + 1)
  -- For simplicity, let's use a timestamp based unique ID or a sequence if available.
  -- A robust implementation would use a sequence table or a dedicated logging service.
  SET v_eintragsnr = CAST(FORMAT_TIMESTAMP('%Y%m%d%H%M%S', CURRENT_TIMESTAMP()) AS INT64);

  -- Determine system date in DDMMYYYY format (simulating DWDate_Gib_Zeitraum)
  SET v_stichtag_sysdate = FORMAT_DATE('%d%m%Y', CURRENT_DATE());

  -- Parameter Defaults & Validation
  -- p_wiederanlaufWert defaults to 0 if not provided or invalid
  IF p_wiederanlaufWert_in IS NULL OR NOT SAFE.PARSE_INT64(p_wiederanlaufWert_in) IS NOT NULL THEN
    SET v_wiederanlaufWert = 0;
  ELSE
    SET v_wiederanlaufWert = CAST(p_wiederanlaufWert_in AS INT64);
  END IF;

  -- p_stichtag defaults to system date if not provided
  IF p_stichtag_in IS NULL THEN
    SET v_stichtag = v_stichtag_sysdate;
  ELSE
    SET v_stichtag = p_stichtag_in;
  END IF;

  -- Validate Stichtag format (DDMMYYYY)
  IF SAFE.PARSE_DATE('%d%m%Y', v_stichtag) IS NULL THEN
    SET v_err_nr = 193; -- Notwendiges Argument fehlt or invalid format
    SET v_err_arg = CONCAT('Stichtag (', v_stichtag, ') is not in DDMMYYYY format');
    SET v_log_message = CONCAT('ERROR: ', v_err_arg);

    INSERT INTO `project.dataset.job_log` (eintragsnr, job_kennung, log_level, err_nr, err_arg, message, stichtag, restart_value, created_at)
    VALUES (v_eintragsnr, v_job_kennung, 'ERROR', v_err_nr, v_err_arg, v_log_message, v_stichtag, CAST(v_wiederanlaufWert AS STRING), CURRENT_TIMESTAMP());

    -- Exit with error
    RAISE USING MESSAGE = v_log_message;
  END IF;

  -- Initial log entry (simulating DWMSG_ErzeugeEintrag)
  INSERT INTO `project.dataset.job_log` (eintragsnr, job_kennung, log_level, message, stichtag, restart_value, created_at)
  VALUES (v_eintragsnr, v_job_kennung, 'INFO', 'Job started', v_stichtag, CAST(v_wiederanlaufWert AS STRING), CURRENT_TIMESTAMP());

  -- Set Stichtag Info (simulating DWMSG_SetzeStichtagInfo)
  -- Already handled by passing v_stichtag to the job_log table.

  -- Main Job Execution Block with Error Handling (simulating trap ERR)
  BEGIN
    -- Log job parameters
    INSERT INTO `project.dataset.job_log` (eintragsnr, job_kennung, log_level, message, stichtag, restart_value, created_at)
    VALUES (v_eintragsnr, v_job_kennung, 'INFO', CONCAT('Job-Nr: ', CAST(v_eintragsnr AS STRING), ', JobKennung: ', v_job_kennung, ', Stichtag: ', v_stichtag, ', Wiederanlaufwert: ', CAST(v_wiederanlaufWert AS STRING)), v_stichtag, CAST(v_wiederanlaufWert AS STRING), CURRENT_TIMESTAMP());

    -- Invoke the core cleanup script (k_drop_temp_table.ksh equivalent)
    CALL `project.dataset.k_drop_temp_table`(
      v_job_kennung,
      v_stichtag,
      v_eintragsnr,
      v_wiederanlaufWert
    );

    -- If core script completes without error
    SET v_log_message = 'The processing completed without recognizable errors';
    INSERT INTO `project.dataset.job_log` (eintragsnr, job_kennung, log_level, message, stichtag, restart_value, created_at)
    VALUES (v_eintragsnr, v_job_kennung, 'INFO', v_log_message, v_stichtag, CAST(v_wiederanlaufWert AS STRING), CURRENT_TIMESTAMP());

    -- Update job status to OK (simulating DWMSG_SetzeStatusOK)
    INSERT INTO `project.dataset.job_status` (eintragsnr, job_kennung, status, updated_at)
    VALUES (v_eintragsnr, v_job_kennung, 'OK', CURRENT_TIMESTAMP());

  EXCEPTION WHEN ERROR THEN
    -- Error handling (simulating DWMSG_Fehlerbehandlung and trap ERR)
    SET v_err_nr = 999; -- Generic application error
    SET v_err_arg = @@error.message;
    SET v_log_message = CONCAT('AppError: Abbruch - ', @@error.message);
    SET v_status = 'ERROR';

    INSERT INTO `project.dataset.job_log` (eintragsnr, job_kennung, log_level, err_nr, err_arg, message, stichtag, restart_value, created_at)
    VALUES (v_eintragsnr, v_job_kennung, 'ERROR', v_err_nr, v_err_arg, v_log_message, v_stichtag, CAST(v_wiederanlaufWert AS STRING), CURRENT_TIMESTAMP());

    INSERT INTO `project.dataset.job_status` (eintragsnr, job_kennung, status, updated_at)
    VALUES (v_eintragsnr, v_job_kennung, 'ERROR', CURRENT_TIMESTAMP());

    -- Re-raise the error to indicate failure
    RAISE USING MESSAGE = v_log_message;
  END;
END;