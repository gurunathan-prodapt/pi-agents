-- BigQuery Stored Procedure: sp_erzeugung_abzug_rechnungsdaten
-- Replaces vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_aurd_rechstan.ksh

CREATE OR REPLACE PROCEDURE `project.dataset.sp_erzeugung_abzug_rechnungsdaten`(
  IN p_stichtag STRING,              -- expected format: DDMMYYYY
  IN p_wiederanlaufWert INT64        -- restart threshold
)
BEGIN
  DECLARE v_sysdate STRING;
  DECLARE v_effective_stichtag STRING;
  DECLARE v_wiederanlaufWert INT64 DEFAULT 0;
  DECLARE v_jobkennung STRING DEFAULT 'BERT_RKOPF_STAN';
  DECLARE v_eintragsnr INT64;
  DECLARE v_logdateiname STRING;
  DECLARE v_status STRING DEFAULT 'RUNNING';
  DECLARE v_errmsg STRING DEFAULT NULL;

  -- Default restart value
  IF p_wiederanlaufWert IS NULL THEN
    SET v_wiederanlaufWert = 0;
  ELSE
    SET v_wiederanlaufWert = p_wiederanlaufWert;
  END IF;

  -- System date in DDMMYYYY
  SET v_sysdate = FORMAT_DATE('%d%m%Y', CURRENT_DATE());

  -- Default cutoff date if not provided
  IF p_stichtag IS NULL OR TRIM(p_stichtag) = '' THEN
    SET v_effective_stichtag = v_sysdate;
  ELSE
    SET v_effective_stichtag = p_stichtag;
  END IF;

  -- Validate required parameter
  IF v_effective_stichtag IS NULL OR TRIM(v_effective_stichtag) = '' THEN
    INSERT INTO `project.dataset.job_error_log`
      (job_kennung, log_ts, error_code, error_message, stichtag, wiederanlaufwert)
    VALUES
      (v_jobkennung, CURRENT_TIMESTAMP(), '193', 'Stichtag missing', v_effective_stichtag, v_wiederanlaufWert);
    RAISE USING MESSAGE = 'Stichtag missing';
  END IF;

  -- Create job entry number and log file name equivalent
  -- (Assuming job_control table manages entry numbers)
  SELECT IFNULL(MAX(eintragsnr), 0) + 1 INTO v_eintragsnr
  FROM `project.dataset.job_control`
  WHERE job_kennung = v_jobkennung;

  SET v_logdateiname = CONCAT(v_jobkennung, '_', CAST(v_eintragsnr AS STRING), '.log');

  -- Insert job start log
  INSERT INTO `project.dataset.job_control`
    (eintragsnr, job_kennung, script_name, logdateiname, stichtag, status, start_ts, sysdate)
  VALUES
    (v_eintragsnr, v_jobkennung, 'sp_erzeugung_abzug_rechnungsdaten', v_logdateiname,
     v_effective_stichtag, 'RUNNING', CURRENT_TIMESTAMP(), v_sysdate);

  BEGIN
    -- Core processing equivalent: This section should encapsulate the migrated logic from k_aurd_rechstan.ksh
    -- Placeholder for the actual data transformation and load logic.
    -- Example (as suggested by the source script's description):
    -- DELETE FROM `project.dataset.target_fos_table`
    -- WHERE dwh_vertrag_id >= v_wiederanlaufWert;

    -- INSERT INTO `project.dataset.target_fos_table`
    -- SELECT
    --   src.*
    -- FROM `project.dataset.source_contract_cache` AS src
    -- WHERE DATE(PARSE_DATE('%d%m%Y', src.gueltig_von)) <= PARSE_DATE('%d%m%Y', v_effective_stichtag)
    --   AND PARSE_DATE('%d%m%Y', v_effective_stichtag) < DATE(PARSE_DATE('%d%m%Y', src.gueltig_bis))
    --   AND DATE(PARSE_DATE('%d%m%Y', src.ladedatum)) < PARSE_DATE('%d%m%Y', v_effective_stichtag)
    --   AND src.dwh_vertrag_id > v_wiederanlaufWert;

    -- Placeholder for actual call to k_aurd_rechstan.ksh's migrated logic
    -- e.g., CALL `project.dataset.sp_k_aurd_rechstan`(v_jobkennung, v_effective_stichtag, v_eintragsnr, v_wiederanlaufWert);

    -- Assume success for wrapper logic demonstration
    SET v_status = 'OK';

    INSERT INTO `project.dataset.job_run_log`
      (eintragsnr, job_kennung, log_ts, message, status)
    VALUES
      (v_eintragsnr, v_jobkennung, CURRENT_TIMESTAMP(), 'Die Abarbeitung wurde ohne erkennbare Fehler beendet', 'OK');

    UPDATE `project.dataset.job_control`
    SET status = 'OK',
        end_ts = CURRENT_TIMESTAMP()
    WHERE eintragsnr = v_eintragsnr
      AND job_kennung = v_jobkennung;

  EXCEPTION WHEN ERROR THEN
    SET v_status = 'ERROR';
    SET v_errmsg = @@error.message;

    INSERT INTO `project.dataset.job_error_log`
      (eintragsnr, job_kennung, log_ts, error_code, error_message, stichtag, wiederanlaufwert)
    VALUES
      (v_eintragsnr, v_jobkennung, CURRENT_TIMESTAMP(), 'APP_ERROR', v_errmsg, v_effective_stichtag, v_wiederanlaufWert);

    UPDATE `project.dataset.job_control`
    SET status = 'ERROR',
        end_ts = CURRENT_TIMESTAMP(),
        error_message = v_errmsg
    WHERE eintragsnr = v_eintragsnr
      AND job_kennung = v_jobkennung;

    RAISE USING MESSAGE = v_errmsg;
  END;
END;