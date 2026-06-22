-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_c_bfc.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_c_bfc.ksh
CREATE OR REPLACE PROCEDURE `isrpt.BERT_V_TA_C_BFC`(
  IN p_h STRING, -- Parameter for help message ('h')
  IN p_s STRING, -- Placeholder for -s parameter
  IN p_l STRING  -- Placeholder for -l parameter
)
OPTIONS(
  description="Migrated KSH wrapper script r_ausd_v_ta_c_bfc.ksh for updating ta_c_bfc"
)
BEGIN
  DECLARE ProgName STRING DEFAULT 'Bindefristcache';
  DECLARE ProgVersion STRING DEFAULT 'V1.0.0';
  DECLARE JobKennung STRING DEFAULT 'BERT_V_TA_C_BFC';
  DECLARE v_sysdate STRING DEFAULT FORMAT_DATE('%d%m%Y', CURRENT_DATE());
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';
  DECLARE DW_EintragsNr INT64 DEFAULT 0;
  DECLARE LogDatei STRING; -- Will be populated by dwmsg_logdateiname
  DECLARE Name_Kernskript STRING DEFAULT 'isrpt.k_ausd_v_ta_c_bfc';
  DECLARE v_status STRING DEFAULT 'INIT';

  -- Handle help parameter (-h)
  IF p_h IS NOT NULL AND p_h = 'h' THEN
    SELECT
      ProgName AS Programm,
      ProgVersion AS Version,
      'Aufruf: Parameter' AS Aufruf,
      '-h zeigt diese Seite an' AS Hilfe,
      '-s <string>  -- Placeholder for option s',
      '-l <string>  -- Placeholder for option l';
    LEAVE; -- Exit the procedure
  END IF;

  -- Validate required parameters (-s and -l)
  IF p_s IS NULL OR p_l IS NULL THEN
    SET ErrNr = 193; -- Example error number for missing parameter
    SET ErrArg = IF(p_s IS NULL, 's', 'l'); -- Identify the missing parameter
  END IF;

  -- If parameter error occurred, log it and exit
  IF ErrNr != 0 THEN
    INSERT INTO `isrpt.dw_job_log`
    (job_kennung, eintrags_nr, log_level, err_nr, err_arg, log_text, created_at)
    VALUES
    (JobKennung, DW_EintragsNr, 'E', ErrNr, ErrArg, CONCAT('Parameterfehler: Missing required parameter -', ErrArg), CURRENT_TIMESTAMP());

    -- Display usage message as per original design
    SELECT 'usage' AS action, ProgName AS programm, ProgVersion AS version, CONCAT('Missing parameter: -', ErrArg) AS error_detail;
    LEAVE;
  END IF;

  -- Main job execution block with error handling
  BEGIN
    -- Initialize job entry number and log file identifier
    CALL `isrpt.dwmsg_ermittle_nr`(DW_EintragsNr);
    CALL `isrpt.dwmsg_logdateiname`(LogDatei, JobKennung, DW_EintragsNr);

    -- Log job start
    INSERT INTO `isrpt.dw_job_log`
    (job_kennung, eintrags_nr, log_level, log_text, created_at)
    VALUES
    (JobKennung, DW_EintragsNr, 'I', CONCAT('Jobstart: ', CURRENT_USER(), ' (Params: -s=', p_s, ', -l=', p_l, ')'), CURRENT_TIMESTAMP());

    -- Log stichtag information
    INSERT INTO `isrpt.dw_job_log`
    (job_kennung, eintrags_nr, log_level, log_text, stichtag, created_at)
    VALUES
    (JobKennung, DW_EintragsNr, 'I', 'SetzeStichtagInfo', v_sysdate, CURRENT_TIMESTAMP());

    -- Mimicking the shell script's output to stdout/tee for job details
    SELECT
      ' ----------------- Job -----------------------' AS line
    UNION ALL SELECT CONCAT(' Job-Nr    : \'', CAST(DW_EintragsNr AS STRING), '\'')
    UNION ALL SELECT CONCAT(' JobKennung: \'', JobKennung, '\'')
    UNION ALL SELECT CONCAT(' Logdatei  : \'', LogDatei, '\'')
    UNION ALL SELECT ' ---------------------------------------------';

    -- Call to the migrated core script (k_ausd_v_ta_c_bfc)
    -- Using EXECUTE IMMEDIATE to call a procedure whose name is stored in a variable
    EXECUTE IMMEDIATE FORMAT('CALL `%s`(?, ?)', Name_Kernskript) USING JobKennung, DW_EintragsNr;

    -- Log successful completion message
    INSERT INTO `isrpt.dw_job_log`
    (job_kennung, eintrags_nr, log_level, log_text, created_at)
    VALUES
    (JobKennung, DW_EintragsNr, 'I', 'Die Abarbeitung wurde ohne erkennbare Fehler beendet', CURRENT_TIMESTAMP());

    -- Update job status to OK using the utility procedure
    CALL `isrpt.dwmsg_setze_status_ok`(DW_EintragsNr);

    SET v_status = 'OK';

  EXCEPTION WHEN ERROR THEN
    -- Error handling block
    INSERT INTO `isrpt.dw_job_log`
    (job_kennung, eintrags_nr, log_level, log_text, created_at)
    VALUES
    (JobKennung, DW_EintragsNr, 'E', CONCAT('AppError: Abbruch - ', @@error.message), CURRENT_TIMESTAMP());

    SET v_status = 'ERROR';
    -- Re-raise the error to inform the caller
    RAISE USING MESSAGE = CONCAT('Execution failed for job ', JobKennung, ': ', @@error.message);
  END;

  -- Final status output (can be captured by an orchestrator)
  SELECT v_status AS job_status;
END;