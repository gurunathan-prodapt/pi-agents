--
-- Target BigQuery Stored Procedure for r_ausd_v_ta_notice.ksh
-- Replaces vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_notice.ksh
--
CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_v_ta_notice`(
  IN p_job_kennung STRING,
  IN p_eintrags_nr INT64
)
BEGIN
  DECLARE v_datum STRING;
  DECLARE v_job_kennung STRING DEFAULT UPPER('BERT_V_TA_NOTICE');

  INSERT INTO `project.dataset.job_log` (job_kennung, eintrags_nr, script_name, log_level, message, created_at)
  VALUES (v_job_kennung, p_eintrags_nr, 'r_ausd_v_ta_notice', 'INFO', 'Job started', CURRENT_TIMESTAMP());

  BEGIN
    -- Determine the processing cutoff date from dwtk_meldungen
    SET v_datum = (SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(DATE(timecreated))), '19000101') FROM `project.dataset.dwtk_meldungen` WHERE job_kennung = 'BERT_DROP_TEMP_TABLE');

    -- Call the core logic stored procedure
    CALL `project.dataset.k_ausd_v_ta_notice`(p_job_kennung, p_eintrags_nr, v_datum);

    INSERT INTO `project.dataset.job_log` (job_kennung, eintrags_nr, script_name, log_level, message, created_at)
    VALUES (v_job_kennung, p_eintrags_nr, 'r_ausd_v_ta_notice', 'INFO', 'Die Abarbeitung wurde ohne erkennbare Fehler beendet', CURRENT_TIMESTAMP());
  EXCEPTION WHEN ERROR THEN
    INSERT INTO `project.dataset.job_log` (job_kennung, eintrags_nr, script_name, log_level, message, created_at)
    VALUES (v_job_kennung, p_eintrags_nr, 'r_ausd_v_ta_notice', 'ERROR', 'Abbruch wegen Fehler', CURRENT_TIMESTAMP());
    RAISE USING MESSAGE = 'AppError: Abbruch';
  END;
END;