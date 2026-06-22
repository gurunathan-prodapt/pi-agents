-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_c_bfc.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_c_bfc.ksh
CREATE OR REPLACE PROCEDURE `isrpt.dwmsg_setze_status_ok`(
  IN p_eintrags_nr INT64
)
BEGIN
  -- This procedure records a success message in the dw_job_log table for the given entry number.
  DECLARE v_job_kennung STRING;

  -- Attempt to get job_kennung from the last entry with this eintrags_nr
  SET v_job_kennung = (
    SELECT job_kennung
    FROM `isrpt.dw_job_log`
    WHERE eintrags_nr = p_eintrags_nr
    ORDER BY created_at DESC
    LIMIT 1
  );

  INSERT INTO `isrpt.dw_job_log`
  (job_kennung, eintrags_nr, log_level, log_text, created_at)
  VALUES
  (COALESCE(v_job_kennung, 'UNKNOWN_JOB'), p_eintrags_nr, 'I', 'Job beendet - OK', CURRENT_TIMESTAMP());
END;