-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_c_bfc.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_c_bfc.ksh
CREATE OR REPLACE PROCEDURE `isrpt.dwmsg_logdateiname`(
  INOUT p_log_datei STRING,
  IN p_job_kennung STRING,
  IN p_eintrags_nr INT64
)
BEGIN
  -- In BigQuery, logging is done by inserting into a table, so 'log_datei' is a conceptual identifier.
  -- This procedure constructs a descriptive string that represents the log "file" name in the legacy system.
  SET p_log_datei = CONCAT(
      'bigquery://', p_job_kennung, '_',
      FORMAT_DATE('%Y%m%d', CURRENT_DATE()), '_',
      CAST(p_eintrags_nr AS STRING), '.log_entry'
  );
END;