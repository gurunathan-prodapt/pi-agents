--
-- Target BigQuery Stored Procedure for k_ausd_v_ta_notice.ksh
-- Replaces vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_notice.ksh
--
CREATE OR REPLACE PROCEDURE `project.dataset.k_ausd_v_ta_notice`(
  IN p_job_kennung STRING,
  IN p_eintrags_nr INT64,
  IN v_datum STRING
)
BEGIN
  DECLARE v_records INT64 DEFAULT 0;

  -- Parameter validation
  IF p_job_kennung IS NULL OR p_job_kennung = '' THEN RAISE USING MESSAGE = 'FEHLER: Jobkennung fehlt'; END IF;
  IF p_eintrags_nr IS NULL THEN RAISE USING MESSAGE = 'FEHLER: EintragsNr fehlt'; END IF;

  -- Truncate the target table
  TRUNCATE TABLE `project.dataset.sof_ta_notice`;

  -- Insert data into the target table
  INSERT INTO `project.dataset.sof_ta_notice`
  (cntrct_id, valid_from, valid_to, entry_date_of_notice)
  SELECT
    n.cntrct_id, n.valid_from, n.valid_to, n.entry_date_of_notice
  FROM `project.dataset.cds_ta_notice` n
  WHERE n.insert_at <= PARSE_DATE('%Y%m%d', v_datum)
    AND (n.modified_at IS NULL OR n.modified_at > PARSE_DATE('%Y%m%d', v_datum))
    AND (n.valid_to IS NULL OR n.valid_to > PARSE_DATE('%Y%m%d', v_datum))
    AND n.is_production = 1;

  -- Get the count of inserted records
  SET v_records = (SELECT COUNT(*) FROM `project.dataset.sof_ta_notice`);

  -- Log the completion of data processing and record count
  INSERT INTO `project.dataset.job_log` (job_kennung, eintrags_nr, script_name, log_level, message, created_at)
  VALUES (UPPER(p_job_kennung), p_eintrags_nr, 'k_ausd_v_ta_notice', 'INFO', CONCAT('ENDE Datenverarbeitung. Records=', CAST(v_records AS STRING)), CURRENT_TIMESTAMP());
END;