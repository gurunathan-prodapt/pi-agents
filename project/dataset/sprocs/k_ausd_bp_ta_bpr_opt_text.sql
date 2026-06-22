--
-- Target: BigQuery Stored Procedure for core logic
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_opt_text.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_opt_text.ksh
--

CREATE OR REPLACE PROCEDURE `project.dataset.k_ausd_bp_ta_bpr_opt_text`(
  IN p_jobkennung STRING,
  IN p_stichtag STRING,
  IN p_eintragsnr INT64,
  IN p_wiederanlaufWert INT64
)
BEGIN
  -- This is where the core business logic from k_ausd_bp_ta_bpr_opt_text.ksh will be translated.
  -- Placeholder for example transformation:
  -- 1. Delete rows based on restart_value if applicable.
  -- 2. Select and insert contract cache data.

  IF p_wiederanlaufWert > 0 THEN
    DELETE FROM `project.dataset.fos_table`
    WHERE DWH_VERTRAG_ID >= p_wiederanlaufWert;
  END IF;

  INSERT INTO `project.dataset.fos_table`
  SELECT
    col1, col2, DWH_VERTRAG_ID -- Actual columns from source should be listed here
  FROM `project.dataset.contract_cache_source` -- Source table for contract cache
  WHERE
    gültig_von <= PARSE_DATE('%d%m%Y', p_stichtag)
    AND PARSE_DATE('%d%m%Y', p_stichtag) < gültig_bis
    AND ladedatum < PARSE_DATE('%d%m%Y', p_stichtag)
    AND (p_wiederanlaufWert = 0 OR DWH_VERTRAG_ID > p_wiederanlaufWert);

  -- Further logging or status updates can be added here, e.g.:
  INSERT INTO `project.dataset.job_log_audit`
    (entry_nr, job_name, script_name, status, message, created_at)
  VALUES
    (p_eintragsnr, p_jobkennung, 'k_ausd_bp_ta_bpr_opt_text', 'INFO', 'Core logic executed successfully', CURRENT_TIMESTAMP());

END;