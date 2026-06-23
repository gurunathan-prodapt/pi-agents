-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_dist.ksh
-- Purpose: Create BigQuery Stored Procedure for core business logic.
CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_cntrct_dist_core`(
  IN p_jobkennung STRING,
  IN p_stichtag STRING,
  IN p_eintragsnr INT64,
  IN p_wiederanlaufwert INT64
)
BEGIN
  DECLARE v_stichtag_date DATE;
  DECLARE v_restart_value INT64;

  SET v_restart_value = IFNULL(p_wiederanlaufwert, 0);
  SET v_stichtag_date = PARSE_DATE('%d%m%Y', p_stichtag);

  -- Assumed target table for FOS data
  DELETE FROM `project.dataset.fos_tabelle`
  WHERE dwh_vertrag_id >= v_restart_value;

  -- Assumed source table for contract cache data
  INSERT INTO `project.dataset.fos_tabelle`
  SELECT
    src.* -- Replace with specific column list for production
  FROM `project.dataset.dwh_vertrag_cache` src
  WHERE DATE(src.gueltig_von) <= v_stichtag_date
    AND v_stichtag_date < DATE(src.gueltig_bis)
    AND DATE(src.ladedatum) < v_stichtag_date
    AND src.dwh_vertrag_id > v_restart_value;
END;