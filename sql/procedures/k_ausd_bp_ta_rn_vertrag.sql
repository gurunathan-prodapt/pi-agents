-- Legacy source: Inferred from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_vertrag.ksh
-- Description: BigQuery SQL stored procedure for the core data processing logic.
CREATE OR REPLACE PROCEDURE `project.dataset.k_ausd_bp_ta_rn_vertrag`(
  IN p_job_kennung STRING,
  IN p_stichtag STRING,
  IN p_eintragsnr INT64,
  IN p_wiederanlaufWert INT64
)
OPTIONS(
  description="Implements the core data manipulation logic for contract snapshot creation, translating k_ausd_bp_ta_rn_vertrag.ksh."
)
BEGIN
  DECLARE v_stichtag_date DATE;
  DECLARE v_restart_value INT64 DEFAULT IFNULL(p_wiederanlaufWert, 0);

  -- Convert the stichtag string (DDMMYYYY) to a DATE type.
  -- This will throw an error if the format is invalid.
  SET v_stichtag_date = PARSE_DATE('%d%m%Y', p_stichtag);

  -- Perform conditional delete operation based on the restart value.
  -- This mimics the 'delete then insert' pattern for restart functionality.
  DELETE FROM `project.dataset.fos_vertrag`
  WHERE dwh_vertrag_id >= v_restart_value;

  -- Insert records into the target table based on the restart value and date conditions.
  INSERT INTO `project.dataset.fos_vertrag`
  SELECT
    src.* -- Select all columns from the source, assuming schema compatibility
  FROM
    `project.dataset.ta_vertrag_cache` AS src
  WHERE
    src.dwh_vertrag_id > v_restart_value
    AND src.gueltig_von <= v_stichtag_date
    AND v_stichtag_date < src.gueltig_bis
    AND src.ladedatum < v_stichtag_date;

  -- No explicit return value needed, success implies successful execution of DML.
END;