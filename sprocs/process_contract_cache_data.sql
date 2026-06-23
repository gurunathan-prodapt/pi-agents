-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_beschr.ksh
-- Target BigQuery Stored Procedure for core business logic (migrated from k_ausd_bp_ta_bpr_beschr.ksh).

CREATE OR REPLACE PROCEDURE `project.dataset.process_contract_cache_data`(
  IN p_stichtag STRING,
  IN p_restart_value INT64,
  IN p_job_nr INT64 -- Included as per build plan, though not explicitly used in this snippet
)
BEGIN
  -- Optional restart cleanup: Delete from target table based on restart_value
  IF p_restart_value > 0 THEN
    DELETE FROM `project.dataset.FOS_TABLE`
    WHERE DWH_VERTRAG_ID >= p_restart_value;
  END IF;

  -- Insert/Merge into target table based on criteria
  -- Note: Column names 'column1', 'column2' are placeholders and should be replaced with actual column names.
  INSERT INTO `project.dataset.FOS_TABLE` (column1, column2, DWH_VERTRAG_ID)
  SELECT
    src.column1, src.column2, src.DWH_VERTRAG_ID
  FROM `project.dataset.DWH_TA_C_VERTRAG` src
  WHERE src.Gueltig_von <= PARSE_DATE('%d%m%Y', p_stichtag)
    AND PARSE_DATE('%d%m%Y', p_stichtag) < src.Gueltig_bis
    AND src.Ladedatum < PARSE_DATE('%d%m%Y', p_stichtag)
    AND src.DWH_VERTRAG_ID > p_restart_value;
END;