-- Original file: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_bpr_beschr.sql
-- Legacy Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_beschr.ksh
-- This BigQuery Stored Procedure migrates the logic from d_ausd_bp_ta_bpr_beschr.sql.

CREATE OR REPLACE PROCEDURE `project.dataset.d_ausd_bp_ta_bpr_beschr`(
  IN p_EintragsNr STRING,
  IN p_JobKennung STRING,
  IN p_Stichtag STRING, -- Expected format DDMMYYYY from k_ausd_bp_ta_bpr_beschr
  IN v_restart STRING,
  IN v_TabName STRING,
  IN v_datum_heute DATE,
  IN v_datum_gestern DATE
)
BEGIN
  -- BigQuery equivalent of DEFINE v_carmen and schema references.
  -- The original SQL uses "@pcrs1" which implies a database link or schema prefix.
  -- In BigQuery, we use fully qualified table names: project.dataset.table.

  -- BigQuery equivalent of COLUMN s_datum new_value v_datum noprint and SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101')
  -- This variable was used to generate a date suffix for table names in older versions,
  -- but the current script seems to have removed its direct use in the INSERT based on revision history.
  -- It is retained here for contextual completeness, but not directly used in the main DML.
  DECLARE v_datum_from_meldungen STRING;
  SET v_datum_from_meldungen = (
    SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
    FROM `dw_source.isrpt.dwtk_meldungen` AS m
    WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
  );

  -- Step01: Delete / Truncate target table
  -- Original: isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_bpr_beschr REUSE STORAGE');
  -- BigQuery: TRUNCATE TABLE statement
  TRUNCATE TABLE `dw_target.isrpt.sof_ta_bpr_beschr`;

  -- Step04: Insert valid basis product descriptions
  INSERT INTO `dw_target.isrpt.sof_ta_bpr_beschr`
  (BPR_ID, PDS_DESCRIPTION)
  SELECT
      bp.bpr_id,
      dbp.pds_description
  FROM
      `dw_source.isrpt.pds_ta_bpr` AS bp
  JOIN
      `dw_source.isrpt.pds_ta_care_description` AS dbp -- Assuming pds_ta_care_description is also in dw_source.isrpt
      ON bp.pds_description_id = dbp.pds_description_id
  WHERE
      bp.modified_at IS NULL
      AND bp.is_production = 1;

  -- BigQuery transactions are per statement or block. No explicit COMMIT needed.

  -- Note: The original script used `spool` for logging.
  -- In BigQuery, results can be logged to a dedicated audit table or via external orchestrator.
  -- The calling procedure (k_ausd_bp_ta_bpr_beschr) is responsible for capturing record counts.

  -- The original script had an 'exit success' which is implicit in BigQuery stored procedures
  -- if no error is raised.

END;