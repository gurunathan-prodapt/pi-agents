--
-- BigQuery Stored Procedure for core SQL logic
-- Replaces: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_disc_zusgf.sql
--
CREATE OR REPLACE PROCEDURE `my_project.my_dataset.d_ausd_v_ta_disc_zusgf_sp`(
  IN p_EintragsNr STRING,
  IN p_JobKennung STRING,
  IN v_TabName STRING,
  OUT p_records_processed INT64
)
BEGIN
  -- Oracle specific: DEFINE v_carmen and v_datum. v_carmen is a DB_LINK, not needed.
  -- v_datum logic: SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum FROM isbert_schema.dwtk_meldungen m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';
  -- This value isn't actually used in the main INSERT statement, only defined.
  -- So, we can omit it for now, or declare it if future logic depends on it.
  -- For now, it's not directly used by the INSERT.

  -- TRUNCATE TABLE sof$ta_disc_zusgf;
  TRUNCATE TABLE `my_project.my_dataset.ta_disc_zusgf`;

  -- Oracle specific: ALTER SESSION ENABLE PARALLEL DML; ALTER SESSION SET optimizer_dynamic_sampling = 4;
  -- Not needed in BigQuery.

  -- Core INSERT logic with translation of Oracle's pipelined table function
  INSERT INTO `my_project.my_dataset.ta_disc_zusgf`
      (cntrct_id, cntrct_obj_version, disc_vector_ty, rabatt_alle)
  SELECT
       dzg.cntrct_id,
       dzg.cntrct_obj_version,
       dzg.disc_vector_ty,
       con.rabatt_alle
  FROM (
        SELECT
               DISTINCT
               cntrct_id,
               disc_vector_ty,
               cntrct_obj_version
          FROM `my_project.my_dataset.ta_discount`
       ) dzg
  LEFT JOIN ( -- Replaces TABLE(sof$sp_discount_functions.concat_discounts(CURSOR(...)))
      SELECT
          cntrct_id,
          cntrct_obj_version,
          -- Simulate concatenation with length limit (500)
          SUBSTR(STRING_AGG(rabatt_formatted ORDER BY rabatt_formatted), 1, 500) AS rabatt_alle
      FROM (
          SELECT
              cntrct_id,
              cntrct_obj_version,
              CONCAT(rabatt, ' (', CAST(rabatthoehe AS STRING), '%)') AS rabatt_formatted
          FROM `my_project.my_dataset.ta_discount`
      )
      GROUP BY
          cntrct_id,
          cntrct_obj_version
  ) con
    ON dzg.cntrct_id          = con.cntrct_id
   AND dzg.cntrct_obj_version = con.cntrct_obj_version;

  -- Get the number of rows processed
  SET p_records_processed = (SELECT COUNT(1) FROM `my_project.my_dataset.ta_disc_zusgf`);

  -- Oracle specific: COMMIT; -- BigQuery DML is atomic, no explicit commit needed.
  -- Oracle specific: ANALYZE TABLE sof$ta_disc_zusgf; -- BigQuery manages statistics automatically.

END;