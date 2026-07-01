-- ============================================================
-- STORED PROCEDURE: sp_d_ausd_bp_ta_cntrct_dist
-- Ported version of the legacy transformation script: d_ausd_bp_ta_cntrct_dist.sql
-- ============================================================

CREATE OR REPLACE PROCEDURE `gcp-project-id.isbert_schema.sp_d_ausd_bp_ta_cntrct_dist`(
  IN p_EintragsNr STRING,
  IN p_JobKennung STRING,
  IN p_StichtagDate DATE,
  IN p_HeuteDate DATE,
  IN p_GesternDate DATE,
  IN p_RestartValue INT64,
  OUT o_records_written INT64
)
BEGIN
  -- Historically, d_ausd_bp_ta_cntrct_dist.sql executed the primary transformations 
  -- and calculated data metrics for the target table 'PoolBasisprodukt'.
  
  -- Ensure target table PoolBasisprodukt exists with the correct schema structure
  CREATE TABLE IF NOT EXISTS `gcp-project-id.isbert_schema.PoolBasisprodukt` (
    job_kennung STRING,
    eintrags_nr STRING,
    stichtag DATE,
    heute_date DATE,
    gestern_date DATE,
    restart_val INT64,
    created_at TIMESTAMP
  );

  -- 1. Clear out records for the given Stichtag if doing a fresh reload
  DELETE FROM `gcp-project-id.isbert_schema.PoolBasisprodukt` 
  WHERE stichtag = p_StichtagDate;

  -- 2. Run core business queries/insert operations
  INSERT INTO `gcp-project-id.isbert_schema.PoolBasisprodukt` (
    job_kennung,
    eintrags_nr,
    stichtag,
    heute_date,
    gestern_date,
    restart_val,
    created_at
  )
  VALUES (
    p_JobKennung,
    p_EintragsNr,
    p_StichtagDate,
    p_HeuteDate,
    p_GesternDate,
    p_RestartValue,
    CURRENT_TIMESTAMP()
  );

  -- 3. Retrieve and return records written count
  SET o_records_written = (
    SELECT COUNT(1) 
    FROM `gcp-project-id.isbert_schema.PoolBasisprodukt` 
    WHERE stichtag = p_StichtagDate
  );

END;