-- ===================================================================
-- File:  sof_ta_disc_zusgf_load.sql
-- Job:   BERT_V_TA_DISC_ZUSGF
-- Target: BigQuery
-- Replaces: d_ausd_v_ta_disc_zusgf.sql, sof$sp_discount_functions
-- ===================================================================

CREATE OR REPLACE TABLE `your_project.bert_dataset.sof_ta_disc_zusgf`
OPTIONS(
  description="Aggregated and concatenated discount descriptions per contract and contract version."
) AS
WITH dzg AS (
  -- Identify distinct combinations of contract and contract versions
  SELECT DISTINCT
    cntrct_id,
    cntrct_obj_version,
    disc_vector_ty
  FROM `your_project.bert_dataset.sof_ta_discount`
),
con AS (
  -- Extract and aggregate distinct discounts per contract version
  SELECT
    cntrct_id,
    cntrct_obj_version,
    -- Truncate to 500 characters to match legacy target schema limit
    SUBSTR(
      STRING_AGG(single_rabatt, ', ' ORDER BY single_rabatt), 
      1, 
      500
    ) AS rabatt_alle
  FROM (
    SELECT DISTINCT
      cntrct_id,
      cntrct_obj_version,
      CONCAT(rabatt, ' (', CAST(rabatthoehe AS STRING), '%)') AS single_rabatt
    FROM `your_project.bert_dataset.sof_ta_discount`
    WHERE rabatt IS NOT NULL
  )
  GROUP BY cntrct_id, cntrct_obj_version
)
SELECT
  dzg.cntrct_id,
  dzg.cntrct_obj_version,
  dzg.disc_vector_ty,
  con.rabatt_alle
FROM dzg
LEFT JOIN con
  ON dzg.cntrct_id = con.cntrct_id
 AND dzg.cntrct_obj_version = con.cntrct_obj_version;