-- ===================================================================
-- Target Platform: Google BigQuery
-- Refactored from: d_ausd_bp_ta_tarifoption.sql
-- Replaces legacy: d_ausd_bp_ta_tarifoption.sql
-- Job:            DW.BERT_AUSD_BP_TA_TARIFOPTION
-- Description:     Prepares and aggregates contract tariff options
-- ===================================================================

DECLARE v_datum STRING;

-- Step 1: Identify the run-date suffix from the metadata table
SET v_datum = (
  SELECT COALESCE(FORMAT_TIMESTAMP('%Y%m%d', MAX(timecreated)), '19000101')
  FROM `target_project.target_dataset.dwtk_meldungen`
  WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
);

-- Step 2: Build staging/intermediate table with joined categories dynamically
EXECUTE IMMEDIATE FORMAT("""
  CREATE OR REPLACE TABLE `target_project.target_dataset.sof_ta_bpr_opt_filter` AS
  SELECT
    t.bpr_id,
    t.cntrct_id,
    t.pds_description,
    l.opt_kategorie
  FROM
    `target_project.target_dataset.sof_ta_l_bpr_optionen_filter` l
  JOIN
    `target_project.target_dataset.sof_ta_bpr_opt_text_%s` t
  ON
    t.bpr_id = l.bpr_id
""", v_datum);

-- Step 3: Pivot and aggregate option strings by contract ID into final table
CREATE OR REPLACE TABLE `target_project.target_dataset.sof_ta_tarifoption` AS
SELECT
  cntrct_id,
  SUBSTR(
    STRING_AGG(
      IF(opt_kategorie = 'BUDGET', pds_description, NULL), 
      ', ' ORDER BY pds_description
    ), 1, 500
  ) AS business_option,
  SUBSTR(
    STRING_AGG(
      IF(opt_kategorie = 'SONST', pds_description, NULL), 
      ', ' ORDER BY pds_description
    ), 1, 500
  ) AS sonstige_option,
  SUBSTR(
    STRING_AGG(
      IF(opt_kategorie = 'GPRS', pds_description, NULL), 
      ', ' ORDER BY pds_description
    ), 1, 500
  ) AS gprs_option
FROM
  `target_project.target_dataset.sof_ta_bpr_opt_filter`
GROUP BY
  cntrct_id;