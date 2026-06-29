-- ===================================================================
-- Target Platform: BigQuery Standard SQL
-- Description: Load valid basis product descriptions into sof_ta_bpr_beschr
-- Re-runability: Safe to restart at any time (includes Truncate-Load pattern)
-- ===================================================================

-- 1. Declare and extract the audit/drop date metadata from dwtk_meldungen
DECLARE v_datum STRING;

SET v_datum = (
  SELECT COALESCE(FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)), '19000101')
  FROM `gcp-bert-prod.isbert_schema.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

-- 2. Clear target table (Equivalent to Oracle TRUNCATE TABLE)
TRUNCATE TABLE `gcp-bert-prod.isbert_schema.sof_ta_bpr_beschr`;

-- 3. Extract, Join, and Load valid active base products
INSERT INTO `gcp-bert-prod.isbert_schema.sof_ta_bpr_beschr`
(
  BPR_ID,
  PDS_DESCRIPTION
)
SELECT
  bp.bpr_id,
  dbp.pds_description
FROM
  `gcp-bert-prod.isbert_schema.pds_ta_bpr` bp
INNER JOIN
  `gcp-bert-prod.isbert_schema.pds_ta_care_description` dbp
ON
  bp.pds_description_id = dbp.pds_description_id
WHERE
  bp.modified_at IS NULL
  AND bp.is_production = 1;

-- 4. Audit message indicating execution completed
SELECT FORMAT("Job completed. Audit Date evaluated: %s. Rows loaded into sof_ta_bpr_beschr.", v_datum);