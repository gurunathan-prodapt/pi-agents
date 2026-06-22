-- Migrated from: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_disc_zusgf.sql
-- Job: BERT_V_TA_DISC_ZUSGF
-- Purpose: Concatenate discount descriptions from sof_ta_discount into sof_ta_disc_zusgf.

-- All table names are placeholders and assume prior migration to BigQuery.
-- Replace `gcp_project_id.dwh_prod` with your actual GCP project ID and BigQuery dataset.

DECLARE v_datum STRING DEFAULT (
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')
  FROM `gcp_project_id.dwh_prod.dwtk_meldungen` m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

-- Step 2: Clear target contents. This replicates the TRUNCATE functionality.
TRUNCATE TABLE `gcp_project_id.dwh_prod.sof_ta_disc_zusgf`;

-- Step 3: Populate target table with concatenated discount information.
INSERT INTO `gcp_project_id.dwh_prod.sof_ta_disc_zusgf` (
  cntrct_id,
  cntrct_obj_version,
  disc_vector_ty,
  rabatt_alle
)
WITH distinct_vectors AS (
  -- Identify unique contract-discount vector combinations
  SELECT DISTINCT
    cntrct_id,
    disc_vector_ty,
    cntrct_obj_version
  FROM `gcp_project_id.dwh_prod.sof_ta_discount`
),
prepared_discounts AS (
  -- Prepare the discount text for concatenation, applying the 500-char limit for individual parts
  SELECT
    cntrct_id,
    cntrct_obj_version,
    SUBSTR(CONCAT(rabatt, ' (', CAST(rabatthoehe AS STRING), '%)'), 1, 500) AS rabatt_text
  FROM `gcp_project_id.dwh_prod.sof_ta_discount`
),
aggregated_discounts AS (
  -- Aggregate and concatenate the discount texts per contract
  SELECT
    cntrct_id,
    cntrct_obj_version,
    STRING_AGG(rabatt_text, ', ' ORDER BY rabatt_text) AS rabatt_alle
  FROM prepared_discounts
  GROUP BY
    cntrct_id,
    cntrct_obj_version
)
SELECT
  dv.cntrct_id,
  dv.cntrct_obj_version,
  dv.disc_vector_ty,
  -- Ensure the final concatenated string also adheres to the 500-character limit
  SUBSTR(ad.rabatt_alle, 1, 500) AS rabatt_alle
FROM distinct_vectors dv
LEFT JOIN aggregated_discounts ad
  ON dv.cntrct_id = ad.cntrct_id
 AND dv.cntrct_obj_version = ad.cntrct_obj_version;