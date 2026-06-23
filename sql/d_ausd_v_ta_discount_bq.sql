-- BigQuery SQL translation of d_ausd_v_ta_discount.sql
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_discount.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount.ksh

-- This script will be integrated into the main BigQuery Stored Procedure.
-- It performs the core data processing logic.

-- Placeholder for v_datum. In the SP, this will be a declared variable.
-- DECLARE v_datum STRING DEFAULT (
--   SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(DATE(timecreated))), '19000101')
--   FROM `project_id.dataset_id.dwtk_meldungen`
--   WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
-- );

-- Core INSERT/SELECT logic.
-- The original script used `INSERT INTO sof$ta_discount`.
-- The translated script uses `CREATE OR REPLACE TABLE`.
-- For integration into a stored procedure, we might want to `TRUNCATE` then `INSERT`,
-- or use a `MERGE` statement if only updating changed records.
-- Given the original had a `TRUNCATE`, `CREATE OR REPLACE TABLE` or `TRUNCATE` + `INSERT` is appropriate.
-- This translated output uses CREATE OR REPLACE TABLE.

CREATE OR REPLACE TABLE `project_id.dataset_id.ta_discount`
CLUSTER BY cntrct_id, discount_id AS
SELECT
  da.cntrct_id,
  da.discount_id,
  d.disc_vector_ty,
  da.cntrct_obj_version,
  cd.cds_description AS rabatt,
  CAST(dv.calc_rule_value AS STRING) AS rabatthoehe
FROM `project_id.dataset_id.cds_ta_discount_bc_assoc` AS da
JOIN `project_id.dataset_id.cds_ta_discount` AS d
  ON da.discount_id = d.discount_id
JOIN `project_id.dataset_id.cds_ta_care_description` AS cd
  ON cd.cds_description_id = d.cds_description_id
 AND cd.language = 1
JOIN `project_id.dataset_id.cds_ta_disc_vector` AS dv
  ON d.discount_id = dv.discount_id
 AND d.disc_vector_ty = dv.disc_vector_ty
 AND d.obj_version = dv.discount_obj_version
WHERE da.insert_at <= PARSE_DATE('%Y%m%d', @run_date_str) -- Using @run_date_str as a placeholder for v_datum
  AND (da.modified_at IS NULL OR da.modified_at > PARSE_DATE('%Y%m%d', @run_date_str))
  AND d.insert_at <= PARSE_DATE('%Y%m%d', @run_date_str)
  AND (d.modified_at IS NULL OR d.modified_at > PARSE_DATE('%Y%m%d', @run_date_str))
  AND d.valid_from <= PARSE_DATE('%Y%m%d', @run_date_str)
  AND (d.valid_to IS NULL OR d.valid_to > PARSE_DATE('%Y%m%d', @run_date_str))
  AND dv.insert_at <= PARSE_DATE('%Y%m%d', @run_date_str)
  AND (dv.modified_at IS NULL OR dv.modified_at > PARSE_DATE('%Y%m%d', @run_date_str))
  AND d.is_production = 1;