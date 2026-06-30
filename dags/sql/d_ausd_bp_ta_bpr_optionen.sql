-- ===================================================================
-- Target File: d_ausd_bp_ta_bpr_optionen.sql
-- Path: dags/sql/d_ausd_bp_ta_bpr_optionen.sql
-- Purpose: Clear and reload contract option IDs
-- Legacy Source: d_ausd_bp_ta_bpr_optionen.sql (Oracle)
-- ===================================================================

-- Note: v_datum is defined to preserve legacy auditing logic, but is not used in the insert step.
DECLARE v_carmen STRING DEFAULT '@pcrs1';
DECLARE v_datum STRING;

SET v_datum = (
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(DATE(m.timecreated))), '19000101')
  FROM `{{ var.value.gcp_project_id }}.{{ var.value.bq_dataset }}.dwtk_meldungen` AS m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
);

-- Step 1: Truncate Target Table
TRUNCATE TABLE `{{ var.value.gcp_project_id }}.{{ var.value.bq_dataset }}.sof_ta_bpr_optionen`;

-- Step 2: Insert options from instantiated base products
INSERT INTO `{{ var.value.gcp_project_id }}.{{ var.value.bq_dataset }}.sof_ta_bpr_optionen` (
  cntrct_id,
  bpr_id
)
SELECT
  bp.cntrct_id,
  bp.bpr_id
FROM `{{ var.value.gcp_project_id }}.{{ var.value.bq_dataset }}.sof_ta_bpr_instance` AS bp;