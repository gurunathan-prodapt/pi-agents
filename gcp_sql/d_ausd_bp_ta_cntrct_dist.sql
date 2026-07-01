-- BigQuery SQL for ausd_bp_ta_cntrct_dist
-- Replaces d_ausd_bp_ta_cntrct_dist.sql and includes audit/parameter lookups.

DECLARE v_stichtag STRING DEFAULT NULL;
DECLARE v_job_kennung STRING DEFAULT 'BERT_DROP_TEMP_TABLE';
DECLARE v_datum STRING DEFAULT '19000101';

-- Step 00: Determine audit v_datum from the metadata audit table
SET v_datum = (
  SELECT COALESCE(FORMAT_TIMESTAMP('%Y%m%d', MAX(m.timecreated)), '19000101')
  FROM `{{ var.value.get('gcp_project_id', 'gcp-project-placeholder') }}.{{ var.value.get('bq_isbert_dataset', 'isbert_schema') }}.dwtk_meldungen` m
  WHERE m.job_kennung = v_job_kennung
);

-- Step 01: Normalize input parameters
SET v_stichtag = COALESCE(
  NULLIF('{{ dag_run.conf.get("stichtag", "") }}', ''),
  FORMAT_DATE('%d%m%Y', CURRENT_DATE())
);

-- Log executing parameter
SELECT FORMAT("Executing for stichtag: %s and audit datum: %s", v_stichtag, v_datum);

-- Step 02: Truncate Target Table
TRUNCATE TABLE `{{ var.value.get('gcp_project_id', 'gcp-project-placeholder') }}.{{ var.value.get('bq_sof_dataset', 'sof') }}.ta_cntrct_dist`;

-- Step 03: Insert distinct contracts
INSERT INTO `{{ var.value.get('gcp_project_id', 'gcp-project-placeholder') }}.{{ var.value.get('bq_sof_dataset', 'sof') }}.ta_cntrct_dist` (CNTRCT_ID)
SELECT DISTINCT cntrct_id
FROM `{{ var.value.get('gcp_project_id', 'gcp-project-placeholder') }}.{{ var.value.get('bq_sof_dataset', 'sof') }}.ta_bpr_basis`
WHERE cntrct_id IS NOT NULL;