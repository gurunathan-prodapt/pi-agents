-- Target File: sql/d_ausd_bp_ta_cntrct_evn.sql
-- Description: Truncates and populates sof_ta_cntrct_evn with aggregated base product event indicators.
-- Replaces Legacy Oracle script: d_ausd_bp_ta_cntrct_evn.sql

-- Step 1: Truncate Target Table
TRUNCATE TABLE `{{ var.value.get('gcp_project_id', 'your_gcp_project') }}.{{ var.value.get('gcp_dataset_name', 'isbert_schema') }}.sof_ta_cntrct_evn`;

-- Step 2: Insert Aggregated & Pivoted Event Indicators
INSERT INTO `{{ var.value.get('gcp_project_id', 'your_gcp_project') }}.{{ var.value.get('gcp_dataset_name', 'isbert_schema') }}.sof_ta_cntrct_evn` (
  cntrct_id,
  evn
)
SELECT
  cntrct_id,
  SUM(
    CASE bpr_id
      WHEN 32   THEN 1
      WHEN 2839 THEN 10
      WHEN 2506 THEN 2
      WHEN 2840 THEN 20
      WHEN 3055 THEN 3
      WHEN 3056 THEN 30
      WHEN 3821 THEN 4
      ELSE 0
    END
  ) AS evn
FROM
  `{{ var.value.get('gcp_project_id', 'your_gcp_project') }}.{{ var.value.get('gcp_dataset_name', 'isbert_schema') }}.sof_ta_bpr_evn` AS bpr
GROUP BY
  cntrct_id;