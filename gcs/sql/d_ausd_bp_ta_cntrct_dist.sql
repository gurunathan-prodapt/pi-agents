-- File: gcs/sql/d_ausd_bp_ta_cntrct_dist.sql
-- Purpose: Load distinct contract IDs from basis product source into target table.
-- Notes:
--   * Oracle $-style identifiers are sanitized to BigQuery-safe names.
--   * Parallel hints and utility package calls are removed.

-- Step 1: Truncate the target table
TRUNCATE TABLE `{{ var.value.gcp_project_id }}.{{ var.value.bq_dataset }}.sof_ta_cntrct_dist`;

-- Step 2: Populate the target table with distinct contract IDs
INSERT INTO `{{ var.value.gcp_project_id }}.{{ var.value.bq_dataset }}.sof_ta_cntrct_dist` (cntrct_id)
SELECT DISTINCT
  cntrct_id
FROM `{{ var.value.gcp_project_id }}.{{ var.value.bq_dataset }}.sof_ta_bpr_basis`
WHERE cntrct_id IS NOT NULL;