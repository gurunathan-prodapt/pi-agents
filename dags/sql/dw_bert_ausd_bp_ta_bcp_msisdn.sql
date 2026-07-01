-- File: dags/sql/dw_bert_ausd_bp_ta_bcp_msisdn.sql
-- Purpose: Modular BigQuery Standard SQL for DW.BERT_AUSD_BP_TA_BCP_MSISDN
-- Notes:
--   * Source shell logic is not available; this script provides a reusable
--     migration scaffold with modular CTEs and procedures.
--   * Replace placeholder source/target tables and business rules with the
--     extracted logic from r_ausd_bp_ta_bcp_msisdn.ksh.
--   * Designed for idempotent execution using CREATE OR REPLACE / MERGE patterns.

DECLARE v_run_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
DECLARE v_run_date DATE DEFAULT CURRENT_DATE();
DECLARE v_batch_id STRING DEFAULT FORMAT_TIMESTAMP('%Y%m%d%H%M%S', v_run_ts);

-- ============================================================================
-- Reusable configuration via Airflow Templates
-- ============================================================================
DECLARE v_project_id STRING DEFAULT '{{ var.value.get("GCP_PROJECT_ID", "YOUR_GCP_PROJECT_ID") }}';
DECLARE v_dataset_bert STRING DEFAULT '{{ var.value.get("BQ_DATASET_BERT", "dw_bert") }}';
DECLARE v_dataset_staging STRING DEFAULT '{{ var.value.get("BQ_DATASET_STAGING", "dw_bert_staging") }}';

-- ============================================================================
-- Modular helper procedures
-- ============================================================================

CREATE TEMP PROCEDURE sp_log_step(step_name STRING, step_status STRING, step_message STRING)
BEGIN
  -- Insert into audit/log table if required
  SELECT
    v_batch_id AS batch_id,
    step_name,
    step_status,
    step_message,
    CURRENT_TIMESTAMP() AS logged_at;
END;

CREATE TEMP PROCEDURE sp_prepare_source()
BEGIN
  CALL sp_log_step('prepare_source', 'STARTED', 'Preparing source data for basis product processing');

  -- TODO: Replace with actual source extraction logic from the shell script.
  -- Example scaffold: normalize and stage source records.
  CREATE TEMP TABLE tmp_source AS
  SELECT
    CAST(NULL AS STRING) AS msisdn,
    CAST(NULL AS STRING) AS product_id,
    CAST(NULL AS STRING) AS customer_id,
    CAST(NULL AS DATE) AS valid_from,
    CAST(NULL AS DATE) AS valid_to,
    v_run_date AS process_date,
    v_batch_id AS batch_id
  WHERE FALSE;

  CALL sp_log_step('prepare_source', 'COMPLETED', 'Source staging completed');
END;

CREATE TEMP PROCEDURE sp_transform_basis_products()
BEGIN
  CALL sp_log_step('transform_basis_products', 'STARTED', 'Transforming instantiated basis products');

  -- TODO: Replace with actual transformation rules.
  CREATE TEMP TABLE tmp_transformed AS
  WITH base AS (
    SELECT
      msisdn,
      product_id,
      customer_id,
      valid_from,
      valid_to,
      process_date,
      batch_id
    FROM tmp_source
  ),
  enriched AS (
    SELECT
      b.*,
      COALESCE(valid_to, DATE '9999-12-31') AS valid_to_norm,
      CASE
        WHEN valid_from IS NULL THEN FALSE
        ELSE TRUE
      END AS is_valid_record
    FROM base b
  )
  SELECT
    msisdn,
    product_id,
    customer_id,
    valid_from,
    valid_to_norm AS valid_to,
    process_date,
    batch_id,
    is_valid_record
  FROM enriched;

  CALL sp_log_step('transform_basis_products', 'COMPLETED', 'Transformation completed');
END;

CREATE TEMP PROCEDURE sp_validate_output()
BEGIN
  CALL sp_log_step('validate_output', 'STARTED', 'Validating transformed output');

  -- Example validation scaffold
  ASSERT (
    SELECT COUNT(*) = 0
    FROM tmp_transformed
    WHERE msisdn IS NULL
  ) AS 'Validation failed: MSISDN must not be NULL';

  CALL sp_log_step('validate_output', 'COMPLETED', 'Validation completed');
END;

CREATE TEMP PROCEDURE sp_merge_target()
BEGIN
  CALL sp_log_step('merge_target', 'STARTED', 'Loading target table');

  -- Idempotent load strategy: MERGE for incremental/update loads
  MERGE `{{ var.value.get("GCP_PROJECT_ID", "YOUR_GCP_PROJECT_ID") }}.{{ var.value.get("BQ_DATASET_BERT", "dw_bert") }}.bert_ausd_bp_ta_bcp_msisdn` T
  USING (
    SELECT
      msisdn,
      product_id,
      customer_id,
      valid_from,
      valid_to,
      process_date,
      batch_id,
      is_valid_record
    FROM tmp_transformed
  ) S
  ON T.msisdn = S.msisdn
 AND T.product_id = S.product_id
  WHEN MATCHED THEN
    UPDATE SET
      customer_id = S.customer_id,
      valid_from = S.valid_from,
      valid_to = S.valid_to,
      process_date = S.process_date,
      batch_id = S.batch_id,
      is_valid_record = S.is_valid_record,
      updated_at = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN
    INSERT (
      msisdn,
      product_id,
      customer_id,
      valid_from,
      valid_to,
      process_date,
      batch_id,
      is_valid_record,
      created_at,
      updated_at
    )
    VALUES (
      S.msisdn,
      S.product_id,
      S.customer_id,
      S.valid_from,
      S.valid_to,
      S.process_date,
      S.batch_id,
      S.is_valid_record,
      CURRENT_TIMESTAMP(),
      CURRENT_TIMESTAMP()
    );

  CALL sp_log_step('merge_target', 'COMPLETED', 'Target load completed');
END;

CREATE TEMP PROCEDURE sp_cleanup()
BEGIN
  CALL sp_log_step('cleanup', 'STARTED', 'Cleaning up temporary artifacts');
  -- Temporary tables are session-scoped and auto-cleaned.
  CALL sp_log_step('cleanup', 'COMPLETED', 'Cleanup completed');
END;

-- ============================================================================
-- Main execution flow
-- ============================================================================
BEGIN
  CALL sp_log_step('main', 'STARTED', 'DW.BERT_AUSD_BP_TA_BCP_MSISDN execution started');

  CALL sp_prepare_source();
  CALL sp_transform_basis_products();
  CALL sp_validate_output();
  CALL sp_merge_target();
  CALL sp_cleanup();

  CALL sp_log_step('main', 'COMPLETED', 'DW.BERT_AUSD_BP_TA_BCP_MSISDN execution completed');
EXCEPTION WHEN ERROR THEN
  CALL sp_log_step('main', 'FAILED', @@error.message);
  RAISE USING MESSAGE = CONCAT('DW.BERT_AUSD_BP_TA_BCP_MSISDN failed: ', @@error.message);
END;