-- Reconciliation SQL Script for ausd_bp_ta_ibcp_ccid
-- Legacy Source: Oracle GL_CODE_COMBINATIONS / IBCP_STAGE_TXN
-- Job: ausd_bp_ta_ibcp_ccid

-- Compare staging table row counts against target core tables to ensure complete load
WITH staging_metrics AS (
  SELECT 
    'dim_ccid_ibcp' AS table_name,
    COUNT(*) AS staging_row_count,
    COUNT(DISTINCT code_combination_id) AS staging_distinct_keys,
    0 AS staging_total_amount
  FROM `prj-ausd-stage-gcp.bq_stage_ta.stg_oracle_ccid`
  WHERE segment1 IN ('AU', '080')
  UNION ALL
  SELECT
    'fact_ibcp_ledger' AS table_name,
    COUNT(*) AS staging_row_count,
    COUNT(DISTINCT txn_id) AS staging_distinct_keys,
    SUM(COALESCE(txn_amount, 0)) AS staging_total_amount
  FROM `prj-ausd-stage-gcp.bq_stage_ta.stg_ibcp_txns`
  WHERE entity_code IN ('AU', '080')
),
core_metrics AS (
  SELECT 
    'dim_ccid_ibcp' AS table_name,
    COUNT(*) AS core_row_count,
    COUNT(DISTINCT code_combination_id) AS core_distinct_keys,
    0 AS core_total_amount
  FROM `prj-ausd-core-gcp.finance_ta.dim_ccid_ibcp`
  WHERE entity_code IN ('AU', '080')
  UNION ALL
  SELECT
    'fact_ibcp_ledger' AS table_name,
    COUNT(*) AS core_row_count,
    COUNT(DISTINCT txn_id) AS core_distinct_keys,
    SUM(COALESCE(txn_amount, 0)) AS core_total_amount
  FROM `prj-ausd-core-gcp.finance_ta.fact_ibcp_ledger`
  WHERE entity_code IN ('AU', '080')
)
SELECT 
  s.table_name,
  s.staging_row_count,
  c.core_row_count,
  (s.staging_row_count - c.core_row_count) AS row_count_delta,
  s.staging_distinct_keys,
  c.core_distinct_keys,
  (s.staging_distinct_keys - c.core_distinct_keys) AS distinct_keys_delta,
  s.staging_total_amount,
  c.core_total_amount,
  (s.staging_total_amount - c.core_total_amount) AS total_amount_delta,
  CASE 
    WHEN (s.staging_row_count - c.core_row_count) = 0 
         AND (s.staging_distinct_keys - c.core_distinct_keys) = 0 
         AND (s.staging_total_amount - c.core_total_amount) = 0.00 THEN 'PASSED'
    ELSE 'FAILED_RECONCILIATION_GAP'
  END AS reconciliation_status
FROM staging_metrics s
JOIN core_metrics c ON s.table_name = c.table_name;