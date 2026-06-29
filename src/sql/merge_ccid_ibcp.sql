-- SQL Transformation and Merge for ausd_bp_ta_ibcp_ccid
-- Legacy Source: Oracle GL_CODE_COMBINATIONS / IBCP_STAGE_TXN
-- Job: ausd_bp_ta_ibcp_ccid

-- Step 1: Merge CCID Dimension Table with filtering for Australian entities ('AU', '080')
MERGE INTO `prj-ausd-core-gcp.finance_ta.dim_ccid_ibcp` T
USING (
  SELECT 
    code_combination_id,
    segment1 AS entity_code,
    segment2 AS cost_center_code,
    segment3 AS account_code,
    segment4 AS sub_account_code,
    segment5 AS intercompany_partner_code,
    -- Concatenated GL Account string following the standard Australian Chart of Accounts layout
    CONCAT(segment1, '-', segment2, '-', segment3, '-', COALESCE(segment4, '0000'), '-', COALESCE(segment5, '000')) AS full_gl_account_string,
    COALESCE(summary_flag, 'N') AS summary_flag,
    COALESCE(enabled_flag, 'N') AS enabled_flag,
    SAFE_CAST(start_date_active AS DATE) AS start_date,
    SAFE_CAST(end_date_active AS DATE) AS end_date,
    CURRENT_TIMESTAMP() AS dw_last_update_ts
  FROM 
    `prj-ausd-stage-gcp.bq_stage_ta.stg_oracle_ccid`
  WHERE 
    -- Filtering for Australian subsidiary code as defined in Chart of Accounts structure
    segment1 IN ('AU', '080')
    AND code_combination_id IS NOT NULL
) S
ON T.code_combination_id = S.code_combination_id
WHEN MATCHED THEN
  UPDATE SET 
    T.entity_code = S.entity_code,
    T.cost_center_code = S.cost_center_code,
    T.account_code = S.account_code,
    T.sub_account_code = S.sub_account_code,
    T.intercompany_partner_code = S.intercompany_partner_code,
    T.full_gl_account_string = S.full_gl_account_string,
    T.summary_flag = S.summary_flag,
    T.enabled_flag = S.enabled_flag,
    T.start_date = S.start_date,
    T.end_date = S.end_date,
    T.dw_last_update_ts = S.dw_last_update_ts
WHEN NOT MATCHED THEN
  INSERT (
    code_combination_id, 
    entity_code, 
    cost_center_code, 
    account_code, 
    sub_account_code, 
    intercompany_partner_code, 
    full_gl_account_string, 
    summary_flag, 
    enabled_flag, 
    start_date, 
    end_date, 
    dw_last_update_ts
  )
  VALUES (
    S.code_combination_id, 
    S.entity_code, 
    S.cost_center_code, 
    S.account_code, 
    S.sub_account_code, 
    S.intercompany_partner_code, 
    S.full_gl_account_string, 
    S.summary_flag, 
    S.enabled_flag, 
    S.start_date, 
    S.end_date, 
    S.dw_last_update_ts
  );

-- Step 2: Merge Reconciled Intercompany Transaction Ledger
MERGE INTO `prj-ausd-core-gcp.finance_ta.fact_ibcp_ledger` T
USING (
  SELECT 
    txn_id,
    code_combination_id,
    entity_code,
    cost_center_code,
    account_code,
    sub_account_code,
    intercompany_partner_code,
    txn_amount,
    txn_currency,
    txn_date,
    CURRENT_TIMESTAMP() AS dw_last_update_ts
  FROM 
    `prj-ausd-stage-gcp.bq_stage_ta.stg_ibcp_txns`
  WHERE 
    txn_id IS NOT NULL
    AND entity_code IN ('AU', '080')
) S
ON T.txn_id = S.txn_id
WHEN MATCHED THEN
  UPDATE SET 
    T.code_combination_id = S.code_combination_id,
    T.entity_code = S.entity_code,
    T.cost_center_code = S.cost_center_code,
    T.account_code = S.account_code,
    T.sub_account_code = S.sub_account_code,
    T.intercompany_partner_code = S.intercompany_partner_code,
    T.txn_amount = S.txn_amount,
    T.txn_currency = S.txn_currency,
    T.txn_date = S.txn_date,
    T.dw_last_update_ts = S.dw_last_update_ts
WHEN NOT MATCHED THEN
  INSERT (
    txn_id,
    code_combination_id,
    entity_code,
    cost_center_code,
    account_code,
    sub_account_code,
    intercompany_partner_code,
    txn_amount,
    txn_currency,
    txn_date,
    dw_last_update_ts
  )
  VALUES (
    S.txn_id,
    S.code_combination_id,
    S.entity_code,
    S.cost_center_code,
    S.account_code,
    S.sub_account_code,
    S.intercompany_partner_code,
    S.txn_amount,
    S.txn_currency,
    S.txn_date,
    S.dw_last_update_ts
  );