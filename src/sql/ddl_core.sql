-- DDL for ausd_bp_ta_ibcp_ccid - BigQuery Core Dimensional/Fact Tables
-- Legacy Source: Oracle GL_CODE_COMBINATIONS / IBCP_STAGE_TXN
-- Job: ausd_bp_ta_ibcp_ccid

CREATE TABLE IF NOT EXISTS `prj-ausd-core-gcp.finance_ta.dim_ccid_ibcp` (
  code_combination_id STRING NOT NULL OPTIONS(description="Conformed Code Combination ID"),
  entity_code STRING OPTIONS(description="Parsed Subsidiary/Entity Code"),
  cost_center_code STRING OPTIONS(description="Parsed Cost Center Code"),
  account_code STRING OPTIONS(description="Parsed Natural Account Code"),
  sub_account_code STRING OPTIONS(description="Parsed Sub-Account Code"),
  intercompany_partner_code STRING OPTIONS(description="Parsed Intercompany Business Partner Code"),
  full_gl_account_string STRING OPTIONS(description="Concatenated Fully Qualified Ledger Key (Entity-CC-Acct-SubAcct-IC)"),
  summary_flag STRING OPTIONS(description="Indicates if this is a summary/rollup account (Y/N)"),
  enabled_flag STRING OPTIONS(description="Indicates if this CCID is currently active (Y/N)"),
  start_date DATE OPTIONS(description="Validity start date"),
  end_date DATE OPTIONS(description="Validity end date"),
  dw_last_update_ts TIMESTAMP OPTIONS(description="Metadata: last ingestion/merge timestamp")
)
CLUSTER BY code_combination_id, entity_code, cost_center_code;

CREATE TABLE IF NOT EXISTS `prj-ausd-core-gcp.finance_ta.fact_ibcp_ledger` (
  txn_id STRING NOT NULL OPTIONS(description="Conformed Unique Transaction ID"),
  code_combination_id STRING OPTIONS(description="Conformed Code Combination ID linking to dim_ccid_ibcp"),
  entity_code STRING OPTIONS(description="Parsed Subsidiary/Entity Code"),
  cost_center_code STRING OPTIONS(description="Parsed Cost Center Code"),
  account_code STRING OPTIONS(description="Parsed Natural Account Code"),
  sub_account_code STRING OPTIONS(description="Parsed Sub-Account Code"),
  intercompany_partner_code STRING OPTIONS(description="Parsed Intercompany Business Partner Code"),
  txn_amount NUMERIC OPTIONS(description="Reconciled intercompany transaction amount in transaction currency"),
  txn_currency STRING OPTIONS(description="Transaction currency code (e.g. AUD, USD)"),
  txn_date DATE OPTIONS(description="Transaction reference date"),
  dw_last_update_ts TIMESTAMP OPTIONS(description="Metadata: last ingestion/merge timestamp")
)
PARTITION BY txn_date
CLUSTER BY code_combination_id, entity_code;