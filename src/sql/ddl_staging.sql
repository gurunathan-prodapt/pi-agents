-- DDL for ausd_bp_ta_ibcp_ccid - BigQuery Staging Tables
-- Legacy Source: Oracle GL_CODE_COMBINATIONS / IBCP_STAGE_TXN
-- Job: ausd_bp_ta_ibcp_ccid

CREATE TABLE IF NOT EXISTS `prj-ausd-stage-gcp.bq_stage_ta.stg_oracle_ccid` (
  code_combination_id STRING OPTIONS(description="Oracle unique identifier for GL code combination"),
  segment1 STRING OPTIONS(description="Entity/Company Segment"),
  segment2 STRING OPTIONS(description="Cost Center Segment"),
  segment3 STRING OPTIONS(description="Account Segment"),
  segment4 STRING OPTIONS(description="Sub-Account Segment"),
  segment5 STRING OPTIONS(description="Intercompany Partner Segment"),
  summary_flag STRING OPTIONS(description="Summary flag (Y/N)"),
  enabled_flag STRING OPTIONS(description="Enabled flag (Y/N)"),
  start_date_active STRING OPTIONS(description="Start date active (raw string format)"),
  end_date_active STRING OPTIONS(description="End date active (raw string format)"),
  dw_load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Timestamp when loaded into BigQuery")
)
CLUSTER BY code_combination_id, segment1;

CREATE TABLE IF NOT EXISTS `prj-ausd-stage-gcp.bq_stage_ta.stg_ibcp_txns` (
  txn_id STRING OPTIONS(description="Unique transaction identifier"),
  code_combination_id STRING OPTIONS(description="Oracle GL code combination ID"),
  entity_code STRING OPTIONS(description="Entity segment code"),
  cost_center_code STRING OPTIONS(description="Cost Center segment code"),
  account_code STRING OPTIONS(description="Account segment code"),
  sub_account_code STRING OPTIONS(description="Sub-Account segment code"),
  intercompany_partner_code STRING OPTIONS(description="Intercompany Partner segment code"),
  txn_amount NUMERIC OPTIONS(description="Transaction amount"),
  txn_currency STRING OPTIONS(description="Transaction currency code"),
  txn_date DATE OPTIONS(description="Date of the transaction"),
  dw_load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Timestamp when loaded into BigQuery")
)
PARTITION BY txn_date
CLUSTER BY code_combination_id;