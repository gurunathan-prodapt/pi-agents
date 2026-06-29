-- ===================================================================
-- Legacy Source: sof$ta_bpr_apn (Oracle)
-- Job: ausd_bp_ta_bpr_apn
-- Purpose: Target table definition for BERT basic product mappings
-- ===================================================================

CREATE TABLE IF NOT EXISTS `gcp-enterprise-dwh.dw_bert.sof_ta_bpr_apn` (
  CNTRCT_ID INT64 OPTIONS(description="Contract Identifier"),
  BPR_ID INT64 OPTIONS(description="Basic Product Identifier"),
  CNTRCT_ID_REF INT64 OPTIONS(description="Reference Contract Identifier"),
  ACCESS_POINT_NAME STRING OPTIONS(description="Access Point Name")
)
CLUSTER BY bpr_id, cntrct_id;