-- Legacy source: sof$ta_cntrct_valid from Oracle
-- Job: DW.BERT_AUSD_V_TA_C_BFC
CREATE TABLE IF NOT EXISTS `{{ params.project }}.{{ params.dataset }}.sof$ta_cntrct_valid` (
  cntrct_validity_id STRING,
  first_period_id STRING,
  following_period_id STRING,
  first_notice_period_id STRING,
  follow_notice_period_id STRING,
  bfc_age DATE
);