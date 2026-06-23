-- Legacy source: sof$ta_period from Oracle
-- Job: DW.BERT_AUSD_V_TA_C_BFC
CREATE TABLE IF NOT EXISTS `{{ params.project }}.{{ params.dataset }}.sof$ta_period` (
  period_id STRING,
  bfc_age DATE
);