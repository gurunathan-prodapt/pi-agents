-- Legacy source: sof$ta_barrier from Oracle
-- Job: DW.BERT_AUSD_V_TA_C_BFC
CREATE TABLE IF NOT EXISTS `{{ params.project }}.{{ params.dataset }}.sof$ta_barrier` (
  cntrct_id STRING,
  bfc_age DATE
);