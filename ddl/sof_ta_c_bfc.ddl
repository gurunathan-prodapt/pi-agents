-- Legacy source: sof$ta_c_bfc from Oracle
-- Job: DW.BERT_AUSD_V_TA_C_BFC
CREATE TABLE IF NOT EXISTS `{{ params.project }}.{{ params.dataset }}.sof$ta_c_bfc` (
  cntrct_id STRING,
  bindefrist DATE,
  bfc_age DATE,
  bfc_count INT64,
  bfc_procedure DATE,
  commitment_reference_date DATE,
  cntrct_validity_id STRING
);