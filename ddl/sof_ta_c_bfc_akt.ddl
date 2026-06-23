-- Legacy source: sof$ta_c_bfc_akt from Oracle
-- Job: DW.BERT_AUSD_V_TA_C_BFC
CREATE TABLE IF NOT EXISTS `{{ params.project }}.{{ params.dataset }}.sof$ta_c_bfc_akt` (
  cntrct_id STRING,
  commitment_reference_date DATE,
  cntrct_validity_id STRING,
  bfc_age DATE,
  bfc_count INT64
);