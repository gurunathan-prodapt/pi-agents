-- Legacy source: isbert_schema.dwtk_meldungen from Oracle
-- Job: DW.BERT_AUSD_V_TA_C_BFC
CREATE TABLE IF NOT EXISTS `{{ params.project }}.{{ params.dataset }}.dwtk_meldungen` (
  timecreated TIMESTAMP,
  job_kennung STRING
);