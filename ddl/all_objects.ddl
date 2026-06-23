-- Legacy source: all_objects from Oracle
-- Job: DW.BERT_AUSD_V_TA_C_BFC
CREATE TABLE IF NOT EXISTS `{{ params.project }}.{{ params.dataset }}.all_objects` (
  created TIMESTAMP,
  object_name STRING,
  object_type STRING
);