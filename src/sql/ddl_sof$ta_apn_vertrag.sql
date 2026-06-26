-- Legacy Source: Table DDL replacement
-- Job: DW.BERT_AUSD_BP_TA_APN_VERTRAG
-- Description: Creates the target table schema for APN contract aggregation if it does not already exist.

CREATE TABLE IF NOT EXISTS `isbert_schema.sof$ta_apn_vertrag` (
  cntrct_id STRING,
  access_point_name STRING,
  cntrct_id_ref STRING
);