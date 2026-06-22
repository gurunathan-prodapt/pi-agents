-- BigQuery DDL for sof_ta_iccid_vertrag
-- Legacy Source: Oracle table sof$ta_iccid_vertrag
-- Job ID: DW.BERT_AUSD_BP_TA_BCP_ICCID

CREATE TABLE IF NOT EXISTS `<project>.<dataset>.sof_ta_iccid_vertrag` (
    cntrct_id STRING,
    tn_iccid STRING,
    tn_imsi_hlr STRING
);