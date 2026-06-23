-- DDL for the target BigQuery table sof_ta_cntrct_templ
-- for job DW.BERT_AUSD_V_TA_CNTRCT_TEMPL.

CREATE TABLE IF NOT EXISTS `sof_ta_cntrct_templ`
(
    CNTRCT_TEMPLATE_ID INT64,
    CDS_DESCRIPTION_ID INT64,
    CDS_DESCRIPTION STRING
);