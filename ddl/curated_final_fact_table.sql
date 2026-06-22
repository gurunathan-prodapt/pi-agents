-- BigQuery DDL for DW.BERT_AUSD_V_TA_CNTRCT_TEMPL
-- Legacy Source: sof$ta_cntrct_templ (Oracle)
CREATE TABLE IF NOT EXISTS `your_gcp_project_id.curated.final_fact_table` (
    CNTRCT_TEMPLATE_ID INT64,
    CDS_DESCRIPTION_ID INT64,
    CDS_DESCRIPTION STRING
);