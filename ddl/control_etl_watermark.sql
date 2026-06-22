-- BigQuery DDL for DW.BERT_AUSD_V_TA_CNTRCT_TEMPL
-- Legacy Source: N/A (new control table)
CREATE TABLE IF NOT EXISTS `your_gcp_project_id.control.etl_watermark` (
    table_name STRING,
    watermark_column STRING,
    last_watermark_value TIMESTAMP,
    updated_at TIMESTAMP
);