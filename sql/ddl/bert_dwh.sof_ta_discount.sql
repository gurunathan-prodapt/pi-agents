-- Header: BigQuery DDL for sof_ta_discount table
-- Legacy Source: sof$ta_discount (from CARMEN DB)
-- Job: BERT_V_TA_DISC_ZUSGF

CREATE TABLE IF NOT EXISTS `bert_dwh.sof_ta_discount` (
    cntrct_id STRING NOT NULL,
    disc_vector_ty STRING,
    cntrct_obj_version INT64 NOT NULL,
    rabatt STRING,
    rabatthoehe NUMERIC,
    -- Placeholder for other columns from original table
    original_data JSON
);