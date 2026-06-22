-- Header: BigQuery DDL for sof_ta_disc_zusgf target table
-- Legacy Source: sof$ta_disc_zusgf
-- Job: BERT_V_TA_DISC_ZUSGF

CREATE TABLE IF NOT EXISTS `bert_dwh.sof_ta_disc_zusgf` (
    cntrct_id STRING NOT NULL,
    cntrct_obj_version INT64 NOT NULL,
    disc_vector_ty STRING,
    rabatt_alle STRING
);