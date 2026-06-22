-- DDL for BigQuery table raw.glv_ta_description
-- Replaces Oracle table glv$ta_description from k_ausd_adressen.ksh
CREATE TABLE IF NOT EXISTS `PROJECT_ID.raw.glv_ta_description`
(
    description_id              INT64,
    language                    STRING,
    short_description           STRING,
    description                 STRING,
    long_description            STRING
);