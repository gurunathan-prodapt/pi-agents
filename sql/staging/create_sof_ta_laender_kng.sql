-- DDL for BigQuery staging table staging.sof_ta_laender_kng
-- Created by k_ausd_adressen.ksh (d_ausd_adressen.sql)
CREATE TABLE IF NOT EXISTS `PROJECT_ID.staging.sof_ta_laender_kng`
(
    country_code                STRING,
    description_id              INT64,
    language                    STRING,
    short_description           STRING,
    description                 STRING,
    long_description            STRING
);