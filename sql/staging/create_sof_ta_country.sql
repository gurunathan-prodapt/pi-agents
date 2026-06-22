-- DDL for BigQuery staging table staging.sof_ta_country
-- Created by k_ausd_adressen.ksh (d_ausd_adressen.sql)
CREATE TABLE IF NOT EXISTS `PROJECT_ID.staging.sof_ta_country`
(
    country_code                STRING,
    description_id              INT64,
    parent_country_code         STRING,
    eu_indicator                INT64,
    sap_code                    STRING,
    corr_code                   STRING,
    valid                       INT64
);