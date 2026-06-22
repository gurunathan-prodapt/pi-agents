-- DDL for BigQuery table raw.glv_ta_country
-- Replaces Oracle table glv$ta_country from k_ausd_adressen.ksh
CREATE TABLE IF NOT EXISTS `PROJECT_ID.raw.glv_ta_country`
(
    country_code                STRING,
    description_id              INT64,
    parent_country_code         STRING,
    eu_indicator                INT64, -- Or BOOL
    sap_code                    STRING,
    corr_code                   STRING,
    valid                       INT64  -- Or BOOL
);