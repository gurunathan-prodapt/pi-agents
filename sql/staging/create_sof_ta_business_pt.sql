-- DDL for BigQuery staging table staging.sof_ta_business_pt
-- Created by k_ausd_adressen.ksh (d_ausd_adressen.sql)
CREATE TABLE IF NOT EXISTS `PROJECT_ID.staging.sof_ta_business_pt`
(
    bp_id                       INT64,
    organisation_name           STRING,
    title                       STRING,
    surname                     STRING,
    first_name                  STRING,
    sales_tax_freed             INT64,
    tm_customerid               STRING
);