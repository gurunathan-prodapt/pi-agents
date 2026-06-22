-- DDL for BigQuery target table target.sof_ta_e_business_ev
-- Created by k_ausd_adressen.ksh (d_ausd_adressen.sql)
CREATE TABLE IF NOT EXISTS `PROJECT_ID.target.sof_ta_e_business_ev`
(
    bp_id                       INT64,
    organisation_name           STRING,
    title                       STRING,
    surname                     STRING,
    first_name                  STRING,
    sales_tax_freed             INT64,
    tm_customerid               STRING
);