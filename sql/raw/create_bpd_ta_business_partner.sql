-- DDL for BigQuery table raw.bpd_ta_business_partner
-- Replaces Oracle table bpd$ta_business_partner from k_ausd_adressen.ksh
CREATE TABLE IF NOT EXISTS `PROJECT_ID.raw.bpd_ta_business_partner`
(
    bp_id                       INT64,
    organisation_name           STRING,
    title                       STRING,
    surname                     STRING,
    first_name                  STRING,
    sales_tax_freed             INT64, -- Or BOOL
    tm_customerid               STRING,
    insert_at                   TIMESTAMP,
    modified_at                 TIMESTAMP
);