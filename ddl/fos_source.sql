-- BigQuery DDL for fos_source dataset
-- Replaces parts of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_rechempf.ksh

CREATE SCHEMA IF NOT EXISTS `fos_source`;

CREATE TABLE IF NOT EXISTS `fos_source.sof_ta_e_reach_re`
(
  inv_def_invrec_id INT64,
  bp_id INT64,
  corp_unit STRING,
  surname_s STRING,
  first_name_g STRING,
  title STRING,
  for_the_attention_of STRING,
  address_attachment STRING,
  street STRING,
  pobox STRING,
  house_nr STRING,
  zip_code STRING,
  city STRING,
  land_sd STRING,
  address_attachment_org STRING
);

CREATE TABLE IF NOT EXISTS `fos_source.sof_ta_e_business_re`
(
  bp_id INT64,
  organisation_name STRING,
  first_name STRING,
  surname STRING,
  title STRING,
  sales_tax_freed BOOL,
  tm_customerid STRING
);

CREATE TABLE IF NOT EXISTS `fos_source.sof_ta_e_regulierer`
(
  inv_def_mopref_id INT64,
  means_of_payment_id INT64,
  mop_bp_id INT64
);