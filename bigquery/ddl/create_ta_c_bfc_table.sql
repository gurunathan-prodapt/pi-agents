-- BigQuery DDL for the target cache table ta_c_bfc
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_c_bfc.ksh
-- This file replaces part of the Oracle table definition and usage.

CREATE TABLE IF NOT EXISTS `{{ project_id }}.{{ dataset_id }}.ta_c_bfc` (
  cntrct_id STRING NOT NULL,
  bindefrist DATE,
  bfc_age INT64,
  bfc_count INT64,
  bfc_procedure DATE,
  commitment_reference_date DATE,
  cntrct_validity_id STRING,
  load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);