-- Legacy source: d_ausd_bp_ta_bpr_apn.sql
-- Job: ausd_bp_ta_bpr_apn
-- Purpose: Initialize the target table sof_ta_bpr_apn inside isbert_schema

CREATE OR REPLACE TABLE `isbert_schema.sof_ta_bpr_apn` (
  cntrct_id INT64 OPTIONS(description="ID of the contract"),
  bpr_id INT64 OPTIONS(description="ID of the basis product"),
  cntrct_id_ref INT64 OPTIONS(description="Referenced contract ID"),
  access_point_name STRING OPTIONS(description="Access Point Name (APN)")
)
OPTIONS(
  description="Consolidated table containing instantiated base products mapped to their APN"
);