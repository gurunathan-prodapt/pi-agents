-- Legacy Target Table: sof$ta_apn_vertrag
-- Legacy Job: ausd_bp_ta_apn_vertrag
-- Replaces: DDL portion of d_ausd_bp_ta_apn_vertrag.sql
--
-- This script creates the target table sof_ta_apn_vertrag in BigQuery.

CREATE OR REPLACE TABLE `{{ var.value.gcp_project }}.{{ var.value.gcp_dataset }}.sof_ta_apn_vertrag`
(
  cntrct_id STRING OPTIONS(description="Contract ID"),
  apn STRING OPTIONS(description="Aggregated list of Access Point Names, comma-separated"),
  cntrct_ref STRING OPTIONS(description="Aggregated list of contract references, comma-separated")
)
OPTIONS(
  description="Aggregated APN and contract references per contract ID for BERT credit scoring cache."
);