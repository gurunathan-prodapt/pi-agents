-- DDL for BigQuery table sof_ta_apn_carmen
-- Replaces data from legacy Oracle source.
-- This table is implicitly used in the join in d_ausd_bp_ta_bpr_apn.sql,
-- but its schema is not directly provided.
-- Assuming basic columns based on usage.
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh

CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.sof_ta_apn_carmen`
(
    cntrct_id STRING,
    access_point_name STRING
    -- Add other columns as per source schema if available.
);