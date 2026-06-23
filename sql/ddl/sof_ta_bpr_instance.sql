-- DDL for BigQuery table sof_ta_bpr_instance
-- Replaces data from legacy Oracle source.
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh

CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.sof_ta_bpr_instance`
(
    cntrct_id STRING,
    bpr_id INT64,
    cntrct_id_ref STRING,
    -- Add other columns as per source schema.
    -- Example:
    -- some_instance_detail STRING
);