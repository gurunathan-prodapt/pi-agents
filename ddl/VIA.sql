-- DDL for BigQuery table VIA
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_bp_ref.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_bp_ref.ksh

CREATE TABLE IF NOT EXISTS `my_project.my_dataset.VIA` (
    -- Primary key for VIA table, assuming it exists. Adjust based on actual schema.
    via_id STRING,
    -- Add other relevant columns based on actual schema, e.g.,
    some_column STRING,
    updated_at TIMESTAMP
);