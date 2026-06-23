-- Legacy Source: TABLE:SOF$TA_VVL_DWH
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_dwh.ksh
CREATE TABLE IF NOT EXISTS `your_gcp_project_id.your_bq_dataset_id.SOF_TA_VVL_DWH` (
    -- TODO: Define actual schema based on source system.
    id STRING,
    processed_data JSON,
    processing_timestamp TIMESTAMP
);