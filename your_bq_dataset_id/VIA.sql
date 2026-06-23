-- Legacy Source: TABLE:VIA
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_dwh.ksh
CREATE TABLE IF NOT EXISTS `your_gcp_project_id.your_bq_dataset_id.VIA` (
    -- TODO: Define actual schema based on source system.
    id STRING,
    status_info JSON,
    update_timestamp TIMESTAMP
);