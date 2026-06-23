-- Legacy Source: TABLE:DWTK_MELDUNGEN
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_dwh.ksh
CREATE TABLE IF NOT EXISTS `your_gcp_project_id.your_bq_dataset_id.DWTK_MELDUNGEN` (
    -- TODO: Define actual schema based on source system.
    id STRING,
    data JSON,
    load_timestamp TIMESTAMP
);