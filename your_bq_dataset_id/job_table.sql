-- Legacy Source: Active job handling from k_ausd_v_ta_vvl_dwh.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_dwh.ksh
CREATE TABLE IF NOT EXISTS `your_gcp_project_id.your_bq_dataset_id.job_table` (
    job_kennung STRING NOT NULL,
    eintrags_nr STRING NOT NULL,
    is_active BOOL NOT NULL,
    last_update_timestamp TIMESTAMP NOT NULL,
    PRIMARY KEY (job_kennung, eintrags_nr) NOT ENFORCED
);