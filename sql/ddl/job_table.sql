-- DDL for job_table, replacing part of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount_rr.ksh
CREATE TABLE IF NOT EXISTS `your_gcp_project.isrpt_isbert_stage.job_table` (
    job_name STRING NOT NULL,
    job_kennung STRING NOT NULL,
    eintrags_nr STRING NOT NULL,
    active_flag BOOLEAN NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP
)
PARTITION BY
    DATE(created_at)
CLUSTER BY
    job_name, job_kennung;