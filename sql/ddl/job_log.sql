-- DDL for job_log, replacing part of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount_rr.ksh
CREATE TABLE IF NOT EXISTS `your_gcp_project.isrpt_isbert_stage.job_log` (
    job_kennung STRING,
    eintrags_nr STRING,
    status STRING,
    message STRING,
    records_processed INT64,
    created_at TIMESTAMP NOT NULL
)
PARTITION BY
    DATE(created_at)
CLUSTER BY
    job_kennung, eintrags_nr;