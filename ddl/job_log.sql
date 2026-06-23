-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_beschr.ksh
-- Target BigQuery DDL for general job logging table job_log.

CREATE TABLE IF NOT EXISTS `project.dataset.job_log` (
    job_kennung STRING,
    status STRING,
    message STRING,
    created_at TIMESTAMP
);