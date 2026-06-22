-- BigQuery DDL for job_audit table
-- Replaces shell-based logging mechanisms from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_vertrag.ksh
-- and vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_vertrag.ksh
CREATE TABLE IF NOT EXISTS `project.dataset.job_audit` (
    job_kennung STRING NOT NULL,
    run_id STRING NOT NULL,
    status STRING NOT NULL,
    message STRING,
    created_at TIMESTAMP NOT NULL,
    record_count INT64
);