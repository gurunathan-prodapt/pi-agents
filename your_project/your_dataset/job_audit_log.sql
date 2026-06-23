-- BigQuery table DDL for job_audit_log
-- Replaces logging functionality from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_templ.ksh
CREATE TABLE IF NOT EXISTS `your_project.your_dataset.job_audit_log` (
    job_kennung STRING NOT NULL,
    eintrags_nr INT64 NOT NULL,
    start_zeit TIMESTAMP NOT NULL,
    ende_zeit TIMESTAMP,
    status STRING, -- e.g., 'RUNNING', 'OK', 'ERROR'
    meldungs_text STRING,
    log_dateiname STRING,
    user_name STRING,
    pid INT64,
    host_name STRING,
    referenz_datum DATE
);