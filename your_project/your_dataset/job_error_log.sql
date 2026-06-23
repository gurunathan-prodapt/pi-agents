-- BigQuery table DDL for job_error_log
-- Replaces error logging functionality from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_templ.ksh
CREATE TABLE IF NOT EXISTS `your_project.your_dataset.job_error_log` (
    job_kennung STRING NOT NULL,
    eintrags_nr INT64 NOT NULL,
    fehler_zeit TIMESTAMP NOT NULL,
    fehler_code INT64,
    fehler_text STRING,
    quell_prozedur STRING,
    stack_trace STRING
);