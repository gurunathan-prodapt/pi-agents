-- DDL for project.dataset.error_log
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_optionen.ksh

CREATE TABLE `your-gcp-project-id.your-dataset.error_log`
(
    job_name   STRING    NOT NULL OPTIONS(description="Name of the job that produced the error"),
    error_nr   INT64             OPTIONS(description="Error code or number"),
    error_arg  STRING            OPTIONS(description="Additional error arguments or message"),
    created_at TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the error was logged")
)
OPTIONS(
    description="Centralized error logging table for migrated jobs."
);