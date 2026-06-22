-- DDL for project.dataset.job_log
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_optionen.ksh

CREATE TABLE `your-gcp-project-id.your-dataset.job_log`
(
    tab_name        STRING    OPTIONS(description="Name of the table being processed"),
    job_kennung     STRING    NOT NULL OPTIONS(description="Job identifier"),
    eintrags_nr     STRING            OPTIONS(description="Entry number"),
    stichtag        STRING            OPTIONS(description="Key date for processing (DDMMYYYY)"),
    wiederanlauf_wert STRING            OPTIONS(description="Restart value"),
    records         INT64             OPTIONS(description="Number of records processed"),
    created_at      TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the job log was created")
)
OPTIONS(
    description="Centralized job logging table for migrated jobs."
);