--
-- BigQuery DDL for job_control table
-- Replaces logging functionality from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_dist.ksh
--
CREATE TABLE IF NOT EXISTS `project.dataset.job_control` (
    job_nr INT64 NOT NULL OPTIONS(description="Unique job identifier, mimicking a primary key."),
    job_kennung STRING OPTIONS(description="Identifier for the job/stored procedure."),
    source_program STRING OPTIONS(description="Original source program name."),
    stichtag STRING OPTIONS(description="The effective cutoff date used for the job (DDMMYYYY)."),
    sysdate STRING OPTIONS(description="The system date when the job started (DDMMYYYY)."),
    restart_value INT64 OPTIONS(description="The restart value used for the job, defaults to 0."),
    created_at TIMESTAMP OPTIONS(description="Timestamp when the job entry was created."),
    finished_at TIMESTAMP OPTIONS(description="Timestamp when the job finished (successfully or with error)."),
    status STRING OPTIONS(description="Current status of the job (e.g., 'RUNNING', 'SUCCESS', 'FAILED').")
);