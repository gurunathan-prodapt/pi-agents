-- DDL for job_control table
-- Legacy source: N/A (audit table for job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_adressen.ksh)
CREATE TABLE IF NOT EXISTS `project.dataset.job_control`
(
    job_run_id          STRING      NOT NULL OPTIONS(description="Unique identifier for each job execution"),
    job_name            STRING      NOT NULL OPTIONS(description="Name of the job/stored procedure"),
    start_time          TIMESTAMP   NOT NULL OPTIONS(description="Start timestamp of the job execution"),
    end_time            TIMESTAMP           OPTIONS(description="End timestamp of the job execution"),
    status              STRING      NOT NULL OPTIONS(description="Status of the job (RUNNING, OK, FAILED)"),
    stichtag            DATE                OPTIONS(description="Stichtag/Cutoff date parameter"),
    wiederanlauf_wert   INT64               OPTIONS(description="Wiederanlaufwert/Restart value parameter"),
    error_message       STRING              OPTIONS(description="Error message if job failed")
)
OPTIONS(
    description="Audit table to track job execution status and parameters"
);