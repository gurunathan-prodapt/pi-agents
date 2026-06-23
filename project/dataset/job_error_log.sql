-- BigQuery DDL for job_error_log table
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_vertrag.ksh
CREATE TABLE project.dataset.job_error_log (
    error_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp of the error"),
    job_entry_nr INT64 NOT NULL OPTIONS(description="Job execution identifier (FK to job_control)"),
    error_message STRING NOT NULL OPTIONS(description="Description of the error"),
    stack_trace STRING OPTIONS(description="Stack trace of the error, if available"),
    stichtag STRING OPTIONS(description="Stichtag parameter at the time of error"),
    wiederanlaufwert INT64 OPTIONS(description="Wiederanlaufwert parameter at the time of error")
)
OPTIONS(
    description="Table to record specific error details during validation or execution"
);