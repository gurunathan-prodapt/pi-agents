-- BigQuery DDL for job_usage_log table
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_vertrag.ksh
CREATE TABLE project.dataset.job_usage_log (
    usage_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp when usage instructions were logged"),
    job_entry_nr INT64 OPTIONS(description="Job execution identifier, if available at the time of logging"),
    message STRING NOT NULL OPTIONS(description="Usage message or validation error detail"),
    provided_stichtag STRING OPTIONS(description="Raw stichtag parameter provided by user"),
    provided_wiederanlaufwert STRING OPTIONS(description="Raw wiederanlaufwert parameter provided by user")
)
OPTIONS(
    description="Table to log instances where usage instructions are displayed (e.g., due to invalid parameters)"
);