-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_templ.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_templ.ksh
CREATE TABLE project.dataset.job_log
(
    job_nr        INT64 NOT NULL OPTIONS(description="Foreign key to job_registry.job_nr"),
    job_kennung   STRING OPTIONS(description="Identifier for the job type"),
    log_level     STRING OPTIONS(description="Level of the log message (e.g., INFO, WARNING, ERROR)"),
    message       STRING OPTIONS(description="Detailed log message"),
    log_timestamp TIMESTAMP OPTIONS(description="Timestamp when the log entry was recorded")
)
OPTIONS(
    description="Stores general informational messages for each job run."
);