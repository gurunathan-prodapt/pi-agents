-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_templ.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_templ.ksh
CREATE TABLE project.dataset.job_error_log
(
    job_nr          INT64 NOT NULL OPTIONS(description="Foreign key to job_registry.job_nr"),
    job_kennung     STRING OPTIONS(description="Identifier for the job type"),
    err_nr          INT64 OPTIONS(description="Error number or code"),
    err_arg         STRING OPTIONS(description="Additional error argument or context"),
    message         STRING OPTIONS(description="Detailed error message"),
    error_timestamp TIMESTAMP OPTIONS(description="Timestamp when the error occurred")
)
OPTIONS(
    description="Stores detailed error information for job failures."
);