-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_instance.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_instance.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.job_error_log` (
    job_name STRING OPTIONS(description="Name of the job where the error occurred"),
    error_nr INT64 OPTIONS(description="Numeric error code from the legacy system or internal mapping"),
    error_arg STRING OPTIONS(description="Argument or context related to the error"),
    error_timestamp TIMESTAMP OPTIONS(description="Timestamp when the error occurred"),
    error_details STRING OPTIONS(description="Detailed error information")
);