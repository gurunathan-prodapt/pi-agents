-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount.ksh
CREATE TABLE `your_project.your_dataset.job_error_log` (
    job_kennung STRING NOT NULL OPTIONS(description="Unique identifier for the job."),
    eintrags_nr STRING NOT NULL OPTIONS(description="Entry number or instance identifier for the job run."),
    err_nr INT64 OPTIONS(description="Error number or code."),
    err_arg STRING OPTIONS(description="Error message or arguments related to the error."),
    error_ts TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the error occurred."),
    script_name STRING OPTIONS(description="Name of the script or procedure where the error occurred.")
);