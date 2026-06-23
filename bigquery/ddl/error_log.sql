-- DDL for error_log, replacing custom error messaging utility f_alis_msgerr.ksh.
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_c_bfc.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_c_bfc.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.error_log` (
    timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the error occurred"),
    run_id STRING NOT NULL OPTIONS(description="Unique identifier for the job run during which the error occurred"),
    job_kennung STRING NOT NULL OPTIONS(description="Identifier for the type of job"),
    eintrags_nr STRING NOT NULL OPTIONS(description="Entry number or instance identifier"),
    error_code INT64 OPTIONS(description="Numerical error code from the legacy system or new classification"),
    error_message STRING NOT NULL OPTIONS(description="Detailed error message")
);