-- DDL for job_table, replacing legacy job management.
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_c_bfc.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_c_bfc.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.job_table` (
    run_id STRING NOT NULL OPTIONS(description="Unique identifier for each job run"),
    job_kennung STRING NOT NULL OPTIONS(description="Identifier for the type of job (e.g., from ksh p_JobKennung)"),
    eintrags_nr STRING NOT NULL OPTIONS(description="Entry number or instance identifier (e.g., from ksh p_EintragsNr)"),
    start_time TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the job started"),
    end_time TIMESTAMP OPTIONS(description="Timestamp when the job ended"),
    status STRING NOT NULL OPTIONS(description="Current status of the job (e.g., 'RUNNING', 'SUCCESS', 'FAILED', 'IGNORED', 'DEACTIVATED')"),
    message STRING OPTIONS(description="Additional information or error message related to the job status"),
    processed_records INT64 OPTIONS(description="Number of records processed by the job")
);