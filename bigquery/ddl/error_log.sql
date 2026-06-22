-- DDL for error_log table, migrated from k_ausd_v_ta_bp_ref.ksh
-- Legacy Job: k_ausd_v_ta_bp_ref.ksh
CREATE TABLE IF NOT EXISTS project.dataset.error_log (
    error_ts TIMESTAMP,
    error_code INT64,
    error_arg STRING,
    job_kennung STRING,
    eintrags_nr STRING,
    script_name STRING,
    message STRING
);