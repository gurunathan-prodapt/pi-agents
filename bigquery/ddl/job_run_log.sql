-- DDL for job_run_log table, migrated from k_ausd_v_ta_bp_ref.ksh
-- Legacy Job: k_ausd_v_ta_bp_ref.ksh
CREATE TABLE IF NOT EXISTS project.dataset.job_run_log (
    run_ts TIMESTAMP,
    job_kennung STRING,
    eintrags_nr STRING,
    tab_name STRING,
    records_processed INT64
);