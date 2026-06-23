--
-- BigQuery DDL for execution_log table
-- Replaces logging functionality from h_alis_sqlplus.ksh
-- JOB: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh
--

CREATE TABLE IF NOT EXISTS `your_gcp_project.your_bq_dataset.execution_log` (
    module_name STRING,
    module_version STRING,
    entry_nr STRING,
    script_name STRING,
    script_params ARRAY<STRING>,
    log_message STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);