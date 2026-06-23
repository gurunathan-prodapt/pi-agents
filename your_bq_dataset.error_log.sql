--
-- BigQuery DDL for error_log table
-- Replaces error reporting functionality from h_alis_sqlplus.ksh
-- JOB: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh
--

CREATE TABLE IF NOT EXISTS `your_gcp_project.your_bq_dataset.error_log` (
    entry_nr STRING,
    severity STRING,
    error_code STRING,
    message STRING,
    module_name STRING,
    module_version STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);