--
-- Target code for legacy job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_upgrade.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_upgrade.ksh
--
-- DDL for the job logging table. This table will replace the custom shell-based logging framework (DWMSG_* functions).
--
CREATE OR REPLACE TABLE `your_gcp_project_id.your_bq_dataset_id.job_log` (
    log_timestamp TIMESTAMP OPTIONS(description="Timestamp of the log entry"),
    job_kennung STRING OPTIONS(description="Identifier for the job"),
    eintrags_nr STRING OPTIONS(description="Entry number or instance ID for the job run"),
    log_level STRING OPTIONS(description="Level of the log entry (e.g., INFO, WARNING, ERROR)"),
    message STRING OPTIONS(description="Log message content"),
    procedure_name STRING OPTIONS(description="Name of the BigQuery stored procedure being executed"),
    status STRING OPTIONS(description="Overall status of the job/step (e.g., STARTED, COMPLETED, FAILED)"),
    error_code INT64 OPTIONS(description="Error code, if applicable"),
    error_argument STRING OPTIONS(description="Additional error argument, if applicable")
);