-- DDL for logging and control tables
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_vertrag.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_vertrag.ksh

CREATE SCHEMA IF NOT EXISTS project.dataset;

CREATE TABLE IF NOT EXISTS project.dataset.job_control (
    eintragsnr INT64 NOT NULL OPTIONS(description="Unique entry number for the job run"),
    job_kennung STRING NOT NULL OPTIONS(description="Identifier for the job type"),
    script_name STRING OPTIONS(description="Name of the script that initiated the job"),
    log_name STRING OPTIONS(description="Name of the generated log file (if applicable)"),
    stichtag_info STRING OPTIONS(description="Information about the 'Stichtag' (key date) parameter"),
    status STRING OPTIONS(description="Current status of the job (e.g., 'RUNNING', 'OK', 'ERROR')"),
    created_ts TIMESTAMP OPTIONS(description="Timestamp when the job entry was created"),
    finished_ts TIMESTAMP OPTIONS(description="Timestamp when the job finished (successfully or with error)")
)
OPTIONS(
    description="Table to store metadata and status of executed jobs, replacing DWMSG_ErzeugeEintrag, DWMSG_SetzeStichtagInfo, DWMSG_SetzeStatusOK."
);

CREATE TABLE IF NOT EXISTS project.dataset.job_runtime_log (
    eintragsnr INT64 NOT NULL OPTIONS(description="Entry number corresponding to job_control"),
    job_kennung STRING NOT NULL OPTIONS(description="Identifier for the job type"),
    log_level STRING OPTIONS(description="Level of the log message (e.g., 'INFO', 'WARNING')"),
    message STRING OPTIONS(description="Log message content"),
    log_ts TIMESTAMP OPTIONS(description="Timestamp when the log message was recorded")
)
OPTIONS(
    description="Table to capture runtime messages and print statements from the original script."
);

CREATE TABLE IF NOT EXISTS project.dataset.job_error_log (
    job_kennung STRING NOT NULL OPTIONS(description="Identifier for the job type"),
    eintragsnr INT64 NOT NULL OPTIONS(description="Entry number corresponding to job_control"),
    error_nr INT64 OPTIONS(description="Error number from the original script's error handling"),
    error_arg STRING OPTIONS(description="Argument associated with the error"),
    error_message STRING OPTIONS(description="Detailed error message from the exception"),
    error_ts TIMESTAMP OPTIONS(description="Timestamp when the error occurred"),
    source_proc STRING OPTIONS(description="Name of the BigQuery stored procedure where the error originated")
)
OPTIONS(
    description="Table to record detailed error information, replacing DWMSG_MeldeFehler and DWMSG_Fehlerbehandlung."
);