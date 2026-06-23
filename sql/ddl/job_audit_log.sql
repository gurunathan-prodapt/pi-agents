-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_bp_ref.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_bp_ref.ksh
-- Description: DDL for the job audit log table in BigQuery.

CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.job_audit_log` (
    job_instance_id STRING NOT NULL OPTIONS(description="Unique identifier for each job execution instance (corresponds to DW_EintragsNr)"),
    job_name STRING OPTIONS(description="Name of the job (e.g., 'Vertragsdatenabgleich', from ProgName)"),
    job_kennung STRING OPTIONS(description="Kennung for the job (e.g., 'BERT_V_TA_BP_REF', from JobKennung)"),
    start_timestamp TIMESTAMP OPTIONS(description="Timestamp when the job instance started"),
    end_timestamp TIMESTAMP OPTIONS(description="Timestamp when the job instance ended"),
    status STRING OPTIONS(description="Current status of the job instance (e.g., 'STARTED', 'RUNNING', 'SUCCESS', 'FAILED')"),
    message_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp for the individual log message"),
    message_type STRING NOT NULL OPTIONS(description="Type of log message (e.g., 'INFO', 'ERROR', 'USAGE', 'WARNING')"),
    message_text STRING OPTIONS(description="Content of the log message"),
    error_code INT64 OPTIONS(description="Numeric error code if an error occurred (corresponds to ErrNr)"),
    error_argument STRING OPTIONS(description="Argument associated with the error (corresponds to ErrArg)"),
    parameters_s STRING OPTIONS(description="Value of the -s parameter passed to the job"),
    parameters_l STRING OPTIONS(description="Value of the -l parameter passed to the job"),
    stichtag_info STRING OPTIONS(description="Date information in DDMMYYYY format (corresponds to v_sysdate)"),
    log_file_name STRING OPTIONS(description="Simulated log file name for reference")
)
OPTIONS(
    description="Table to store audit and logging information for BigQuery job executions."
);