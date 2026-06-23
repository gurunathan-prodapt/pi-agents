-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.job_log` (
    job_identifier STRING OPTIONS(description="Unique identifier for a specific job run instance"),
    job_name STRING OPTIONS(description="Name of the BigQuery Stored Procedure or job being executed"),
    start_time TIMESTAMP OPTIONS(description="Timestamp when the job started"),
    end_time TIMESTAMP OPTIONS(description="Timestamp when the job ended"),
    status STRING OPTIONS(description="Status of the job (e.g., 'RUNNING', 'SUCCESS', 'FAILED')"),
    message STRING OPTIONS(description="Detailed message or error description for the job run"),
    records_processed INT64 OPTIONS(description="Number of records processed by the job's core logic"),
    stichtag DATE OPTIONS(description="Stichtag parameter used for the job run"),
    eintrags_nr STRING OPTIONS(description="EintragsNr parameter used for the job run"),
    wiederanlauf_wert STRING OPTIONS(description="WiederanlaufWert parameter used for the job run")
)
OPTIONS(
    description="Logging table for BigQuery job executions, replacing legacy shell script tracking."
);