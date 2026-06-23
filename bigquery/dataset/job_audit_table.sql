-- Migrated from: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh

-- DDL for BigQuery Job Audit Table
CREATE TABLE IF NOT EXISTS `dataset.job_audit` (
    job_audit_id INT64 OPTIONS(description="Unique identifier for each job execution"),
    job_name STRING NOT NULL OPTIONS(description="Name of the job/script executed"),
    start_time TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the job started"),
    end_time TIMESTAMP OPTIONS(description="Timestamp when the job ended"),
    status STRING NOT NULL OPTIONS(description="Status of the job (RUNNING, SUCCESS, FAILED)"),
    job_kennung STRING OPTIONS(description="Jobkennung parameter (j)"),
    eintrags_nr STRING OPTIONS(description="EintragsNr parameter (f)"),
    stichtag DATE OPTIONS(description="Stichtag parameter (s) parsed to date"),
    wiederanlauf_wert INT64 OPTIONS(description="Wiederanlaufwert parameter (l)"),
    record_count INT64 OPTIONS(description="Number of records processed or affected"),
    error_message STRING OPTIONS(description="Error message if the job failed"),
    creation_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Timestamp of record creation")
);

-- Note: job_audit_id would typically be an auto-incrementing field. In BigQuery,
-- this usually implies generating it within the INSERT statement using a sequence
-- or a UUID, or using an external system to manage it. For simplicity,
-- we'll assume it's populated during INSERT or managed by a separate process.