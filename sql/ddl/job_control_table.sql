-- Migration of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_msisdn.ksh
-- BigQuery DDL for job_control_table
-- Replaces temporary file for record counting and legacy FOS job management.

CREATE TABLE IF NOT EXISTS `dataset.job_control_table` (
    job_kennung STRING NOT NULL OPTIONS(description="Identifier for the job, e.g., from p_JobKennung"),
    eintrags_nr STRING NOT NULL OPTIONS(description="Entry number for the job, e.g., from p_EintragsNr"),
    stichtag DATE NOT NULL OPTIONS(description="Reference date for the job, converted from p_Stichtag (DDMMYYYY)"),
    wiederanlauf_wert STRING OPTIONS(description="Restart value for the job, e.g., from p_wiederanlaufWert"),
    start_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the job started"),
    end_timestamp TIMESTAMP OPTIONS(description="Timestamp when the job ended"),
    status STRING OPTIONS(description="Status of the job (e.g., 'SUCCESS', 'FAILED', 'RUNNING')"),
    processed_records INT64 OPTIONS(description="Number of records processed by the job"),
    error_message STRING OPTIONS(description="Error message if the job failed")
)
PARTITION BY stichtag
CLUSTER BY job_kennung, eintrags_nr;