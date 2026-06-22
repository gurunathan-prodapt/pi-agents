-- Legacy Source: Status tracking functionality from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_optionen.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_optionen.ksh
--
-- This DDL creates the BigQuery table to track overall job run statuses.
-- Replace `your_gcp_project.your_bq_dataset` with your actual GCP project ID and BigQuery dataset name.

CREATE TABLE IF NOT EXISTS `your_gcp_project.your_bq_dataset.job_status` (
    job_run_id STRING NOT NULL OPTIONS(description="Unique ID for each job run, corresponding to JobKennung"),
    job_name STRING NOT NULL OPTIONS(description="Name of the job (e.g., r_ausd_bp_ta_bpr_optionen.ksh)"),
    start_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the job run started"),
    end_timestamp TIMESTAMP OPTIONS(description="Timestamp when the job run ended"),
    status STRING NOT NULL OPTIONS(description="Current status of the job run (RUNNING, SUCCESS, FAILED)"),
    stichtag DATE OPTIONS(description="Reference date for the job run (p_stichtag)"),
    wiederanlaufwert INT64 OPTIONS(description="Restart value for the job (p_wiederanlaufWert)")
);