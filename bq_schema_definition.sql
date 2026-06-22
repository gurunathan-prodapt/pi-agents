-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn.ksh

-- This file contains the DDL for the BigQuery tables used by the r_ausd_bp_ta_msisdn job.

-- Set default project and dataset for convenience. Replace with your actual project and dataset.
-- ALTER SESSION SET CURRENT_PROJECT = 'my-gcp-project';
-- ALTER SESSION SET CURRENT_DATASET = 'isbert_dataset';

-- 1. Source Table: sof_ta_msisdn_his (equivalent to contract_cache_source for this job's context)
-- Inferred schema based on usage in d_ausd_bp_ta_msisdn.sql
CREATE TABLE IF NOT EXISTS `my-gcp-project.isbert_dataset.sof_ta_msisdn_his`
(
    bpri_com_id         STRING      OPTIONS(description="Basic Product Instance Component ID"),
    msisdn              STRING      OPTIONS(description="Mobile Station International Subscriber Directory Number"),
    callnumber_role_id  STRING      OPTIONS(description="Role ID of the call number"),
    valid_to            DATE        OPTIONS(description="Validity end date of the record"),
    -- Add other potential columns if known from source DWH contract cache
    creation_date       TIMESTAMP,
    last_update_date    TIMESTAMP
)
OPTIONS(
    description="Historical MSISDN data, used as source for current valid entries (legacy: sof$ta_msisdn_his)"
);

-- 2. Target Table: sof_ta_msisdn (equivalent to PoolBasisprodukt_target for this job's context)
-- Schema derived from the INSERT statement in d_ausd_bp_ta_msisdn.sql
CREATE TABLE IF NOT EXISTS `my-gcp-project.isbert_dataset.sof_ta_msisdn`
(
    BPR_INSTANCE_ID     STRING      OPTIONS(description="Basic Product Instance ID"),
    MSISDN              STRING      OPTIONS(description="Mobile Station International Subscriber Directory Number"),
    CALLNUMBER_ROLE_ID  STRING      OPTIONS(description="Role ID of the call number"),
    VALID_TO            DATE        OPTIONS(description="Validity end date of the MSISDN")
)
OPTIONS(
    description="Current valid MSISDN entries for basic products (legacy: sof$ta_msisdn, target for this job)"
);

-- 3. Audit Table: job_audit
-- To log job status, errors, and execution details.
CREATE TABLE IF NOT EXISTS `my-gcp-project.isbert_dataset.job_audit`
(
    job_id              STRING      NOT NULL OPTIONS(description="Identifier for the job (e.g., r_ausd_bp_ta_msisdn)"),
    run_id              STRING      NOT NULL OPTIONS(description="Unique identifier for each job run"),
    start_timestamp     TIMESTAMP   NOT NULL OPTIONS(description="Timestamp when the job run started"),
    end_timestamp       TIMESTAMP           OPTIONS(description="Timestamp when the job run ended"),
    status              STRING              OPTIONS(description="Status of the job run (e.g., 'RUNNING', 'SUCCESS', 'FAILED')"),
    error_message       STRING              OPTIONS(description="Detailed error message if the job failed"),
    stichtag            DATE                OPTIONS(description="Cutoff date used for the job run"),
    wiederanlaufwert    INT64               OPTIONS(description="Restart value used for the job run")
)
OPTIONS(
    description="Audit log for all job executions"
);

-- 4. Job Result Counts Table: job_result_counts
-- To store metrics like the number of records processed.
CREATE TABLE IF NOT EXISTS `my-gcp-project.isbert_dataset.job_result_counts`
(
    job_id              STRING      NOT NULL OPTIONS(description="Identifier for the job"),
    run_id              STRING      NOT NULL OPTIONS(description="Unique identifier for the job run"),
    stichtag            DATE                OPTIONS(description="Cutoff date associated with the count"),
    record_count        INT64               OPTIONS(description="Number of records processed or generated"),
    timestamp           TIMESTAMP   NOT NULL OPTIONS(description="Timestamp when the count was recorded")
)
OPTIONS(
    description="Stores record counts for job outputs"
);

-- 5. Additional Source Table: dwtk_meldungen
-- Referenced in d_ausd_bp_ta_msisdn.sql for v_datum derivation
CREATE TABLE IF NOT EXISTS `my-gcp-project.isbert_dataset.dwtk_meldungen`
(
    timecreated         TIMESTAMP   NOT NULL OPTIONS(description="Timestamp of the message creation"),
    job_kennung         STRING      NOT NULL OPTIONS(description="Job identifier related to the message"),
    message_text        STRING              OPTIONS(description="Content of the message")
    -- Add other columns as per actual source table definition
)
OPTIONS(
    description="Table for system messages and job-related events (legacy: isbert_schema.dwtk_meldungen)"
);