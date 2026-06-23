-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_c_bfc.ksh
-- This file defines the BigQuery tables required for the migration.

-- Create the dataset if it doesn't exist
CREATE SCHEMA IF NOT EXISTS `isbert_dataset`;

-- Table for job logging and status management, replacing shell script's internal logic
CREATE TABLE IF NOT EXISTS `isbert_dataset.job_status_log` (
    job_kennung STRING NOT NULL OPTIONS(description="Identifier for the job."),
    eintrags_nr STRING NOT NULL OPTIONS(description="Entry number or instance identifier for the job run."),
    start_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the job started."),
    end_timestamp TIMESTAMP OPTIONS(description="Timestamp when the job ended."),
    status STRING NOT NULL OPTIONS(description="Current status of the job (e.g., 'RUNNING', 'COMPLETED', 'FAILED', 'DEACTIVATED')."),
    record_count INT64 OPTIONS(description="Number of records processed by the job."),
    message STRING OPTIONS(description="Additional messages or error details for the job run.")
)
OPTIONS(
    description="Logs the status and execution details of jobs, replacing the shell script's job tracking."
);

-- Mock table for `isbert_schema.dwtk_meldungen` to infer `timecreated` and `job_kennung`
-- NOTE: This schema is inferred. Please replace with actual source schema if available.
CREATE TABLE IF NOT EXISTS `isbert_dataset.dwtk_meldungen` (
    timecreated TIMESTAMP OPTIONS(description="Creation timestamp of the message."),
    job_kennung STRING OPTIONS(description="Job identifier related to the message.")
)
OPTIONS(
    description="Mock table for original Oracle `isbert_schema.dwtk_meldungen`. Schema inferred from usage."
);


-- Main target table `ta_c_bfc`
-- Schema inferred from INSERT and MERGE statements in d_ausd_v_ta_c_bfc.sql
CREATE TABLE IF NOT EXISTS `isbert_dataset.ta_c_bfc` (
    cntrct_id STRING NOT NULL OPTIONS(description="Contract Identifier."),
    bindefrist DATE OPTIONS(description="Binding period date, calculated by bfc_get_bindefrist."),
    bfc_age DATE OPTIONS(description="Age of the BFC (BindeFristCaching) data."),
    bfc_count INT64 OPTIONS(description="Count of BFC related items."),
    bfc_procedure DATE OPTIONS(description="Date when the BFC procedure was last run or its version date."),
    commitment_reference_date DATE OPTIONS(description="Commitment reference date."),
    cntrct_validity_id STRING OPTIONS(description="Contract validity identifier.")
)
OPTIONS(
    description="Main target table for BindeFristCaching data, migrated from Oracle `sof$ta_c_bfc`."
);

-- Staging table `ta_c_bfc_akt`
-- Schema inferred from INSERT ... SELECT statement in d_ausd_v_ta_c_bfc.sql
CREATE TABLE IF NOT EXISTS `isbert_dataset.ta_c_bfc_akt` (
    cntrct_id STRING NOT NULL OPTIONS(description="Contract Identifier."),
    commitment_reference_date DATE OPTIONS(description="Commitment reference date."),
    cntrct_validity_id STRING OPTIONS(description="Contract validity identifier."),
    bfc_age DATE OPTIONS(description="Age of the BFC data, derived from various source tables."),
    bfc_count INT64 OPTIONS(description="Count of BFC related items.")
)
OPTIONS(
    description="Staging table for active BindeFristCaching data, migrated from Oracle `sof$ta_c_bfc_akt`."
);

-- Source table `ta_cntrct_crs`
-- Schema inferred from usage in d_ausd_v_ta_c_bfc.sql
-- NOTE: This schema is inferred. Please replace with actual source schema if available.
CREATE TABLE IF NOT EXISTS `isbert_dataset.ta_cntrct_crs` (
    cntrct_id STRING NOT NULL OPTIONS(description="Contract Identifier."),
    commitment_reference_date DATE OPTIONS(description="Commitment reference date."),
    cntrct_validity_id STRING OPTIONS(description="Contract validity identifier."),
    bfc_age DATE OPTIONS(description="BFC age for this contract.")
)
OPTIONS(
    description="Mock table for original Oracle `sof$ta_cntrct_crs`. Schema inferred from usage."
);

-- Source table `ta_barrier`
-- Schema inferred from usage in d_ausd_v_ta_c_bfc.sql
-- NOTE: This schema is inferred. Please replace with actual source schema if available.
CREATE TABLE IF NOT EXISTS `isbert_dataset.ta_barrier` (
    cntrct_id STRING NOT NULL OPTIONS(description="Contract Identifier."),
    bfc_age DATE OPTIONS(description="BFC age for this barrier.")
)
OPTIONS(
    description="Mock table for original Oracle `sof$ta_barrier`. Schema inferred from usage."
);

-- Source table `ta_cntrct_valid`
-- Schema inferred from usage in d_ausd_v_ta_c_bfc.sql
-- NOTE: This schema is inferred. Please replace with actual source schema if available.
CREATE TABLE IF NOT EXISTS `isbert_dataset.ta_cntrct_valid` (
    cntrct_validity_id STRING NOT NULL OPTIONS(description="Contract validity identifier."),
    bfc_age DATE OPTIONS(description="BFC age for this contract validity."),
    first_period_id STRING OPTIONS(description="First period identifier."),
    following_period_id STRING OPTIONS(description="Following period identifier."),
    first_notice_period_id STRING OPTIONS(description="First notice period identifier."),
    follow_notice_period_id STRING OPTIONS(description="Follow notice period identifier.")
)
OPTIONS(
    description="Mock table for original Oracle `sof$ta_cntrct_valid`. Schema inferred from usage."
);

-- Source table `ta_period`
-- Schema inferred from usage in d_ausd_v_ta_c_bfc.sql
-- NOTE: This schema is inferred. Please replace with actual source schema if available.
CREATE TABLE IF NOT EXISTS `isbert_dataset.ta_period` (
    period_id STRING NOT NULL OPTIONS(description="Period Identifier."),
    bfc_age DATE OPTIONS(description="BFC age for this period.")
)
OPTIONS(
    description="Mock table for original Oracle `sof$ta_period`. Schema inferred from usage."
);