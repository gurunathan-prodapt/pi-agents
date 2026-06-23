--
-- BigQuery DDL for placeholder source tables used in d_ausd_v_ta_c_bfc.sql
-- These schemas are inferred from usage and are minimal. Actual schemas must be derived
-- from the source Oracle database.
--

CREATE TABLE IF NOT EXISTS `your_project.your_dataset.sof_ta_cntrct_crs` (
    cntrct_id STRING NOT NULL,
    commitment_reference_date DATE,
    cntrct_validity_id STRING,
    bfc_age DATE
    -- Add other columns as needed from source schema
);

CREATE TABLE IF NOT EXISTS `your_project.your_dataset.sof_ta_barrier` (
    cntrct_id STRING NOT NULL,
    bfc_age DATE
    -- Add other columns as needed
);

CREATE TABLE IF NOT EXISTS `your_project.your_dataset.sof_ta_cntrct_valid` (
    cntrct_validity_id STRING NOT NULL,
    first_period_id STRING,
    following_period_id STRING,
    first_notice_period_id STRING,
    follow_notice_period_id STRING,
    bfc_age DATE
    -- Add other columns as needed
);

CREATE TABLE IF NOT EXISTS `your_project.your_dataset.sof_ta_period` (
    period_id STRING NOT NULL,
    bfc_age DATE
    -- Add other columns as needed
);