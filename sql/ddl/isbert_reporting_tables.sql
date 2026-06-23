-- BigQuery DDL for tables used by k_ausd_v_ta_notice.ksh migration
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_notice.ksh

-- IMPORTANT: Replace `your_project_id` and `your_dataset_id` with actual BigQuery project and dataset IDs.
-- For example, `your_project_id.your_dataset_id.table_name` might become `gcp-project-12345.isbert_reporting.table_name`.

-- Table: your_project_id.isbert_reporting.ta_notice (Target table for the processed data)
CREATE TABLE IF NOT EXISTS `your_project_id.isbert_reporting.ta_notice` (
    cntrct_id STRING,
    valid_from DATE,
    valid_to DATE,
    entry_date_of_notice DATE
);

-- Table: your_project_id.isbert_reporting.job_table (For tracking job execution status and metrics)
CREATE TABLE IF NOT EXISTS `your_project_id.isbert_reporting.job_table` (
    job_kennung STRING NOT NULL,
    eintrags_nr STRING NOT NULL, -- Corresponds to p_EintragsNr, often a date string
    tab_name STRING,
    status STRING NOT NULL, -- e.g., 'ACTIVE', 'COMPLETED', 'FAILED'
    record_count INT64,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP,
    error_message STRING
);

-- Table: your_project_id.isbert_reporting.error_log (For detailed error logging)
CREATE TABLE IF NOT EXISTS `your_project_id.isbert_reporting.error_log` (
    error_number INT64,
    error_argument STRING,
    procedure_name STRING NOT NULL,
    created_at TIMESTAMP NOT NULL,
    error_message STRING
);

-- Table: your_project_id.isbert_reporting.cds_ta_notice (Source table, assuming a structure based on original SQL)
-- This table is assumed to exist in BigQuery and corresponds to `cds$ta_notice` in the Oracle source.
-- The `$` in the original table name has been replaced with `_` for BigQuery compatibility.
CREATE TABLE IF NOT EXISTS `your_project_id.isbert_reporting.cds_ta_notice` (
    cntrct_id STRING,
    valid_from TIMESTAMP, -- Assuming TIMESTAMP for source, will cast to DATE in SP
    valid_to TIMESTAMP,   -- Assuming TIMESTAMP for source, will cast to DATE in SP
    entry_date_of_notice TIMESTAMP, -- Assuming TIMESTAMP for source, will cast to DATE in SP
    insert_at TIMESTAMP,
    modified_at TIMESTAMP,
    is_production INT64    -- 1 for true, 0 for false
);