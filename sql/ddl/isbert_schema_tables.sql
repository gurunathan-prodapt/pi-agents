-- BigQuery DDL for tables from legacy job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_c_bfc.ksh
-- Replaces Oracle tables and provides structure for intermediate/logging tables.

-- Placeholder for your GCP Project ID and BigQuery Dataset ID
-- Replace `your-gcp-project` with your actual GCP Project ID.
-- Replace `isbert_schema` with your actual BigQuery Dataset ID.
-- Example: `CREATE TABLE `my-gcp-project.my_dataset.dwtk_meldungen` (...)`

-- DDL for isbert_schema.dwtk_meldungen
CREATE TABLE IF NOT EXISTS `your-gcp-project.isbert_schema.dwtk_meldungen` (
    job_kennung STRING NOT NULL,
    timecreated TIMESTAMP
)
OPTIONS(
    description = 'Replicated from Oracle isbert_schema.dwtk_meldungen, used to determine v_datum.'
);

-- DDL for isbert_schema.all_objects (replicated from PCRS1 for object metadata)
CREATE TABLE IF NOT EXISTS `your-gcp-project.isbert_schema.all_objects` (
    object_name STRING NOT NULL,
    object_type STRING NOT NULL,
    created TIMESTAMP
)
OPTIONS(
    description = 'Replicated from Oracle all_objects@PCRS1, used to determine v_bfc_procedure.'
);

-- DDL for sof$ta_cntrct_crs
CREATE TABLE IF NOT EXISTS `your-gcp-project.isbert_schema.sof$ta_cntrct_crs` (
    cntrct_id INTEGER NOT NULL,
    commitment_reference_date DATE,
    cntrct_validity_id INTEGER,
    bfc_age DATE
)
OPTIONS(
    description = 'Replicated from Oracle sof$ta_cntrct_crs, contract related information.'
);

-- DDL for sof$ta_barrier
CREATE TABLE IF NOT EXISTS `your-gcp-project.isbert_schema.sof$ta_barrier` (
    cntrct_id INTEGER NOT NULL,
    bfc_age DATE
)
OPTIONS(
    description = 'Replicated from Oracle sof$ta_barrier.'
);

-- DDL for sof$ta_cntrct_valid
CREATE TABLE IF NOT EXISTS `your-gcp-project.isbert_schema.sof$ta_cntrct_valid` (
    cntrct_validity_id INTEGER NOT NULL,
    bfc_age DATE,
    first_period_id INTEGER,
    following_period_id INTEGER,
    first_notice_period_id INTEGER,
    follow_notice_period_id INTEGER
)
OPTIONS(
    description = 'Replicated from Oracle sof$ta_cntrct_valid.'
);

-- DDL for sof$ta_period
CREATE TABLE IF NOT EXISTS `your-gcp-project.isbert_schema.sof$ta_period` (
    period_id INTEGER NOT NULL,
    bfc_age DATE
)
OPTIONS(
    description = 'Replicated from Oracle sof$ta_period.'
);

-- DDL for sof$ta_c_bfc_akt (temporary table structure for intermediate results)
CREATE TABLE IF NOT EXISTS `your-gcp-project.isbert_schema.sof$ta_c_bfc_akt` (
    cntrct_id INTEGER NOT NULL,
    commitment_reference_date DATE,
    cntrct_validity_id INTEGER,
    bfc_age DATE,
    bfc_count INTEGER
)
OPTIONS(
    description = 'BigQuery temporary table equivalent for Oracle sof$ta_c_bfc_akt.'
);

-- DDL for sof$ta_c_bfc (main target table for binding period data)
CREATE TABLE IF NOT EXISTS `your-gcp-project.isbert_schema.sof$ta_c_bfc` (
    cntrct_id INTEGER NOT NULL,
    bindefrist DATE,
    bfc_age DATE,
    bfc_count INTEGER,
    bfc_procedure DATE,
    commitment_reference_date DATE,
    cntrct_validity_id INTEGER
)
OPTIONS(
    description = 'Main target table for binding period data, equivalent to Oracle sof$ta_c_bfc.'
);

-- DDL for job_error_log (auxiliary table for logging errors)
CREATE TABLE IF NOT EXISTS `your-gcp-project.isbert_schema.job_error_log` (
    log_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    job_id STRING,
    error_message STRING,
    severity STRING
)
OPTIONS(
    description = 'Auxiliary table for logging job errors.'
);

-- DDL for job_table (auxiliary table for job metadata)
CREATE TABLE IF NOT EXISTS `your-gcp-project.isbert_schema.job_table` (
    job_id STRING NOT NULL,
    job_name STRING,
    description STRING,
    last_run_time TIMESTAMP
)
OPTIONS(
    description = 'Auxiliary table for job metadata.'
);

-- DDL for job_run_log (auxiliary table for tracking job runs)
CREATE TABLE IF NOT EXISTS `your-gcp-project.isbert_schema.job_run_log` (
    run_id STRING NOT NULL DEFAULT GENERATE_UUID(),
    job_id STRING,
    start_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    end_time TIMESTAMP,
    status STRING,
    parameters JSON
)
OPTIONS(
    description = 'Auxiliary table for tracking job runs.'
);