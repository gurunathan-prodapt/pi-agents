-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs3.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_crs3.ksh
-- Description: DDL for BigQuery tables required for the migration.

-- Table for job logging
CREATE TABLE IF NOT EXISTS `YOUR_PROJECT_ID.YOUR_DATASET_ID.dw_job_log` (
    job_entry_id INT64 NOT NULL OPTIONS(description="Unique identifier for each job execution instance"),
    job_kennung STRING NOT NULL OPTIONS(description="Identifier for the job type (e.g., BERT_V_TA_CNTRCT_CRS3)"),
    program_name STRING OPTIONS(description="Name of the program/script"),
    program_version STRING OPTIONS(description="Version of the program/script"),
    start_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Timestamp when the job started"),
    end_timestamp TIMESTAMP OPTIONS(description="Timestamp when the job ended"),
    status STRING OPTIONS(description="Status of the job (RUNNING, OK, ERROR)"),
    log_message STRING OPTIONS(description="General log message for the job entry")
);

-- Table for detailed error logging
CREATE TABLE IF NOT EXISTS `YOUR_PROJECT_ID.YOUR_DATASET_ID.dw_error_log` (
    error_id INT64 OPTIONS(description="Unique identifier for each error log entry"),
    job_entry_id INT64 NOT NULL OPTIONS(description="Reference to dw_job_log.job_entry_id"),
    job_kennung STRING NOT NULL OPTIONS(description="Identifier for the job type"),
    error_code INT64 OPTIONS(description="Numeric error code"),
    error_argument STRING OPTIONS(description="Argument related to the error (e.g., parameter name)"),
    error_message STRING OPTIONS(description="Detailed error message"),
    error_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Timestamp when the error occurred")
);

-- Target table for contract data (derived from d_ausd_v_ta_cntrct_crs3.sql INSERT statement)
CREATE TABLE IF NOT EXISTS `YOUR_PROJECT_ID.YOUR_DATASET_ID.sof_ta_cntrct_crs3` (
    cntrct_id INT64,
    obj_version INT64,
    contract_number STRING,
    cntrct_template_id INT64,
    cntrct_validity_id INT64,
    valid_from DATE,
    com_per_ext_rea_cv INT64,
    billcycle_id INT64,
    vo_code STRING,
    cntrct_start_date DATE,
    cntrct_st INT64,
    cntrct_parent INT64,
    cntrct_ty INT64,
    cost_centre STRING,
    cost_centre_user STRING,
    commitment_reference_date DATE,
    order_number STRING,
    rv_num STRING,
    twinbill STRING,
    twin_vertrag_id INT64
);

-- Source table for contract data (derived from d_ausd_v_ta_cntrct_crs3.sql SELECT statement)
CREATE TABLE IF NOT EXISTS `YOUR_PROJECT_ID.YOUR_DATASET_ID.sof_ta_cntrct_crs2` (
    cntrct_id INT64,
    obj_version INT64,
    contract_number STRING,
    cntrct_template_id INT64,
    cntrct_validity_id INT64,
    valid_from DATE,
    com_per_ext_rea_cv INT64,
    billcycle_id INT64,
    vo_code STRING,
    cntrct_start_date DATE,
    cntrct_st INT64,
    cntrct_parent INT64,
    cntrct_ty INT64,
    cost_centre STRING,
    cost_centre_user STRING,
    commitment_reference_date DATE,
    order_number STRING,
    rv_num STRING
);

-- Source table for messages/metadata (derived from d_ausd_v_ta_cntrct_crs3.sql SELECT statement)
CREATE TABLE IF NOT EXISTS `YOUR_PROJECT_ID.YOUR_DATASET_ID.dwtk_meldungen` (
    timecreated TIMESTAMP,
    job_kennung STRING
);