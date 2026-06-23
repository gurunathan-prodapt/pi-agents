-- DDLs for BigQuery tables migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_def.ksh
-- and related SQL script d_ausd_v_ta_inv_def.sql

-- Target job_status_log table for orchestration and logging
CREATE TABLE IF NOT EXISTS `my_gcp_project.isrpt_isbert.job_status_log` (
    job_name STRING NOT NULL OPTIONS(description="Name of the BigQuery job/stored procedure."),
    job_kennung STRING OPTIONS(description="Job identifier from legacy system (p_JobKennung)."),
    entry_number STRING OPTIONS(description="Entry number from legacy system (p_EintragsNr)."),
    status STRING NOT NULL OPTIONS(description="Current status of the job (e.g., 'RUNNING', 'SUCCESS', 'FAILED', 'IGNORED', 'DEACTIVATED')."),
    start_time TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the job started."),
    end_time TIMESTAMP OPTIONS(description="Timestamp when the job ended."),
    records_processed INT64 OPTIONS(description="Number of records processed by the job."),
    message STRING OPTIONS(description="Detailed message or error information.")
)
OPTIONS(
    description="Logs the execution status and details of BigQuery jobs orchestrated from legacy ksh scripts."
);

-- Target sof_ta_inv_def table
-- Schema inferred from INSERT statement in d_ausd_v_ta_inv_def.sql
CREATE TABLE IF NOT EXISTS `my_gcp_project.isrpt_isbert.sof_ta_inv_def` (
    inv_definition_id INT64 NOT NULL,
    acc_ref_id INT64,
    inv_pay_ty_cv STRING,
    inv_media_cv STRING,
    billcycle_id INT64,
    sales_tax_freed BOOL,
    INV_CONT_CONFIG_ID INT64,
    rechn_inh_konfig_text STRING
)
OPTIONS(
    description="Target table for 'sof$ta_inv_def' from Oracle, populated by sp_d_ausd_v_ta_inv_def."
);

-- Target via table (Placeholder - schema details from MERGE statement were not available in d_ausd_v_ta_inv_def.sql)
-- Please update this schema once the full MERGE logic is identified.
CREATE TABLE IF NOT EXISTS `my_gcp_project.isrpt_isbert.via` (
    id INT64 NOT NULL,
    description STRING,
    created_at TIMESTAMP
    -- TODO: Define actual schema based on the MERGE statement in d_ausd_v_ta_inv_def.sql
)
OPTIONS(
    description="Target table for 'VIA' from Oracle. Schema is a placeholder and needs to be completed."
);

-- Source dwtk_meldungen table (Assuming ingestion pipelines populate this)
-- Schema inferred from SELECT statement in d_ausd_v_ta_inv_def.sql
CREATE TABLE IF NOT EXISTS `my_gcp_project.isrpt_isbert.dwtk_meldungen` (
    job_kennung STRING NOT NULL,
    timecreated TIMESTAMP NOT NULL
    -- TODO: Add other columns from the original Oracle 'isbert_schema.dwtk_meldungen' table
)
OPTIONS(
    description="Source table 'dwtk_meldungen' from Oracle. Data is expected to be ingested into BigQuery."
);

-- Source cds_ta_inv_definition table (Assuming ingestion pipelines populate this)
-- Schema inferred from SELECT statement in d_ausd_v_ta_inv_def.sql
CREATE TABLE IF NOT EXISTS `my_gcp_project.isrpt_isbert.cds_ta_inv_definition` (
    inv_definition_id INT64 NOT NULL,
    acc_ref_id INT64,
    inv_pay_ty_cv STRING,
    inv_media_cv STRING,
    billcycle_id INT64,
    sales_tax_freed BOOL,
    INV_CONT_CONFIG_ID INT64,
    insert_at TIMESTAMP,
    modified_at TIMESTAMP,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_production BOOL
    -- TODO: Add other columns from the original Oracle 'CDS$TA_INV_DEFINITION' table
)
OPTIONS(
    description="Source table 'CDS$TA_INV_DEFINITION' from Oracle. Data is expected to be ingested into BigQuery."
);

-- Source cds_ta_inv_cont_config table (Assuming ingestion pipelines populate this)
-- Schema inferred from SELECT statement in d_ausd_v_ta_inv_def.sql
CREATE TABLE IF NOT EXISTS `my_gcp_project.isrpt_isbert.cds_ta_inv_cont_config` (
    INV_CONT_CONFIG_ID INT64 NOT NULL,
    CDS_DESCRIPTION_ID INT64,
    insert_at TIMESTAMP,
    modified_at TIMESTAMP,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_production BOOL
    -- TODO: Add other columns from the original Oracle 'CDS$TA_INV_CONT_CONFIG' table
)
OPTIONS(
    description="Source table 'CDS$TA_INV_CONT_CONFIG' from Oracle. Data is expected to be ingested into BigQuery."
);

-- Source cds_ta_care_description table (Assuming ingestion pipelines populate this)
-- Schema inferred from SELECT statement in d_ausd_v_ta_inv_def.sql
CREATE TABLE IF NOT EXISTS `my_gcp_project.isrpt_isbert.cds_ta_care_description` (
    CDS_DESCRIPTION_ID INT64 NOT NULL,
    CDS_DESCRIPTION STRING
    -- TODO: Add other columns from the original Oracle 'CDS$TA_CARE_DESCRIPTION' table
)
OPTIONS(
    description="Source table 'CDS$TA_CARE_DESCRIPTION' from Oracle. Data is expected to be ingested into BigQuery."
);