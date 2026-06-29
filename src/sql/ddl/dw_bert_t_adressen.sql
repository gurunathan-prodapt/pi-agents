-- Legacy Job: BERT_P_ADRESSEN
-- Legacy Source: DW.BERT_P_ADRESSEN.xml
-- Target Platform: BigQuery
-- Purpose: Schema DDL for address target table and execution audit metadata.

-- Create target schema if it does not exist
CREATE SCHEMA IF NOT EXISTS `dw_bert`;

-- Create target master address table with SCD Type 2 parameters
CREATE TABLE IF NOT EXISTS `dw_bert.t_adressen` (
  address_id STRING NOT NULL OPTIONS(description="Unique key of the address record"),
  street_name STRING OPTIONS(description="Standardized street name"),
  postal_code STRING OPTIONS(description="Standardized postal / zip code"),
  city STRING OPTIONS(description="Standardized city name"),
  country_code STRING OPTIONS(description="Two-letter country code"),
  valid_from TIMESTAMP NOT NULL OPTIONS(description="SCD2 Validity Start Timestamp"),
  valid_to TIMESTAMP NOT NULL OPTIONS(description="SCD2 Validity End Timestamp"),
  is_current BOOLEAN NOT NULL OPTIONS(description="Flag indicating if the record is currently active"),
  source_last_modified TIMESTAMP OPTIONS(description="Last modified timestamp in the staging source"),
  load_ts TIMESTAMP NOT NULL OPTIONS(description="BigQuery load timestamp"),
  batch_id STRING NOT NULL OPTIONS(description="Unique batch GUID of the loading execution")
)
PARTITION BY DATE(valid_from)
CLUSTER BY address_id;

-- Create metadata run log table
CREATE TABLE IF NOT EXISTS `dw_bert.metadata_job_runs` (
  job_name STRING NOT NULL OPTIONS(description="Name of the orchestrated job"),
  start_time TIMESTAMP NOT NULL OPTIONS(description="Job execution start timestamp"),
  end_time TIMESTAMP OPTIONS(description="Job execution completion timestamp"),
  status STRING NOT NULL OPTIONS(description="Final execution status (SUCCESS, FAILED)"),
  rows_affected INT64 OPTIONS(description="Count of rows processed during this execution"),
  message STRING OPTIONS(description="Detailed response message or error string"),
  batch_id STRING OPTIONS(description="Unique batch GUID associated with the execution")
)
PARTITION BY DATE(start_time)
CLUSTER BY job_name, status;