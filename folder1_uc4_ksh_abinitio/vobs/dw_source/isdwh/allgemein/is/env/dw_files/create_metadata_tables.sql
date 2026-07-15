-- DDL for tracking and managing configuration parameter mapping across legacy and modern GCP targets.
CREATE TABLE IF NOT EXISTS `control_metadata.env_variable_mapping` (
  legacy_var_name STRING NOT NULL OPTIONS(description="Legacy Ab Initio variable name (e.g., AI_SERIAL)"),
  gcp_target_type STRING NOT NULL OPTIONS(description="Target repository type: GCS_BUCKET, AIRFLOW_VAR, SECRET_MGR"),
  gcp_target_key STRING NOT NULL OPTIONS(description="The matching reference key in target infrastructure"),
  environment_phase STRING NOT NULL OPTIONS(description="Environment tier: dev, qa, prod"),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PRIMARY KEY (legacy_var_name, environment_phase) NOT ENFORCED;

-- DDL for tracking pipeline executions and status transitions, replacing legacy log-file tracking.
CREATE TABLE IF NOT EXISTS `control_metadata.pipeline_execution_audit` (
  job_id STRING NOT NULL OPTIONS(description="Unique operational ID generated per DAG pipeline execution run"),
  pipeline_name STRING NOT NULL OPTIONS(description="Legacy run wrapper name or current Airflow DAG identifier"),
  start_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  end_timestamp TIMESTAMP,
  execution_status STRING NOT NULL OPTIONS(description="Tracks task states: RUNNING, SUCCESS, FAILED, TIMEOUT"),
  records_processed INT64,
  error_message STRING
)
PRIMARY KEY (job_id) NOT ENFORCED;

-- DDL for resolving Oracle connection names to target modern BigQuery datasets.
CREATE TABLE IF NOT EXISTS `control_metadata.database_schema_mapping` (
  legacy_oracle_schema STRING NOT NULL OPTIONS(description="Physical source schema on Oracle database"),
  bq_project_id STRING NOT NULL OPTIONS(description="Target BigQuery GCP Project Reference"),
  bq_dataset_name STRING NOT NULL OPTIONS(description="Target BigQuery Dataset Name representing schema target"),
  active_status STRING DEFAULT 'ACTIVE'
)
PRIMARY KEY (legacy_oracle_schema) NOT ENFORCED;