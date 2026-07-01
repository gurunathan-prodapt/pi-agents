-- BigQuery Standard SQL
-- File: sql/bert/r_drop_temp_table.sql

-- Reusable configuration via script variables.
DECLARE gcp_project_id STRING DEFAULT '{{ var.value.gcp_project_id }}';
DECLARE staging_dataset STRING DEFAULT '{{ var.value.bert_staging_dataset }}';

-- -------------------------------------------------------------------
-- Helper procedure: safely drop a table if it exists.
-- -------------------------------------------------------------------
CREATE TEMP PROCEDURE drop_table_if_exists(
  project_id STRING,
  dataset_id STRING,
  table_id STRING
)
BEGIN
  EXECUTE IMMEDIATE FORMAT(
    'DROP TABLE IF EXISTS `%s.%s.%s`',
    project_id,
    dataset_id,
    table_id
  );
END;

-- -------------------------------------------------------------------
-- Main cleanup routine.
-- -------------------------------------------------------------------
BEGIN
  CALL drop_table_if_exists(gcp_project_id, staging_dataset, 'temp_rech');
  CALL drop_table_if_exists(gcp_project_id, staging_dataset, 'temp_vert');
  CALL drop_table_if_exists(gcp_project_id, staging_dataset, 'temp_gp');
  CALL drop_table_if_exists(gcp_project_id, staging_dataset, 'temp_basis');
  CALL drop_table_if_exists(gcp_project_id, staging_dataset, 'temp_adress');
  CALL drop_table_if_exists(gcp_project_id, staging_dataset, 'temp_stamm');
END;