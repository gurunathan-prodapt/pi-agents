-- DML for initial configuration data for dw_global_config table
-- Legacy source: vobs/dw_source/istools/seu/template/.dw_global
-- Job: vobs/dw_source/istools/seu/template/.dw_global

-- Inserts or updates initial configuration values that mirror the parameters
-- required by the `dw_global_init` stored procedure.
-- Replace `your_project_id.your_dataset_name` with your actual BigQuery project and dataset.
INSERT INTO `your_project_id.your_dataset_name.dw_global_config` (config_key, config_value, config_type, description, updated_at)
VALUES
  ('DW_DIR_ROOT', '/app/dw/root', 'path', 'Root directory for Data Warehouse', CURRENT_TIMESTAMP()),
  ('DW_DIR_PROT', '/app/dw/prot', 'path', 'Protected directory for Data Warehouse', CURRENT_TIMESTAMP()),
  ('DW_DIR_CUBES', '/app/dw/cubes', 'path', 'Cubes directory for Data Warehouse', CURRENT_TIMESTAMP()),
  ('DW_DIR_IMP_D1', '/app/dw/imp_d1', 'path', 'Import directory D1', CURRENT_TIMESTAMP()),
  ('DW_DIR_IMP_XTRA', '/app/dw/imp_xtra', 'path', 'Import directory XTRA', CURRENT_TIMESTAMP()),
  ('DW_DIR_IMP_CTEL', '/app/dw/imp_ctel', 'path', 'Import directory CTEL', CURRENT_TIMESTAMP()),
  ('ORACLE_HOME', '/opt/oracle/product/19c', 'path', 'Oracle Home directory', CURRENT_TIMESTAMP()),
  ('EXISTING_LD_LIBRARY_PATH', '/usr/local/lib', 'path', 'Existing LD_LIBRARY_PATH from environment', CURRENT_TIMESTAMP()),
  ('EXISTING_PATH', '/usr/local/bin:/usr/bin:/bin', 'path', 'Existing PATH from environment', CURRENT_TIMESTAMP()),
  ('COGNOS_SETUP_EXISTS', 'true', 'boolean', 'Flag indicating if Cognos setup script exists and needs external action', CURRENT_TIMESTAMP())
ON CONFLICT (config_key) DO UPDATE SET
  config_value = EXCLUDED.config_value,
  config_type = EXCLUDED.config_type,
  description = EXCLUDED.description,
  updated_at = EXCLUDED.updated_at;