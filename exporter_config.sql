-- BigQuery DDL for exporter_config table
-- Replaces: Configuration files (e.g., k_exis_v2_defaults.cfg) for job vobs/dw_source/isdwh/exporter/is/bin/r_exis_v2
-- This table stores migrated job configurations, including SQL nodes, distribution settings, and metadata.

CREATE TABLE IF NOT EXISTS `your_gcp_project_id.your_bigquery_dataset.exporter_config` (
    job_name STRING NOT NULL OPTIONS(description="Unique identifier for the exporter job, e.g., 'r_exis_v2'"),
    config_key STRING NOT NULL OPTIONS(description="Key for the configuration item, e.g., 'OUTPUT_SQL', 'DISTRIBUTION', 'META'"),
    config_value JSON OPTIONS(description="JSON representation of the configuration value. Can be a string for SQL, or an object for complex settings."),
    description STRING OPTIONS(description="Human-readable description of the config item."),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (job_name, config_key) NOT ENFORCED
);

-- Example Data (Illustrative - actual data will be manually loaded from legacy configs)
-- INSERT INTO `your_gcp_project_id.your_bigquery_dataset.exporter_config` (job_name, config_key, config_value, description) VALUES
-- ('r_exis_v2', 'PRE_SQL', '{"sql": "SELECT \'Hello from PRE_SQL\' AS message;", "description": "Pre-execution SQL"}'::JSON, 'SQL to run before main extraction'),
-- ('r_exis_v2', 'OUTPUT_SQL', '{"sql": "SELECT current_timestamp() AS ts, \'data\' AS value;", "description": "Main data extraction SQL"}'::JSON, 'Main SQL for data extraction'),
-- ('r_exis_v2', 'POST_SQL', '{"sql": "SELECT \'Goodbye from POST_SQL\' AS message;", "description": "Post-execution SQL"}'::JSON, 'SQL to run after main extraction'),
-- ('r_exis_v2', 'DISTRIBUTION', '{"method": "SFTP", "target_path": "/outbound/r_exis_v2", "compression": "GZIP", "email_on_success": "true", "email_recipients": "success@example.com"}'::JSON, 'File distribution settings'),
-- ('r_exis_v2', 'META', '{"file_prefix": "r_exis_v2_", "file_suffix": ".csv", "delimiter": "|", "header": "true"}'::JSON, 'Metadata for exported files');