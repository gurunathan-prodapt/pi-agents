-- BigQuery DDL for the configuration key-value table
-- Stores parameters from legacy p_ConfigFile and p_DefaultFile
-- Legacy source: vobs/dw_source/isdwh/exporter/is/bin/r_exis_v2
CREATE TABLE IF NOT EXISTS dwh_exporter.config_kv (
    job_name STRING NOT NULL OPTIONS(description="Name of the job this configuration applies to (e.g., 'r_exis_v2')"),
    config_key STRING NOT NULL OPTIONS(description="Configuration parameter key"),
    config_value STRING OPTIONS(description="Configuration parameter value"),
    config_type STRING OPTIONS(description="Expected data type of the value (e.g., 'STRING', 'INT', 'BOOLEAN', 'SQL')"),
    description STRING OPTIONS(description="Description of the configuration parameter"),
    updated_at TIMESTAMP OPTIONS(description="Timestamp of the last update")
)
PARTITION BY
    DATE(updated_at)
CLUSTER BY
    job_name, config_key;