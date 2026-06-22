-- DDL for a generic configuration table
-- Legacy source: vobs/dw_source/istools/seu/template/.dw_global
-- Job: vobs/dw_source/istools/seu/template/.dw_global

-- This table can store key-value configuration pairs, which could be used
-- to supply parameters to the `dw_global_init` stored procedure if not
-- directly passed by an orchestrator.
-- Replace `your_project_id.your_dataset_name` with your actual BigQuery project and dataset.
CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_name.dw_global_config` (
    config_key STRING NOT NULL OPTIONS(description="Unique configuration key"),
    config_value STRING OPTIONS(description="Value of the configuration"),
    config_type STRING OPTIONS(description="Type of the configuration (e.g., 'string', 'boolean', 'path')"),
    description STRING OPTIONS(description="Description of the configuration item"),
    updated_at TIMESTAMP OPTIONS(description="Timestamp of last update")
)
PRIMARY KEY (config_key);