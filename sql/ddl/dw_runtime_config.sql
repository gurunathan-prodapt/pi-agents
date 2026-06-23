-- Migrates legacy source: vobs/dw_source/istools/seu/template/.dw_init
-- Job: vobs/dw_source/istools/seu/template/.dw_init

CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.dw_runtime_config`
(
    config_name STRING NOT NULL,
    config_value STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
OPTIONS (
    description = "Stores runtime configuration variables derived from legacy .dw_init and .dw_global scripts."
);