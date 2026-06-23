-- DDL for BigQuery configuration table based on .dw_global
-- Legacy source: vobs/dw_source/istools/seu/template/.dw_init
-- Job: vobs/dw_source/istools/seu/template/.dw_init

CREATE TABLE IF NOT EXISTS `project.dataset.dw_global_config`
(
    config_key STRING NOT NULL OPTIONS(description="Configuration key from .dw_global"),
    config_value STRING NOT NULL OPTIONS(description="Configuration value from .dw_global"),
    description STRING OPTIONS(description="Optional description for the configuration item")
)
OPTIONS(
    description="Configuration values migrated from .dw_global"
);