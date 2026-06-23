-- DDL for BigQuery configuration table based on .dw_lokal
-- Legacy source: vobs/dw_source/istools/seu/template/.dw_init
-- Job: vobs/dw_source/istools/seu/template/.dw_init

CREATE TABLE IF NOT EXISTS `project.dataset.dw_lokal_config`
(
    config_key STRING NOT NULL OPTIONS(description="Configuration key from .dw_lokal"),
    config_value STRING NOT NULL OPTIONS(description="Configuration value from .dw_lokal"),
    description STRING OPTIONS(description="Optional description for the configuration item")
)
OPTIONS(
    description="Configuration values migrated from .dw_lokal"
);