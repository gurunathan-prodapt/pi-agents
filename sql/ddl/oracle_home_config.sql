-- Migrates legacy source: vobs/dw_source/istools/seu/template/.dw_init
-- Job: vobs/dw_source/istools/seu/template/.dw_init

CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.oracle_home_config`
(
    candidate STRING NOT NULL,
    is_active BOOL NOT NULL DEFAULT FALSE,
    priority INT NOT NULL DEFAULT 0, -- Higher priority means preferred
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
OPTIONS (
    description = "Lookup table for valid Oracle HOME paths, replacing filesystem probing logic."
);