-- Migrates legacy source: vobs/dw_source/istools/seu/template/.dw_init
-- Job: vobs/dw_source/istools/seu/template/.dw_init

-- Initial data for oracle_home_config. These entries replace the filesystem checks
-- originally found in .dw_init. Adjust paths and active status as per your environment.
INSERT INTO `your_project_id.your_dataset_id.oracle_home_config` (candidate, is_active, priority)
VALUES
    ('/appl/local/oracle/8.1.6', TRUE, 5), -- Highest priority active version
    ('/appl/local/oracle/7.3.4', FALSE, 4),
    ('/appl/local/oracle/oracle.7.3.3', FALSE, 3),
    ('/appl/local/oracle/7.3.2', FALSE, 2),
    ('/appl/local/oracle/7.2.3', FALSE, 1)
ON CONFLICT (candidate) DO UPDATE SET
    is_active = EXCLUDED.is_active,
    priority = EXCLUDED.priority;