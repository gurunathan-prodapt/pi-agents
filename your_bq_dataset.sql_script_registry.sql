--
-- BigQuery DDL and DML for sql_script_registry table
-- Replaces filesystem script existence and readability checks from h_alis_sqlplus.ksh
-- JOB: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh
--

CREATE TABLE IF NOT EXISTS `your_gcp_project.your_bq_dataset.sql_script_registry` (
    script_name STRING NOT NULL OPTIONS(description="Original name of the SQL*Plus script"),
    is_readable BOOLEAN NOT NULL OPTIONS(description="Indicates if the migrated script is considered 'readable'/'executable'"),
    target_procedure_name STRING NOT NULL OPTIONS(description="The fully qualified name of the target BigQuery stored procedure that replaces this script"),
    description STRING OPTIONS(description="A brief description of the script's purpose"),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY(script_name) NOT ENFORCED
);

-- Example DML for initial population. This should be expanded with all migrated scripts.
INSERT INTO `your_gcp_project.your_bq_dataset.sql_script_registry` (script_name, is_readable, target_procedure_name, description)
VALUES
    ('vobs/dw_source/isdwh/exporter/apt/sql/d_exis_apt_bestandsdaten.sql', TRUE, 'your_gcp_project.your_bq_dataset.migrated_d_exis_apt_bestandsdaten', 'Migrated D_EXIS_APT_BESTANDSDATEN SQL script'),
    -- Add more entries for other migrated SQL scripts as needed
    ('your/path/to/another/sql_script.sql', TRUE, 'your_gcp_project.your_bq_dataset.migrated_another_sql_script', 'Description for another migrated script');