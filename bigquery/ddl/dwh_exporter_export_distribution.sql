-- BigQuery DDL for the export distribution table
-- Stores instructions for file distribution post-export
-- Legacy source: vobs/dw_source/isdwh/exporter/is/bin/r_exis_v2
CREATE TABLE IF NOT EXISTS dwh_exporter.export_distribution (
    distribution_id STRING NOT NULL OPTIONS(description="Unique identifier for the distribution rule"),
    job_name STRING NOT NULL OPTIONS(description="Name of the job this rule applies to"),
    file_pattern STRING NOT NULL OPTIONS(description="Pattern to match exported files (e.g., '*.csv')"),
    distribution_method STRING NOT NULL OPTIONS(description="Method of distribution (e.g., 'SFTP', 'EMAIL', 'GCS_MOVE')"),
    target_path STRING OPTIONS(description="Target destination for the file (e.g., SFTP path, GCS bucket)"),
    recipient STRING OPTIONS(description="Email recipient for 'EMAIL' method"),
    options_json JSON OPTIONS(description="JSON object for method-specific options (e.g., compression, subject)"),
    is_active BOOLEAN DEFAULT TRUE OPTIONS(description="Indicates if the distribution rule is active"),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Record creation timestamp"),
    updated_at TIMESTAMP OPTIONS(description="Timestamp of the last update")
)
PARTITION BY
    DATE(created_at)
CLUSTER BY
    job_name, distribution_method;