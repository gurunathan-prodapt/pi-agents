-- BigQuery DDL for exporter_distribution_queue table
-- Replaces: Ad-hoc file distribution methods (scp, sftp, mailx) for job vobs/dw_source/isdwh/exporter/is/bin/r_exis_v2
-- This table serves as a queue for external file distribution tasks.

CREATE TABLE IF NOT EXISTS `your_gcp_project_id.your_bigquery_dataset.exporter_distribution_queue` (
    queue_id STRING NOT NULL OPTIONS(description="Unique identifier for each distribution task"),
    job_name STRING NOT NULL OPTIONS(description="Unique identifier for the exporter job, e.g., 'r_exis_v2'"),
    run_id STRING NOT NULL OPTIONS(description="Unique identifier for the job run that generated the file"),
    file_path_gcs STRING NOT NULL OPTIONS(description="GCS URI of the file to be distributed"),
    distribution_method STRING NOT NULL OPTIONS(description="Method of distribution (e.g., 'SFTP', 'SCP', 'EMAIL')"),
    target_details JSON NOT NULL OPTIONS(description="JSON object containing target-specific details (e.g., SFTP host/path, email recipients)"),
    status STRING NOT NULL OPTIONS(description="Status of the distribution task (e.g., 'PENDING', 'PROCESSING', 'COMPLETED', 'FAILED')"),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    processed_at TIMESTAMP OPTIONS(description="Timestamp when the distribution task was processed"),
    error_message STRING OPTIONS(description="Error message if distribution failed"),
    PRIMARY KEY (queue_id) NOT ENFORCED
)
PARTITION BY DATE(created_at)
CLUSTER BY job_name, status;