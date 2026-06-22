-- BigQuery DDL for the export ready files table
-- Manages metadata for generated output and ready-files
-- Legacy source: vobs/dw_source/isdwh/exporter/is/bin/r_exis_v2
CREATE TABLE IF NOT EXISTS dwh_exporter.export_readyfiles (
    file_id STRING NOT NULL OPTIONS(description="Unique identifier for the exported file"),
    job_id STRING NOT NULL OPTIONS(description="Identifier for the overall job instance"),
    run_id STRING NOT NULL OPTIONS(description="Identifier for the specific run"),
    file_name STRING NOT NULL OPTIONS(description="Name of the generated output file"),
    gcs_path STRING NOT NULL OPTIONS(description="Google Cloud Storage path of the output file"),
    ready_file_path STRING OPTIONS(description="Google Cloud Storage path for the corresponding 'ready' file"),
    status STRING NOT NULL OPTIONS(description="Status of the file (e.g., 'CREATED', 'DISTRIBUTED', 'FAILED')"),
    creation_time TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the file was created"),
    distribution_start_time TIMESTAMP OPTIONS(description="Timestamp when distribution started"),
    distribution_end_time TIMESTAMP OPTIONS(description="Timestamp when distribution ended"),
    metadata_json JSON OPTIONS(description="JSON representation of file-specific metadata"),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Record creation timestamp")
)
PARTITION BY
    DATE(creation_time)
CLUSTER BY
    job_id, run_id, file_name;