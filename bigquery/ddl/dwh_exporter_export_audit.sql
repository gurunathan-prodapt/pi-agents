-- BigQuery DDL for the export audit table
-- Stores detailed audit information for export steps
-- Legacy source: vobs/dw_source/isdwh/exporter/is/bin/r_exis_v2
CREATE TABLE IF NOT EXISTS dwh_exporter.export_audit (
    audit_id STRING NOT NULL OPTIONS(description="Unique identifier for the audit record"),
    job_id STRING NOT NULL OPTIONS(description="Identifier for the overall job instance"),
    run_id STRING NOT NULL OPTIONS(description="Identifier for the specific run"),
    step_name STRING NOT NULL OPTIONS(description="Name of the export step (e.g., 'exportcore', 'filepartition', 'sqlpartition')"),
    status STRING NOT NULL OPTIONS(description="Status of the step (e.g., 'STARTED', 'COMPLETED', 'FAILED')"),
    start_time TIMESTAMP OPTIONS(description="Step start timestamp"),
    end_time TIMESTAMP OPTIONS(description="Step end timestamp"),
    log_message STRING OPTIONS(description="Detailed log message for the step"),
    metadata_json JSON OPTIONS(description="JSON representation of step-specific metadata"),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Record creation timestamp")
)
PARTITION BY
    DATE(start_time)
CLUSTER BY
    job_id, run_id, step_name;