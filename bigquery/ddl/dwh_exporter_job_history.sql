-- BigQuery DDL for the job history table
-- Equivalent to legacy DWH$TA_K_MELDUNGEN
-- Tracks job execution metadata
-- Legacy source: vobs/dw_source/isdwh/exporter/is/bin/r_exis_v2
CREATE TABLE IF NOT EXISTS dwh_exporter.job_history (
    job_id STRING NOT NULL OPTIONS(description="Unique identifier for the job instance"),
    run_id STRING NOT NULL OPTIONS(description="Unique identifier for the specific run"),
    job_name STRING NOT NULL OPTIONS(description="Name of the job (e.g., 'r_exis_v2')"),
    start_time TIMESTAMP NOT NULL OPTIONS(description="Job start timestamp"),
    end_time TIMESTAMP OPTIONS(description="Job end timestamp"),
    status STRING NOT NULL OPTIONS(description="Job status (e.g., 'RUNNING', 'SUCCESS', 'FAILED')"),
    message STRING OPTIONS(description="Detailed message about job status or error"),
    parameters_json JSON OPTIONS(description="JSON representation of job parameters"),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Record creation timestamp")
)
PARTITION BY
    DATE(start_time)
CLUSTER BY
    job_name, job_id, run_id;