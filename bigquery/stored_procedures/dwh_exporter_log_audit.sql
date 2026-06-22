-- BigQuery Stored Procedure to log audit messages.
-- Legacy source: vobs/dw_source/isdwh/exporter/is/bin/r_exis_v2
CREATE OR REPLACE PROCEDURE dwh_exporter.log_audit(
    IN p_job_id STRING,
    IN p_run_id STRING,
    IN p_step_name STRING,
    IN p_status STRING,
    IN p_log_message STRING,
    IN p_metadata_json JSON
)
BEGIN
    INSERT INTO dwh_exporter.export_audit (
        audit_id, job_id, run_id, step_name, status, start_time, end_time, log_message, metadata_json
    )
    VALUES (
        GENERATE_UUID(), p_job_id, p_run_id, p_step_name, p_status, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), p_log_message, p_metadata_json
    );
END;