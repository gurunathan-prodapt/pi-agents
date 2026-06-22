-- Main BigQuery Stored Procedure for the r_exis_v2 exporter framework.
-- Replaces the core functionality of the KornShell script.
-- Legacy source: vobs/dw_source/isdwh/exporter/is/bin/r_exis_v2
CREATE OR REPLACE PROCEDURE dwh_exporter.r_exis_v2(
    IN p_job_name STRING,
    IN p_run_id STRING,
    IN p_parameters JSON
)
BEGIN
    DECLARE v_job_id STRING;
    DECLARE v_start_time TIMESTAMP;
    DECLARE v_status STRING DEFAULT 'RUNNING';
    DECLARE v_message STRING DEFAULT 'Job started successfully.';
    DECLARE v_config_file STRING;
    DECLARE v_default_file STRING;
    DECLARE v_total_from_str STRING;
    DECLARE v_total_to_str STRING;
    DECLARE v_initial_from_str STRING;
    DECLARE v_file_partition_cfg STRING;
    DECLARE v_sql_partition_cfg STRING;

    SET v_job_id = GENERATE_UUID();
    SET v_start_time = CURRENT_TIMESTAMP();

    -- Log job start
    CALL dwh_exporter.log_audit(v_job_id, p_run_id, 'r_exis_v2_main', 'STARTED', 'Main job execution started.', JSON_OBJECT('parameters', p_parameters));

    -- Insert into job_history
    INSERT INTO dwh_exporter.job_history (
        job_id, run_id, job_name, start_time, status, parameters_json
    )
    VALUES (
        v_job_id, p_run_id, p_job_name, v_start_time, v_status, p_parameters
    );

    -- Phase 1: Configuration & Initialization
    BEGIN
        -- Example of retrieving configuration values
        CALL dwh_exporter.get_config_value(p_job_name, 'P_CONFIG_FILE', v_config_file);
        CALL dwh_exporter.get_config_value(p_job_name, 'P_DEFAULT_FILE', v_default_file);
        CALL dwh_exporter.get_config_value(p_job_name, 'TOTAL_FROM', v_total_from_str);
        CALL dwh_exporter.get_config_value(p_job_name, 'TOTAL_TO', v_total_to_str);
        CALL dwh_exporter.get_config_value(p_job_name, 'INITIAL_FROM', v_initial_from_str);
        CALL dwh_exporter.get_config_value(p_job_name, 'FILE_PARTITION', v_file_partition_cfg);
        CALL dwh_exporter.get_config_value(p_job_name, 'SQL_PARTITION', v_sql_partition_cfg);

        -- Resolve timestamps (using the UDF as an example)
        DECLARE v_total_from TIMESTAMP;
        DECLARE v_total_to TIMESTAMP;
        DECLARE v_initial_from TIMESTAMP;

        -- Assuming 'YYYY-MM-DD HH24:MI:SS' is a common format
        SET v_total_from = dwh_exporter.resolve_timestamp(v_total_from_str, '%Y-%m-%d %H:%M:%S', FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)));
        SET v_total_to = dwh_exporter.resolve_timestamp(v_total_to_str, '%Y-%m-%d %H:%M:%S', FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', CURRENT_TIMESTAMP()));
        SET v_initial_from = dwh_exporter.resolve_timestamp(v_initial_from_str, '%Y-%m-%d %H:%M:%S', FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP('2000-01-01 00:00:00'))); -- Placeholder default

        -- More initialization and parameter handling here...
        -- This would include parsing p_parameters for overrides, handling INITIAL mode, etc.

        CALL dwh_exporter.log_audit(v_job_id, p_run_id, 'r_exis_v2_main', 'INFO', 'Configuration loaded and timestamps resolved.',
            JSON_OBJECT(
                'total_from', v_total_from,
                'total_to', v_total_to,
                'initial_from', v_initial_from
            ));

        -- Phase 2: Execution Planning (Placeholder)
        -- This part would involve dynamic SQL to determine file/SQL partitions based on configs
        -- For a simplified example, we'll just log an info message.
        CALL dwh_exporter.log_audit(v_job_id, p_run_id, 'r_exis_v2_main', 'INFO', 'Execution planning completed (placeholder).', NULL);

        -- Phase 3: Data Export Core (Placeholder for exportcore equivalent)
        -- This would involve iterating over partitions, executing SQL, and writing to GCS.
        -- CALL dwh_exporter.export_core(v_job_id, p_run_id, ...); -- A future sub-procedure

        CALL dwh_exporter.log_audit(v_job_id, p_run_id, 'r_exis_v2_main', 'INFO', 'Data export core completed (placeholder).', NULL);

        -- Phase 4: File Distribution (Placeholder)
        -- This would trigger Cloud Functions/Workflows for distribution.
        -- INSERT INTO dwh_exporter.export_readyfiles (...) to trigger external processes.
        CALL dwh_exporter.log_audit(v_job_id, p_run_id, 'r_exis_v2_main', 'INFO', 'File distribution initiated (placeholder).', NULL);

        SET v_status = 'SUCCESS';
        SET v_message = 'Job completed successfully.';

    EXCEPTION WHEN ERROR THEN
        SET v_status = 'FAILED';
        SET v_message = CONCAT('Job failed with error: ', @@error.message);
        CALL dwh_exporter.log_audit(v_job_id, p_run_id, 'r_exis_v2_main', 'FAILED', v_message, JSON_OBJECT('error_message', @@error.message, 'stack_trace', @@error.stack_trace));
    END;

    -- Update job_history with final status
    UPDATE dwh_exporter.job_history
    SET end_time = CURRENT_TIMESTAMP(),
        status = v_status,
        message = v_message
    WHERE job_id = v_job_id AND run_id = p_run_id;

END;