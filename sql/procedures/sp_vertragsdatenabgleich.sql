-- Main orchestration stored procedure for contract data reconciliation.
-- Replaces the KornShell wrapper logic of r_ausd_v_ta_action_assoc.ksh.
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_action_assoc.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_action_assoc.ksh
CREATE OR REPLACE PROCEDURE project.dataset.sp_vertragsdatenabgleich(
    IN p_stichtag_date DATE,
    IN p_job_version STRING,
    IN p_debug BOOL
)
BEGIN
    DECLARE v_job_id STRING;
    DECLARE v_job_name STRING DEFAULT 'ContractDataReconciliation';
    DECLARE v_start_time TIMESTAMP;
    DECLARE v_parameters_json JSON;
    DECLARE v_error_message STRING;
    DECLARE v_error_stack_trace STRING;

    SET v_start_time = CURRENT_TIMESTAMP();
    SET v_parameters_json = TO_JSON(STRUCT(p_stichtag_date, p_job_version, p_debug));

    -- 1. Initialize job entry
    CALL project.dataset.util_create_job_entry(
        v_job_name,
        p_job_version,
        v_parameters_json,
        v_job_id
    );

    CALL project.dataset.util_log_detail(
        v_job_id,
        'INFO',
        FORMAT('Job %s started with Stichtag: %s', v_job_name, FORMAT_DATE('%Y-%m-%d', p_stichtag_date)),
        'sp_vertragsdatenabgleich'
    );

    -- 2. Set Stichtag info
    CALL project.dataset.util_set_stichtag_info(v_job_id, p_stichtag_date);

    -- 3. Main processing logic with error handling
    BEGIN
        -- Log debug information if enabled
        IF p_debug THEN
            CALL project.dataset.util_log_detail(
                v_job_id,
                'DEBUG',
                'Debug mode is ON.',
                'sp_vertragsdatenabgleich'
            );
        END IF;

        -- Placeholder call to the core business logic procedure
        -- This procedure (sp_k_ausd_v_ta_action_assoc) needs to be migrated separately.
        CALL project.dataset.sp_k_ausd_v_ta_action_assoc(v_job_id, p_stichtag_date);

        -- 4. Update status to OK on success
        CALL project.dataset.util_set_status_ok(v_job_id);
        CALL project.dataset.util_log_detail(
            v_job_id,
            'INFO',
            'Core business logic executed successfully.',
            'sp_vertragsdatenabgleich'
        );

    EXCEPTION WHEN ERROR THEN
        SET v_error_message = @@error.message;
        SET v_error_stack_trace = @@error.stack_trace;

        -- 5. Handle errors
        CALL project.dataset.util_handle_error(
            v_job_id,
            v_error_message,
            'sp_vertragsdatenabgleich',
            v_error_stack_trace
        );

        -- Re-raise the error to signal job failure to the caller/scheduler
        RAISE USING MESSAGE = FORMAT('Job failed for job_id %s: %s', v_job_id, v_error_message);
    END;

END;