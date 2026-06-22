-- BigQuery Stored Procedure for DW.BERT_AUSD_BP_TA_ICCID_VERTRAG
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_vertrag.ksh
-- Purpose: Top-level orchestration, parameter handling, and logging.

CREATE OR REPLACE PROCEDURE `project.target_dataset.r_ausd_bp_ta_iccid_vertrag_sp`(
    p_stichtag STRING,
    p_wiederanlaufWert INT64
)
BEGIN
    DECLARE job_kennung STRING DEFAULT 'AUSD_BP_TA_ICCID_VERTRAG';
    DECLARE eintrags_nr STRING DEFAULT '00001'; -- Default or determined from metadata
    DECLARE job_uuid STRING;
    DECLARE v_log_message STRING;
    DECLARE v_status STRING DEFAULT 'RUNNING';

    SET job_uuid = GENERATE_UUID();

    -- Initialize job registry entry
    INSERT INTO `project.audit_dataset.job_registry` (job_id, job_name, start_time, status, parameters)
    VALUES (job_uuid, job_kennung, CURRENT_TIMESTAMP(), 'RUNNING', TO_JSON(STRUCT(p_stichtag, p_wiederanlaufWert)));

    INSERT INTO `project.audit_dataset.job_log` (job_id, log_time, log_level, message)
    VALUES (job_uuid, CURRENT_TIMESTAMP(), 'INFO', FORMAT("r_ausd_bp_ta_iccid_vertrag_sp started with parameters: p_stichtag='%s', p_wiederanlaufWert=%d", p_stichtag, p_wiederanlaufWert));

    -- Parameter validation
    IF p_stichtag IS NULL OR SAFE.PARSE_DATE('%Y%m%d', p_stichtag) IS NULL THEN
        SET v_log_message = FORMAT('Error: Required parameter p_stichtag is missing or invalid. Expected YYYYMMDD, got: %s', p_stichtag);
        SET v_status = 'FAILED';
        INSERT INTO `project.audit_dataset.job_log` (job_id, log_time, log_level, message)
        VALUES (job_uuid, CURRENT_TIMESTAMP(), 'ERROR', v_log_message);
        UPDATE `project.audit_dataset.job_registry`
        SET
            end_time = CURRENT_TIMESTAMP(),
            status = v_status,
            error_message = v_log_message,
            last_update_time = CURRENT_TIMESTAMP()
        WHERE job_id = job_uuid;
        RAISE USING MESSAGE = v_log_message;
    END IF;

    -- Call k_ausd_bp_ta_iccid_vertrag_sp
    BEGIN
        CALL `project.target_dataset.k_ausd_bp_ta_iccid_vertrag_sp`(
            job_kennung,
            eintrags_nr,
            p_stichtag,
            CAST(p_wiederanlaufWert AS STRING), -- k_sp expects STRING
            job_uuid
        );
        SET v_status = 'SUCCEEDED';
        SET v_log_message = 'Successfully completed k_ausd_bp_ta_iccid_vertrag_sp execution.';
        INSERT INTO `project.audit_dataset.job_log` (job_id, log_time, log_level, message)
        VALUES (job_uuid, CURRENT_TIMESTAMP(), 'INFO', v_log_message);

    EXCEPTION WHEN ERROR THEN
        SET v_log_message = FORMAT('Error calling k_ausd_bp_ta_iccid_vertrag_sp: %s', @@error.message);
        SET v_status = 'FAILED';
        INSERT INTO `project.audit_dataset.job_log` (job_id, log_time, log_level, message)
        VALUES (job_uuid, CURRENT_TIMESTAMP(), 'ERROR', v_log_message);
        -- k_sp already updated job_registry on failure, just re-raise here for airflow
        RAISE USING MESSAGE = v_log_message;
    END;

    -- Final update of job registry (if not already failed by k_sp)
    UPDATE `project.audit_dataset.job_registry`
    SET
        end_time = CURRENT_TIMESTAMP(),
        status = v_status,
        last_update_time = CURRENT_TIMESTAMP()
    WHERE job_id = job_uuid AND status = 'RUNNING'; -- Only update if k_sp didn't mark as FAILED

END;