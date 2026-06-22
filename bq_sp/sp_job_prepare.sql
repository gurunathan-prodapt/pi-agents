--
-- BigQuery Stored Procedure to manage job activation/deactivation.
-- Replaces implied job management from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs2.ksh
--
CREATE OR REPLACE PROCEDURE `bq_dataset.sp_job_prepare`(
    IN p_job_kennung STRING,
    IN p_action STRING, -- 'ACTIVATE', 'DEACTIVATE', 'CHECK_ACTIVE'
    OUT p_is_active BOOL
)
BEGIN
    DECLARE v_current_status STRING;

    -- Initialize p_is_active
    SET p_is_active = FALSE;

    -- Check current status if job_kennung exists
    SELECT status INTO v_current_status
    FROM `bq_dataset.job_table`
    WHERE job_kennung = p_job_kennung;

    IF v_current_status IS NULL THEN
        -- Job does not exist, insert it as INACTIVE by default
        INSERT INTO `bq_dataset.job_table` (job_kennung, job_description, status, last_update_time, updated_by)
        VALUES (p_job_kennung, 'Description for ' || p_job_kennung, 'INACTIVE', CURRENT_TIMESTAMP(), 'system_init');
        SET v_current_status = 'INACTIVE';
    END IF;

    IF p_action = 'ACTIVATE' THEN
        UPDATE `bq_dataset.job_table`
        SET
            status = 'ACTIVE',
            last_update_time = CURRENT_TIMESTAMP(),
            updated_by = 'sp_job_prepare'
        WHERE job_kennung = p_job_kennung;
        SET p_is_active = TRUE;
        SELECT 'Job ' || p_job_kennung || ' activated.' AS message;
    ELSEIF p_action = 'DEACTIVATE' THEN
        UPDATE `bq_dataset.job_table`
        SET
            status = 'INACTIVE',
            last_update_time = CURRENT_TIMESTAMP(),
            updated_by = 'sp_job_prepare'
        WHERE job_kennung = p_job_kennung;
        SET p_is_active = FALSE;
        SELECT 'Job ' || p_job_kennung || ' deactivated.' AS message;
    ELSEIF p_action = 'CHECK_ACTIVE' THEN
        IF v_current_status = 'ACTIVE' THEN
            SET p_is_active = TRUE;
        END IF;
    ELSE
        RAISE USING MESSAGE = 'Invalid action for sp_job_prepare: ' || p_action;
    END IF;
END;