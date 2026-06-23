-- BigQuery Stored Procedure for orchestration and control flow
-- Replaces vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_acc.ksh
CREATE OR REPLACE PROCEDURE `my_project.my_dataset.r_ausd_vertrag_control`(
    p_JobKennung STRING,
    p_EintragsNr STRING
)
BEGIN
    DECLARE v_TabName STRING DEFAULT 'ta_inv_acc';
    DECLARE v_records INT64;
    DECLARE job_active_count INT64;
    DECLARE error_message STRING;
    DECLARE error_code INT64;

    -- Error handling block
    BEGIN
        -- Parameter validation
        IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
            SET error_code = 193; -- Corresponding to ksh ErrNr 193
            SET error_message = 'FEHLER: Jobkennung ist nicht gesetzt.';
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = error_message;
        END IF;

        IF p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '' THEN
            SET error_code = 193; -- Corresponding to ksh ErrNr 193
            SET error_message = 'FEHLER: EintragsNr ist nicht gesetzt.';
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = error_message;
        END IF;

        -- Check if an active job with the same Jobkennung and TabName already exists
        SELECT COUNT(*)
        INTO job_active_count
        FROM `my_project.my_dataset.job_table`
        WHERE job_kennung = p_JobKennung
          AND tab_name = v_TabName
          AND active_flag = TRUE;

        IF job_active_count > 0 THEN
            -- Log that an active job was ignored and exit gracefully without error
            INSERT INTO `my_project.my_dataset.error_log` (error_ts, error_nr, error_arg, procedure_name, message)
            VALUES (CURRENT_TIMESTAMP(), 0, NULL, 'r_ausd_vertrag_control', FORMAT('Active job for Jobkennung: %s and TabName: %s already exists. Ignoring execution.', p_JobKennung, v_TabName));
            RETURN; -- Exit the procedure
        END IF;

        -- Deactivate old active jobs for the current TabName
        UPDATE `my_project.my_dataset.job_table`
        SET active_flag = FALSE,
            completed_ts = CURRENT_TIMESTAMP(),
            error_message = 'Deactivated by new job run'
        WHERE tab_name = v_TabName
          AND active_flag = TRUE
          AND job_kennung <> p_JobKennung; -- Only deactivate *other* jobs for this table, not potentially an identical one being rerun

        -- Insert a new job entry
        INSERT INTO `my_project.my_dataset.job_table` (job_kennung, eintrags_nr, tab_name, active_flag, created_ts)
        VALUES (p_JobKennung, p_EintragsNr, v_TabName, TRUE, CURRENT_TIMESTAMP());

        -- Call the data transformation stored procedure
        CALL `my_project.my_dataset.d_ausd_v_ta_inv_acc`(p_EintragsNr, p_JobKennung);

        -- Get the record count from the target table
        SELECT COUNT(*)
        INTO v_records
        FROM `my_project.sof_dataset.sof$ta_inv_acc`;

        -- Update the job table with completion details
        UPDATE `my_project.my_dataset.job_table`
        SET active_flag = FALSE,
            completed_ts = CURRENT_TIMESTAMP(),
            record_count = v_records,
            error_code = 0,
            error_message = 'Successfully completed'
        WHERE job_kennung = p_JobKennung
          AND eintrags_nr = p_EintragsNr
          AND tab_name = v_TabName
          AND active_flag = TRUE; -- Ensure we update the active entry

    EXCEPTION WHEN ERROR THEN
        SET error_message = @@error.message;
        SET error_code = IFNULL(error_code, -1); -- Default error code if not set by validation
        -- Log the error
        INSERT INTO `my_project.my_dataset.error_log` (error_ts, error_nr, error_arg, procedure_name, message)
        VALUES (CURRENT_TIMESTAMP(), error_code, NULL, 'r_ausd_vertrag_control', error_message);

        -- Update job status with error
        UPDATE `my_project.my_dataset.job_table`
        SET active_flag = FALSE,
            completed_ts = CURRENT_TIMESTAMP(),
            error_code = error_code,
            error_message = error_message
        WHERE job_kennung = p_JobKennung
          AND eintrags_nr = p_EintragsNr
          AND tab_name = v_TabName
          AND active_flag = TRUE;
    END;

END;