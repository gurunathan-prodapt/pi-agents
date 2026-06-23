-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_discount.ksh
-- Description: BigQuery Stored Procedure that replaces the KornShell script's orchestration logic.
-- It handles parameter validation, job state management, invokes the data transformation,
-- and logs execution details.
CREATE OR REPLACE PROCEDURE dataset.r_ausd_vertrag_control(
    p_JobKennung STRING,
    p_EintragsNr STRING
)
BEGIN
    DECLARE v_TabName STRING DEFAULT 'ta_p_discount';
    DECLARE v_RecordsCount INT64;
    DECLARE v_ErrorMessage STRING;

    -- Parameter Validation (replacing shell script's 'pruefeParameterGesetzt' and 'if' conditions)
    IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
        SET v_ErrorMessage = 'ERROR: Parameter p_JobKennung is missing or empty.';
        INSERT INTO dataset.job_run_log (job_kennung, eintragsnr, tab_name, processed_at, error_message)
        VALUES (COALESCE(p_JobKennung, 'UNKNOWN'), COALESCE(p_EintragsNr, 'UNKNOWN'), v_TabName, CURRENT_TIMESTAMP(), v_ErrorMessage);
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_ErrorMessage;
    END IF;

    IF p_EintragsNr IS NULL OR p_EintragsNr = '' THEN
        SET v_ErrorMessage = 'ERROR: Parameter p_EintragsNr is missing or empty.';
        INSERT INTO dataset.job_run_log (job_kennung, eintragsnr, tab_name, processed_at, error_message)
        VALUES (COALESCE(p_JobKennung, 'UNKNOWN'), COALESCE(p_EintragsNr, 'UNKNOWN'), v_TabName, CURRENT_TIMESTAMP(), v_ErrorMessage);
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_ErrorMessage;
    END IF;

    -- Job Control: Deactivate old active jobs
    -- Corresponds to the shell script's logic to deactivate older active jobs for the same JobKennung.
    UPDATE dataset.job_table
    SET
        active_flag = FALSE,
        updated_at = CURRENT_TIMESTAMP()
    WHERE
        job_kennung = p_JobKennung
        AND active_flag = TRUE
        AND eintragsnr <> p_EintragsNr;

    -- Job Control: Register/update current job
    -- Uses MERGE to either insert a new job entry or update an existing one to 'active'.
    MERGE INTO dataset.job_table AS T
    USING (SELECT p_JobKennung AS job_kennung, p_EintragsNr AS eintragsnr) AS S
    ON T.job_kennung = S.job_kennung AND T.eintragsnr = S.eintragsnr
    WHEN MATCHED THEN
        UPDATE SET T.active_flag = TRUE, T.updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (job_kennung, eintragsnr, active_flag, created_at, updated_at)
        VALUES (S.job_kennung, S.eintragsnr, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

    BEGIN
        -- Execute the data transformation stored procedure
        CALL dataset.d_ausd_v_ta_p_discount(p_EintragsNr, p_JobKennung);

        -- Get processed records count (replacing reading from tmpFile)
        -- Since d_ausd_v_ta_p_discount truncates the table, COUNT(*) on the whole table is sufficient.
        SELECT COUNT(*) INTO v_RecordsCount
        FROM dataset.ta_p_discount;

        -- Log successful execution
        INSERT INTO dataset.job_run_log (job_kennung, eintragsnr, tab_name, records_count, processed_at, error_message)
        VALUES (p_JobKennung, p_EintragsNr, v_TabName, v_RecordsCount, CURRENT_TIMESTAMP(), NULL);

        -- Deactivate current job upon successful completion
        UPDATE dataset.job_table
        SET
            active_flag = FALSE,
            updated_at = CURRENT_TIMESTAMP()
        WHERE
            job_kennung = p_JobKennung
            AND eintragsnr = p_EintragsNr;

    EXCEPTION WHEN ERROR THEN
        -- Handle errors: log the error and re-raise it
        SET v_ErrorMessage = CONCAT('Error during data processing for JobKennung: ', p_JobKennung, ', EintragsNr: ', p_EintragsNr, ' - ', @@error.message);
        INSERT INTO dataset.job_run_log (job_kennung, eintragsnr, tab_name, processed_at, error_message)
        VALUES (p_JobKennung, p_EintragsNr, v_TabName, 0, CURRENT_TIMESTAMP(), v_ErrorMessage);

        -- Deactivate job in case of error
        UPDATE dataset.job_table
        SET
            active_flag = FALSE,
            updated_at = CURRENT_TIMESTAMP()
        WHERE
            job_kennung = p_JobKennung
            AND eintragsnr = p_EintragsNr;

        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_ErrorMessage;
    END;

END;