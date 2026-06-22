-- BigQuery Stored Procedure for internal control logic
-- Replaces vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_vertrag.ksh
CREATE OR REPLACE PROCEDURE `project.dataset.k_ausd_bp_ta_rn_vertrag`(
    IN p_job_kennung STRING,
    IN p_run_id STRING, -- Corresponds to p_EintragsNr
    IN p_stichtag STRING,
    IN p_wiederanlaufWert INT64
)
BEGIN
    DECLARE v_error_message STRING;
    DECLARE records_processed INT64;
    DECLARE v_datum_heute DATE;
    DECLARE v_datum_gestern DATE;
    DECLARE v_parsed_stichtag DATE;

    BEGIN
        -- Log start of procedure
        INSERT INTO `project.dataset.job_audit` (job_kennung, run_id, status, message, created_at)
        VALUES (p_job_kennung, p_run_id, 'RUNNING', 'Starting k_ausd_bp_ta_rn_vertrag procedure.', CURRENT_TIMESTAMP());

        -- Parameter validation
        IF p_stichtag IS NULL OR p_stichtag = '' THEN
            RAISE BQ.INVALID_ARGUMENT(CONCAT('Required parameter Stichtag (p_stichtag) is missing. JobKennung: ', p_job_kennung, ', RunId: ', p_run_id));
        END IF;

        -- Check date format
        BEGIN
            SET v_parsed_stichtag = PARSE_DATE('%d%m%Y', p_stichtag);
        EXCEPTION WHEN ERROR THEN
            RAISE BQ.INVALID_ARGUMENT(CONCAT('Stichtag (p_stichtag) has invalid format. Expected DDMMYYYY, got ', p_stichtag, '. JobKennung: ', p_job_kennung, ', RunId: ', p_run_id));
        END;

        -- Date calculations (replacing gestern.ksh logic)
        SET v_datum_heute = CURRENT_DATE();
        SET v_datum_gestern = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

        -- Log calculated dates if needed, or simply pass them
        -- The p_wiederanlaufWert is passed but its implementation logic
        -- for filtering in d_ausd_bp_ta_rn_vertrag is noted as unresolved.
        -- For this migration, it is passed but not utilized in the DML.

        -- Execute the SQL transformation stored procedure
        CALL `project.dataset.d_ausd_bp_ta_rn_vertrag`(p_job_kennung, p_run_id, records_processed);

        -- Log success
        INSERT INTO `project.dataset.job_audit` (job_kennung, run_id, status, message, created_at, record_count)
        VALUES (p_job_kennung, p_run_id, 'SUCCESS', CONCAT('k_ausd_bp_ta_rn_vertrag completed. Records processed: ', records_processed), CURRENT_TIMESTAMP(), records_processed);

    EXCEPTION WHEN ERROR THEN
        SET v_error_message = @@error.message;
        -- Log failure
        INSERT INTO `project.dataset.job_audit` (job_kennung, run_id, status, message, created_at)
        VALUES (p_job_kennung, p_run_id, 'FAILED', CONCAT('k_ausd_bp_ta_rn_vertrag failed: ', v_error_message), CURRENT_TIMESTAMP());
        RAISE; -- Re-raise the error for the calling procedure
    END;
END;