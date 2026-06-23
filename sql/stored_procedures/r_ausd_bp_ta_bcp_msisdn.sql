-- Migration of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_msisdn.ksh
-- BigQuery Stored Procedure for orchestration and data processing.
-- Encapsulates parameter handling, validation, date calculations,
-- and execution of the migrated SQL logic from d_ausd_bp_ta_bcp_msisdn.sql.

CREATE OR REPLACE PROCEDURE `dataset.r_ausd_bp_ta_bcp_msisdn`(
    p_JobKennung STRING,
    p_EintragsNr STRING,
    p_Stichtag STRING, -- Expected format DDMMYYYY
    p_wiederanlaufWert STRING DEFAULT NULL
)
BEGIN
    DECLARE v_start_timestamp TIMESTAMP;
    DECLARE v_end_timestamp TIMESTAMP;
    DECLARE v_status STRING;
    DECLARE v_error_message STRING;
    DECLARE v_processed_records INT64;
    DECLARE v_stichtag_date DATE;
    DECLARE v_datum_heute DATE;
    DECLARE v_datum_gestern DATE;

    SET v_start_timestamp = CURRENT_TIMESTAMP();
    SET v_status = 'RUNNING';

    -- Log job start
    INSERT INTO `dataset.job_control_table` (
        job_kennung, eintrags_nr, stichtag, wiederanlauf_wert, start_timestamp, status
    )
    VALUES (
        p_JobKennung, p_EintragsNr, NULL, p_wiederanlaufWert, v_start_timestamp, v_status
    );

    -- Parameter Validation
    IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
        SET v_error_message = 'ERROR: Required parameter p_JobKennung is missing or empty.';
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
    END IF;

    IF p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '' THEN
        SET v_error_message = 'ERROR: Required parameter p_EintragsNr is missing or empty.';
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
    END IF;

    IF p_Stichtag IS NULL OR TRIM(p_Stichtag) = '' THEN
        SET v_error_message = 'ERROR: Required parameter p_Stichtag is missing or empty.';
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
    END IF;

    -- Date Validation and Conversion for p_Stichtag
    BEGIN
        SET v_stichtag_date = PARSE_DATE('%d%m%Y', p_Stichtag);
    EXCEPTION WHEN ERROR THEN
        SET v_error_message = 'ERROR: Invalid date format for p_Stichtag. Expected DDMMYYYY.';
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
    END;

    -- Update job_control_table with valid stichtag
    UPDATE `dataset.job_control_table`
    SET stichtag = v_stichtag_date
    WHERE job_kennung = p_JobKennung
      AND eintrags_nr = p_EintragsNr
      AND start_timestamp = v_start_timestamp;

    -- Date Calculation (replaces gestern.ksh logic)
    SET v_datum_heute = CURRENT_DATE();
    SET v_datum_gestern = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

    BEGIN
        -- Core Data Processing (from d_ausd_bp_ta_bcp_msisdn.sql)
        -- Truncate the target table before insertion
        TRUNCATE TABLE `dataset.sof_ta_bcp_msisdn`;

        -- Anreicherung der Daten mit den MSISDN des BCP-Vertrages
        INSERT INTO `dataset.sof_ta_bcp_msisdn`
        (
            CNTRCT_ID,
            BPR_ID,
            CNTRCT_ID_REF,
            TN_TEL_MSISDN
        )
        SELECT
            DISTINCT
            bp.cntrct_id,
            bp.bpr_id,
            bp.cntrct_id_ref,
            rn.tn_tel_msisdn
        FROM
            `dataset.sof_ta_bpr_bcp` AS bp
        INNER JOIN
            `dataset.sof_ta_rn_vertrag` AS rn
        ON
            bp.cntrct_id_ref = rn.cntrct_id;

        -- Get processed record count
        SET v_processed_records = (SELECT COUNT(*) FROM `dataset.sof_ta_bcp_msisdn`);

        SET v_status = 'SUCCESS';

    EXCEPTION WHEN ERROR THEN
        SET v_status = 'FAILED';
        SET v_error_message = CONCAT('BigQuery SQL Error: ', @@error.message);
        -- Re-raise the error after updating status
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;

    FINALLY
        SET v_end_timestamp = CURRENT_TIMESTAMP();

        -- Update job_control_table with final status and metrics
        UPDATE `dataset.job_control_table`
        SET
            end_timestamp = v_end_timestamp,
            status = v_status,
            processed_records = IF(v_status = 'SUCCESS', v_processed_records, NULL),
            error_message = IF(v_status = 'FAILED', v_error_message, NULL)
        WHERE
            job_kennung = p_JobKennung
            AND eintrags_nr = p_EintragsNr
            AND start_timestamp = v_start_timestamp;
    END;

END;