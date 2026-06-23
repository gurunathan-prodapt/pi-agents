-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_beschr.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_beschr.ksh
--
-- Migrated BigQuery Stored Procedure. This procedure orchestrates the data preparation
-- process, handling parameter parsing, validation, date derivation, calling the
-- core SQL transformation, performing record counting, and logging job status.

CREATE OR REPLACE PROCEDURE `<project_id>.<dataset>.r_ausd_bp_ta_bpr_beschr`(
    IN job_kennung STRING,
    IN eintrags_nr STRING,
    IN stichtag_str STRING, -- Expected format 'DDMMYYYY'
    IN wiederanlauf_wert STRING
)
BEGIN
    DECLARE v_stichtag DATE;
    DECLARE v_start_timestamp TIMESTAMP;
    DECLARE v_end_timestamp TIMESTAMP;
    DECLARE v_status STRING DEFAULT 'SUCCESS';
    DECLARE v_record_count INT64 DEFAULT 0;
    DECLARE v_error_message STRING;
    DECLARE v_aktueller_tag DATE;
    DECLARE v_gestern_tag DATE;

    -- Record start time
    SET v_start_timestamp = CURRENT_TIMESTAMP();

    BEGIN
        -- 1. Parameter Validation (replacing logic from k_ausd_bp_ta_bpr_beschr.ksh and h_alis_parameter.ksh)
        IF job_kennung IS NULL OR job_kennung = '' THEN
            SET v_error_message = 'ERROR: Parameter Jobkennung must be provided.';
            RAISE USING MESSAGE v_error_message;
        END IF;

        IF eintrags_nr IS NULL OR eintrags_nr = '' THEN
            SET v_error_message = 'ERROR: Parameter EintragsNr must be provided.';
            RAISE USING MESSAGE v_error_message;
        END IF;

        IF stichtag_str IS NULL OR stichtag_str = '' THEN
            SET v_error_message = 'ERROR: Parameter Stichtag must be provided.';
            RAISE USING MESSAGE v_error_message;
        END IF;

        -- 2. Date Validation and Conversion for Stichtag (replacing DWDate_Datum_Check)
        SET v_stichtag = SAFE.PARSE_DATE('%d%m%Y', stichtag_str);
        IF v_stichtag IS NULL THEN
            SET v_error_message = 'ERROR: Invalid Stichtag format. Expected DDMMYYYY.';
            RAISE USING MESSAGE v_error_message;
        END IF;

        -- 3. Date Derivation (replacing gestern.ksh logic)
        SET v_aktueller_tag = CURRENT_DATE();
        SET v_gestern_tag = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

        -- Optional: Log derived dates
        -- SELECT FORMAT('Processing Stichtag: %t, Aktueller Tag: %t, Gestern Tag: %t', v_stichtag, v_aktueller_tag, v_gestern_tag);

        -- 4. Execute core SQL transformation (replacing SQL*Plus call to d_ausd_bp_ta_bpr_beschr.sql)
        CALL `<project_id>.<dataset>.d_ausd_bp_ta_bpr_beschr_core`(
            v_stichtag,
            wiederanlauf_wert
        );

        -- 5. Record Counting (replacing temporary file read)
        -- This assumes that 'd_ausd_bp_ta_bpr_beschr_core' writes its primary output
        -- to '<project_id>.<dataset>.target_result_table' and that table
        -- has a _DATA_DATE column matching the 'Stichtag'.
        -- Adjust the table name and filter condition as per the actual core SQL migration.
        SET v_record_count = (
            SELECT COUNT(*)
            FROM `<project_id>.<dataset>.target_result_table`
            WHERE _DATA_DATE = v_stichtag
        );

        SET v_status = 'SUCCESS';

    EXCEPTION WHEN ERROR THEN
        SET v_status = 'FAILED';
        SET v_error_message = @@error.message;
        -- Continue to logging, then re-raise the error.
        SELECT FORMAT('Job FAILED: %s', v_error_message) AS error_info;
    END;

    -- Record end time
    SET v_end_timestamp = CURRENT_TIMESTAMP();

    -- 6. Audit Logging (replacing FOSJobErzeugeEintrag)
    INSERT INTO `<project_id>.<dataset>.job_audit_table` (
        job_id,
        entry_number,
        stichtag,
        start_timestamp,
        end_timestamp,
        status,
        record_count,
        error_message
    )
    VALUES (
        job_kennung,
        eintrags_nr,
        v_stichtag,
        v_start_timestamp,
        v_end_timestamp,
        v_status,
        v_record_count,
        v_error_message
    );

    IF v_status = 'FAILED' THEN
        -- Re-raise the error after logging it to the audit table
        RAISE USING MESSAGE v_error_message;
    END IF;

END;