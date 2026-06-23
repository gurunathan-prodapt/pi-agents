-- BigQuery Stored Procedure for k_ausd_bp_ta_rn_da_vda_tk.ksh orchestration logic
-- Replaces legacy job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_da_vda_tk.ksh

CREATE OR REPLACE PROCEDURE my_project.my_dataset.r_ausd_bp_ta_rn_da_vda_tk(
    IN p_JobKennung STRING,
    IN p_EintragsNr STRING,
    IN p_Stichtag STRING,
    IN p_wiederanlaufWert STRING
)
BEGIN
    DECLARE v_job_name STRING DEFAULT 'k_ausd_bp_ta_rn_da_vda_tk'; -- Name of the original ksh job
    DECLARE v_records INT64;
    DECLARE v_datum_heute DATE;
    DECLARE v_datum_gestern DATE;
    DECLARE v_stichtag_date DATE;
    DECLARE v_final_wiederanlaufWert STRING;
    DECLARE v_error_message STRING;

    -- Error handling block
    BEGIN

        -- 1. Parameter Validation
        IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
            RAISE USING MESSAGE 'ERROR: JobKennung must be provided.';
        END IF;

        IF p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '' THEN
            RAISE USING MESSAGE 'ERROR: EintragsNr must be provided.';
        END IF;

        IF p_Stichtag IS NULL OR TRIM(p_Stichtag) = '' THEN
            RAISE USING MESSAGE 'ERROR: Stichtag must be provided.';
        END IF;

        -- 2. Date format validation for p_Stichtag (DDMMYYYY)
        SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);
        IF v_stichtag_date IS NULL THEN
            RAISE USING MESSAGE 'ERROR: Stichtag must be in DDMMYYYY format.';
        END IF;

        -- 3. Default p_wiederanlaufWert to '0' if null or empty
        SET v_final_wiederanlaufWert = COALESCE(p_wiederanlaufWert, '0');
        IF TRIM(v_final_wiederanlaufWert) = '' THEN
            SET v_final_wiederanlaufWert = '0';
        END IF;

        -- 4. Derive dates (replacing gestern.ksh)
        SET v_datum_heute = CURRENT_DATE();
        SET v_datum_gestern = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

        -- 5. Call the migrated core SQL logic (d_ausd_bp_ta_rn_da_vda_tk.sql counterpart)
        --    Assumes a BigQuery Stored Procedure named d_ausd_bp_ta_rn_da_vda_tk exists in the same dataset
        --    and accepts parameters relevant for its processing.
        --    The specific parameters for d_ausd_bp_ta_rn_da_vda_tk will depend on its own migration.
        --    Passing the validated date and other job control parameters.
        CALL my_project.my_dataset.d_ausd_bp_ta_rn_da_vda_tk(
            v_stichtag_date,            -- As DATE type for easier SQL use
            p_JobKennung,
            p_EintragsNr,
            v_final_wiederanlaufWert    -- Potentially used for restart logic in the core SQL
        );

        -- 6. Capture record count from the target table
        --    Assumes the core SQL logic writes to 'my_project.my_dataset.bp_target_table'.
        SELECT COUNT(*) INTO v_records
        FROM my_project.my_dataset.bp_target_table;

        -- 7. Log successful run to job_run_log
        INSERT INTO my_project.my_dataset.job_run_log (
            tab_name,
            job_kennung,
            eintrags_nr,
            stichtag,
            wiederanlauf_wert,
            records_processed,
            created_at
        ) VALUES (
            v_job_name,
            p_JobKennung,
            p_EintragsNr,
            p_Stichtag,
            v_final_wiederanlaufWert,
            v_records,
            CURRENT_TIMESTAMP()
        );

    EXCEPTION WHEN ERROR THEN
        -- 8. Log error to job_error_log
        SET v_error_message = @@error.message;

        INSERT INTO my_project.my_dataset.job_error_log (
            job_name,
            entry_nr,
            stichtag,
            error_message,
            created_at
        ) VALUES (
            v_job_name,
            p_EintragsNr,
            p_Stichtag,
            FORMAT('Error encountered: %s (Statement ID: %s)', v_error_message, @@error.statement_id),
            CURRENT_TIMESTAMP()
        );
        RAISE USING MESSAGE v_error_message; -- Re-raise the error to signal failure to the orchestrator
    END;
END;