-- BigQuery Stored Procedure for the business logic, migrated from d_ausd_v_ta_bp_ref.sql
-- Legacy Job: k_ausd_v_ta_bp_ref.ksh
CREATE OR REPLACE PROCEDURE project.dataset.d_ausd_v_ta_bp_ref_logic(
    IN p_stichtag DATE,
    IN p_job_kennung STRING,
    IN p_eintrags_nr STRING
)
BEGIN
    -- Legacy source: d_ausd_v_ta_bp_ref.sql
    -- This procedure encapsulates the core data processing logic.

    DECLARE v_error_code INT64 DEFAULT 0;
    DECLARE v_error_message STRING;

    -- Determine the 'stichtag' (key date) if not provided.
    -- In the original Oracle script, v_datum was derived from dwtk_meldungen.
    -- For BigQuery, we rely on the input parameter p_stichtag.
    -- If a specific derivation from a BigQuery equivalent of dwtk_meldungen is needed,
    -- that logic would be added here.

    -- Truncate target table
    BEGIN
        TRUNCATE TABLE project.dataset.sof_ta_bp_ref;
    EXCEPTION WHEN ERROR THEN
        SET v_error_code = @@error.code;
        SET v_error_message = @@error.message;
        INSERT INTO project.dataset.error_log (error_ts, error_code, error_arg, job_kennung, eintrags_nr, script_name, message)
        VALUES (CURRENT_TIMESTAMP(), v_error_code, NULL, p_job_kennung, p_eintrags_nr, 'd_ausd_v_ta_bp_ref_logic', 'Error truncating table: ' || v_error_message);
        RAISE; -- Re-raise the error
    END;

    -- Insert data into the target table
    BEGIN
        INSERT INTO project.dataset.sof_ta_bp_ref (
            cntrct_cp2_id,
            bp_id
        )
        SELECT
            br.cntrct_cp2_id,
            br.bp_id
        FROM
            project.dataset.cds_ta_bp_ref AS br -- Assuming cds$ta_bp_ref maps to project.dataset.cds_ta_bp_ref
        WHERE
            DATE(br.insert_at) <= p_stichtag
            AND (DATE(br.modified_at) IS NULL OR DATE(br.modified_at) > p_stichtag)
            AND DATE(br.valid_from) <= p_stichtag
            AND (DATE(br.valid_to) IS NULL OR DATE(br.valid_to) > p_stichtag)
            AND br.is_production = 1
            AND br.bp_ref_ty = 4;

    EXCEPTION WHEN ERROR THEN
        SET v_error_code = @@error.code;
        SET v_error_message = @@error.message;
        INSERT INTO project.dataset.error_log (error_ts, error_code, error_arg, job_kennung, eintrags_nr, script_name, message)
        VALUES (CURRENT_TIMESTAMP(), v_error_code, NULL, p_job_kennung, p_eintrags_nr, 'd_ausd_v_ta_bp_ref_logic', 'Error inserting data: ' || v_error_message);
        RAISE; -- Re-raise the error
    END;

END;