-- BigQuery Stored Procedure for core data processing
-- Replaces k_ausd_bp_ta_bpr_apn.ksh, called by r_ausd_bp_ta_bpr_apn.ksh (now ausd_bp_ta_bpr_apn)
CREATE OR REPLACE PROCEDURE `my_project.my_dataset.k_ausd_bp_ta_bpr_apn`(
    IN p_job_entry_number INT64,
    IN p_stichtag_date DATE,
    IN p_wiederanlaufwert INT64
)
BEGIN
    DECLARE v_deleted_rows INT64;
    DECLARE v_inserted_rows INT64;
    DECLARE v_job_name STRING DEFAULT 'k_ausd_bp_ta_bpr_apn';
    DECLARE v_message STRING;
    DECLARE v_status STRING;

    BEGIN
        -- Log start of core processing
        UPDATE `my_project.my_dataset.job_audit`
        SET message = CONCAT('Starting core processing (', v_job_name, ') with Stichtag: ', CAST(p_stichtag_date AS STRING), ' and Wiederanlaufwert: ', CAST(p_wiederanlaufwert AS STRING)),
            status = 'RUNNING'
        WHERE job_entry_number = p_job_entry_number;

        -- If p_wiederanlaufwert > 0, delete existing records from fos_table
        IF p_wiederanlaufwert > 0 THEN
            DELETE FROM `my_project.my_dataset.fos_table`
            WHERE dwh_vertrag_id >= p_wiederanlaufwert;
            SET v_deleted_rows = @@row_count;
        ELSE
            SET v_deleted_rows = 0;
        END IF;

        -- Insert filtered data into fos_table
        INSERT INTO `my_project.my_dataset.fos_table` (
            dwh_vertrag_id,
            gueltig_von,
            gueltig_bis,
            ladedatum,
            col_a,
            col_b,
            stichtag_lauf,
            created_ts
        )
        SELECT
            s.dwh_vertrag_id,
            s.gueltig_von,
            s.gueltig_bis,
            s.ladedatum,
            s.col_a,
            s.col_b,
            p_stichtag_date AS stichtag_lauf,
            CURRENT_TIMESTAMP() AS created_ts
        FROM `my_project.my_dataset.contract_cache` AS s
        WHERE
            s.gueltig_von <= p_stichtag_date
            AND p_stichtag_date < s.gueltig_bis
            AND s.ladedatum < p_stichtag_date
            AND (p_wiederanlaufwert = 0 OR s.dwh_vertrag_id > p_wiederanlaufwert);

        SET v_inserted_rows = @@row_count;

        SET v_status = 'SUCCESS';
        SET v_message = CONCAT('Core processing completed successfully. Deleted rows: ', CAST(v_deleted_rows AS STRING), ', Inserted rows: ', CAST(v_inserted_rows AS STRING));

        -- Log success
        UPDATE `my_project.my_dataset.job_audit`
        SET end_timestamp = CURRENT_TIMESTAMP(),
            status = v_status,
            message = v_message
        WHERE job_entry_number = p_job_entry_number;

    EXCEPTION WHEN ERROR THEN
        SET v_status = 'FAILED';
        SET v_message = CONCAT('Core processing failed: ', @@error.message);

        -- Log failure
        UPDATE `my_project.my_dataset.job_audit`
        SET end_timestamp = CURRENT_TIMESTAMP(),
            status = v_status,
            message = v_message
        WHERE job_entry_number = p_job_entry_number;
        
        RAISE USING MESSAGE = v_message; -- Re-raise the error to the caller
    END;
END;