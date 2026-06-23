-- BigQuery Stored Procedure for data processing
-- Replaces: vobs/dw_source/isrpt/isbert/SQL/aufbereitung/sql/d_ausd_v_ta_discount_rr.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount_rr.ksh

CREATE OR REPLACE PROCEDURE `my_project.my_dataset.d_ausd_v_ta_discount_rr`(
    IN p_eintrags_nr STRING,
    IN p_job_kennung STRING,
    IN p_process_date DATE, -- Derived from dwtk_meldungen in ksh script
    OUT o_records_processed INT64
)
BEGIN
    DECLARE v_start_timestamp TIMESTAMP;
    DECLARE v_end_timestamp TIMESTAMP;
    DECLARE v_status STRING;
    DECLARE v_error_message STRING;

    SET v_start_timestamp = CURRENT_TIMESTAMP();
    SET o_records_processed = 0;

    -- Logging start of the data processing
    INSERT INTO `my_project.my_dataset.job_log`
    (job_kennung, eintrags_nr, start_timestamp, end_timestamp, status, records_processed, message)
    VALUES
    (p_job_kennung, p_eintrags_nr, v_start_timestamp, NULL, 'RUNNING', 0, 'Data processing started.');

    BEGIN
        -- Truncate target table
        TRUNCATE TABLE `my_project.my_dataset.ta_discount_rr`;

        -- Insert data into the target table
        INSERT INTO `my_project.my_dataset.ta_discount_rr`(
            cntrct_id,
            discount_id,
            disc_vector_ty,
            cntrct_obj_version,
            cntrct_template_id,
            disc_invoice_item_id,
            rabatt,
            rabatthoehe,
            rabattierte_rech_pos
        )
        SELECT
            da.cntrct_id,
            da.discount_id,
            d.disc_vector_ty,
            da.cntrct_obj_version,
            d.cntrct_template_id,
            d.disc_invoice_item_id,
            cd.cds_description,
            dv.CALC_RULE_VALUE,
            cdii.CDS_DESCRIPTION
        FROM
            `my_project.my_dataset.cds_ta_discount_bc_assoc`        AS da
        INNER JOIN
            `my_project.my_dataset.cds_ta_discount`                 AS d
            ON da.discount_id = d.discount_id
        INNER JOIN
            `my_project.my_dataset.cds_ta_care_description`         AS cd
            ON cd.cds_description_id = d.CDS_DESCRIPTION_ID
        INNER JOIN
            `my_project.my_dataset.cds_ta_disc_vector`              AS dv
            ON d.discount_id = dv.discount_id
            AND d.disc_vector_ty = dv.disc_vector_ty
            AND d.obj_version = dv.discount_obj_version
        INNER JOIN
            `my_project.my_dataset.cds_ta_disc_invoice_item`        AS dii
            ON d.DISC_INVOICE_ITEM_ID = dii.DISC_INVOICE_ITEM_ID
        INNER JOIN
            `my_project.my_dataset.cds_ta_care_description`         AS cdii
            ON dii.CDS_DESCRIPTION_ID = cdii.CDS_DESCRIPTION_ID
        WHERE
            cd.LANGUAGE = 1
            AND cdii.LANGUAGE = 1
            AND da.insert_at <= p_process_date
            AND (da.modified_at IS NULL OR da.modified_at > p_process_date)
            AND d.insert_at <= p_process_date
            AND (d.modified_at IS NULL OR d.modified_at > p_process_date)
            AND d.valid_from <= p_process_date
            AND (d.valid_to IS NULL OR d.valid_to > p_process_date)
            AND dv.insert_at <= p_process_date
            AND (dv.modified_at IS NULL OR dv.modified_at > p_process_date)
            AND d.is_production = 1
            AND dii.insert_at <= p_process_date
            AND (dii.modified_at IS NULL OR dii.modified_at > p_process_date);

        SET o_records_processed = @@row_count;
        SET v_status = 'SUCCESS';
        SET v_error_message = 'Data processing completed successfully.';

    EXCEPTION WHEN ERROR THEN
        SET v_status = 'FAILURE';
        SET v_error_message = @@error.message;
        -- Log detailed error into error_log table
        INSERT INTO `my_project.my_dataset.error_log`
        (job_kennung, eintrags_nr, error_code, error_argument, message)
        VALUES
        (p_job_kennung, p_eintrags_nr, -1, 'SQL_EXECUTION_ERROR', v_error_message);
        RAISE USING MESSAGE 'Data processing failed: ' || v_error_message;
    END;

    SET v_end_timestamp = CURRENT_TIMESTAMP();

    -- Update job_log with final status
    UPDATE `my_project.my_dataset.job_log`
    SET
        end_timestamp = v_end_timestamp,
        status = v_status,
        records_processed = o_records_processed,
        message = v_error_message
    WHERE
        job_kennung = p_job_kennung AND eintrags_nr = p_eintrags_nr AND start_timestamp = v_start_timestamp;

END;