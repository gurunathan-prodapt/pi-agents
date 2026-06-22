-- Header: BigQuery Stored Procedure for core transformation
-- Legacy Source: d_ausd_v_ta_disc_zusgf.sql
-- Job: BERT_V_TA_DISC_ZUSGF

CREATE OR REPLACE PROCEDURE `bert_dwh.d_ausd_v_ta_disc_zusgf`(
    IN p_eintrags_nr INT64,
    IN p_job_kennung STRING
)
BEGIN
    DECLARE v_datum STRING;
    DECLARE records_inserted INT64;

    -- Get v_datum, defaulting if no data in dwtk_meldungen
    SELECT COALESCE(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')
    INTO v_datum
    FROM `bert_dwh.dwtk_meldungen` AS m;

    -- Log the start of the transformation
    INSERT INTO `bert_dwh.job_log` (job_kennung, eintrags_nr, log_timestamp, message, log_level)
    VALUES (p_job_kennung, p_eintrags_nr, CURRENT_TIMESTAMP(), 'Starting core transformation d_ausd_v_ta_disc_zusgf', 'INFO');

    -- Truncate target table before insertion
    TRUNCATE TABLE `bert_dwh.sof_ta_disc_zusgf`;

    INSERT INTO `bert_dwh.sof_ta_disc_zusgf` (cntrct_id, cntrct_obj_version, disc_vector_ty, rabatt_alle)
    SELECT
        dzg.cntrct_id,
        dzg.cntrct_obj_version,
        dzg.disc_vector_ty,
        con.rabatt_alle
    FROM (
        SELECT DISTINCT cntrct_id, disc_vector_ty, cntrct_obj_version FROM `bert_dwh.sof_ta_discount`
    ) AS dzg
    LEFT JOIN (
        SELECT
            cntrct_id,
            cntrct_obj_version,
            STRING_AGG(rabatt_text, ', ' ORDER BY rabatt_text) AS rabatt_alle
        FROM (
            SELECT DISTINCT
                cntrct_id,
                cntrct_obj_version,
                CONCAT(CAST(rabatt AS STRING), ' (', CAST(rabatthoehe AS STRING), '%)') AS rabatt_text
            FROM `bert_dwh.sof_ta_discount`
        ) AS discount_lines
        GROUP BY cntrct_id, cntrct_obj_version
    ) AS con
        ON dzg.cntrct_id = con.cntrct_id AND dzg.cntrct_obj_version = con.cntrct_obj_version;

    SET records_inserted = ROW_COUNT();

    -- Update job control with records processed
    UPDATE `bert_dwh.job_control`
    SET records_processed = records_inserted,
        last_update_time = CURRENT_TIMESTAMP()
    WHERE job_kennung = p_job_kennung AND eintrags_nr = p_eintrags_nr;

    -- Log the completion of the transformation
    INSERT INTO `bert_dwh.job_log` (job_kennung, eintrags_nr, log_timestamp, message, log_level)
    VALUES (p_job_kennung, p_eintrags_nr, CURRENT_TIMESTAMP(), CONCAT('Core transformation d_ausd_v_ta_disc_zusgf completed. Inserted ', CAST(records_inserted AS STRING), ' records.'), 'INFO');

EXCEPTION WHEN ERROR THEN
    -- Log the error
    INSERT INTO `bert_dwh.job_error_log` (job_kennung, eintrags_nr, error_timestamp, error_message, stack_trace)
    VALUES (p_job_kennung, p_eintrags_nr, CURRENT_TIMESTAMP(), ERROR_MESSAGE(), @@error.stack_trace);

    -- Update job control status to FAILED
    UPDATE `bert_dwh.job_control`
    SET job_status = 'FAILED',
        end_time = CURRENT_TIMESTAMP(),
        error_message = ERROR_MESSAGE(),
        last_update_time = CURRENT_TIMESTAMP()
    WHERE job_kennung = p_job_kennung AND eintrags_nr = p_eintrags_nr;

    -- Re-raise the error to propagate it
    RAISE;
END;