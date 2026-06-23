-- BigQuery Stored Procedure for k_ausd_v_ta_discount_rr.ksh orchestration
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount_rr.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount_rr.ksh

CREATE OR REPLACE PROCEDURE `your_project.your_dataset.control_ausd_v_ta_discount_rr`(
    p_JobKennung STRING,
    p_EintragsNr STRING
)
BEGIN
    DECLARE v_run_id STRING;
    DECLARE v_datum_str STRING;
    DECLARE v_record_count INT64;
    DECLARE v_status STRING DEFAULT 'RUNNING';
    DECLARE v_message STRING DEFAULT 'Job started successfully.';
    DECLARE v_error_message STRING;
    DECLARE v_error_step STRING;

    SET v_run_id = GENERATE_UUID();

    -- Parameter Validation
    IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
        SET v_status = 'FAILED';
        SET v_message = 'Parameter p_JobKennung cannot be NULL or empty.';
        SET v_error_message = v_message;
        SET v_error_step = 'Parameter Validation';
        INSERT INTO `your_project.your_dataset.job_error_log` (run_id, job_kennung, error_time, error_message, severity, step)
        VALUES(v_run_id, p_JobKennung, CURRENT_TIMESTAMP(), v_error_message, 'ERROR', v_error_step);
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;

    -- Initialize job_table entry
    INSERT INTO `your_project.your_dataset.job_table` (job_kennung, eintrags_nr, run_id, start_time, status, message)
    VALUES (p_JobKennung, p_EintragsNr, v_run_id, CURRENT_TIMESTAMP(), v_status, v_message);

    BEGIN
        -- Determine processing date (v_datum)
        SET v_error_step = 'Determine Processing Date';
        SELECT
            IFNULL(FORMAT_DATE('%Y%m%d', MAX(DATE(m.timecreated))), '19000101')
        INTO
            v_datum_str
        FROM
            `your_project.raw_isbert.dwtk_meldungen` AS m
        WHERE
            m.job_kennung = 'BERT_DROP_TEMP_TABLE';

        IF v_datum_str IS NULL THEN
            SET v_datum_str = '19000101'; -- Default value if no data found
            SET v_message = 'No processing date found, defaulting to 19000101.';
            INSERT INTO `your_project.your_dataset.job_error_log` (run_id, job_kennung, error_time, error_message, severity, step)
            VALUES(v_run_id, p_JobKennung, CURRENT_TIMESTAMP(), v_message, 'WARNING', v_error_step);
        END IF;

        -- Truncate target table
        SET v_error_step = 'Truncate Target Table';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE `your_project.curated_rpt.sof_ta_discount_rr`;';

        -- Execute core transformation logic
        SET v_error_step = 'Execute Transformation Logic';
        INSERT INTO `your_project.curated_rpt.sof_ta_discount_rr`(
                cntrct_id,
                discount_id,
                disc_vector_ty,
                cntrct_obj_version,
                cntrct_template_id,
                disc_invoice_item_id,
                rabatt,
                rabatthoehe,
                rabattierte_rech_pos)
        SELECT
                da.cntrct_id,
                da.discount_id,
                d.disc_vector_ty,
                da.cntrct_obj_version,
                d.cntrct_template_id,
                d.disc_invoice_item_id,
                cd.cds_description AS rabatt,
                dv.CALC_RULE_VALUE AS rabatthoehe,
                cdii.CDS_DESCRIPTION AS rabattierte_rech_pos
        FROM
                `your_project.raw_isbert.cds_ta_discount_bc_assoc` AS da
        INNER JOIN
                `your_project.raw_isbert.cds_ta_discount` AS d
        ON
                da.discount_id          = d.discount_id
        INNER JOIN
                `your_project.raw_isbert.cds_ta_care_description` AS cd
        ON
                cd.cds_description_id   = d.cds_description_id
        INNER JOIN
                `your_project.raw_isbert.cds_ta_disc_invoice_item` AS dii
        ON
                d.disc_invoice_item_id  = dii.disc_invoice_item_id
        INNER JOIN
                `your_project.raw_isbert.cds_ta_care_description` AS cdii
        ON
                dii.cds_description_id  = cdii.cds_description_id
        INNER JOIN
                `your_project.raw_isbert.cds_ta_disc_vector` AS dv
        ON
                d.discount_id           = dv.discount_id
            AND d.disc_vector_ty        = dv.disc_vector_ty
            AND d.obj_version           = dv.discount_obj_version
        WHERE
                cd.`language`             = 1
        AND
                cdii.`language`           = 1
        AND
                da.insert_at <= PARSE_TIMESTAMP('%Y%m%d', v_datum_str)
        AND     (   da.modified_at IS NULL
                 OR da.modified_at > PARSE_TIMESTAMP('%Y%m%d', v_datum_str) )
        AND
                d.insert_at <= PARSE_TIMESTAMP('%Y%m%d', v_datum_str)
        AND     (   d.modified_at IS NULL
                 OR d.modified_at > PARSE_TIMESTAMP('%Y%m%d', v_datum_str) )
        AND     d.valid_from <= PARSE_TIMESTAMP('%Y%m%d', v_datum_str)
        AND     (   d.valid_to IS NULL
                 OR d.valid_to > PARSE_TIMESTAMP('%Y%m%d', v_datum_str) )
        AND
                dv.insert_at   <= PARSE_TIMESTAMP('%Y%m%d', v_datum_str)
        AND     (   dv.modified_at IS NULL
                 OR dv.modified_at > PARSE_TIMESTAMP('%Y%m%d', v_datum_str) )
        AND     d.is_production = 1
        AND
                dii.insert_at   <= PARSE_TIMESTAMP('%Y%m%d', v_datum_str)
        AND     (   dii.modified_at IS NULL
                 OR dii.modified_at > PARSE_TIMESTAMP('%Y%m%d', v_datum_str) );

        -- Get record count
        SET v_error_step = 'Count Records';
        SELECT COUNT(*) INTO v_record_count FROM `your_project.curated_rpt.sof_ta_discount_rr`;

        SET v_status = 'COMPLETED';
        SET v_message = 'Job completed successfully.';

    EXCEPTION WHEN ERROR THEN
        SET v_status = 'FAILED';
        SET v_error_message = @@error.message;
        SET v_message = 'Job failed at step: ' || v_error_step || ' with error: ' || v_error_message;

        INSERT INTO `your_project.your_dataset.job_error_log` (run_id, job_kennung, error_time, error_message, severity, step)
        VALUES(v_run_id, p_JobKennung, CURRENT_TIMESTAMP(), v_error_message, 'ERROR', v_error_step);
    END;

    -- Finalize job_table entry
    UPDATE `your_project.your_dataset.job_table`
    SET
        end_time = CURRENT_TIMESTAMP(),
        status = v_status,
        record_count = IFNULL(v_record_count, 0),
        message = v_message
    WHERE
        run_id = v_run_id;

    IF v_status = 'FAILED' THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_message;
    END IF;

END;