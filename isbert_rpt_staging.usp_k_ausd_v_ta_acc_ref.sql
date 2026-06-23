-- BigQuery Stored Procedure for job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_acc_ref.ksh
-- This procedure orchestrates the data loading into sof_ta_acc_ref
-- from cds_ta_acc_ref, replicating the logic from the original KornShell and Oracle SQL scripts.
CREATE OR REPLACE PROCEDURE `isbert_rpt_staging.usp_k_ausd_v_ta_acc_ref`(
    p_job_kennung STRING,
    p_eintragsnr  STRING
)
BEGIN
    DECLARE v_datum           STRING;
    DECLARE v_record_count    INT64 DEFAULT 0;
    DECLARE v_job_status      STRING;
    DECLARE v_log_message     STRING;
    DECLARE v_start_time      TIMESTAMP;
    DECLARE v_end_time        TIMESTAMP;
    DECLARE v_active_jobs_count INT664;

    SET v_start_time = CURRENT_TIMESTAMP();
    SET v_job_status = 'RUNNING';
    SET v_log_message = 'Job started.';

    -- Initialize or update job control entry
    MERGE INTO `isbert_rpt_staging.job_control` T
    USING (SELECT p_job_kennung AS job_kennung, p_eintragsnr AS eintragsnr) S
    ON T.job_kennung = S.job_kennung AND T.eintragsnr = S.eintragsnr
    WHEN MATCHED THEN
        UPDATE SET T.status = 'RUNNING', T.last_run_timestamp = v_start_time, T.updated_at = v_start_time
    WHEN NOT MATCHED THEN
        INSERT (job_kennung, eintragsnr, status, last_run_timestamp, created_at, updated_at)
        VALUES (S.job_kennung, S.eintragsnr, 'RUNNING', v_start_time, v_start_time, v_start_time);

    -- Log job start
    INSERT INTO `isbert_rpt_staging.job_run_log` (job_kennung, eintragsnr, start_timestamp, status, log_message)
    VALUES (p_job_kennung, p_eintragsnr, v_start_time, 'RUNNING', 'Job started. Processing data from cds_ta_acc_ref.');

    BEGIN
        -- Check if an active job with the same parameters already exists (ignoring this current run's initial status)
        SELECT COUNT(1)
        INTO v_active_jobs_count
        FROM `isbert_rpt_staging.job_control`
        WHERE job_kennung = p_job_kennung
          AND eintragsnr = p_eintragsnr
          AND status = 'ACTIVE';

        IF v_active_jobs_count > 0 THEN
            SET v_job_status = 'SKIPPED';
            SET v_log_message = 'Job already active. Skipping execution.';
            RAISE USING MESSAGE = v_log_message;
        END IF;

        -- Determine processing date (v_datum) from dwtk_meldungen
        -- Corresponds to: SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum FROM isbert_schema.dwtk_meldungen m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
        SELECT
            COALESCE(FORMAT_DATE('%Y%m%d', MAX(DATE(m.timecreated))), '19000101')
        INTO
            v_datum
        FROM
            `isbert_rpt_staging.dwtk_meldungen` m
        WHERE
            m.job_kennung = 'BERT_DROP_TEMP_TABLE';

        -- Truncate target table sof_ta_acc_ref
        -- Corresponds to: DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_acc_ref')
        TRUNCATE TABLE `isbert_rpt_staging.sof_ta_acc_ref`;

        -- Insert data into sof_ta_acc_ref from cds_ta_acc_ref
        -- Corresponds to: INSERT INTO sof$ta_acc_ref (...) SELECT ... FROM cds$ta_acc_ref ...
        INSERT `isbert_rpt_staging.sof_ta_acc_ref` (
            insert_at,
            modified_at,
            valid_from,
            valid_to,
            is_production,
            ta_acc_ref_key,
            ta_acc_ref_id,
            ta_acc_id,
            ta_acc_code,
            ta_acc_bezeichnung,
            ta_acc_gueltigkeit_von,
            ta_acc_gueltigkeit_bis
        )
        SELECT
            t.insert_at,
            t.modified_at,
            t.valid_from,
            t.valid_to,
            t.is_production,
            t.ta_acc_ref_key,
            t.ta_acc_ref_id,
            t.ta_acc_id,
            t.ta_acc_code,
            t.ta_acc_bezeichnung,
            t.ta_acc_gueltigkeit_von,
            t.ta_acc_gueltigkeit_bis
        FROM
            `isbert_rpt_staging.cds_ta_acc_ref` t
        WHERE
            (DATE(t.insert_at) >= PARSE_DATE('%Y%m%d', v_datum) OR
             DATE(t.modified_at) >= PARSE_DATE('%Y%m%d', v_datum) OR
             DATE(t.valid_from) >= PARSE_DATE('%Y%m%d', v_datum) OR
             DATE(t.valid_to) >= PARSE_DATE('%Y%m%d', v_datum))
            AND t.is_production = 1;

        SET v_record_count = @@ROW_COUNT;
        SET v_job_status = 'SUCCESS';
        SET v_log_message = FORMAT('Job completed successfully. %d records processed.', v_record_count);

    EXCEPTION WHEN ERROR THEN
        SET v_job_status = 'FAILED';
        SET v_log_message = FORMAT('Job failed: %s', @@error.message);
        -- Log detailed error information
        INSERT INTO `isbert_rpt_staging.job_error_log` (job_kennung, eintragsnr, error_timestamp, error_message, sql_state, stack_trace)
        VALUES (p_job_kennung, p_eintragsnr, CURRENT_TIMESTAMP(), @@error.message, @@error.code, @@error.stack_trace);
    END;

    SET v_end_time = CURRENT_TIMESTAMP();

    -- Update job control and run log with final status
    UPDATE `isbert_rpt_staging.job_control`
    SET
        status = IF(v_job_status = 'RUNNING', 'INACTIVE', v_job_status), -- If job was running but no explicit success/fail, mark inactive
        last_run_timestamp = v_end_time,
        updated_at = v_end_time
    WHERE job_kennung = p_job_kennung AND eintragsnr = p_eintragsnr;

    UPDATE `isbert_rpt_staging.job_run_log`
    SET
        end_timestamp = v_end_time,
        status = v_job_status,
        processed_records = v_record_count,
        log_message = v_log_message
    WHERE job_kennung = p_job_kennung
      AND eintragsnr = p_eintragsnr
      AND start_timestamp = v_start_time
      AND status = 'RUNNING'; -- Only update the specific run that started

    IF v_job_status = 'FAILED' OR v_job_status = 'SKIPPED' THEN
        -- Re-raise error to indicate procedure failure if it occurred, otherwise just log.
        RAISE USING MESSAGE = v_log_message;
    END IF;

END;