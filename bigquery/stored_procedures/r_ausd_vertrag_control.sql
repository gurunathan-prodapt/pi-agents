-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_notice.ksh
--                vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_notice.sql
-- Description: BigQuery Stored Procedure encapsulating the orchestration and data transformation logic.
-- This procedure replaces the ksh control script and its invoked SQL script.
CREATE OR REPLACE PROCEDURE `mydataset.r_ausd_vertrag_control`(
    p_JobKennung STRING,
    p_EintragsNr STRING
)
BEGIN
    DECLARE v_job_id STRING;
    DECLARE v_records_processed INT64 DEFAULT 0;
    DECLARE v_job_status STRING;

    -- Generate a unique job ID for this run
    SET v_job_id = GENERATE_UUID();

    -- Initialize job status to FAILED in case of early exit
    SET v_job_status = 'FAILED';

    BEGIN
        -- 1. Parameter Validation
        IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
            CALL `mydataset.log_error`(v_job_id, p_JobKennung, p_EintragsNr, 'Parameter p_JobKennung is missing or empty.', 'ERROR', 'r_ausd_vertrag_control');
            RAISE USING MESSAGE 'Parameter p_JobKennung cannot be null or empty.';
        END IF;

        IF p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '' THEN
            CALL `mydataset.log_error`(v_job_id, p_JobKennung, p_EintragsNr, 'Parameter p_EintragsNr is missing or empty.', 'ERROR', 'r_ausd_vertrag_control');
            RAISE USING MESSAGE 'Parameter p_EintragsNr cannot be null or empty.';
        END IF;

        -- 2. Log job start and mark as ACTIVE
        INSERT INTO `mydataset.job_table` (job_id, job_kennung, eintrags_nr, table_name, status, start_time, message)
        VALUES (v_job_id, p_JobKennung, p_EintragsNr, 'ta_notice', 'ACTIVE', CURRENT_TIMESTAMP(), 'Job started.');

        -- 3. Deactivate old active jobs for the same table_name if any exist and are not this job
        UPDATE `mydataset.job_table`
        SET
            status = 'DEACTIVATED',
            end_time = CURRENT_TIMESTAMP(),
            message = 'Deactivated by a new job run.'
        WHERE
            table_name = 'ta_notice'
            AND status = 'ACTIVE'
            AND job_id != v_job_id;

        -- 4. Core SQL Logic (Migrated from d_ausd_v_ta_notice.sql)
        --    This section needs to be populated with the actual translated BigQuery SQL
        --    from d_ausd_v_ta_notice.sql. For now, it's a placeholder.

        -- Example of a MERGE statement assuming d_ausd_v_ta_notice.sql performs updates/inserts
        -- This is a simplified example, the actual logic will be more complex based on the source SQL.
        MERGE INTO `mydataset.SOF_TA_NOTICE` AS T
        USING (
            SELECT
                t1.id AS meld_id,
                t1.data AS meld_data,
                t2.data AS notice_data
            FROM
                `mydataset.DWTK_MELDUNGEN` AS t1
            INNER JOIN
                `mydataset.CDS_TA_NOTICE` AS t2
                ON t1.id = t2.id -- Assuming a join condition
            WHERE
                -- Add filtering logic from original SQL
                CAST(t1.created_at AS DATE) = CURRENT_DATE() -- Example filter
        ) AS S
        ON T.id = S.meld_id AND T.job_kennung = p_JobKennung AND T.eintrags_nr = p_EintragsNr
        WHEN NOT MATCHED THEN
            INSERT (id, job_kennung, eintrags_nr, data, processed_at)
            VALUES (S.meld_id, p_JobKennung, p_EintragsNr, S.meld_data || ' - ' || S.notice_data, CURRENT_TIMESTAMP());

        -- Another example for VIA table
        INSERT INTO `mydataset.VIA` (id, job_kennung, eintrags_nr, data, processed_at)
        SELECT
            id,
            p_JobKennung,
            p_EintragsNr,
            data,
            CURRENT_TIMESTAMP()
        FROM
            `mydataset.DWTK_MELDUNGEN`
        WHERE NOT EXISTS (SELECT 1 FROM `mydataset.VIA` WHERE id = `mydataset.DWTK_MELDUNGEN`.id AND job_kennung = p_JobKennung AND eintrags_nr = p_EintragsNr)
        LIMIT 10; -- Example: just inserting a few rows from one source. Actual logic needs to be translated.


        -- 5. Get record count
        SELECT COUNT(1) INTO v_records_processed
        FROM `mydataset.SOF_TA_NOTICE`
        WHERE job_id = v_job_id; -- Or a more specific filter for this run's inserts

        -- 6. Update job status to COMPLETED
        SET v_job_status = 'COMPLETED';

    EXCEPTION WHEN ERROR THEN
        -- Log error if any exception occurs
        CALL `mydataset.log_error`(v_job_id, p_JobKennung, p_EintragsNr, @@error.message, 'FATAL', @@error.stack_trace);
        SET v_job_status = 'FAILED';
        RAISE; -- Re-raise the error to propagate it
    END;

    -- Final update to job_table
    UPDATE `mydataset.job_table`
    SET
        status = v_job_status,
        end_time = CURRENT_TIMESTAMP(),
        record_count = v_records_processed,
        message = CASE WHEN v_job_status = 'COMPLETED' THEN 'Job completed successfully.' ELSE 'Job failed.' END
    WHERE
        job_id = v_job_id;

END;
-- Helper Stored Procedure for error logging
CREATE OR REPLACE PROCEDURE `mydataset.log_error`(
    p_job_id STRING,
    p_job_kennung STRING,
    p_eintrags_nr STRING,
    p_error_message STRING,
    p_severity STRING,
    p_stack_trace STRING
)
BEGIN
    INSERT INTO `mydataset.error_log` (log_time, job_id, job_kennung, eintrags_nr, error_message, severity, stack_trace)
    VALUES (CURRENT_TIMESTAMP(), p_job_id, p_job_kennung, p_eintrags_nr, p_error_message, p_severity, p_stack_trace);
END;