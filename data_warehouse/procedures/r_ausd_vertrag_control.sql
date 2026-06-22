-- BigQuery Stored Procedure encapsulating logic from k_ausd_v_ta_barrier.ksh and d_ausd_v_ta_barrier.sql
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_barrier.ksh
CREATE OR REPLACE PROCEDURE `my-project.data_warehouse.r_ausd_vertrag_control`(
    p_job_kennung STRING,
    p_eintrags_nr STRING
)
BEGIN
    DECLARE v_datum STRING;
    DECLARE v_records_processed INT64 DEFAULT 0;
    DECLARE job_start_time TIMESTAMP;
    DECLARE job_end_time TIMESTAMP;
    DECLARE job_status STRING;
    DECLARE job_message STRING;
    DECLARE job_name STRING DEFAULT 'r_ausd_vertrag_control';

    -- Start job logging
    SET job_start_time = CURRENT_TIMESTAMP();
    SET job_status = 'RUNNING';
    SET job_message = 'Job started.';

    BEGIN TRANSACTION;

    INSERT INTO `my-project.data_warehouse.job_control_log` (job_name, job_kennung, entry_nr, start_time, status, message)
    VALUES (job_name, p_job_kennung, p_eintrags_nr, job_start_time, job_status, job_message);

    -- Check for an active job (simplified: assumes only one instance should run at a time for this job_name and job_kennung)
    -- More sophisticated logic would handle p_eintrags_nr and other context.
    IF EXISTS (
        SELECT 1 FROM `my-project.data_warehouse.job_control_log`
        WHERE job_name = job_name AND job_kennung = p_job_kennung AND status = 'RUNNING'
        AND start_time < job_start_time -- Check for other *previously started* running jobs
    ) THEN
        SET job_status = 'SKIPPED';
        SET job_message = 'Skipping execution, another instance of this job is already running.';
        UPDATE `my-project.data_warehouse.job_control_log`
        SET
            end_time = CURRENT_TIMESTAMP(),
            status = job_status,
            message = job_message
        WHERE job_name = job_name AND job_kennung = p_job_kennung AND start_time = job_start_time;
        SELECT CONCAT(job_name, ': ', job_message) AS status_update;
        ROLLBACK TRANSACTION;
        RETURN;
    END IF;

    -- Step 1: Calculate v_datum (from d_ausd_v_ta_barrier.sql)
    -- NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101')
    SET v_datum = (
        SELECT
            IFNULL(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
        FROM `my-project.oracle_raw.dwtk_meldungen` AS m
        WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    );

    -- Log the determined v_datum
    UPDATE `my-project.data_warehouse.job_control_log`
    SET message = CONCAT('Job started. v_datum determined: ', v_datum)
    WHERE job_name = job_name AND job_kennung = p_job_kennung AND start_time = job_start_time;

    -- Step 2: Truncate the target table
    -- TRUNCATE TABLE sof$ta_barrier;
    TRUNCATE TABLE `my-project.data_warehouse.sof_ta_barrier`;

    -- Step 3: Insert transformed data
    -- Core logic from d_ausd_v_ta_barrier.sql
    INSERT INTO `my-project.data_warehouse.sof_ta_barrier` (
        cntrct_id,
        barrier_kind_id,
        barrier_init_cv,
        barrier_reason_cv,
        bfc_age,
        sperrart,
        sperr_beginn,
        sperr_ende,
        sperrgrund,
        ist_stillegung
    )
    SELECT
        b.cntrct_id,
        b.barrier_kind_id,
        b.barrier_init_cv,
        b.barrier_reason_cv,
        GREATEST(b.insert_at, bc.insert_at) AS bfc_age, -- Assuming insert_at is TIMESTAMP
        dk.cds_description AS sperrart,
        COALESCE(b.net_barr_on_date, b.valid_from) AS sperr_beginn, -- Assuming these are DATE types
        COALESCE(b.net_barr_off_date, b.valid_to) AS sperr_ende,     -- Assuming these are DATE types
        CASE bc.barrier_reason_cv
            WHEN '1' THEN 'Kartenverlust'
            WHEN '2' THEN 'Kundenwunsch'
            WHEN '3' THEN 'Betreiberinterne Sperre'
            WHEN '4' THEN 'Ruecksperrung nach Entsperrung'
            WHEN '5' THEN 'Sperre aufgrund Zahlungsverzug'
            WHEN '6' THEN 'Sperre nach Verlustanzeige'
            WHEN '7' THEN 'Sperre nach Kartenausgabe'
            WHEN '8' THEN 'Sperre nach Missbrauchsfall'
            WHEN '9' THEN 'Sperre aufgrund Bonitaet'
            WHEN '10' THEN 'Sperre aufgrund Betrugsverdacht'
            WHEN '11' THEN 'Sperre aufgrund Kulanz'
            WHEN '12' THEN 'Sperre wegen Dateninkonsistenz'
            WHEN '13' THEN 'Sperre wegen Kartenstatus'
            WHEN '14' THEN 'Sperre wegen Rueckbuchung'
            WHEN '15' THEN 'Sperre wegen Reklamation'
            WHEN '16' THEN 'Sperre wegen technischem Defekt'
            WHEN '17' THEN 'Sperre wegen Systemumstellung'
            WHEN '18' THEN 'Sperre wegen Datenbereinigung'
            ELSE 'Unbekannter Sperrgrund' -- Default case for DECODE
        END AS sperrgrund,
        CASE WHEN bc.closure = 1 THEN TRUE ELSE FALSE END AS ist_stillegung -- Changed to BOOL
    FROM
        `my-project.oracle_raw.cds_ta_barrier` AS b
    JOIN
        `my-project.oracle_raw.cds_ta_barrier_class` AS bc
        ON b.barrier_class_id = bc.barrier_class_id
    JOIN
        `my-project.oracle_raw.cds_ta_barrier_kind` AS bk
        ON bc.barrier_kind_id = bk.barrier_kind_id
    LEFT JOIN -- Use LEFT JOIN if dk.cds_description might not always exist for all bk.cds_description_id
        `my-project.oracle_raw.cds_ta_care_description` AS dk
        ON bk.cds_description_id = dk.cds_description_id
    WHERE
        b.valid_from >= PARSE_DATE('%Y%m%d', v_datum)
        AND b.is_production = 1;

    SET v_records_processed = @@row_count;

    SET job_status = 'SUCCESS';
    SET job_message = CONCAT('Job completed successfully. Records processed: ', v_records_processed);

    COMMIT TRANSACTION;

EXCEPTION WHEN ERROR THEN
    SET job_status = 'FAILED';
    SET job_message = CONCAT('Job failed with error: ', @@error.message);
    ROLLBACK TRANSACTION;

FINALLY
    SET job_end_time = CURRENT_TIMESTAMP();
    UPDATE `my-project.data_warehouse.job_control_log`
    SET
        end_time = job_end_time,
        status = job_status,
        message = job_message,
        records_processed = v_records_processed
    WHERE job_name = job_name AND job_kennung = p_job_kennung AND start_time = job_start_time;

    -- For debugging/monitoring, output the final status
    SELECT CONCAT(job_name, ' (', p_job_kennung, '): ', job_status, ' - ', job_message) AS final_status_report;
END;