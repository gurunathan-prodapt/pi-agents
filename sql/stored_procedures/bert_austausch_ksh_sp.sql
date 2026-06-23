-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_austausch.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_austausch.ksh

-- This BigQuery stored procedure orchestrates the BERT report data extraction and transformation.
-- It replaces the functionality of r_ausd_austausch.ksh, k_ausd_austausch.ksh, and gestern.ksh.

CREATE OR REPLACE PROCEDURE `project.reporting_dataset.BERT_AUSTAUSCH_KSH_SP`(
    IN p_stichtag_input STRING,  -- Input as DDMMYYYY or NULL
    IN p_wiederanlauf_wert_input INT64 -- Input for restart logic, 0 if not set
)
BEGIN
    DECLARE v_job_kennung STRING DEFAULT 'BERT_AUSTAUSCH';
    DECLARE v_sysdate DATE;
    DECLARE v_stichtag DATE;
    DECLARE v_wiederanlauf_wert INT64;
    DECLARE v_eintrags_nr STRING;
    DECLARE v_job_status STRING;
    DECLARE v_error_message STRING;
    DECLARE v_log_entry_id STRING;

    -- Helper procedure for logging
    -- This assumes `project.admin_dataset.log_message` exists.
    -- CREATE OR REPLACE PROCEDURE `project.admin_dataset.log_message`(
    --     job_name STRING,
    --     run_id STRING,
    --     level STRING,
    --     message STRING
    -- )
    -- BEGIN
    --     INSERT INTO `project.admin_dataset.job_log` (log_time, job_name, run_id, level, message)
    --     VALUES (CURRENT_TIMESTAMP(), job_name, run_id, level, message);
    -- END;

    -- Determine run_id for logging
    SET v_eintrags_nr = GENERATE_UUID();

    -- Initialize wiederanlauf_wert
    SET v_wiederanlauf_wert = IFNULL(p_wiederanlauf_wert_input, 0);

    -- Determine sysdate (today's date)
    SET v_sysdate = CURRENT_DATE();

    -- Date determination logic (replaces gestern.ksh and r_ausd_austausch.ksh date logic)
    IF p_stichtag_input IS NULL OR p_stichtag_input = '' THEN
        -- If Stichtag not set, use sysdate (simplified from original MIN(sysdate,max_ladedatum))
        SET v_stichtag = v_sysdate;
    ELSE
        -- Parse provided Stichtag (DDMMYYYY)
        SET v_stichtag = PARSE_DATE('%d%m%Y', p_stichtag_input);
    END IF;

    -- Date for gestern.ksh: today and yesterday
    DECLARE v_datum_heute DATE DEFAULT v_sysdate;
    DECLARE v_datum_gestern DATE DEFAULT DATE_SUB(v_sysdate, INTERVAL 1 DAY);

    -- Log job start and insert into job_control table
    INSERT INTO `project.admin_dataset.job_control` (job_name, start_time, status, run_id, stichtag, wiederanlaufwert)
    VALUES (v_job_kennung, CURRENT_TIMESTAMP(), 'RUNNING', v_eintrags_nr, v_stichtag, v_wiederanlauf_wert);

    CALL `project.admin_dataset.log_message`(v_job_kennung, v_eintrags_nr, 'INFO', FORMAT('Job started with Stichtag: %F, Wiederanlaufwert: %d', v_stichtag, v_wiederanlauf_wert));
    CALL `project.admin_dataset.log_message`(v_job_kennung, v_eintrags_nr, 'INFO', FORMAT('Calculated today: %F, yesterday: %F', v_datum_heute, v_datum_gestern));


    BEGIN
        -- Call the data transformation stored procedure
        CALL `project.reporting_dataset.D_AUSD_AUSTAUSCH_SP`(
            v_job_kennung,
            v_eintrags_nr,
            v_stichtag,
            v_wiederanlauf_wert,
            v_datum_heute,
            v_datum_gestern
        );

        SET v_job_status = 'SUCCEEDED';
        CALL `project.admin_dataset.log_message`(v_job_kennung, v_eintrags_nr, 'INFO', 'Job completed successfully.');

    EXCEPTION WHEN ERROR THEN
        SET v_job_status = 'FAILED';
        SET v_error_message = @@error.message;
        CALL `project.admin_dataset.log_message`(v_job_kennung, v_eintrags_nr, 'ERROR', FORMAT('Job failed: %s', v_error_message));
        -- Re-raise the error to propagate it
        RAISE;

    END;

    -- Update job_control table with final status and end time
    UPDATE `project.admin_dataset.job_control`
    SET
        end_time = CURRENT_TIMESTAMP(),
        status = v_job_status,
        message = v_error_message
    WHERE
        run_id = v_eintrags_nr;

END;
;