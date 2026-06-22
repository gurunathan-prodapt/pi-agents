-- BigQuery Stored Procedure for orchestrator logic
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_da_vda_tk.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_da_vda_tk.ksh

CREATE OR REPLACE PROCEDURE `project.dataset.sp_bereitstellung_basisprodukte_bert`(
    p_stichtag STRING,
    p_wiederanlaufWert INT64
)
OPTIONS(
  description="Orchestrates the extraction and provision of contract cache data for the BERT system."
)
BEGIN
    -- Variable declarations
    DECLARE v_progname STRING DEFAULT 'sp_bereitstellung_basisprodukte_bert';
    DECLARE v_progversion STRING DEFAULT '1.0';
    DECLARE v_jobkennung STRING;
    DECLARE v_sysdate STRING;
    DECLARE v_effective_stichtag STRING;
    DECLARE v_effective_restart INT64;
    DECLARE v_job_nr INT64;
    DECLARE v_logdatei STRING; -- Placeholder for audit trail, actual file not used in BQ
    DECLARE v_errmsg STRING;
    DECLARE v_job_status STRING;

    -- Initialize system date in DDMMYYYY format
    SET v_sysdate = FORMAT_DATE('%d%m%Y', CURRENT_DATE());

    -- Default p_wiederanlaufWert if NULL
    SET v_effective_restart = IFNULL(p_wiederanlaufWert, 0);

    -- Default p_stichtag if NULL or empty, otherwise use provided value
    SET v_effective_stichtag = IFNULL(NULLIF(TRIM(p_stichtag), ''), v_sysdate);

    -- Generate a unique job identifier (job_kennung)
    SET v_jobkennung = CONCAT(v_progname, '_', FORMAT_TIMESTAMP('%Y%m%d%H%M%S', CURRENT_TIMESTAMP()));

    -- Generate a new job_nr by incrementing the max existing job_nr
    SET v_job_nr = (SELECT IFNULL(MAX(job_nr), 0) + 1 FROM `project.dataset.job_audit`);

    -- Placeholder for log_file, not directly used in BigQuery context
    SET v_logdatei = CONCAT('/var/log/bq_jobs/', v_jobkennung, '.log');

    -- Parameter validation
    IF v_effective_stichtag IS NULL OR LENGTH(v_effective_stichtag) != 8 THEN
        SET v_errmsg = 'Invalid Stichtag parameter. Must be in DDMMYYYY format.';
        INSERT INTO `project.dataset.job_log` (job_kennung, progname, progversion, log_level, message, created_at)
        VALUES (v_jobkennung, v_progname, v_progversion, 'ERROR', v_errmsg, CURRENT_TIMESTAMP());
        RAISE USING MESSAGE v_errmsg;
    END IF;

    -- Initial audit entry: STARTED
    INSERT INTO `project.dataset.job_audit` (job_nr, job_kennung, progname, progversion, stichtag, restart_value, log_file, status, created_at, message)
    VALUES (v_job_nr, v_jobkennung, v_progname, v_progversion, v_effective_stichtag, v_effective_restart, v_logdatei, 'STARTED', CURRENT_TIMESTAMP(), 'Job started successfully.');

    BEGIN
        -- Core Logic Block
        -- If restart value is greater than 0, perform cleanup in target table
        IF v_effective_restart > 0 THEN
            DELETE FROM `project.dataset.target_fos_table`
            WHERE DWH_VERTRAG_ID >= v_effective_restart;

            INSERT INTO `project.dataset.job_log` (job_kennung, progname, progversion, log_level, message, created_at)
            VALUES (v_jobkennung, v_progname, v_progversion, 'INFO', CONCAT('Restart cleanup performed for DWH_VERTRAG_ID >= ', v_effective_restart), CURRENT_TIMESTAMP());
        END IF;

        -- Integrate kernel script logic (k_ausd_bp_ta_rn_da_vda_tk.ksh equivalent)
        -- This is a direct INSERT INTO ... SELECT FROM ... based on inferred logic.
        -- NOTE: Column list for INSERT and SELECT is a placeholder and should be
        -- expanded based on actual schema of 'source_contract_cache' and 'target_fos_table'.
        INSERT INTO `project.dataset.target_fos_table` (
            -- TODO: Specify target columns from target_fos_table here. Example:
            DWH_VERTRAG_ID, SOME_OTHER_COL, LAST_UPDATE_DATE, LADEDATUM
        )
        SELECT
            -- TODO: Specify source columns from source_contract_cache here. Example:
            s.DWH_VERTRAG_ID, s.SOME_OTHER_COL, s.LAST_UPDATE_DATE, s.LADEDATUM
        FROM
            `project.dataset.source_contract_cache` AS s
        WHERE
            PARSE_DATE('%d%m%Y', v_effective_stichtag) BETWEEN s.GUELTIG_VON AND s.GUELTIG_BIS
            AND s.LADEDATUM < PARSE_DATE('%d%m%Y', v_effective_stichtag)
            AND s.DWH_VERTRAG_ID > v_effective_restart; -- Apply restart filter during insert

        -- Final audit entry: OK
        SET v_job_status = 'OK';
        INSERT INTO `project.dataset.job_audit` (job_nr, job_kennung, progname, progversion, stichtag, restart_value, log_file, status, created_at, message)
        VALUES (v_job_nr, v_jobkennung, v_progname, v_progversion, v_effective_stichtag, v_effective_restart, v_logdatei, v_job_status, CURRENT_TIMESTAMP(), 'Job completed successfully.');

    EXCEPTION WHEN ERROR THEN
        SET v_errmsg = @@error.message;
        SET v_job_status = 'ERROR';

        -- Audit entry: ERROR
        INSERT INTO `project.dataset.job_audit` (job_nr, job_kennung, progname, progversion, stichtag, restart_value, log_file, status, created_at, message)
        VALUES (v_job_nr, v_jobkennung, v_progname, v_progversion, v_effective_stichtag, v_effective_restart, v_logdatei, v_job_status, CURRENT_TIMESTAMP(), CONCAT('Job failed: ', v_errmsg));

        -- Log the error
        INSERT INTO `project.dataset.job_log` (job_kennung, progname, progversion, log_level, message, created_at)
        VALUES (v_jobkennung, v_progname, v_progversion, 'ERROR', CONCAT('Job execution failed: ', v_errmsg), CURRENT_TIMESTAMP());

        RAISE USING MESSAGE v_errmsg;
    END;

END;