-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis.ksh
-- Description: BigQuery Stored Procedure for orchestrating data preparation based on the ksh script.

CREATE OR REPLACE PROCEDURE `your_gcp_project_id.your_orchestration_dataset.r_ausd_bp_ta_bpr_basis`(
    p_job_kennung STRING,
    p_eintrags_nr STRING,
    p_stichtag_str STRING,
    p_wiederanlauf_wert STRING,
    p_source_project_id STRING,
    p_source_dataset_id STRING,
    p_staging_project_id STRING,
    p_staging_dataset_id STRING,
    p_logging_project_id STRING,
    p_logging_dataset_id STRING
)
OPTIONS(strict_mode=true)
BEGIN
    -- Declare variables
    DECLARE v_job_name STRING DEFAULT 'r_ausd_bp_ta_bpr_basis';
    DECLARE v_error_message STRING;
    DECLARE v_records_processed INT64 DEFAULT 0;
    DECLARE v_current_date DATE;
    DECLARE v_yesterday_date DATE;
    DECLARE v_stichtag_date DATE;
    DECLARE v_wiederanlauf_wert_final STRING;

    -- Logging table FQDNs
    DECLARE logging_job_table_fqdn STRING;
    DECLARE logging_error_log_fqdn STRING;
    DECLARE staging_sof_ta_bpr_basis_fqdn STRING;
    DECLARE core_proc_fqdn STRING;

    SET logging_job_table_fqdn = CONCAT('`', p_logging_project_id, '.', p_logging_dataset_id, '.job_table`');
    SET logging_error_log_fqdn = CONCAT('`', p_logging_project_id, '.', p_logging_dataset_id, '.job_error_log`');
    SET staging_sof_ta_bpr_basis_fqdn = CONCAT('`', p_staging_project_id, '.', p_staging_dataset_id, '.sof_ta_bpr_basis`');
    SET core_proc_fqdn = CONCAT('`', your_gcp_project_id, '.', your_orchestration_dataset, '.core_d_ausd_bp_ta_bpr_basis_proc`');

    -- Defaulting p_wiederanlauf_wert
    SET v_wiederanlauf_wert_final = COALESCE(p_wiederanlauf_wert, '0');

    -- Parameter Validation
    IF p_job_kennung IS NULL OR p_job_kennung = '' THEN
        SET v_error_message = 'ERROR: p_job_kennung is mandatory and cannot be NULL or empty.';
        EXECUTE IMMEDIATE CONCAT('INSERT INTO ', logging_error_log_fqdn, ' (job_name, entry_nr, stichtag, error_message, created_at) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP())')
            USING v_job_name, p_eintrags_nr, p_stichtag_str, v_error_message;
        RAISE USING MESSAGE v_error_message;
    END IF;

    IF p_eintrags_nr IS NULL OR p_eintrags_nr = '' THEN
        SET v_error_message = 'ERROR: p_eintrags_nr is mandatory and cannot be NULL or empty.';
        EXECUTE IMMEDIATE CONCAT('INSERT INTO ', logging_error_log_fqdn, ' (job_name, entry_nr, stichtag, error_message, created_at) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP())')
            USING v_job_name, p_eintrags_nr, p_stichtag_str, v_error_message;
        RAISE USING MESSAGE v_error_message;
    END IF;

    IF p_stichtag_str IS NULL OR p_stichtag_str = '' THEN
        SET v_error_message = 'ERROR: p_stichtag_str is mandatory and cannot be NULL or empty.';
        EXECUTE IMMEDIATE CONCAT('INSERT INTO ', logging_error_log_fqdn, ' (job_name, entry_nr, stichtag, error_message, created_at) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP())')
            USING v_job_name, p_eintrags_nr, p_stichtag_str, v_error_message;
        RAISE USING MESSAGE v_error_message;
    END IF;

    -- Date Validation for p_stichtag_str
    BEGIN
        SET v_stichtag_date = SAFE.PARSE_DATE('%Y%m%d', p_stichtag_str);
        IF v_stichtag_date IS NULL THEN
            SET v_error_message = CONCAT('ERROR: Invalid date format for p_stichtag_str: ', p_stichtag_str, '. Expected YYYYMMDD.');
            EXECUTE IMMEDIATE CONCAT('INSERT INTO ', logging_error_log_fqdn, ' (job_name, entry_nr, stichtag, error_message, created_at) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP())')
                USING v_job_name, p_eintrags_nr, p_stichtag_str, v_error_message;
            RAISE USING MESSAGE v_error_message;
        END IF;
    EXCEPTION WHEN ERROR THEN
        SET v_error_message = CONCAT('ERROR: Unexpected error during date parsing for p_stichtag_str: ', p_stichtag_str, '. Original error: ', @@error.message);
        EXECUTE IMMEDIATE CONCAT('INSERT INTO ', logging_error_log_fqdn, ' (job_name, entry_nr, stichtag, error_message, created_at) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP())')
            USING v_job_name, p_eintrags_nr, p_stichtag_str, v_error_message;
        RAISE USING MESSAGE v_error_message;
    END;

    -- Date Derivation
    SET v_current_date = CURRENT_DATE();
    SET v_yesterday_date = DATE_SUB(v_current_date, INTERVAL 1 DAY);

    -- Start job logging (equivalent to FOSJobErzeugeEintrag initial call)
    EXECUTE IMMEDIATE CONCAT('
        INSERT INTO ', logging_job_table_fqdn, ' (
            job_name, status_a, status_i, start_date, end_date, job_type,
            restart_flag, record_count, description, job_kennung, eintrags_nr,
            stichtag, wiederanlaufwert, created_at
        )
        VALUES (
            ?, ''RUNNING'', ''START'', ?, NULL, ''DATA_PREP'',
            ?, 0, ''Data preparation for Basisprodukt'', ?, ?,
            ?, ?, CURRENT_TIMESTAMP()
        )')
    USING v_job_name, v_current_date, v_wiederanlauf_wert_final, p_job_kennung, p_eintrags_nr,
          p_stichtag_str, v_wiederanlauf_wert_final;


    -- Execute Core SQL Logic
    BEGIN
        CALL core_proc_fqdn(
            p_stichtag_str,
            p_source_project_id,
            p_source_dataset_id,
            p_staging_project_id,
            p_staging_dataset_id
        );

        -- Get Record Count from the target table after transformations
        EXECUTE IMMEDIATE CONCAT('SELECT COUNT(*) FROM ', staging_sof_ta_bpr_basis_fqdn) INTO v_records_processed;


        -- Update job logging with success and record count
        EXECUTE IMMEDIATE CONCAT('
            INSERT INTO ', logging_job_table_fqdn, ' (
                job_name, status_a, status_i, start_date, end_date, job_type,
                restart_flag, record_count, description, job_kennung, eintrags_nr,
                stichtag, wiederanlaufwert, created_at
            )
            VALUES (
                ?, ''SUCCESS'', ''END'', ?, CURRENT_DATE(), ''DATA_PREP'',
                ?, ?, ''Data preparation for Basisprodukt completed'', ?, ?,
                ?, ?, CURRENT_TIMESTAMP()
            )')
        USING v_job_name, v_current_date, v_wiederanlauf_wert_final, v_records_processed, p_job_kennung, p_eintrags_nr,
              p_stichtag_str, v_wiederanlauf_wert_final;


    EXCEPTION WHEN ERROR THEN
        SET v_error_message = CONCAT('ERROR during core SQL execution: ', @@error.message);
        EXECUTE IMMEDIATE CONCAT('INSERT INTO ', logging_error_log_fqdn, ' (job_name, entry_nr, stichtag, error_message, created_at) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP())')
            USING v_job_name, p_eintrags_nr, p_stichtag_str, v_error_message;

        -- Update job logging with failure
        EXECUTE IMMEDIATE CONCAT('
            INSERT INTO ', logging_job_table_fqdn, ' (
                job_name, status_a, status_i, start_date, end_date, job_type,
                restart_flag, record_count, description, job_kennung, eintrags_nr,
                stichtag, wiederanlaufwert, created_at
            )
            VALUES (
                ?, ''FAILED'', ''ERROR'', ?, CURRENT_DATE(), ''DATA_PREP'',
                ?, 0, ?, ?, ?,
                ?, ?, CURRENT_TIMESTAMP()
            )')
        USING v_job_name, v_current_date, v_wiederanlauf_wert_final, v_error_message, p_job_kennung, p_eintrags_nr,
              p_stichtag_str, v_wiederanlauf_wert_final;

        RAISE USING MESSAGE v_error_message;
    END;

END;