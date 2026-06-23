-- Target for legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount.ksh
-- Description: BigQuery stored procedure replicating the wrapper logic for contract data reconciliation.
CREATE OR REPLACE PROCEDURE your_gcp_project_id.your_bq_dataset_id.vertragsdatenabgleich_wrapper_proc(
    IN p_help BOOLEAN DEFAULT FALSE,
    IN p_stichtag DATE DEFAULT CURRENT_DATE()
)
BEGIN
    DECLARE v_job_entry_nr INT64;
    DECLARE v_job_kennung STRING;
    DECLARE v_script_name STRING DEFAULT 'r_ausd_v_ta_discount_wrapper';
    DECLARE v_prog_name STRING DEFAULT 'Vertragsdatenabgleich';
    DECLARE v_prog_version STRING DEFAULT '1.0.0-BQ';
    DECLARE v_log_filename STRING;

    IF p_help THEN
        SELECT 'Usage: CALL your_gcp_project_id.your_bq_dataset_id.vertragsdatenabgleich_wrapper_proc(p_help => TRUE/FALSE, p_stichtag => \'YYYY-MM-DD\');' AS help_message;
        SELECT '  -h: Display this help message.' AS help_message;
        SELECT '  -f <date>: Stichtag for data processing (default: current date).' AS help_message;
        RETURN;
    END IF;

    -- 1. Generate Job Entry Number and Kennung
    CALL your_gcp_project_id.your_bq_dataset_id.dwmsg_ermittlenr_proc(v_job_entry_nr, v_job_kennung);
    CALL your_gcp_project_id.your_bq_dataset_id.dwmsg_logdateiname_proc(v_job_kennung, v_log_filename);

    -- 2. Log Job Start and Metadata
    CALL your_gcp_project_id.your_bq_dataset_id.dwmsg_erzeugeeintrag_proc(
        v_job_entry_nr, v_job_kennung, v_script_name, 'INFO',
        CONCAT('*******************************************************************************')
    );
    CALL your_gcp_project_id.your_bq_dataset_id.dwmsg_erzeugeeintrag_proc(
        v_job_entry_nr, v_job_kennung, v_script_name, 'INFO',
        CONCAT(v_prog_name, ' Version ', v_prog_version)
    );
    CALL your_gcp_project_id.your_bq_dataset_id.dwmsg_erzeugeeintrag_proc(
        v_job_entry_nr, v_job_kennung, v_script_name, 'INFO',
        CONCAT('Job Kennung: ', v_job_kennung, ', Entry No: ', CAST(v_job_entry_nr AS STRING))
    );
    CALL your_gcp_project_id.your_bq_dataset_id.dwmsg_erzeugeeintrag_proc(
        v_job_entry_nr, v_job_kennung, v_script_name, 'INFO',
        CONCAT('Start Time: ', FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', CURRENT_TIMESTAMP()))
    );
    CALL your_gcp_project_id.your_bq_dataset_id.dwmsg_erzeugeeintrag_proc(
        v_job_entry_nr, v_job_kennung, v_script_name, 'INFO',
        CONCAT('Stichtag: ', CAST(p_stichtag AS STRING))
    );
    CALL your_gcp_project_id.your_bq_dataset_id.dwmsg_erzeugeeintrag_proc(
        v_job_entry_nr, v_job_kennung, v_script_name, 'INFO',
        CONCAT('Log Identifier (Conceptual): ', v_log_filename)
    );
    CALL your_gcp_project_id.your_bq_dataset_id.dwmsg_erzeugeeintrag_proc(
        v_job_entry_nr, v_job_kennung, v_script_name, 'INFO',
        CONCAT('*******************************************************************************')
    );

    -- 3. Set initial job status
    CALL your_gcp_project_id.your_bq_dataset_id.dwmsg_setzestichtaginfo_proc(
        v_job_entry_nr, v_job_kennung, p_stichtag, 'RUNNING', 'Job started'
    );

    BEGIN
        -- 4. Invoke Core Processing Script (k_ausd_v_ta_discount)
        CALL your_gcp_project_id.your_bq_dataset_id.k_ausd_v_ta_discount_proc(v_job_kennung, v_job_entry_nr, p_stichtag);

        -- 5. On successful completion
        CALL your_gcp_project_id.your_bq_dataset_id.dwmsg_setzestatusok_proc(v_job_entry_nr, v_job_kennung, v_script_name);

    EXCEPTION WHEN ERROR THEN
        -- 6. On error
        DECLARE error_message STRING;
        SET error_message = @@error.message;
        CALL your_gcp_project_id.your_bq_dataset_id.dwmsg_fehlerbehandlung_proc(
            v_job_entry_nr, v_job_kennung, v_script_name, error_message
        );
    END;

    CALL your_gcp_project_id.your_bq_dataset_id.dwmsg_erzeugeeintrag_proc(
        v_job_entry_nr, v_job_kennung, v_script_name, 'INFO',
        CONCAT('Wrapper procedure finished for Job Kennung: ', v_job_kennung)
    );
END;