-- BigQuery Stored Procedure for core data processing
-- Converts vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_rn_da_vda_tk.sql
-- Legacy Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_da_vda_tk.ksh

CREATE OR REPLACE PROCEDURE my_project.my_dataset.sp_ausd_bp_ta_rn_da_vda_tk(
    IN p_job_run_id STRING,
    IN p_stichtag STRING, -- This parameter is passed for context but not directly used in this procedure's core logic
    IN p_entry_number INT64, -- Placeholder if needed for future logic based on k_ausd_bp_ta_rn_da_vda_tk.ksh
    IN p_restart_threshold INT64 -- Placeholder if needed for future logic based on k_ausd_bp_ta_rn_da_vda_tk.ksh
)
BEGIN
    DECLARE v_datum STRING;

    -- Determine v_datum from dwtk_meldungen, equivalent to Oracle's SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101')
    SELECT
        COALESCE(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')
    INTO
        v_datum
    FROM
        my_project.my_dataset.dwtk_meldungen AS m
    WHERE
        m.job_kennung = 'BERT_DROP_TEMP_TABLE';

    -- Log the determined v_datum
    INSERT INTO my_project.my_dataset.job_audit_log (job_run_id, job_name, status, message)
    VALUES (p_job_run_id, 'sp_ausd_bp_ta_rn_da_vda_tk', 'INFO', CONCAT('Determined v_datum: ', v_datum));

    -- Truncate the target table, equivalent to Oracle's TRUNCATE TABLE sof$ta_rn_da_vda_tk
    TRUNCATE TABLE my_project.my_dataset.sof_ta_rn_da_vda_tk;

    -- Log the truncation
    INSERT INTO my_project.my_dataset.job_audit_log (job_run_id, job_name, status, message)
    VALUES (p_job_run_id, 'sp_ausd_bp_ta_rn_da_vda_tk', 'INFO', 'Truncated target table sof_ta_rn_da_vda_tk');

    -- Insert data into the target table, equivalent to Oracle's INSERT INTO ... SELECT
    INSERT INTO my_project.my_dataset.sof_ta_rn_da_vda_tk (
        contract_id,
        product_code,
        DA_RN_msisdn,
        VDA_RN_msisdn,
        TK_RN_msisdn,
        valid_from_dt,
        valid_to_dt,
        data_value
    )
    SELECT
        contract_id,
        product_code,
        DA_RN_msisdn,
        VDA_RN_msisdn,
        TK_RN_msisdn,
        valid_from_dt,
        valid_to_dt,
        data_value
    FROM
        my_project.my_dataset.sof_ta_rn_einzeln AS rp
    WHERE
        rp.DA_RN_msisdn IS NOT NULL
        OR rp.VDA_RN_msisdn IS NOT NULL
        OR rp.TK_RN_msisdn IS NOT NULL;

    -- Log the completion of data insertion
    INSERT INTO my_project.my_dataset.job_audit_log (job_run_id, job_name, status, message)
    VALUES (p_job_run_id, 'sp_ausd_bp_ta_rn_da_vda_tk', 'INFO', 'Data insertion into sof_ta_rn_da_vda_tk completed successfully.');

EXCEPTION WHEN ERROR THEN
    -- Log any errors that occur
    INSERT INTO my_project.my_dataset.job_audit_log (job_run_id, job_name, status, message)
    VALUES (p_job_run_id, 'sp_ausd_bp_ta_rn_da_vda_tk', 'FAILED', CONCAT('Error during execution: ', @@error.message));
    RAISE;
END;