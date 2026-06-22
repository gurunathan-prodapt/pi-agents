-- BigQuery Stored Procedure for core SQL transformation
-- Replaces vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_rn_vertrag.sql
CREATE OR REPLACE PROCEDURE `project.dataset.d_ausd_bp_ta_rn_vertrag`(
    IN p_job_kennung STRING,
    IN p_run_id STRING,
    OUT p_records_inserted INT64
)
BEGIN
    DECLARE v_error_message STRING;

    BEGIN
        -- Log start of procedure
        INSERT INTO `project.dataset.job_audit` (job_kennung, run_id, status, message, created_at)
        VALUES (p_job_kennung, p_run_id, 'RUNNING', 'Starting d_ausd_bp_ta_rn_vertrag procedure.', CURRENT_TIMESTAMP());

        -- Truncate target table
        TRUNCATE TABLE `project.dataset.sof_ta_rn_vertrag`;

        -- Perform aggregation and insertion
        INSERT INTO `project.dataset.sof_ta_rn_vertrag` (
            CNTRCT_ID, TN_MULTI_SINGLE, TN_TEL_MSISDN, TN_TEL_STATUS, TN_TEL_VALID_TO,
            TN_FAX_MSISDN, TN_FAX_STATUS, TN_FAX_VALID_TO, TN_DAT_MSISDN, TN_DAT_STATUS, TN_DAT_VALID_TO,
            TC_MULTI_SINGLE, TC_TEL_MSISDN, TC_TEL_STATUS, TC_TEL_VALID_TO, TC_FAX_MSISDN, TC_FAX_STATUS,
            TC_FAX_VALID_TO, TC_DAT_MSISDN, TC_DAT_STATUS, TC_DAT_VALID_TO, TB_MULTI_SINGLE, TB_TEL_MSISDN,
            TB_TEL_STATUS, TB_TEL_VALID_TO, TB_FAX_MSISDN, TB_FAX_STATUS, TB_FAX_VALID_TO, TB_DAT_MSISDN,
            TB_DAT_STATUS, TB_DAT_VALID_TO, MS_RN_1_MSISDN, MS_RN_1_STATUS, MS_RN_1_VALID_TO,
            MS_RN_2_MSISDN, MS_RN_2_STATUS, MS_RN_2_VALID_TO
        )
        SELECT
            cntrct_id,
            MAX(TN_multi_single),
            MAX(TN_TEL_msisdn),
            MAX(TN_TEL_status),
            MAX(TN_TEL_valid_to),
            MAX(TN_FAX_msisdn),
            MAX(TN_FAX_status),
            MAX(TN_FAX_valid_to),
            MAX(TN_DAT_msisdn),
            MAX(TN_DAT_status),
            MAX(TN_DAT_valid_to),
            MAX(TC_multi_single),
            MAX(TC_TEL_msisdn),
            MAX(TC_TEL_status),
            MAX(TC_TEL_valid_to),
            MAX(TC_FAX_msisdn),
            MAX(TC_FAX_status),
            MAX(TC_FAX_valid_to),
            MAX(TC_DAT_msisdn),
            MAX(TC_DAT_status),
            MAX(TC_DAT_valid_to),
            MAX(TB_multi_single),
            MAX(TB_TEL_msisdn),
            MAX(TB_TEL_status),
            MAX(TB_TEL_valid_to),
            MAX(TB_FAX_msisdn),
            MAX(TB_FAX_status),
            MAX(TB_FAX_valid_to),
            MAX(TB_DAT_msisdn),
            MAX(TB_DAT_status),
            MAX(TB_DAT_valid_to),
            MAX(MS_RN_1_msisdn),
            MAX(MS_RN_1_status),
            MAX(MS_RN_1_valid_to),
            MAX(MS_RN_2_msisdn),
            MAX(MS_RN_2_status),
            MAX(MS_RN_2_valid_to)
        FROM `project.dataset.sof_ta_rn_einzeln`
        GROUP BY cntrct_id;

        SET p_records_inserted = @@row_count;

        -- Log success
        INSERT INTO `project.dataset.job_audit` (job_kennung, run_id, status, message, created_at, record_count)
        VALUES (p_job_kennung, p_run_id, 'SUCCESS', 'd_ausd_bp_ta_rn_vertrag completed successfully.', CURRENT_TIMESTAMP(), p_records_inserted);

    EXCEPTION WHEN ERROR THEN
        SET v_error_message = @@error.message;
        -- Log failure
        INSERT INTO `project.dataset.job_audit` (job_kennung, run_id, status, message, created_at)
        VALUES (p_job_kennung, p_run_id, 'FAILED', CONCAT('d_ausd_bp_ta_rn_vertrag failed: ', v_error_message), CURRENT_TIMESTAMP());
        RAISE; -- Re-raise the error for the calling procedure
    END;
END;