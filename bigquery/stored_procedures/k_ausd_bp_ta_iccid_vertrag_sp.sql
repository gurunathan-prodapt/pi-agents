-- BigQuery Stored Procedure for DW.BERT_AUSD_BP_TA_ICCID_VERTRAG
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_iccid_vertrag.ksh
-- Purpose: Control script logic, date validation, and core SQL transformation execution.

CREATE OR REPLACE PROCEDURE `project.target_dataset.k_ausd_bp_ta_iccid_vertrag_sp`(
    p_JobKennung STRING,
    p_EintragsNr STRING,
    p_Stichtag STRING,
    p_wiederanlaufWert STRING,
    job_uuid STRING
)
BEGIN
    DECLARE v_datum_heute DATE;
    DECLARE v_datum_gestern DATE;
    DECLARE transformed_rows INT64;
    DECLARE v_log_message STRING;
    DECLARE v_status STRING DEFAULT 'RUNNING';

    -- Log job start
    INSERT INTO `project.audit_dataset.job_log` (job_id, log_time, log_level, message)
    VALUES (job_uuid, CURRENT_TIMESTAMP(), 'INFO', FORMAT("k_ausd_bp_ta_iccid_vertrag_sp started with parameters: p_JobKennung='%s', p_EintragsNr='%s', p_Stichtag='%s', p_wiederanlaufWert='%s'", p_JobKennung, p_EintragsNr, p_Stichtag, p_wiederanlaufWert));

    -- Validate p_Stichtag format
    IF SAFE.PARSE_DATE('%Y%m%d', p_Stichtag) IS NULL THEN
        SET v_log_message = FORMAT('Error: Invalid p_Stichtag format. Expected YYYYMMDD, got: %s', p_Stichtag);
        SET v_status = 'FAILED';
        INSERT INTO `project.audit_dataset.job_log` (job_id, log_time, log_level, message)
        VALUES (job_uuid, CURRENT_TIMESTAMP(), 'ERROR', v_log_message);
        UPDATE `project.audit_dataset.job_registry`
        SET
            end_time = CURRENT_TIMESTAMP(),
            status = v_status,
            error_message = v_log_message,
            last_update_time = CURRENT_TIMESTAMP()
        WHERE job_id = job_uuid;
        RAISE USING MESSAGE = v_log_message;
    END IF;

    SET v_datum_heute = CURRENT_DATE();
    SET v_datum_gestern = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

    -- Main transformation logic
    BEGIN
        -- Truncate target table
        EXECUTE IMMEDIATE 'TRUNCATE TABLE `project.target_dataset.sof_ta_iccid_vertrag`';
        SET v_log_message = 'Truncated `project.target_dataset.sof_ta_iccid_vertrag`.';
        INSERT INTO `project.audit_dataset.job_log` (job_id, log_time, log_level, message)
        VALUES (job_uuid, CURRENT_TIMESTAMP(), 'INFO', v_log_message);

        -- Insert data (from d_ausd_bp_ta_iccid_vertrag_insert.sql content)
        INSERT INTO `project.target_dataset.sof_ta_iccid_vertrag`
        (
            CNTRCT_ID,
            TN_ICCID,   TN_IMSI_MCC,   TN_IMSI_MNC,   TN_IMSI_HLR,   TN_IMSI_SI,   TN_STATUS,   TN_VALID_TO,
            TC_ICCID,   TC_IMSI_MCC,   TC_IMSI_MNC,   TC_IMSI_HLR,   TC_IMSI_SI,   TC_STATUS,   TC_VALID_TO,
            TB_ICCID,   TB_IMSI_MCC,   TB_IMSI_MNC,   TB_IMSI_HLR,   TB_IMSI_SI,   TB_STATUS,   TB_VALID_TO,
            MS1_ICCID,  MS1_IMSI_MCC,  MS1_IMSI_MNC,  MS1_IMSI_HLR,  MS1_IMSI_SI,  MS1_STATUS,  MS1_VALID_TO,
            MS2_ICCID,  MS2_IMSI_MCC,  MS2_IMSI_MNC,  MS2_IMSI_HLR,  MS2_IMSI_SI,  MS2_STATUS,  MS2_VALID_TO,
            TN_E_ID, TN_CARD_TYPE_NAME,
            TC_E_ID, TC_CARD_TYPE_NAME,
            TB_E_ID, TB_CARD_TYPE_NAME,
            MS1_E_ID, MS1_CARD_TYPE_NAME,
            MS2_E_ID, MS2_CARD_TYPE_NAME,
            MS3_ICCID, MS3_E_ID, MS3_CARD_TYPE_NAME, MS3_IMSI_MCC, MS3_IMSI_MNC, MS3_IMSI_HLR, MS3_IMSI_SI, MS3_STATUS, MS3_VALID_TO,
            MS4_ICCID, MS4_E_ID, MS4_CARD_TYPE_NAME, MS4_IMSI_MCC, MS4_IMSI_MNC, MS4_IMSI_HLR, MS4_IMSI_SI, MS4_STATUS, MS4_VALID_TO,
            MS5_ICCID, MS5_E_ID, MS5_CARD_TYPE_NAME, MS5_IMSI_MCC, MS5_IMSI_MNC, MS5_IMSI_HLR, MS5_IMSI_SI, MS5_STATUS, MS5_VALID_TO,
            MS6_ICCID, MS6_E_ID, MS6_CARD_TYPE_NAME, MS6_IMSI_MCC, MS6_IMSI_MNC, MS6_IMSI_HLR, MS6_IMSI_SI, MS6_STATUS, MS6_VALID_TO,
            MS7_ICCID, MS7_E_ID, MS7_CARD_TYPE_NAME, MS7_IMSI_MCC, MS7_IMSI_MNC, MS7_IMSI_HLR, MS7_IMSI_SI, MS7_STATUS, MS7_VALID_TO,
            MS8_ICCID, MS8_E_ID, MS8_CARD_TYPE_NAME, MS8_IMSI_MCC, MS8_IMSI_MNC, MS8_IMSI_HLR, MS8_IMSI_SI, MS8_STATUS, MS8_VALID_TO,
            MS9_ICCID, MS9_E_ID, MS9_CARD_TYPE_NAME, MS9_IMSI_MCC, MS9_IMSI_MNC, MS9_IMSI_HLR, MS9_IMSI_SI, MS9_STATUS, MS9_VALID_TO,
            MS10_ICCID, MS10_E_ID, MS10_CARD_TYPE_NAME, MS10_IMSI_MCC, MS10_IMSI_MNC, MS10_IMSI_HLR, MS10_IMSI_SI, MS10_STATUS, MS10_VALID_TO
        )
        SELECT
            cntrct_id,
            MAX(TN_ICCID) AS TN_ICCID,
            MAX(TN_IMSI_MCC) AS TN_IMSI_MCC,
            MAX(TN_IMSI_MNC) AS TN_IMSI_MNC,
            MAX(TN_IMSI_HLR) AS TN_IMSI_HLR,
            MAX(TN_IMSI_SI) AS TN_IMSI_SI,
            MAX(TN_STATUS) AS TN_STATUS,
            MAX(TN_VALID_TO) AS TN_VALID_TO,
            MAX(TC_ICCID) AS TC_ICCID,
            MAX(TC_IMSI_MCC) AS TC_IMSI_MCC,
            MAX(TC_IMSI_MNC) AS TC_IMSI_MNC,
            MAX(TC_IMSI_HLR) AS TC_IMSI_HLR,
            MAX(TC_IMSI_SI) AS TC_IMSI_SI,
            MAX(TC_STATUS) AS TC_STATUS,
            MAX(TC_VALID_TO) AS TC_VALID_TO,
            MAX(TB_ICCID) AS TB_ICCID,
            MAX(TB_IMSI_MCC) AS TB_IMSI_MCC,
            MAX(TB_IMSI_MNC) AS TB_IMSI_MNC,
            MAX(TB_IMSI_HLR) AS TB_IMSI_HLR,
            MAX(TB_IMSI_SI) AS TB_IMSI_SI,
            MAX(TB_STATUS) AS TB_STATUS,
            MAX(TB_VALID_TO) AS TB_VALID_TO,
            MAX(MS1_ICCID) AS MS1_ICCID,
            MAX(MS1_IMSI_MCC) AS MS1_IMSI_MCC,
            MAX(MS1_IMSI_MNC) AS MS1_IMSI_MNC,
            MAX(MS1_IMSI_HLR) AS MS1_IMSI_HLR,
            MAX(MS1_IMSI_SI) AS MS1_IMSI_SI,
            MAX(MS1_STATUS) AS MS1_STATUS,
            MAX(MS1_VALID_TO) AS MS1_VALID_TO,
            MAX(MS2_ICCID) AS MS2_ICCID,
            MAX(MS2_IMSI_MCC) AS MS2_IMSI_MCC,
            MAX(MS2_IMSI_MNC) AS MS2_IMSI_MNC,
            MAX(MS2_IMSI_HLR) AS MS2_IMSI_HLR,
            MAX(MS2_IMSI_SI) AS MS2_IMSI_SI,
            MAX(MS2_STATUS) AS MS2_STATUS,
            MAX(MS2_VALID_TO) AS MS2_VALID_TO,
            MAX(TN_E_ID) AS TN_E_ID,
            MAX(TN_CARD_TYPE_NAME) AS TN_CARD_TYPE_NAME,
            MAX(TC_E_ID) AS TC_E_ID,
            MAX(TC_CARD_TYPE_NAME) AS TC_CARD_TYPE_NAME,
            MAX(TB_E_ID) AS TB_E_ID,
            MAX(TB_CARD_TYPE_NAME) AS TB_CARD_TYPE_NAME,
            MAX(MS1_E_ID) AS MS1_E_ID,
            MAX(MS1_CARD_TYPE_NAME) AS MS1_CARD_TYPE_NAME,
            MAX(MS2_E_ID) AS MS2_E_ID,
            MAX(MS2_CARD_TYPE_NAME) AS MS2_CARD_TYPE_NAME,
            MAX(MS3_ICCID) AS MS3_ICCID,
            MAX(MS3_E_ID) AS MS3_E_ID,
            MAX(MS3_CARD_TYPE_NAME) AS MS3_CARD_TYPE_NAME,
            MAX(MS3_IMSI_MCC) AS MS3_IMSI_MCC,
            MAX(MS3_IMSI_MNC) AS MS3_IMSI_MNC,
            MAX(MS3_IMSI_HLR) AS MS3_IMSI_HLR,
            MAX(MS3_IMSI_SI) AS MS3_IMSI_SI,
            MAX(MS3_STATUS) AS MS3_STATUS,
            MAX(MS3_VALID_TO) AS MS3_VALID_TO,
            MAX(MS4_ICCID) AS MS4_ICCID,
            MAX(MS4_E_ID) AS MS4_E_ID,
            MAX(MS4_CARD_TYPE_NAME) AS MS4_CARD_TYPE_NAME,
            MAX(MS4_IMSI_MCC) AS MS4_IMSI_MCC,
            MAX(MS4_IMSI_MNC) AS MS4_IMSI_MNC,
            MAX(MS4_IMSI_HLR) AS MS4_IMSI_HLR,
            MAX(MS4_IMSI_SI) AS MS4_IMSI_SI,
            MAX(MS4_STATUS) AS MS4_STATUS,
            MAX(MS4_VALID_TO) AS MS4_VALID_TO,
            MAX(MS5_ICCID) AS MS5_ICCID,
            MAX(MS5_E_ID) AS MS5_E_ID,
            MAX(MS5_CARD_TYPE_NAME) AS MS5_CARD_TYPE_NAME,
            MAX(MS5_IMSI_MCC) AS MS5_IMSI_MCC,
            MAX(MS5_IMSI_MNC) AS MS5_IMSI_MNC,
            MAX(MS5_IMSI_HLR) AS MS5_IMSI_HLR,
            MAX(MS5_IMSI_SI) AS MS5_IMSI_SI,
            MAX(MS5_STATUS) AS MS5_STATUS,
            MAX(MS5_VALID_TO) AS MS5_VALID_TO,
            MAX(MS6_ICCID) AS MS6_ICCID,
            MAX(MS6_E_ID) AS MS6_E_ID,
            MAX(MS6_CARD_TYPE_NAME) AS MS6_CARD_TYPE_NAME,
            MAX(MS6_IMSI_MCC) AS MS6_IMSI_MCC,
            MAX(MS6_IMSI_MNC) AS MS6_IMSI_MNC,
            MAX(MS6_IMSI_HLR) AS MS6_IMSI_HLR,
            MAX(MS6_IMSI_SI) AS MS6_IMSI_SI,
            MAX(MS6_STATUS) AS MS6_STATUS,
            MAX(MS6_VALID_TO) AS MS6_VALID_TO,
            MAX(MS7_ICCID) AS MS7_ICCID,
            MAX(MS7_E_ID) AS MS7_E_ID,
            MAX(MS7_CARD_TYPE_NAME) AS MS7_CARD_TYPE_NAME,
            MAX(MS7_IMSI_MCC) AS MS7_IMSI_MCC,
            MAX(MS7_IMSI_MNC) AS MS7_IMSI_MNC,
            MAX(MS7_IMSI_HLR) AS MS7_IMSI_HLR,
            MAX(MS7_IMSI_SI) AS MS7_IMSI_SI,
            MAX(MS7_STATUS) AS MS7_STATUS,
            MAX(MS7_VALID_TO) AS MS7_VALID_TO,
            MAX(MS8_ICCID) AS MS8_ICCID,
            MAX(MS8_E_ID) AS MS8_E_ID,
            MAX(MS8_CARD_TYPE_NAME) AS MS8_CARD_TYPE_NAME,
            MAX(MS8_IMSI_MCC) AS MS8_IMSI_MCC,
            MAX(MS8_IMSI_MNC) AS MS8_IMSI_MNC,
            MAX(MS8_IMSI_HLR) AS MS8_IMSI_HLR,
            MAX(MS8_IMSI_SI) AS MS8_IMSI_SI,
            MAX(MS8_STATUS) AS MS8_STATUS,
            MAX(MS8_VALID_TO) AS MS8_VALID_TO,
            MAX(MS9_ICCID) AS MS9_ICCID,
            MAX(MS9_E_ID) AS MS9_E_ID,
            MAX(MS9_CARD_TYPE_NAME) AS MS9_CARD_TYPE_NAME,
            MAX(MS9_IMSI_MCC) AS MS9_IMSI_MCC,
            MAX(MS9_IMSI_MNC) AS MS9_IMSI_MNC,
            MAX(MS9_IMSI_HLR) AS MS9_IMSI_HLR,
            MAX(MS9_IMSI_SI) AS MS9_IMSI_SI,
            MAX(MS9_STATUS) AS MS9_STATUS,
            MAX(MS9_VALID_TO) AS MS9_VALID_TO,
            MAX(MS10_ICCID) AS MS10_ICCID,
            MAX(MS10_E_ID) AS MS10_E_ID,
            MAX(MS10_CARD_TYPE_NAME) AS MS10_CARD_TYPE_NAME,
            MAX(MS10_IMSI_MCC) AS MS10_IMSI_MCC,
            MAX(MS10_IMSI_MNC) AS MS10_IMSI_MNC,
            MAX(MS10_IMSI_HLR) AS MS10_IMSI_HLR,
            MAX(MS10_IMSI_SI) AS MS10_IMSI_SI,
            MAX(MS10_STATUS) AS MS10_STATUS,
            MAX(MS10_VALID_TO) AS MS10_VALID_TO
        FROM `project.source_dataset.sof_ta_iccid_einzeln` AS rp
        GROUP BY cntrct_id;

        SET transformed_rows = @@row_count;
        SET v_log_message = FORMAT('Successfully inserted %d records into `project.target_dataset.sof_ta_iccid_vertrag`.', transformed_rows);
        INSERT INTO `project.audit_dataset.job_log` (job_id, log_time, log_level, message)
        VALUES (job_uuid, CURRENT_TIMESTAMP(), 'INFO', v_log_message);

        SET v_status = 'SUCCEEDED';

    EXCEPTION WHEN ERROR THEN
        SET v_log_message = FORMAT('Error during core transformation: %s', @@error.message);
        SET v_status = 'FAILED';
        INSERT INTO `project.audit_dataset.job_log` (job_id, log_time, log_level, message)
        VALUES (job_uuid, CURRENT_TIMESTAMP(), 'ERROR', v_log_message);
        RAISE USING MESSAGE = v_log_message;
    END;

    -- Update job registry with completion status
    UPDATE `project.audit_dataset.job_registry`
    SET
        end_time = CURRENT_TIMESTAMP(),
        status = v_status,
        processed_records = transformed_rows,
        error_message = IF(v_status = 'FAILED', v_log_message, NULL),
        last_update_time = CURRENT_TIMESTAMP()
    WHERE job_id = job_uuid;

    IF v_status = 'FAILED' THEN
        RAISE USING MESSAGE = 'k_ausd_bp_ta_iccid_vertrag_sp failed.';
    END IF;

END;