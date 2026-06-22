--
-- BigQuery Stored Procedure for orchestration logic of DW.BERT_AUSD_V_TA_CNTRCT_TEMPL
-- Legacy Source: r_ausd_v_ta_cntrct_templ.ksh, k_ausd_v_ta_cntrct_templ.ksh (KornShell)
--
CREATE OR REPLACE PROCEDURE `your_project.your_dataset.r_ausd_v_ta_cntrct_templ`(
  IN p_JobKennung STRING,
  IN p_EintragsNr INT64
)
BEGIN
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';
  DECLARE DW_EintragsNr INT64 DEFAULT p_EintragsNr;
  DECLARE v_sysdate DATE DEFAULT CURRENT_DATE();
  DECLARE v_TabName STRING DEFAULT 'ta_cntrct_templ';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_status STRING DEFAULT 'OK';
  DECLARE job_id_val STRING DEFAULT 'DW.BERT_AUSD_V_TA_CNTRCT_TEMPL';

  -- Parameter validation (equivalent to shell script's parameter checks)
  IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
    SET ErrNr = 193;
    SET ErrArg = 'Jobkennung';
  END IF;
  -- Add more parameter checks as needed, based on full shell script analysis
  -- For now, basic check is included.

  IF ErrNr != 0 THEN
    -- Log error to BigQuery error log table
    INSERT INTO `your_project.your_dataset.job_error_log` (job_id, error_time, error_code, error_message, error_arg)
    VALUES (job_id_val, CURRENT_TIMESTAMP(), ErrNr, 'Parameter validation failed', ErrArg);
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = CONCAT('FEHLER: ', CAST(ErrNr AS STRING), ' ', ErrArg);
  END IF;

  -- Log job start
  INSERT INTO `your_project.your_dataset.job_log` (job_id, start_time, status, message)
  VALUES (job_id_val, CURRENT_TIMESTAMP(), 'START', CONCAT('Job started with JobKennung: ', p_JobKennung, ', EintragsNr: ', CAST(p_EintragsNr AS STRING)));

  BEGIN -- Main SQL execution block
    -- Execute core data transformation (equivalent of d_ausd_v_ta_cntrct_templ.sql)
    -- The transformation logic is directly embedded here as a single script.
    -- DECLARE v_datum is handled within the transformation script itself.

    -- Execute the transformation SQL
    EXECUTE IMMEDIATE (
      SELECT
        '''
        -- Declare processing date variable
        DECLARE v_datum STRING DEFAULT (
          SELECT IFNULL(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), \'19000101\')
          FROM `your_project.isbert_schema.dwtk_meldungen` m
          WHERE m.job_kennung = \'BERT_DROP_TEMP_TABLE\'
        );

        -- Truncate target table
        TRUNCATE TABLE `your_project.your_dataset.sof_ta_cntrct_templ`;

        -- Insert data into target table
        INSERT INTO `your_project.your_dataset.sof_ta_cntrct_templ`
        (
          CNTRCT_TEMPLATE_ID,
          CDS_DESCRIPTION_ID,
          CDS_DESCRIPTION
        )
        SELECT
          ct.cntrct_template_id,
          ct.cds_description_id,
          cd.cds_description
        FROM `your_project.your_dataset.cds_ta_cntrct_template` ct
        JOIN `your_project.your_dataset.cds_ta_care_description` cd
          ON ct.cds_description_id = cd.cds_description_id
        WHERE ct.insert_at <= PARSE_DATE('%Y%m%d', v_datum)
          AND (ct.modified_at IS NULL OR ct.modified_at > PARSE_DATE('%Y%m%d', v_datum))
          AND ct.valid_from <= PARSE_DATE('%Y%m%d', v_datum)
          AND (ct.valid_to IS NULL OR ct.valid_to > PARSE_DATE('%Y%m%d', v_datum))
          AND ct.is_production = 1
          AND cd.language = 1;
        '''
    );
    SET v_records = @@row_count; -- Capture affected rows

    -- Log job result
    INSERT INTO `your_project.your_dataset.job_result` (job_id, end_time, records_processed, status)
    VALUES (job_id_val, CURRENT_TIMESTAMP(), v_records, 'SUCCESS');

    -- Log job status
    INSERT INTO `your_project.your_dataset.job_status` (job_id, status_time, status)
    VALUES (job_id_val, CURRENT_TIMESTAMP(), 'COMPLETED');

  EXCEPTION WHEN ERROR THEN
    SET v_status = 'ERROR';
    -- Log core transformation error
    INSERT INTO `your_project.your_dataset.job_error_log` (job_id, error_time, error_code, error_message, error_arg)
    VALUES (job_id_val, CURRENT_TIMESTAMP(), ERROR_CODE(), ERROR_MESSAGE(), 'Transformation failed');

    -- Log overall job status as FAILED
    INSERT INTO `your_project.your_dataset.job_status` (job_id, status_time, status)
    VALUES (job_id_val, CURRENT_TIMESTAMP(), 'FAILED');

    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = CONCAT('AppError: Abbruch - ', ERROR_MESSAGE());
  END;
END;