-- Target for: BigQuery Core Transformation Stored Procedure
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_assign.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_assign.ksh

CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.k_ausd_v_ta_inv_assign`(
  p_job_id STRING,
  p_entry_nr INT64
)
BEGIN
  DECLARE v_datum_str STRING;
  DECLARE v_processed_rows INT64;

  BEGIN EXCEPTION WHEN ERROR THEN
    CALL `your_project_id.your_dataset_id.DWMSG_Fehlerbehandlung`(
      p_job_id,
      p_entry_nr,
      BQ.exception().error_code,
      BQ.exception().message,
      BQ.exception().stack_trace
    );
  END;

  CALL `your_project_id.your_dataset_id.DWMSG_ErzeugeEintrag`(
    p_job_id,
    p_entry_nr,
    'INFO',
    'Starting core transformation procedure k_ausd_v_ta_inv_assign.'
  );

  -- 1. Date Variable Determination (v_datum)
  SET v_datum_str = COALESCE((
    SELECT FORMAT_DATE('%Y%m%d', MAX(DATE(m.timecreated)))
    FROM `your_project_id.your_dataset_id.dwtk_meldungen` m
    WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
  ), '19000101');

  CALL `your_project_id.your_dataset_id.DWMSG_ErzeugeEintrag`(
    p_job_id,
    p_entry_nr,
    'INFO',
    CONCAT('Determined v_datum_str: ', v_datum_str)
  );

  -- 2. Truncate Target Table
  CALL `your_project_id.your_dataset_id.DWMSG_ErzeugeEintrag`(
    p_job_id,
    p_entry_nr,
    'INFO',
    'Truncating target table sof$ta_inv_assign.'
  );
  EXECUTE IMMEDIATE 'TRUNCATE TABLE `your_project_id.your_dataset_id.sof$ta_inv_assign`;';

  -- 3. Insert Data into Target Table
  CALL `your_project_id.your_dataset_id.DWMSG_ErzeugeEintrag`(
    p_job_id,
    p_entry_nr,
    'INFO',
    'Inserting data into sof$ta_inv_assign.'
  );

  INSERT INTO `your_project_id.your_dataset_id.sof$ta_inv_assign` (
    cntrct_id,
    inv_definition_id
  )
  SELECT
    ia.cntrct_id,
    ia.inv_definition_id
  FROM
    `your_project_id.your_dataset_id.cds$ta_inv_assignment` ia
  WHERE
    ia.insert_at <= PARSE_DATE('%Y%m%d', v_datum_str)
    AND (ia.modified_at IS NULL OR ia.modified_at > PARSE_DATE('%Y%m%d', v_datum_str))
    AND ia.valid_from <= PARSE_DATE('%Y%m%d', v_datum_str)
    AND (ia.valid_to IS NULL OR ia.valid_to > PARSE_DATE('%Y%m%d', v_datum_str))
    AND ia.is_production = 1;

  SET v_processed_rows = ROW_COUNT();

  CALL `your_project_id.your_dataset_id.DWMSG_ErzeugeEintrag`(
    p_job_id,
    p_entry_nr,
    'INFO',
    CONCAT('Data insert completed. Rows inserted: ', CAST(v_processed_rows AS STRING))
  );

  CALL `your_project_id.your_dataset_id.DWMSG_SetzeStatusOK`(p_job_id, p_entry_nr, v_processed_rows);

  CALL `your_project_id.your_dataset_id.DWMSG_ErzeugeEintrag`(
    p_job_id,
    p_entry_nr,
    'INFO',
    'Finished core transformation procedure k_ausd_v_ta_inv_assign.'
  );

END;