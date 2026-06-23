-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_period.ksh
-- Target: BigQuery Stored Procedure for orchestrating data preparation.

CREATE OR REPLACE PROCEDURE `project.dataset.r_ausd_vertrag_control`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING
)
BEGIN
  DECLARE v_TabName STRING DEFAULT 'ta_period';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_err_nr INT64 DEFAULT 0;
  DECLARE v_err_arg STRING DEFAULT '';
  DECLARE v_sql_script_name STRING DEFAULT 'd_ausd_v_ta_period.sql';

  -- Parameter validation
  IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
    SET v_err_nr = 193;
    SET v_err_arg = 'Jobkennung';
  END IF;

  IF (p_EintragsNr IS NULL OR p_EintragsNr = '') AND v_err_nr = 0 THEN
    SET v_err_nr = 193;
    SET v_err_arg = 'EintragsNr';
  END IF;

  IF v_err_nr != 0 THEN
    RAISE USING MESSAGE = CONCAT('FEHLER: 0 E ', CAST(v_err_nr AS STRING), ' ', v_err_arg);
  END IF;

  -- Deactivate old active jobs for the same job key if required
  UPDATE `project.dataset.job_table`
  SET active_flag = FALSE,
      status = 'DEACTIVATED',
      updated_ts = CURRENT_TIMESTAMP()
  WHERE job_kennung = p_JobKennung
    AND active_flag = TRUE;

  -- Insert current job execution record
  INSERT INTO `project.dataset.job_table` (
    job_kennung,
    eintrags_nr,
    script_name,
    tab_name,
    status,
    active_flag,
    created_ts,
    updated_ts
  )
  VALUES (
    p_JobKennung,
    p_EintragsNr,
    v_sql_script_name,
    v_TabName,
    'RUNNING',
    TRUE,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
  );

  -- Begin block for SQL logic to capture ROW_COUNT properly and handle exceptions
  BEGIN
    -- Determine v_datum from dwtk_meldungen, equivalent to:
    -- SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') FROM isbert_schema.dwtk_meldungen m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';
    DECLARE v_datum_str STRING;
    SET v_datum_str = (
      SELECT
        IFNULL(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
      FROM
        `project.dataset.dwtk_meldungen` AS m
      WHERE
        m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    );

    DECLARE v_datum DATE;
    SET v_datum = PARSE_DATE('%Y%m%d', v_datum_str);

    -- Truncate target table `project.dataset.ta_period`, equivalent to:
    -- isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_period');
    TRUNCATE TABLE `project.dataset.ta_period`;

    -- Main INSERT logic translated from d_ausd_v_ta_period.sql
    INSERT INTO `project.dataset.ta_period`(
          period_id,
          number_time_measurement,
          time_meas_cv,
          einheit,
          bfc_age
    )
    SELECT
          p.period_id,
          p.number_time_measurement,
          p.time_meas_cv,
          d.description,
          p.insert_at
    FROM
          `project.dataset.cds_ta_period`            AS p
    JOIN  `project.dataset.cds_ta_time_meas_cv`      AS tm
      ON  tm.time_meas_cv   = p.time_meas_cv
    JOIN  `project.dataset.cds_ta_description`       AS d
      ON  tm.DESCRIPTION_ID = d.DESCRIPTION_ID
    WHERE
          p.insert_at <= v_datum
    AND     (   p.modified_at IS NULL
           OR p.modified_at > v_datum);

    SET v_records = @@row_count; -- Capture the number of inserted rows

  EXCEPTION WHEN ERROR THEN
    -- Update job status to FAILED if an error occurs during SQL execution
    UPDATE `project.dataset.job_table`
    SET status = 'FAILED',
        active_flag = FALSE,
        updated_ts = CURRENT_TIMESTAMP(),
        error_message = ERROR_MESSAGE()
    WHERE job_kennung = p_JobKennung
      AND eintrags_nr = p_EintragsNr
      AND script_name = v_sql_script_name
      AND active_flag = TRUE;
    RAISE; -- Re-raise the error to propagate it
  END;

  -- Persist result count and update job status
  UPDATE `project.dataset.job_table`
  SET record_count = v_records,
      status = 'DONE',
      active_flag = FALSE,
      updated_ts = CURRENT_TIMESTAMP()
  WHERE job_kennung = p_JobKennung
    AND eintrags_nr = p_EintragsNr
    AND script_name = v_sql_script_name
    AND active_flag = TRUE;

  -- Return the record count (optional, for external orchestration)
  SELECT v_records AS v_records;
END;