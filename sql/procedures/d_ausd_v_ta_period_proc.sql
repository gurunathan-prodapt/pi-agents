-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/d_ausd_v_ta_period.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_period.ksh

CREATE OR REPLACE PROCEDURE `project.dataset.d_ausd_v_ta_period` (
  IN p_job_kennung STRING DEFAULT 'BERT_DROP_TEMP_TABLE',
  IN p_target_table_name STRING DEFAULT 'sof_ta_period', -- Default to sof_ta_period for clarity
  IN p_carmen_project STRING,
  IN p_carmen_dataset STRING,
  IN p_as_of_date DATE DEFAULT NULL
)
BEGIN
  DECLARE v_as_of_date DATE;
  DECLARE v_sql STRING;
  DECLARE v_rows_inserted INT64 DEFAULT 0;

  -- Error handling
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    SELECT
      'ERROR' AS status,
      'Procedure `d_ausd_v_ta_period` failed while loading period data.' AS message,
      p_target_table_name AS target_table,
      v_as_of_date AS as_of_date;
  END;

  -- Determine cutoff date:
  -- 1) explicit parameter p_as_of_date
  -- 2) latest timestamp from control table for job_kennung
  -- 3) fallback to 1900-01-01
  SET v_as_of_date = COALESCE(
    p_as_of_date,
    (
      SELECT DATE(MAX(m.timecreated))
      FROM `project.isbert_schema.dwtk_meldungen` m
      WHERE m.job_kennung = p_job_kennung
    ),
    DATE '1900-01-01'
  );

  -- Truncate target table to mimic previous run cleanup (isbert_schema.DWPA_UTIL_SKRIPT.runstatement)
  SET v_sql = FORMAT('TRUNCATE TABLE `project.dataset.%s`', p_target_table_name);
  EXECUTE IMMEDIATE v_sql;

  -- Insert transformed data
  SET v_sql = FORMAT("""
    INSERT INTO `project.dataset.%s` (
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
      d.description AS einheit,
      p.insert_at AS bfc_age
    FROM `%s.%s.cds$ta_period` p
    JOIN `%s.%s.cds$ta_time_meas_cv` tm
      ON tm.time_meas_cv = p.time_meas_cv
    JOIN `%s.%s.cds$ta_description` d
      ON tm.description_id = d.description_id
    WHERE p.insert_at <= @as_of_date
      AND (p.modified_at IS NULL OR p.modified_at > @as_of_date)
  """,
    p_target_table_name,
    p_carmen_project, p_carmen_dataset,
    p_carmen_project, p_carmen_dataset,
    p_carmen_project, p_carmen_dataset
  );

  EXECUTE IMMEDIATE v_sql
  USING v_as_of_date AS as_of_date;

  SET v_rows_inserted = @@row_count;

  -- Success result for the caller (orchestration procedure)
  SELECT
    'OK' AS status,
    v_as_of_date AS as_of_date,
    p_target_table_name AS target_table,
    v_rows_inserted AS rows_inserted;
END;