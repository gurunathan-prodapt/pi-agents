SELECT 'variablendefinitionen' AS log_msg;

DECLARE v_datum STRING;

-- Step 1: Resolve reporting cut-off date
EXECUTE IMMEDIATE FORMAT("""
  SELECT 
    COALESCE(
      FORMAT_DATETIME('%%Y%%m%%d', MAX(m.timecreated)),
      '19000101'
    )
  FROM 
    `%s.%s.dwtk_meldungen` m
  WHERE 
    m.job_kennung = 'BERT_DROP_TEMP_TABLE'
""", @GCP_PROJECT, @BQ_DATASET) INTO v_datum;

SELECT 'tracing und settings' AS log_msg;

SELECT 'tabelle von vorherigem lauf loeschen' AS log_msg;

-- Step 2: Empty the target period table
EXECUTE IMMEDIATE FORMAT("""
  TRUNCATE TABLE `%s.%s.sof_ta_period`
""", @GCP_PROJECT, @BQ_DATASET);

SELECT 'zieltabelle anlegen: carmen-period-tabelle' AS log_msg;

-- Step 3: Populate Target Table
EXECUTE IMMEDIATE FORMAT("""
  INSERT INTO `%s.%s.sof_ta_period` (
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
    `%s.%s.cds_ta_period` p
  INNER JOIN
    `%s.%s.cds_ta_time_meas_cv` tm
    ON tm.time_meas_cv = p.time_meas_cv
  INNER JOIN
    `%s.%s.cds_ta_description` d
    ON tm.description_id = d.description_id
  WHERE
    p.insert_at <= PARSE_DATETIME('%%Y%%m%%d', ? )
    AND (
      p.modified_at IS NULL
      OR p.modified_at > PARSE_DATETIME('%%Y%%m%%d', ? )
    )
""", @GCP_PROJECT, @BQ_DATASET, @GCP_PROJECT, @CARMEN_STAGE_DATASET, @GCP_PROJECT, @CARMEN_STAGE_DATASET, @GCP_PROJECT, @CARMEN_STAGE_DATASET) USING v_datum, v_datum;

SELECT 'Verarbeitung fehlerfrei beendet.' AS log_msg;