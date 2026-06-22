-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_geschaeftspartner.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_geschaeftspartner.ksh

CREATE OR REPLACE PROCEDURE your_project.your_dataset_procs.k_ausd_geschaeftspartner_main(
  p_JobKennung STRING,
  p_EintragsNr STRING,
  p_Stichtag STRING,
  p_wiederanlaufWert INT64
)
OPTIONS(description="BigQuery Stored Procedure for orchestration of k_ausd_geschaeftspartner.ksh")
BEGIN
  -- Variable Declarations
  DECLARE v_stichtag_date DATE;
  DECLARE v_datum_heute DATE;
  DECLARE v_datum_gestern DATE;
  DECLARE v_records_processed INT64 DEFAULT 0;
  DECLARE current_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

  -- Log start of job
  INSERT INTO your_project.your_dataset_logging.job_log (timestamp, job_name, message, level, records_processed)
  VALUES (current_timestamp, 'k_ausd_geschaeftspartner', 'Job started.', 'INFO', NULL);

  -- Parameter Validation
  ASSERT p_JobKennung IS NOT NULL AND p_JobKennung != '', 'ERROR: JobKennung parameter is missing.';
  ASSERT p_EintragsNr IS NOT NULL AND p_EintragsNr != '', 'ERROR: EintragsNr parameter is missing.';
  ASSERT p_Stichtag IS NOT NULL AND p_Stichtag != '', 'ERROR: Stichtag parameter is missing.';

  -- Date Format Check (DDMMYYYY)
  SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);
  ASSERT v_stichtag_date IS NOT NULL, 'ERROR: Stichtag has invalid date format. Expected DDMMYYYY.';

  -- Initialize wiederanlaufWert if not provided
  IF p_wiederanlaufWert IS NULL THEN
    SET p_wiederanlaufWert = 0;
  END IF;

  -- Derive v_datum_heute and v_datum_gestern
  SET v_datum_heute = CURRENT_DATE();
  SET v_datum_gestern = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

  -- Log parameters and derived dates
  INSERT INTO your_project.your_dataset_logging.job_log (timestamp, job_name, message, level, records_processed)
  VALUES (current_timestamp, 'k_ausd_geschaeftspartner', CONCAT('Parameters - JobKennung: ', p_JobKennung, ', EintragsNr: ', p_EintragsNr, ', Stichtag: ', p_Stichtag, ', WiederanlaufWert: ', CAST(p_wiederanlaufWert AS STRING), ', Stichtag_DATE: ', FORMAT_DATE('%Y-%m-%d', v_stichtag_date), ', Heute: ', FORMAT_DATE('%Y-%m-%d', v_datum_heute), ', Gestern: ', FORMAT_DATE('%Y-%m-%d', v_datum_gestern)), 'DEBUG', NULL);

  -- Execute the core ETL stored procedure
  CALL your_project.your_dataset_procs.d_ausd_geschaeftspartner_proc(
    p_EintragsNr,
    p_JobKennung,
    v_stichtag_date,
    p_wiederanlaufWert,
    v_datum_heute,
    v_datum_gestern,
    v_records_processed
  );

  -- Log completion and record count
  INSERT INTO your_project.your_dataset_logging.job_log (timestamp, job_name, message, level, records_processed)
  VALUES (current_timestamp, 'k_ausd_geschaeftspartner', 'Job completed successfully.', 'INFO', v_records_processed);

EXCEPTION WHEN ERROR THEN
  -- Log error
  INSERT INTO your_project.your_dataset_logging.job_log (timestamp, job_name, message, level, records_processed)
  VALUES (current_timestamp, 'k_ausd_geschaeftspartner', CONCAT('Job failed: ', @@error.message), 'ERROR', NULL);
  RAISE; -- Re-raise the error to propagate it to the caller (Airflow)
END;