-- Migrated from legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_geschaeftspartner.ksh
-- For job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_geschaeftspartner.ksh

CREATE OR REPLACE PROCEDURE `your_project.your_dataset.r_ausd_vertrag_control_sp`(
  IN p_job_kennung STRING,
  IN p_eintrags_nr STRING,
  IN p_stichtag_ddmmyyyy STRING,
  IN p_wiederanlauf_wert INT64 -- Original ksh initialized to 0 if empty.
)
BEGIN

  -- Declare variables
  DECLARE v_stichtag_date DATE;
  DECLARE v_stichtag_yyyymmdd STRING;
  DECLARE v_today_date DATE;
  DECLARE v_yesterday_date DATE;
  DECLARE v_records_processed INT64;
  DECLARE v_table_name STRING DEFAULT 'PoolVertrag'; -- From original ksh: v_TabName='PoolVertrag'

  -- ========================= Parameter Validation ==================================
  -- Check for mandatory parameters as per original ksh
  IF p_job_kennung IS NULL OR TRIM(p_job_kennung) = '' THEN
    RAISE USING MESSAGE = 'Parameter p_job_kennung must be provided.';
  END IF;

  IF p_eintrags_nr IS NULL OR TRIM(p_eintrags_nr) = '' THEN
    RAISE USING MESSAGE = 'Parameter p_eintrags_nr must be provided.';
  END IF;

  IF p_stichtag_ddmmyyyy IS NULL OR TRIM(p_stichtag_ddmmyyyy) = '' THEN
    RAISE USING MESSAGE = 'Parameter p_stichtag_ddmmyyyy must be provided.';
  END IF;

  -- Validate date format (DDMMYYYY) for p_stichtag
  SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_stichtag_ddmmyyyy);
  IF v_stichtag_date IS NULL THEN
    RAISE USING MESSAGE = CONCAT('Invalid date format for p_stichtag_ddmmyyyy: ', p_stichtag_ddmmyyyy, '. Expected DDMMYYYY.');
  END IF;
  SET v_stichtag_yyyymmdd = FORMAT_DATE('%Y%m%d', v_stichtag_date);


  -- ========================= Date Derivation ==================================
  -- Original ksh calls 'gestern.ksh' for today and yesterday.
  -- In BigQuery, use native date functions.
  SET v_today_date = CURRENT_DATE();
  SET v_yesterday_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

  -- Log derived dates (optional, for debugging/tracing)
  -- SELECT CONCAT('Processing for Stichtag (DDMMYYYY): ', p_stichtag_ddmmyyyy, ', YYYYMMDD: ', v_stichtag_yyyymmdd, ', Today: ', FORMAT_DATE('%Y%m%d', v_today_date), ', Yesterday: ', FORMAT_DATE('%Y%m%d', v_yesterday_date));

  -- ========================= Execute Core Data Transformation Logic ==================================
  -- Call the migrated SQL procedure
  CALL `your_project.your_dataset.d_ausd_geschaeftspartner_proc`(
    v_stichtag_yyyymmdd,
    p_job_kennung,
    v_records_processed
  );

  -- ========================= Job Management: Record Entry in Job Tracking Table ==================================
  -- Original ksh had FOSJobErzeugeEintrag.
  -- This is replaced by an INSERT into the BigQuery job_tracking_table.

  INSERT INTO `your_project.your_dataset.job_tracking_table`
    (job_kennung, entry_nr, table_name, status_code_1, status_code_2, stichtag, records_processed, notes, created_at)
  VALUES
    (p_job_kennung, p_eintrags_nr, v_table_name, 'A', 'I', v_stichtag_date, v_records_processed, 'Initialbefuellung', CURRENT_TIMESTAMP());

  -- Deactivate old active jobs - This logic was commented out in the original ksh and is not implemented here.
  -- If needed, `FOSJobDeaktivate` logic would involve UPDATE statements on `job_tracking_table`.

END;