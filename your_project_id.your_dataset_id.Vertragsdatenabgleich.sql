-- Target for: BigQuery Wrapper Stored Procedure
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_assign.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_assign.ksh

CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.Vertragsdatenabgleich`(
  p_job_id STRING, -- e.g., 'R_AUSD_V_TA_INV_ASSIGN'
  p_reporting_date DATE -- The 'Stichtag' for which the job is run
)
BEGIN
  DECLARE v_entry_nr INT64;

  -- Main error handling block for the wrapper procedure
  BEGIN EXCEPTION WHEN ERROR THEN
    CALL `your_project_id.your_dataset_id.DWMSG_Fehlerbehandlung`(
      p_job_id,
      v_entry_nr,
      BQ.exception().error_code,
      BQ.exception().message,
      BQ.exception().stack_trace
    );
  END;

  -- 1. Initialize logging and get new entry number
  CALL `your_project_id.your_dataset_id.DWMSG_ErmittleNr`(p_job_id, v_entry_nr);

  -- 2. Log job start and set reporting date info
  CALL `your_project_id.your_dataset_id.DWMSG_SetzeStichtagInfo`(p_job_id, v_entry_nr, p_reporting_date);
  CALL `your_project_id.your_dataset_id.DWMSG_ErzeugeEintrag`(
    p_job_id,
    v_entry_nr,
    'INFO',
    CONCAT('Wrapper procedure Vertragsdatenabgleich started for reporting date: ', FORMAT_DATE('%Y-%m-%d', p_reporting_date))
  );

  -- 3. Call the core transformation procedure
  CALL `your_project_id.your_dataset_id.k_ausd_v_ta_inv_assign`(p_job_id, v_entry_nr);

  CALL `your_project_id.your_dataset_id.DWMSG_ErzeugeEintrag`(
    p_job_id,
    v_entry_nr,
    'INFO',
    'Wrapper procedure Vertragsdatenabgleich finished.'
  );

END;