-- Target for: Helper Stored Procedures
-- Legacy Source: N/A (date handling utility)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_assign.ksh

CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.DWMSG_SetzeStichtagInfo`(
  p_job_id STRING,
  p_entry_nr INT64,
  p_reporting_date DATE
)
BEGIN
  MERGE `your_project_id.your_dataset_id.job_table` AS T
  USING (SELECT p_job_id AS job_id, p_entry_nr AS entry_nr) AS S
  ON T.job_id = S.job_id AND T.entry_nr = S.entry_nr
  WHEN MATCHED THEN
    UPDATE SET reporting_date = p_reporting_date
  WHEN NOT MATCHED THEN
    INSERT (job_id, entry_nr, status, start_time, reporting_date)
    VALUES (p_job_id, p_entry_nr, 'RUNNING', CURRENT_TIMESTAMP(), p_reporting_date);

  CALL `your_project_id.your_dataset_id.DWMSG_ErzeugeEintrag`(
    p_job_id,
    p_entry_nr,
    'INFO',
    CONCAT('Reporting date set to: ', FORMAT_DATE('%Y-%m-%d', p_reporting_date))
  );
END;