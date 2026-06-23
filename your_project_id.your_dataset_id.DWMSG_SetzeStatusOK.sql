-- Target for: Helper Stored Procedures
-- Legacy Source: N/A (status utility)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_assign.ksh

CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.DWMSG_SetzeStatusOK`(
  p_job_id STRING,
  p_entry_nr INT64,
  p_processed_rows INT64
)
BEGIN
  UPDATE `your_project_id.your_dataset_id.job_table`
  SET
    status = 'SUCCESS',
    end_time = CURRENT_TIMESTAMP(),
    processed_rows = p_processed_rows
  WHERE job_id = p_job_id AND entry_nr = p_entry_nr;

  CALL `your_project_id.your_dataset_id.DWMSG_ErzeugeEintrag`(
    p_job_id,
    p_entry_nr,
    'INFO',
    CONCAT('Job completed successfully. Processed rows: ', CAST(p_processed_rows AS STRING))
  );
END;