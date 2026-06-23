-- Target for: Helper Stored Procedures
-- Legacy Source: N/A (utility function)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_assign.ksh

CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.DWMSG_ErmittleNr`(
  IN p_job_id STRING,
  OUT p_new_entry_nr INT64
)
BEGIN
  SET p_new_entry_nr = COALESCE((
    SELECT MAX(entry_nr) + 1
    FROM `your_project_id.your_dataset_id.job_table`
    WHERE job_id = p_job_id
  ), 1);
END;