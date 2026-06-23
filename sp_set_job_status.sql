-- BigQuery SQL Reusable helper procedure for status updates for job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_instance.ksh
CREATE OR REPLACE PROCEDURE `project.dataset.sp_set_job_status`(
  IN p_job_name STRING,
  IN p_job_nr INT64,
  IN p_status STRING
)
BEGIN
  MERGE `project.dataset.job_status` T
  USING (
    SELECT p_job_name AS job_name, p_job_nr AS job_nr, p_status AS status
  ) S
  ON T.job_name = S.job_name AND T.job_nr = S.job_nr
  WHEN MATCHED THEN
    UPDATE SET status = S.status, updated_at = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN
    INSERT (job_name, job_nr, status, updated_at)
    VALUES (S.job_name, S.job_nr, S.status, CURRENT_TIMESTAMP());
END;