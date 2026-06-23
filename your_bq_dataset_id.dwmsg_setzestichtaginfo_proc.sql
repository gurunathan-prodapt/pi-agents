-- Target for legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount.ksh
-- Description: BigQuery stored procedure to initialize or update job status with stichtag information.
CREATE OR REPLACE PROCEDURE your_gcp_project_id.your_bq_dataset_id.dwmsg_setzestichtaginfo_proc(
    IN p_job_entry_nr INT64,
    IN p_job_kennung STRING,
    IN p_stichtag DATE,
    IN p_status_code STRING,
    IN p_status_text STRING
)
BEGIN
    MERGE your_gcp_project_id.your_bq_dataset_id.job_status AS T
    USING (SELECT p_job_entry_nr AS job_entry_nr, p_job_kennung AS job_kennung) AS S
    ON T.job_entry_nr = S.job_entry_nr AND T.job_kennung = S.job_kennung
    WHEN MATCHED THEN
        UPDATE SET stichtag = p_stichtag, status_code = p_status_code, status_text = p_status_text, last_updated = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (job_entry_nr, job_kennung, stichtag, status_code, status_text, last_updated)
        VALUES (p_job_entry_nr, p_job_kennung, p_stichtag, p_status_code, p_status_text, CURRENT_TIMESTAMP());
END;