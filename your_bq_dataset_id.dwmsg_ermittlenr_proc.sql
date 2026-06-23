-- Target for legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount.ksh
-- Description: BigQuery stored procedure to generate a new job entry number and job identifier.
CREATE OR REPLACE PROCEDURE your_gcp_project_id.your_bq_dataset_id.dwmsg_ermittlenr_proc(
    OUT p_job_entry_nr INT64,
    OUT p_job_kennung STRING
)
BEGIN
    SET p_job_entry_nr = (SELECT COALESCE(MAX(job_entry_nr), 0) + 1 FROM your_gcp_project_id.your_bq_dataset_id.job_log);
    SET p_job_kennung = GENERATE_UUID();
END;