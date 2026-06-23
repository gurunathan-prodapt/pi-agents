-- Target for legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount.ksh
-- Description: BigQuery stored procedure to insert an entry into the job_log table.
CREATE OR REPLACE PROCEDURE your_gcp_project_id.your_bq_dataset_id.dwmsg_erzeugeeintrag_proc(
    IN p_job_entry_nr INT64,
    IN p_job_kennung STRING,
    IN p_script_name STRING,
    IN p_log_level STRING,
    IN p_message STRING
)
BEGIN
    INSERT INTO your_gcp_project_id.your_bq_dataset_id.job_log (
        job_entry_nr,
        job_kennung,
        script_name,
        log_timestamp,
        log_level,
        message
    )
    VALUES (
        p_job_entry_nr,
        p_job_kennung,
        p_script_name,
        CURRENT_TIMESTAMP(),
        p_log_level,
        p_message
    );
END;