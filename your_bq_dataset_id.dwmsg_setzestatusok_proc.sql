-- Target for legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount.ksh
-- Description: BigQuery stored procedure to set job status to OK.
CREATE OR REPLACE PROCEDURE your_gcp_project_id.your_bq_dataset_id.dwmsg_setzestatusok_proc(
    IN p_job_entry_nr INT64,
    IN p_job_kennung STRING,
    IN p_script_name STRING
)
BEGIN
    -- Log the success
    CALL your_gcp_project_id.your_bq_dataset_id.dwmsg_erzeugeeintrag_proc(
        p_job_entry_nr,
        p_job_kennung,
        p_script_name,
        'INFO',
        'Job completed successfully.'
    );

    -- Update job status to OK
    UPDATE your_gcp_project_id.your_bq_dataset_id.job_status
    SET
        status_code = 'OK',
        status_text = 'Completed successfully',
        last_updated = CURRENT_TIMESTAMP()
    WHERE job_entry_nr = p_job_entry_nr AND job_kennung = p_job_kennung;
END;