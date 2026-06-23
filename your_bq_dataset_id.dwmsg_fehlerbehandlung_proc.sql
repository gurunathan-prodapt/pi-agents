-- Target for legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount.ksh
-- Description: BigQuery stored procedure to handle errors by logging and updating job status.
CREATE OR REPLACE PROCEDURE your_gcp_project_id.your_bq_dataset_id.dwmsg_fehlerbehandlung_proc(
    IN p_job_entry_nr INT64,
    IN p_job_kennung STRING,
    IN p_script_name STRING,
    IN p_error_message STRING
)
BEGIN
    -- Log the error
    CALL your_gcp_project_id.your_bq_dataset_id.dwmsg_erzeugeeintrag_proc(
        p_job_entry_nr,
        p_job_kennung,
        p_script_name,
        'ERROR',
        CONCAT('Job failed: ', p_error_message)
    );

    -- Update job status to ERROR
    UPDATE your_gcp_project_id.your_bq_dataset_id.job_status
    SET
        status_code = 'ERROR',
        status_text = p_error_message,
        last_updated = CURRENT_TIMESTAMP()
    WHERE job_entry_nr = p_job_entry_nr AND job_kennung = p_job_kennung;

    -- Optionally, raise the error further
    RAISE USING MESSAGE CONCAT('Job ', p_job_kennung, ' failed with error: ', p_error_message);
END;