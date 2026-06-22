-- Legacy Source: Job status update functionality similar to DWMSG_SetzeStatusOK
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_optionen.ksh
--
-- Helper procedure to update the status in the BigQuery job_status table.
-- Replace `your_gcp_project.your_bq_dataset` with your actual GCP project ID and BigQuery dataset name.

CREATE OR REPLACE PROCEDURE `your_gcp_project.your_bq_dataset.f_alis_update_job_status`(
    IN p_job_run_id STRING,
    IN p_job_name STRING,
    IN p_status STRING,
    IN p_stichtag DATE,
    IN p_wiederanlaufwert INT64,
    IN p_start_timestamp TIMESTAMP DEFAULT NULL
)
BEGIN
    IF p_status = 'RUNNING' THEN
        INSERT INTO `your_gcp_project.your_bq_dataset.job_status` (
            job_run_id, job_name, start_timestamp, status, stichtag, wiederanlaufwert
        )
        VALUES (
            p_job_run_id, p_job_name, COALESCE(p_start_timestamp, CURRENT_TIMESTAMP()), p_status, p_stichtag, p_wiederanlaufwert
        );
    ELSE
        UPDATE `your_gcp_project.your_bq_dataset.job_status`
        SET
            end_timestamp = CURRENT_TIMESTAMP(),
            status = p_status
        WHERE job_run_id = p_job_run_id AND job_name = p_job_name;
    END IF;
END;