-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_iccid_einzeln.ksh
-- Description: BigQuery stored procedure for logging job execution details.

CREATE OR REPLACE PROCEDURE your_project_id.your_dataset_id.p_log_job_entry(
    IN p_job_kennung STRING,
    IN p_eintrags_nr INT64,
    IN p_tab_name STRING,
    IN p_stichtag DATE,
    IN p_records INT64,
    IN p_status STRING,
    IN p_message STRING
)
BEGIN
    INSERT INTO your_project_id.your_dataset_id.job_log (
        job_kennung,
        eintrags_nr,
        tab_name,
        stichtag,
        records,
        status,
        message
    )
    VALUES (
        p_job_kennung,
        p_eintrags_nr,
        p_tab_name,
        p_stichtag,
        p_records,
        p_status,
        p_message
    );
END;