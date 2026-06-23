-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_barrier.ksh
-- Description: BigQuery Stored Procedure to determine a new unique job number.
-- This procedure mimics the `DWMSG_ErmittleNr` shell function.
CREATE OR REPLACE PROCEDURE `my_project_id.my_dataset_id.DWMSG_ErmittleNr_SP`(
    OUT p_dw_eintrags_nr INT64
)
BEGIN
    -- This is a simplified approach. In a real-world scenario, you might use a sequence table
    -- or a more robust job ID generation mechanism to ensure uniqueness and atomicity.
    SET p_dw_eintrags_nr = (SELECT IFNULL(MAX(job_nr), 0) + 1 FROM `my_project_id.my_dataset_id.job_log_table`);
END;