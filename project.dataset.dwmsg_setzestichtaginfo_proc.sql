-- Target for: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh
-- Description: BigQuery stored procedure to record the 'Stichtag' (key date) for a job run.
-- Replaces functionality from f_alis_msgerr.ksh/DWMSG_SetzeStichtagInfo.
CREATE OR REPLACE PROCEDURE project.dataset.dwmsg_setzestichtaginfo(
    IN p_job_kennung STRING,
    IN p_entry_nr INT64,
    IN p_stichtag DATE
)
BEGIN
    INSERT INTO project.dataset.job_stichtag (job_kennung, entry_nr, log_timestamp, stichtag)
    VALUES (p_job_kennung, p_entry_nr, CURRENT_TIMESTAMP(), p_stichtag);
END;