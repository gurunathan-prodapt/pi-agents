-- Target for: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh
-- Description: BigQuery stored procedure to atomically retrieve and increment a job entry number.
-- Replaces functionality from f_alis_msgerr.ksh/DWMSG_ErmittleNr.
CREATE OR REPLACE PROCEDURE project.dataset.dwmsg_ermittlenr(
    IN p_job_kennung STRING,
    OUT p_entry_nr INT64
)
BEGIN
    -- Atomically get and increment the entry number for the given job_kennung
    MERGE INTO project.dataset.job_entry_sequence T
    USING (SELECT p_job_kennung AS job_kennung) S
    ON T.job_kennung = S.job_kennung
    WHEN MATCHED THEN
        UPDATE SET T.entry_nr = T.entry_nr + 1, T.last_update_timestamp = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (job_kennung, entry_nr, last_update_timestamp)
        VALUES (p_job_kennung, 1, CURRENT_TIMESTAMP());

    SELECT entry_nr
    INTO p_entry_nr
    FROM project.dataset.job_entry_sequence
    WHERE job_kennung = p_job_kennung;
END;