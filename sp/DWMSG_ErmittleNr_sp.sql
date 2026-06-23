-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_disc_zusgf.ksh
-- Purpose: Utility SP to determine a unique job entry number.

CREATE OR REPLACE PROCEDURE `project_id.dataset_id.DWMSG_ErmittleNr_sp`(
  OUT p_job_nr INT64
)
BEGIN
  -- Generates a unique INT64 number using microseconds since epoch
  SET p_job_nr = UNIX_MICROS(CURRENT_TIMESTAMP());
END;