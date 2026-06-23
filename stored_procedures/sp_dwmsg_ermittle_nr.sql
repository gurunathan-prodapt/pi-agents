-- Stored procedure for determining a new entry number (DW_EintragsNr)
-- Legacy Source: r_ausd_v_ta_inv_def.ksh (via DWMSG_ErmittleNr)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_def.ksh

CREATE OR REPLACE PROCEDURE `project_id.dataset_id.sp_dwmsg_ermittle_nr`(
  OUT p_entry_number INT64
)
BEGIN
  -- For simplicity, using UNIX_SECONDS as a pseudo-unique entry number.
  -- In a real scenario, this might involve a sequence table or a more robust UUID generation.
  SET p_entry_number = UNIX_SECONDS(CURRENT_TIMESTAMP());
END;