-- BigQuery Stored Procedure for DWMSG_SetzeStichtagInfo
-- Utility for setting key date information, replacing logic from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vertrag_tmp.ksh
CREATE OR REPLACE PROCEDURE `my_project.my_utils_dataset.DWMSG_SetzeStichtagInfo`(
  IN p_stichtag_str STRING, -- e.g., 'YYYYMMDD'
  OUT p_stichtag_date DATE
)
BEGIN
  SET p_stichtag_date = PARSE_DATE('%Y%m%d', p_stichtag_str);
END;