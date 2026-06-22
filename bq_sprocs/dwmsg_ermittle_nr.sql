-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_c_bfc.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_c_bfc.ksh
CREATE OR REPLACE PROCEDURE `isrpt.dwmsg_ermittle_nr`(OUT p_eintrags_nr INT64)
BEGIN
  -- Logic to generate or fetch a unique entry number.
  -- This assumes dw_job_log stores all entries and a new entry number is sequentially assigned.
  SET p_eintrags_nr = (SELECT COALESCE(MAX(eintrags_nr), 0) + 1 FROM `isrpt.dw_job_log`);
END;