-- Helper procedure for job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_evn.ksh
-- Purpose: Validates DDMMYYYY input and returns a normalized DATE.
CREATE OR REPLACE PROCEDURE `project.dataset.sp_validate_stichtag`(
  IN p_stichtag STRING,
  OUT o_stichtag_date DATE
)
BEGIN
  DECLARE v_tmp_date DATE;

  IF p_stichtag IS NULL OR TRIM(p_stichtag) = '' THEN
    SET v_tmp_date = CURRENT_DATE();
  ELSE
    BEGIN
      SET v_tmp_date = PARSE_DATE('%d%m%Y', p_stichtag);
    EXCEPTION WHEN ERROR THEN
      SIGNAL SQLSTATE '45000'
        SET MESSAGE = CONCAT('Invalid stichtag format. Expected DDMMYYYY, got: ', p_stichtag);
    END;
  END IF;

  SET o_stichtag_date = v_tmp_date;
END;