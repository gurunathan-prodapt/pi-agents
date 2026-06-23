-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh
-- Original function: konvertiereZeitspanne
-- Purpose: Calculates Anfangsdatum and Endedatum based on a numeric Zeitspanne and Kennzahl.
CREATE OR REPLACE PROCEDURE `konvertiereZeitspanne`(
    INOUT p_VarAnfang STRING,
    INOUT p_VarEnde STRING,
    IN p_Spanne STRING,
    IN p_Kennzahl STRING,
    OUT ErrNr INT64,
    OUT ErrArg STRING
)
BEGIN
  DECLARE v_offset_unit STRING;
  DECLARE v_spanne_int INT64;
  DECLARE v_current_date DATE;

  -- Initialize ErrNr and ErrArg if not already set
  IF ErrNr IS NULL THEN
    SET ErrNr = 0;
  END IF;

  IF ErrNr != 0 THEN
    RETURN;
  END IF;

  -- Pre-conditions: Spanne is numeric, Kennzahl is valid.
  -- The original script assumed these checks were done upstream.
  -- Here we'll perform basic validation for numeric `p_Spanne`.
  SET v_spanne_int = SAFE_CAST(p_Spanne AS INT64);
  IF v_spanne_int IS NULL THEN
    SET ErrNr = 195;
    SET ErrArg = CONCAT('Zeitspanne (', p_Spanne, ') ist kein gueltiger numerischer Wert.');
    RETURN;
  END IF;

  -- Determine offset unit
  IF p_Kennzahl = 'bst' THEN
    SET v_offset_unit = 'MONTH';
  ELSE
    SET v_offset_unit = 'DAY';
  END IF;

  SET v_current_date = CURRENT_DATE();

  -- Calculate dates. The original script used a negative span for past dates.
  -- DWDate_Gib_Zeitraum -$p_Spanne -> meaning DATE_SUB (current_date, INTERVAL $p_Spanne $Offset_Unit)
  SET p_VarAnfang = FORMAT_DATE('%Y%m%d', DATE_SUB(v_current_date, INTERVAL ABS(v_spanne_int) DAY)); -- Assuming it means subtracting from current date
  SET p_VarEnde = FORMAT_DATE('%Y%m%d', v_current_date); -- End date is usually current date for timespan backwards

  -- Re-evaluate logic for Kennzahl and Offset_Unit based on `DWDate_Gib_Zeitraum` behavior.
  -- The original ksh: DWDate_Gib_Zeitraum -$p_Spanne $Offset_Unit "YYYYMMDD" Anfangsdatum Endedatum
  -- This typically means "calculate a period ending today, going back $p_Spanne units"
  -- So, Endedatum = CURRENT_DATE(), Anfangsdatum = DATE_SUB(CURRENT_DATE(), INTERVAL $p_Spanne $Offset_Unit).

  IF v_offset_unit = 'MONTH' THEN
    SET p_VarAnfang = FORMAT_DATE('%Y%m%d', DATE_SUB(v_current_date, INTERVAL ABS(v_spanne_int) MONTH));
  ELSE -- Default to DAY
    SET p_VarAnfang = FORMAT_DATE('%Y%m%d', DATE_SUB(v_current_date, INTERVAL ABS(v_spanne_int) DAY));
  END IF;
  SET p_VarEnde = FORMAT_DATE('%Y%m%d', v_current_date);
END;