-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh
-- Original function: konvertiereSDName
-- Purpose: Converts descriptive Stammdaten (SD) names to abbreviations.
CREATE OR REPLACE PROCEDURE `konvertiereSDName`(
    INOUT System STRING, -- Renamed from System to SD_Name for clarity, but keeping original for now
    OUT ErrNr INT64,
    OUT ErrArg STRING
)
BEGIN
  -- Initialize ErrNr and ErrArg if not already set
  IF ErrNr IS NULL THEN
    SET ErrNr = 0;
  END IF;

  IF ErrNr != 0 THEN
    RETURN;
  END IF;

  IF `is_empty`(System) THEN
    SET ErrNr = 196;
    SET ErrArg = 'h_alis_parameter V3.0.9 konvertiereSDSystem'; -- Original ksh had typo here: konvertiereSDSystem
    RETURN;
  END IF;

  SET System = `normalize_lower`(System);

  SET System = (
    SELECT
      CASE System
        WHEN 'vo' THEN 'vo' -- Original did nothing
        WHEN 'rahmenvertrag' THEN 'rv'
        WHEN 'tarif' THEN 'trf'
        WHEN 'tstatus' THEN 'ts'
        WHEN 'zahlmodus' THEN 'zm'
        WHEN 'kdg_grund' THEN 'kdg'
        WHEN 'gutschrift' THEN 'gut'
        WHEN 'aufladung' THEN 'auf'
        WHEN 'leistung' THEN 'l_leist'
        WHEN 'gutschrift_grund' THEN 'l_gutgr'
        WHEN 'sap_gutschrift_grund' THEN 'sap_l_gutgr'
        WHEN 'produkt' THEN 'l_prod'
        WHEN 'mahnverfahren_sapist' THEN 'l_mahnv_ist'
        WHEN 'mahnverfahren_sapfi' THEN 'l_mahnv_fi'
        WHEN 'mahnstufentyp_sapist' THEN 'l_mahnstyp_ist'
        WHEN 'bewegart' THEN 'bwa'
        ELSE NULL
      END
  );

  IF System IS NULL THEN
    SET ErrNr = 195;
    SET ErrArg = CONCAT('Unbekannte Stammdaten-Datenherkunft ', System, ' !');
    SET System = '???';
  END IF;
END;