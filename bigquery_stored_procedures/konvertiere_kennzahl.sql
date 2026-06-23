-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_parameter.ksh
-- Original function: konvertiereKennzahl
-- Purpose: Converts descriptive Kennzahl names to canonical short codes.
CREATE OR REPLACE PROCEDURE `konvertiereKennzahl`(
    INOUT Kennzahl STRING,
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

  IF `is_empty`(Kennzahl) THEN
    SET ErrNr = 196;
    SET ErrArg = 'h_alis_parameter V3.0.9 konvertiereKennzahl';
    RETURN;
  END IF;

  SET Kennzahl = `normalize_lower`(Kennzahl);

  SET Kennzahl = (
    SELECT
      CASE Kennzahl
        WHEN 'zugang' THEN 'zug'
        WHEN 'abgang' THEN 'abg'
        WHEN 'abgang_zukunft' THEN 'abz'
        WHEN 'bestand' THEN 'bst'
        WHEN 'tarifwechsel' THEN 'twe'
        WHEN 'plan' THEN 'pln'
        WHEN 'gutschrift' THEN 'gut'
        WHEN 'aufladung' THEN 'auf'
        WHEN 'restguthaben' THEN 'rst'
        WHEN 'teilnehmerverbindungsdaten' THEN 'tvd'
        WHEN 'uskonto' THEN 'usk'
        WHEN 'usteilnehmer' THEN 'ust'
        WHEN 'leistungsklasse' THEN 'lkl'
        WHEN 'loeschung' THEN 'loe'
        WHEN 'reaktivierung' THEN 'rak'
        WHEN 'standard_rechnung' THEN 'srs'
        WHEN 'standard_gutschrift' THEN 'sgs'
        WHEN 'gutschrift_rv' THEN 'sg_rv'
        WHEN 'rechnungen_rv_dpps' THEN 'sr_rv_dpps'
        WHEN 'bewegart' THEN 'bwa'
        WHEN 'kundenstamm' THEN 'ksd'
        WHEN 'mahnstufe' THEN 'mahn'
        WHEN 'metadatenstruktur' THEN 'mds'
        WHEN 'd1news' THEN 'd1n'
        WHEN 'rubrik' THEN 'rub'
        WHEN 'liefermodus' THEN 'lmo'
        WHEN 'netznutzungsklassen' THEN 'nnk'
        WHEN 'tagesverkehrskurven' THEN 'tvk'
        WHEN 'gespraechsziele' THEN 'gz'
        WHEN 'gespraechslaengenverteilung' THEN 'glv'
        WHEN 'zonenkennung' THEN 'zonek'
        WHEN 'zonentyp' THEN 'zonet'
        WHEN 'netznutzungsklassentyp' THEN 'nnkt'
        WHEN 'tarifart' THEN 'trfa'
        WHEN 'gespraechstyp' THEN 'gtyp'
        WHEN 'basisdienst' THEN 'basisd'
        WHEN 'nationalinternational' THEN 'natint'
        WHEN 'glaengenintervall' THEN 'glint'
        ELSE NULL
      END
  );

  IF Kennzahl IS NULL THEN
    SET ErrNr = 198; -- Wert des Parameters unbekannt
    SET ErrArg = Kennzahl;
    SET Kennzahl = '???';
  END IF;
END;