-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/gestern.ksh

DECLARE Var_Nummer_Null INT64 DEFAULT 0;
DECLARE Var_Nummer_Heute_Tag INT64;
DECLARE Var_Nummer_Heute_Monat INT64;
DECLARE Var_Nummer_Heute_Jahr INT64;
DECLARE Var_Datum_Heute STRING;
DECLARE Var_Monat_Heute STRING;
DECLARE Var_Nummer_Gestern_Tag INT64;
DECLARE Var_Nummer_Gestern_Monat INT64;
DECLARE Var_Nummer_Gestern_Jahr INT64;
DECLARE Var_Datum_Gestern STRING;
DECLARE Var_Monat_Gestern STRING;

-- Datum ermitteln
SET Var_Nummer_Heute_Tag = EXTRACT(DAY FROM CURRENT_DATE());
SET Var_Nummer_Heute_Monat = EXTRACT(MONTH FROM CURRENT_DATE());
SET Var_Nummer_Heute_Jahr = EXTRACT(YEAR FROM CURRENT_DATE());

-- Vortag berechnen
IF Var_Nummer_Heute_Tag > 1 THEN
  SET Var_Nummer_Gestern_Tag = Var_Nummer_Heute_Tag - 1;
  SET Var_Nummer_Gestern_Monat = Var_Nummer_Heute_Monat;
  SET Var_Nummer_Gestern_Jahr = Var_Nummer_Heute_Jahr;
ELSEIF Var_Nummer_Heute_Tag = 1 THEN
  IF Var_Nummer_Heute_Monat > 1 THEN
    SET Var_Nummer_Gestern_Monat = Var_Nummer_Heute_Monat - 1;
    SET Var_Nummer_Gestern_Jahr = Var_Nummer_Heute_Jahr;

    CASE Var_Nummer_Gestern_Monat
      WHEN 1 THEN SET Var_Nummer_Gestern_Tag = 31;
      WHEN 2 THEN SET Var_Nummer_Gestern_Tag = 28;
      WHEN 3 THEN SET Var_Nummer_Gestern_Tag = 31;
      WHEN 5 THEN SET Var_Nummer_Gestern_Tag = 31;
      WHEN 7 THEN SET Var_Nummer_Gestern_Tag = 31;
      WHEN 8 THEN SET Var_Nummer_Gestern_Tag = 31;
      WHEN 10 THEN SET Var_Nummer_Gestern_Tag = 31;
      WHEN 12 THEN SET Var_Nummer_Gestern_Tag = 31;
      ELSE SET Var_Nummer_Gestern_Tag = 30;
    END CASE;

    IF MOD(Var_Nummer_Heute_Jahr, 4) = 0
       AND MOD(Var_Nummer_Heute_Jahr, 100) > 0
       AND Var_Nummer_Gestern_Monat = 2 THEN
      SET Var_Nummer_Gestern_Tag = 29;
    END IF;

  ELSE
    SET Var_Nummer_Gestern_Monat = 12;
    SET Var_Nummer_Gestern_Jahr = Var_Nummer_Heute_Jahr - 1;
    SET Var_Nummer_Gestern_Tag = 31;
  END IF;
ELSE
  SELECT 'Fehler !!!!' AS error_message;
END IF;

-- Datum formatieren
SET Var_Datum_Heute = CONCAT(
  CAST(Var_Nummer_Heute_Jahr AS STRING),
  LPAD(CAST(Var_Nummer_Heute_Monat AS STRING), 2, '0'),
  LPAD(CAST(Var_Nummer_Heute_Tag AS STRING), 2, '0')
);

SET Var_Monat_Heute = CONCAT(
  CAST(Var_Nummer_Heute_Jahr AS STRING),
  LPAD(CAST(Var_Nummer_Heute_Monat AS STRING), 2, '0')
);

SET Var_Datum_Gestern = CONCAT(
  CAST(Var_Nummer_Gestern_Jahr AS STRING),
  LPAD(CAST(Var_Nummer_Gestern_Monat AS STRING), 2, '0'),
  LPAD(CAST(Var_Nummer_Gestern_Tag AS STRING), 2, '0')
);

SET Var_Monat_Gestern = CONCAT(
  CAST(Var_Nummer_Gestern_Jahr AS STRING),
  LPAD(CAST(Var_Nummer_Gestern_Monat AS STRING), 2, '0')
);

-- Ausgabe
SELECT
  Var_Datum_Heute AS TodayDate,
  Var_Datum_Gestern AS YesterdayDate,
  Var_Monat_Heute AS TodayMonth,
  Var_Monat_Gestern AS YesterdayMonth;