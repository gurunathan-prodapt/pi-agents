/* ---------------------------------------------------------------------
-- Erstellt : 09.02.2004; Sascha Blumenthal
-- Parameter:
--   P1:    Fehlereintragnummer des aufrufenden Skriptes
--          
-- Zweck/Aufgabe:
--    Datensaetze in DWH$TA_F_MPS_NUTZUNG, fuer die zum Zeitpunkt des Imports
--    die Vertriebsart nicht ermittelt werden konnte, muessen nach der Aktualisierung
--    der VBA-Hierarchie bereinigt werden. D.h. die eingetragene Default-VBA-ID muss
--    an Hand des VBA-Textes aktualisiert werden
--
-- HISTORY
-- Sascha Blumenthal; 09.02.2004     
--                    Erstellung
-------------------------------------------------------------------------*/

DECLARE EintragsNr INT64;
DECLARE ErrText STRING;
DECLARE ErrC INT64;
DECLARE FehlerNr INT64;

-- Converted from EintragsNr := TO_NUMBER('&1');
-- Note: @P1 is passed as a script runtime execution parameter
SET EintragsNr = CAST(@P1 AS INT64);

BEGIN
  -- Start atomic transaction boundaries
  BEGIN TRANSACTION;

  -- Falls der Ebenen-6-Text in der VBA-Lookup enthalten ist, die entsprechende ID in die Fakten
  -- eintragen. Falls nicht, die Default-ID in den Fakten stehen lassen. Dies leistet das
  -- innere Select durch einen Join der Lookup- und der Faktentabelle
  MERGE INTO dwh$ta_f_mps_nutzung AS n
  USING (
    SELECT UPPER(m2_vba_ebene6_text) AS lookup_ebene6_text,
           MIN(m2_vba_ebene7_id) AS min_ebene7_id
      FROM dwh$vi_l_m2_vba
     GROUP BY 1
  ) AS v
  ON UPPER(n.m2_vba_ebene6_text) = v.lookup_ebene6_text
     AND n.m2_vba_ebene6_text IS NOT NULL
  WHEN MATCHED THEN
    UPDATE SET m2_vba_ebene6_id = COALESCE(v.min_ebene7_id, n.m2_vba_ebene6_id);

  -- Im Nachgang die Ebenen-6-Texte der Datensaetze entfernen, deren ID ermittelt werden 
  -- konnte 
  UPDATE dwh$ta_f_mps_nutzung AS n
     SET m2_vba_ebene6_text = NULL
   WHERE n.m2_vba_ebene6_text IS NOT NULL
     AND n.m2_vba_ebene6_id <> (
        SELECT v.m2_vba_ebene7_id
          FROM dwh$vi_l_m2_vba AS v
         WHERE UPPER(v.m2_vba_ebene6_text) = 'UNBEKANNT'
         LIMIT 1
     );

  -- Nun das gleiche fuer die Ebene-7: 
  -- Falls der Ebenen-7-Text in der VBA-Lookup enthalten ist, die entsprechende ID in die Fakten
  -- eintragen. Falls nicht, die Default-ID in den Fakten stehen lassen. Dies leistet das
  -- innere Select durch einen Join der Lookup- und der Faktentabelle
  MERGE INTO dwh$ta_f_mps_nutzung AS n
  USING (
    SELECT UPPER(m2_vba_ebene7_text) AS lookup_ebene7_text,
           MIN(m2_vba_ebene7_id) AS min_ebene7_id
      FROM dwh$vi_l_m2_vba
     GROUP BY 1
  ) AS v
  ON UPPER(n.m2_vba_ebene7_text) = v.lookup_ebene7_text
     AND n.m2_vba_ebene7_text IS NOT NULL
  WHEN MATCHED THEN
    UPDATE SET m2_vba_ebene7_id = COALESCE(v.min_ebene7_id, n.m2_vba_ebene7_id);
   
  -- Im Nachgang die Ebenen-7-Texte der Datensaetze entfernen, deren ID ermittelt werden 
  -- konnte 
  UPDATE dwh$ta_f_mps_nutzung AS n
     SET m2_vba_ebene7_text = NULL
   WHERE n.m2_vba_ebene7_text IS NOT NULL
     AND n.m2_vba_ebene7_id <> (
        SELECT v.m2_vba_ebene7_id
          FROM dwh$vi_l_m2_vba AS v
         WHERE UPPER(v.m2_vba_ebene7_text) = 'UNBEKANNT'
         LIMIT 1
     ); 

  -- Commit changes on safe execution
  COMMIT TRANSACTION;

EXCEPTION WHEN ERROR THEN
  -- Abort transaction changes on failure
  ROLLBACK TRANSACTION;

  -- Capture execution exception metadata
  SET ErrText = @@error.message;
  SET ErrC = CAST(@@error.code AS INT64);

  -- Assign hardcoded placeholder mapping for package constant dwpa_globals.k_alis_err_unknown
  SET FehlerNr = -20001;

  -- Call placeholder procedure handling targeted logging routines
  CALL dwpa_meldung_fehler('F', EintragsNr, FehlerNr, ErrText, CAST(ErrC AS STRING));

  -- Raise the caught exception to notify external calling scheduler/orchestrator
  ERROR ErrText;

END;