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

DECLARE v_eintrags_nr INT64;
DECLARE v_err_text STRING;
DECLARE v_err_code STRING;
DECLARE v_fehler_nr INT64;

BEGIN
  -- Set parameter value converted to numeric format
  SET v_eintrags_nr = SAFE_CAST(@p_eintrags_nr AS INT64);
  SET v_fehler_nr = -99999; -- placeholder representing dwpa_globals.k_alis_err_unknown

  -- Begin transaction block to ensure execution safety and rollback capabilities
  BEGIN TRANSACTION;

  -- ---------------------------------------------------------------------
  -- UPDATE 1: Update Level 6 VBA ID
  -- ---------------------------------------------------------------------
  UPDATE `dwh.dwh$ta_f_mps_nutzung` n 
     SET n.m2_vba_ebene6_id = COALESCE(
            (
               SELECT MIN(COALESCE(v.m2_vba_ebene7_id, n.m2_vba_ebene6_id))
                 FROM `dwh.dwh$vi_l_m2_vba` v
                WHERE UPPER(n.m2_vba_ebene6_text) = UPPER(v.m2_vba_ebene6_text)
            ),
            n.m2_vba_ebene6_id
         )
   WHERE n.m2_vba_ebene6_text IS NOT NULL;

  -- ---------------------------------------------------------------------
  -- UPDATE 2: Clear Level 6 texts for successfully mapped records
  -- ---------------------------------------------------------------------
  UPDATE `dwh.dwh$ta_f_mps_nutzung` n 
     SET n.m2_vba_ebene6_text = NULL
   WHERE n.m2_vba_ebene6_text IS NOT NULL
     AND n.m2_vba_ebene6_id <> (
        SELECT MIN(v.m2_vba_ebene7_id)
          FROM `dwh.dwh$vi_l_m2_vba` v
         WHERE UPPER(v.m2_vba_ebene6_text) = 'UNBEKANNT'
     );

  -- ---------------------------------------------------------------------
  -- UPDATE 3: Update Level 7 VBA ID
  -- ---------------------------------------------------------------------
  UPDATE `dwh.dwh$ta_f_mps_nutzung` n 
     SET n.m2_vba_ebene7_id = COALESCE(
            (
               SELECT MIN(v.m2_vba_ebene7_id)
                 FROM `dwh.dwh$vi_l_m2_vba` v
                WHERE UPPER(v.m2_vba_ebene7_text) = UPPER(n.m2_vba_ebene7_text)
            ),
            n.m2_vba_ebene7_id
         )
   WHERE n.m2_vba_ebene7_text IS NOT NULL;
   
  -- ---------------------------------------------------------------------
  -- UPDATE 4: Clear Level 7 texts for successfully mapped records
  -- ---------------------------------------------------------------------
  UPDATE `dwh.dwh$ta_f_mps_nutzung` n 
     SET n.m2_vba_ebene7_text = NULL
   WHERE n.m2_vba_ebene7_text IS NOT NULL
     AND n.m2_vba_ebene7_id <> (
        SELECT MIN(v.m2_vba_ebene7_id)
          FROM `dwh.dwh$vi_l_m2_vba` v
         WHERE UPPER(v.m2_vba_ebene7_text) = 'UNBEKANNT'
     );

  -- Commit changes only if all actions succeeded
  COMMIT TRANSACTION;

EXCEPTION WHEN ERROR THEN
  -- Rollback transaction block on any failures
  ROLLBACK TRANSACTION;

  -- Populate error details from context
  SET v_err_text = @@error.message;
  SET v_err_code = @@error.statement_text;

  -- Execute error logger call (Mocking external dependency procedure)
  CALL `dwh_utility.dwpa_meldung_fehler`('F', v_eintrags_nr, v_fehler_nr, v_err_text, v_err_code);

  -- Escalate and raise error context back to the orchestrator
  ERROR(v_err_text);
END;