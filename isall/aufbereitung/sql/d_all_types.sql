-- ===================================================================
-- Datei:  d_all_types.sql
-- Datum:  28.08.2026
-- Autor:  DataStreak Discovery Engine Showcase
-- ===================================================================
--
-- Zweck:
--   Refresh der ALL_TYPES-Zwischentabelle aus der Rohdatentabelle,
--   Teil der Showcase-Kette (SQL-Schritt des ALL_TYPES_MASTER Jobs).
----------------------------------------------------------------------

DECLARE target_table STRING;
DECLARE source_table STRING;

SET target_table = CONCAT(@gcp_project, '.', @bq_dataset, '.sof$ta_all_types');
SET source_table = CONCAT(@gcp_project, '.', @bq_dataset, '.cds$ta_all_types_raw');

SELECT 'tabelle von vorherigem lauf loeschen' AS log_message;

BEGIN
  EXECUTE IMMEDIATE CONCAT('TRUNCATE TABLE `', target_table, '`');
EXCEPTION WHEN ERROR THEN
  SELECT 'Truncate failed or table not found; continuing execution.' AS log_message;
END;

BEGIN
  SELECT 'zieltabelle befuellen' AS log_message;

  EXECUTE IMMEDIATE CONCAT(
    'INSERT INTO `', target_table, '` (all_types_id, source_system, processed_at) ',
    'SELECT r.all_types_id, r.source_system, CURRENT_DATETIME() ',
    'FROM `', source_table, '` AS r ',
    'WHERE r.status = \'READY\''
  );

  SELECT 'Verarbeitung fehlerfrei beendet.' AS log_message;

EXCEPTION WHEN ERROR THEN
  RAISE USING message = "Process failed during INSERT operation.";
END;