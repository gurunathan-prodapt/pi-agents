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

SELECT 'tabelle von vorherigem lauf loeschen' AS log_message;

-- Emulating WHENEVER SQLERROR CONTINUE for the initial truncate step
BEGIN
  EXECUTE IMMEDIATE FORMAT("""
    TRUNCATE TABLE `%s.%s.sof$ta_all_types`
  """, @GCP_PROJECT, @BQ_DATASET);
EXCEPTION WHEN ERROR THEN
  -- Error is intentionally caught and swallowed, execution proceeds
  -- (Equivalent to WHENEVER SQLERROR CONTINUE)
  SELECT 'Truncate failed or table not found, proceeding anyway.' AS log_message;
END;

SELECT 'zieltabelle befuellen' AS log_message;

-- Emulating WHENEVER SQLERROR EXIT FAILURE with a transaction block
BEGIN
  BEGIN TRANSACTION;

  EXECUTE IMMEDIATE FORMAT("""
    INSERT INTO `%s.%s.sof$ta_all_types` (
      all_types_id,
      source_system,
      processed_at
    )
    SELECT
      r.all_types_id,
      r.source_system,
      CURRENT_DATETIME()
    FROM
      `%s.%s.cds$ta_all_types_raw` AS r
    WHERE
      r.status = 'READY'
  """, @GCP_PROJECT, @BQ_DATASET, @GCP_PROJECT, @BQ_DATASET);

  COMMIT TRANSACTION;

  SELECT 'Verarbeitung fehlerfrei beendet.' AS log_message;

EXCEPTION WHEN ERROR THEN
  ROLLBACK TRANSACTION;
  ERROR(FORMAT('Migration Transaction aborted with error: %s', @@error.message));
END;