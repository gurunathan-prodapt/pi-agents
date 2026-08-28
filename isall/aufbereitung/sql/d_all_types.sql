BEGIN
  -- Log progress of truncate step
  SELECT 'tabelle von vorherigem lauf loeschen' AS execution_step;

  -- Clear staging target table
  EXECUTE IMMEDIATE CONCAT('TRUNCATE TABLE `', @gcp_project, '.', @bq_dataset, '.sof$ta_all_types`');

  -- Log progress of insert step
  SELECT 'zieltabelle befuellen' AS execution_step;

  -- Populating the target staging table with qualified raw data
  EXECUTE IMMEDIATE CONCAT(
    'INSERT INTO `', @gcp_project, '.', @bq_dataset, '.sof$ta_all_types` (',
    '  all_types_id,',
    '  source_system,',
    '  processed_at',
    ') ',
    'SELECT ',
    '  r.all_types_id, ',
    '  r.source_system, ',
    '  CURRENT_DATETIME() ',
    'FROM ',
    '  `', @gcp_project, '.', @bq_dataset, '.cds$ta_all_types_raw` AS r ',
    'WHERE ',
    '  r.status = \'READY\''
  );

  -- Log successful completion
  SELECT 'Verarbeitung fehlerfrei beendet.' AS execution_status;

EXCEPTION WHEN ERROR THEN
  -- Implements equivalent of WHENEVER SQLERROR EXIT FAILURE
  SELECT 
    CONCAT(
      'Script Execution Failed. Error: ', @@error.message, 
      ' | Code: ', CAST(@@error.code AS STRING), 
      ' | Statement: ', @@error.statement_text
    ) AS error_diagnostic;
  
  -- Re-throw exception to guarantee the process fails the orchestrator run
  RAISE USING MESSAGE = CONCAT('ALL_TYPES_MASTER Job Step Failed: ', @@error.message);
END;