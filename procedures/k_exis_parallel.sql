-- Create the central logging table if it does not already exist
CREATE TABLE IF NOT EXISTS `dwh_operations.ccr_logs` (
  eintrags_nr STRING OPTIONS(description="Unique tracking or entry number passed by scheduling systems"),
  job_kennung STRING OPTIONS(description="Job identifier or name"),
  log_level STRING OPTIONS(description="Log severity classification (e.g., INFO, DEBUG, ERROR)"),
  message STRING OPTIONS(description="Detailed tracking or error message"),
  created_at TIMESTAMP OPTIONS(description="Timestamp of the log entry insertion")
);

-- Create autonomous helper logging procedure
CREATE OR REPLACE PROCEDURE `dwh_operations.write_ccr_log`(
  IN p_EintragsNr STRING,
  IN p_JobKennung STRING,
  IN p_LogLevel STRING,
  IN p_Message STRING
)
BEGIN
  -- Insert metadata into persistent operational logs
  INSERT INTO `dwh_operations.ccr_logs` (
    eintrags_nr, 
    job_kennung, 
    log_level, 
    message, 
    created_at
  )
  VALUES (
    p_EintragsNr, 
    p_JobKennung, 
    p_LogLevel, 
    p_Message, 
    CURRENT_TIMESTAMP()
  );
END;

-- Core Parallel Orchestrator & Exporter Procedure
CREATE OR REPLACE PROCEDURE `dwh_operations.k_exis_parallel`(
  IN p_EintragsNr STRING,
  IN p_JobKennung STRING,
  IN p_Skript_Table STRING,   -- Name of the target table to query (e.g. 'project.dataset.table')
  IN p_Routing_Col STRING,    -- Column name used as the routing key for hashing
  IN p_Ausgabe STRING,         -- Target Cloud Storage Base URI (e.g. 'gs://bucket/exports/file')
  IN p_Anfang STRING,          -- Processing window start timestamp (Format: YYYYMMDDHH24MISS)
  IN p_Ende STRING,            -- Processing window end timestamp (Format: YYYYMMDDHH24MISS)
  IN p_Debug INT64,            -- Debug Flag (1 = Enabled, 0 = Disabled)
  IN p_Anzahl INT64            -- Total number of parallel logical threads
)
BEGIN
  -- Variable Declarations
  DECLARE l_count INT64 DEFAULT 0;
  DECLARE v_DateiPar STRING;
  DECLARE v_Fehler_Count INT64 DEFAULT 0;
  DECLARE v_Sql_Query STRING;
  DECLARE v_Timestamp_Start TIMESTAMP;
  DECLARE v_Timestamp_End TIMESTAMP;

  -- Convert legacy format string timestamps to native BigQuery timestamps
  SET v_Timestamp_Start = PARSE_TIMESTAMP('%Y%m%d%H%M%S', p_Anfang);
  SET v_Timestamp_End = PARSE_TIMESTAMP('%Y%m%d%H%M%S', p_Ende);

  -- Create a transactional isolation-safe temporary table for thread execution tracking
  CREATE TEMP TABLE temp_fehler_log (
    thread_id INT64,
    status STRING,
    error_msg STRING
  );

  -- Start-of-run logging
  IF p_Debug = 1 THEN
    CALL `dwh_operations.write_ccr_log`(
      p_EintragsNr, 
      p_JobKennung, 
      'DEBUG',
      CONCAT('Initiating execution loop. Target Source: ', p_Skript_Table, ', Total threads defined: ', CAST(p_Anzahl AS STRING))
    );
  END IF;

  -- Sequential loop simulating parallel segmented tasks with built-in sandbox error isolation
  WHILE l_count < p_Anzahl DO
    -- Dynamically generate the output file URI per segment
    SET v_DateiPar = CONCAT(p_Ausgabe, '_thread_', CAST(l_count AS STRING));

    IF p_Debug = 1 THEN
      CALL `dwh_operations.write_ccr_log`(
        p_EintragsNr, 
        p_JobKennung, 
        'DEBUG',
        CONCAT('Spawning execution thread block ', CAST(l_count AS STRING), ' of ', CAST(p_Anzahl AS STRING))
      );
    END IF;

    -- Wrap block in EXCEPTION handler to simulate asynchronous failure capture
    BEGIN
      -- Create dynamic SQL query utilizing MOD mapping of FARM_FINGERPRINT hashes
      SET v_Sql_Query = """
        EXPORT DATA OPTIONS(
          uri=@output_destination_uri,
          format='CSV',
          overwrite=true,
          header=true,
          field_delimiter=';'
        ) AS
        SELECT * 
        FROM `""" || p_Skript_Table || """`
        WHERE processing_timestamp >= @start_time 
          AND processing_timestamp <= @end_time
          AND MOD(ABS(FARM_FINGERPRINT(CAST(""" || p_Routing_Col || """ AS STRING))), @total_threads) = @current_thread
      """;

      -- Execute Dynamic Safe Export Statement
      EXECUTE IMMEDIATE v_Sql_Query
      USING 
        CONCAT(v_DateiPar, '_*.csv') AS output_destination_uri,
        v_Timestamp_Start AS start_time, 
        v_Timestamp_End AS end_time, 
        p_Anzahl AS total_threads, 
        l_count AS current_thread;

    EXCEPTION WHEN ERROR THEN
      -- Log errors to isolation table instead of aborting the master loop
      INSERT INTO temp_fehler_log (thread_id, status, error_msg)
      VALUES (l_count, 'FEHLER', @@error.message);
      
      CALL `dwh_operations.write_ccr_log`(
        p_EintragsNr, 
        p_JobKennung, 
        'ERROR',
        CONCAT('Thread ', CAST(l_count AS STRING), ' encountered failure: ', @@error.message)
      );
    END;

    -- Increment partition index
    SET l_count = l_count + 1;
  END WHILE;

  -- Check for errors across processed segments
  SELECT COUNT(1) INTO v_Fehler_Count FROM temp_fehler_log WHERE status = 'FEHLER';

  IF p_Debug = 1 THEN
    CALL `dwh_operations.write_ccr_log`(
      p_EintragsNr, 
      p_JobKennung, 
      'DEBUG',
      CONCAT('Parallel loop iteration finished. Failed thread steps count: ', CAST(v_Fehler_Count AS STRING))
    );
  END IF;

  -- Clean up temporary resources
  DROP TABLE temp_fehler_log;

  -- Propagate errors to orchestration layers if sub-thread failures occurred
  IF v_Fehler_Count > 0 THEN
    ERROR 'Database Execution Error: One or more parallel export threads failed.';
  END IF;

  -- Success confirmation logging
  CALL `dwh_operations.write_ccr_log`(
    p_EintragsNr, 
    p_JobKennung, 
    'INFO',
    'Execution finished successfully. Segment exports have been structured onto GCS.'
  );

END;