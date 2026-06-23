-- Migrated from core logic of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_evn.ksh
-- Purpose: Encapsulates the main data transformation and loading logic.
CREATE OR REPLACE PROCEDURE `project.dataset.k_ausd_bp_ta_cntrct_evn`(
  IN p_jobkennung STRING,
  IN p_stichtag STRING,
  IN p_job_nr INT64,
  IN p_wiederanlaufWert INT64
)
BEGIN
  DECLARE v_stichtag_date DATE;
  DECLARE v_restart_value INT64 DEFAULT IFNULL(p_wiederanlaufWert, 0);

  -- Validate and normalize date
  CALL `project.dataset.sp_validate_stichtag`(p_stichtag, v_stichtag_date);

  -- Log start
  CALL `project.dataset.sp_log_job_event`(
    p_job_nr,
    p_jobkennung,
    'START',
    p_stichtag,
    v_restart_value,
    'Core procedure started'
  );

  BEGIN
    -- Restart handling: delete already processed records
    IF v_restart_value > 0 THEN
      DELETE FROM `project.dataset.fos_table`
      WHERE DWH_VERTRAG_ID >= v_restart_value;

      CALL `project.dataset.sp_log_job_event`(
        p_job_nr,
        p_jobkennung,
        'INFO',
        p_stichtag,
        v_restart_value,
        CONCAT('Deleted records from target where DWH_VERTRAG_ID >= ', CAST(v_restart_value AS STRING))
      );
    END IF;

    -- Insert filtered data
    INSERT INTO `project.dataset.fos_table`
    SELECT
      src.* -- Assuming target schema is compatible with source. Replace with explicit columns if needed.
    FROM `project.dataset.contract_cache` AS src
    WHERE DATE(src.Gueltig_von) <= v_stichtag_date
      AND v_stichtag_date < DATE(src.Gueltig_bis)
      AND DATE(src.LADEDATUM) < v_stichtag_date
      AND src.DWH_VERTRAG_ID > v_restart_value;

    CALL `project.dataset.sp_log_job_event`(
      p_job_nr,
      p_jobkennung,
      'SUCCESS',
      p_stichtag,
      v_restart_value,
      'Core procedure completed successfully'
    );

  EXCEPTION WHEN ERROR THEN
    CALL `project.dataset.sp_log_job_event`(
      p_job_nr,
      p_jobkennung,
      'ERROR',
      p_stichtag,
      v_restart_value,
      CONCAT('Core procedure failed: ', @@error.message)
    );
    SIGNAL SQLSTATE '45000'
      SET MESSAGE = CONCAT('Core procedure failed: ', @@error.message);
  END;
END;