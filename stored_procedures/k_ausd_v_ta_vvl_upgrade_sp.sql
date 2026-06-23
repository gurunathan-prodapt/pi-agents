-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_upgrade.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_upgrade.ksh
-- Description: Migrated BigQuery Stored Procedure for k_ausd_v_ta_vvl_upgrade.ksh orchestration and d_ausd_v_ta_vvl_upgrade.sql logic.

CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.k_ausd_v_ta_vvl_upgrade_sp`(
  IN p_job_kennung STRING,
  IN p_eintrags_nr STRING
)
BEGIN
  DECLARE v_records_processed INT64;
  DECLARE v_stichtag_str STRING;
  DECLARE v_job_active BOOL;
  DECLARE v_error_message STRING DEFAULT '';

  -- Parameter validation
  IF p_job_kennung IS NULL OR TRIM(p_job_kennung) = '' THEN
    SET v_error_message = 'Parameter validation failed: Jobkennung is missing.';
    INSERT INTO `your_project_id.your_dataset_id.job_log` (log_time, job_id, entry_nr, log_level, message)
    VALUES (CURRENT_TIMESTAMP(), p_job_kennung, p_eintrags_nr, 'ERROR', v_error_message);
    RAISE USING MESSAGE = v_error_message;
  END IF;

  IF p_eintrags_nr IS NULL OR TRIM(p_eintrags_nr) = '' THEN
    SET v_error_message = 'Parameter validation failed: EintragsNr is missing.';
    INSERT INTO `your_project_id.your_dataset_id.job_log` (log_time, job_id, entry_nr, log_level, message)
    VALUES (CURRENT_TIMESTAMP(), p_job_kennung, p_eintrags_nr, 'ERROR', v_error_message);
    RAISE USING MESSAGE = v_error_message;
  END IF;

  -- Check if job is already active for the given p_job_kennung and p_eintrags_nr
  SELECT EXISTS(
    SELECT 1 FROM `your_project_id.your_dataset_id.job_status_table`
    WHERE job_id = p_job_kennung
      AND entry_nr = p_eintrags_nr
      AND status = 'ACTIVE'
  ) INTO v_job_active;

  IF v_job_active THEN
    SET v_error_message = FORMAT('Job (Jobkennung: %s, EintragsNr: %s) is already active. Ignoring this run.', p_job_kennung, p_eintrags_nr);
    INSERT INTO `your_project_id.your_dataset_id.job_log` (log_time, job_id, entry_nr, log_level, message)
    VALUES (CURRENT_TIMESTAMP(), p_job_kennung, p_eintrags_nr, 'WARN', v_error_message);
    -- Exit gracefully as per "aktive Jobs werden ignoriert"
    RETURN;
  END IF;

  -- Register job as active
  INSERT INTO `your_project_id.your_dataset_id.job_status_table` (job_id, entry_nr, status, start_time, records_processed)
  VALUES (p_job_kennung, p_eintrags_nr, 'ACTIVE', CURRENT_TIMESTAMP(), 0);

  BEGIN
    -- Get Stichtag (v_datum) from legacy dwtk_meldungen equivalent
    SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
    FROM `your_project_id.your_dataset_id.dwtk_meldungen` AS m
    WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    INTO v_stichtag_str;

    INSERT INTO `your_project_id.your_dataset_id.job_log` (log_time, job_id, entry_nr, log_level, message)
    VALUES (CURRENT_TIMESTAMP(), p_job_kennung, p_eintrags_nr, 'INFO', FORMAT('Stichtag determined: %s. Starting data processing.', v_stichtag_str));

    -- Truncate target table, replacing legacy PL/SQL call
    TRUNCATE TABLE `your_project_id.your_dataset_id.sof_ta_vvl_upgrade`;
    INSERT INTO `your_project_id.your_dataset_id.job_log` (log_time, job_id, entry_nr, log_level, message)
    VALUES (CURRENT_TIMESTAMP(), p_job_kennung, p_eintrags_nr, 'INFO', 'Truncated target table: sof_ta_vvl_upgrade.');

    -- Core data processing logic from d_ausd_v_ta_vvl_upgrade.sql
    INSERT INTO `your_project_id.your_dataset_id.sof_ta_vvl_upgrade`(
              vertrags_id,
              upgradegrund,
              upgradedatum)
       SELECT
              vvl.vertrags_id,
              CASE
                 WHEN ba.beschreibung = 'DPPS Diensttyp A13 (EG-Upgrade)'
                 THEN 'Endgeraeteupgrade'
                 ELSE ba.beschreibung
              END AS upgradegrund,
              vvl2.upgr_datum AS upgradedatum
       FROM   `your_project_id.your_dataset_id.sof_ta_vvl_dwh`       AS vvl,
              `your_project_id.your_dataset_id.dwh_ta_l_bindefr_aendgr_carm` AS ba,
              (SELECT
                       vertrags_id,
                       MAX(aenderung_am) AS upgr_datum
                 FROM `your_project_id.your_dataset_id.sof_ta_vvl_dwh` AS vvlt
               GROUP BY vertrags_id)  AS vvl2
       WHERE
              ba.vvl_aendgrund_id   = vvl.vvl_aendgrund_id
       AND    vvl.vertrags_id       = vvl2.vertrags_id
       AND    vvl.aenderung_am      = vvl2.upgr_datum;

    SET v_records_processed = @@row_count; -- Capture row count of the last DML statement

    -- Deactivate older active jobs for this p_job_kennung
    UPDATE `your_project_id.your_dataset_id.job_status_table`
    SET status = 'INACTIVE', end_time = CURRENT_TIMESTAMP()
    WHERE job_id = p_job_kennung
      AND entry_nr <> p_eintrags_nr
      AND status = 'ACTIVE';
    INSERT INTO `your_project_id.your_dataset_id.job_log` (log_time, job_id, entry_nr, log_level, message)
    VALUES (CURRENT_TIMESTAMP(), p_job_kennung, p_eintrags_nr, 'INFO', 'Deactivated older active jobs.');

    -- Update current job status to COMPLETED
    UPDATE `your_project_id.your_dataset_id.job_status_table`
    SET status = 'COMPLETED',
        end_time = CURRENT_TIMESTAMP(),
        records_processed = v_records_processed
    WHERE job_id = p_job_kennung
      AND entry_nr = p_eintrags_nr
      AND status = 'ACTIVE'; -- Only update if still active (not cancelled, etc.)

    INSERT INTO `your_project_id.your_dataset_id.job_log` (log_time, job_id, entry_nr, log_level, message)
    VALUES (CURRENT_TIMESTAMP(), p_job_kennung, p_eintrags_nr, 'INFO', FORMAT('Job completed successfully. Records processed: %d', v_records_processed));

  EXCEPTION WHEN ERROR THEN
    SET v_error_message = ERROR_MESSAGE();
    -- Log SQL execution error
    INSERT INTO `your_project_id.your_dataset_id.job_log` (log_time, job_id, entry_nr, log_level, message, error_details)
    VALUES (CURRENT_TIMESTAMP(), p_job_kennung, p_eintrags_nr, 'ERROR', 'SQL execution failed within k_ausd_v_ta_vvl_upgrade_sp.', v_error_message);

    -- Update job status to FAILED
    UPDATE `your_project_id.your_dataset_id.job_status_table`
    SET status = 'FAILED',
        end_time = CURRENT_TIMESTAMP(),
        error_message = v_error_message
    WHERE job_id = p_job_kennung
      AND entry_nr = p_eintrags_nr
      AND status = 'ACTIVE'; -- Only update if still active

    RAISE; -- Re-raise the caught exception
  END;
END;