--
-- Target code for DW.BERT_AUSD_BP_TA_TARIFOPTION
-- Legacy source: k_ausd_bp_ta_tarifoption.ksh
--
-- This BigQuery stored procedure encapsulates the logic of k_ausd_bp_ta_tarifoption.ksh.
-- It handles parameter validation, date derivations, and executes the core SQL transformation.

CREATE OR REPLACE PROCEDURE `project.dataset.sp_k_ausd_bp_ta_tarifoption`(
  IN p_jobkennung STRING,
  IN p_eintragsnr STRING,
  IN p_stichtag STRING, -- Expected format: DDMMYYYY
  IN p_wiederanlaufwert INT64
)
BEGIN
  DECLARE v_err_msg STRING;
  DECLARE v_stichtag_date DATE;
  DECLARE v_datum_heute DATE DEFAULT CURRENT_DATE();
  DECLARE v_datum_gestern DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_wiederanlaufwert_init INT64 DEFAULT IFNULL(p_wiederanlaufwert, 0);

  -- Log entry for start of procedure
  INSERT INTO `project.dataset.job_audit_log` (log_time, job_kennung, entry_nr, message_type, message, status)
  VALUES (CURRENT_TIMESTAMP(), p_jobkennung, CAST(p_eintragsnr AS INT64), 'INFO', 'Starting sp_k_ausd_bp_ta_tarifoption', 'RUNNING');

  BEGIN
    -- Parameter validation
    IF p_jobkennung IS NULL OR TRIM(p_jobkennung) = '' THEN
      SET v_err_msg = 'Jobkennung parameter is required.';
      RAISE USING MESSAGE v_err_msg;
    END IF;

    IF p_stichtag IS NULL OR TRIM(p_stichtag) = '' THEN
      SET v_err_msg = 'Stichtag parameter is required.';
      RAISE USING MESSAGE v_err_msg;
    END IF;

    IF p_eintragsnr IS NULL OR TRIM(p_eintragsnr) = '' THEN
      SET v_err_msg = 'EintragsNr parameter is required.';
      RAISE USING MESSAGE v_err_msg;
    END IF;

    -- Validate Stichtag format (DDMMYYYY)
    SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_stichtag);
    IF v_stichtag_date IS NULL THEN
      SET v_err_msg = FORMAT('Invalid Stichtag format or value: %s. Expected DDMMYYYY.', p_stichtag);
      RAISE USING MESSAGE v_err_msg;
    END IF;

    -- Log successful parameter validation
    INSERT INTO `project.dataset.job_audit_log` (log_time, job_kennung, entry_nr, message_type, message, status)
    VALUES (CURRENT_TIMESTAMP(), p_jobkennung, CAST(p_eintragsnr AS INT64), 'INFO', 'Parameters validated successfully.', 'RUNNING');

    -- Execute the core SQL script logic (d_ausd_bp_ta_tarifoption_bq.sql)
    -- This block is directly embedded from sql/d_ausd_bp_ta_tarifoption_bq.sql
    BEGIN
      DECLARE v_datum_sql STRING;
      DECLARE v_sql_dynamic STRING;

      -- Determine v_datum_sql based on the latest timecreated from dwtk_meldungen
      SET v_datum_sql = (
        SELECT
          COALESCE(MAX(FORMAT_DATE('%Y%m%d', CAST(m.timecreated AS DATE))), '19000101')
        FROM `project.dataset.dwtk_meldungen` AS m
        WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
      );

      -- Step01: Drop existing temporary tables for restartability
      EXECUTE IMMEDIATE FORMAT("""
      DROP TABLE IF EXISTS `project.dataset.sof_ta_bpr_opt_filter`;
      """);
      EXECUTE IMMEDIATE FORMAT("""
      DROP TABLE IF EXISTS `project.dataset.sof_ta_tarifoption`;
      """);

      -- Step9b: Create intermediate table sof_ta_bpr_opt_filter
      -- Uses dynamic table name for sof_ta_bpr_opt_text_<date>
      SET v_sql_dynamic = FORMAT("""
      CREATE OR REPLACE TABLE `project.dataset.sof_ta_bpr_opt_filter`
      AS
      SELECT
        t.bpr_id,
        t.cntrct_id,
        t.pds_description,
        l.opt_kategorie
      FROM `project.dataset.sof_ta_l_bpr_optionen_filter` AS l
      JOIN `project.dataset.sof_ta_bpr_opt_text_%s` AS t
        ON t.bpr_id = l.bpr_id;
      """, v_datum_sql);

      EXECUTE IMMEDIATE v_sql_dynamic;

      -- Step9c: Populate the final sof_ta_tarifoption table
      -- This step incorporates the custom UDFs and corrected LEAD function partitioning.
      CREATE OR REPLACE TABLE `project.dataset.sof_ta_tarifoption`
      AS
      SELECT
        cntrct_id,
        RTRIM(SUBSTR(LTRIM(pds_des1, ', '), 1, 500)) AS business_option,
        RTRIM(SUBSTR(LTRIM(pds_des2, ', '), 1, 500)) AS sonstige_option,
        RTRIM(SUBSTR(LTRIM(pds_des3, ', '), 1, 500)) AS gprs_option
      FROM (
        SELECT
          bpr_opt.cntrct_id,
          bpr_opt.bpr_id,
          -- Corrected LEAD function with deterministic ORDER BY
          LEAD(bpr_opt.cntrct_id, 1, -1) OVER (ORDER BY bpr_opt.cntrct_id, bpr_opt.pds_description) AS lagi,
          -- Use custom UDFs for concatenation logic
          CASE
            WHEN bpr_opt.opt_kategorie = 'BUDGET' THEN `project.dataset.concat1`(bpr_opt.pds_description, bpr_opt.cntrct_id)
            ELSE `project.dataset.concat1r`(bpr_opt.pds_description, bpr_opt.cntrct_id)
          END AS pds_des1,
          CASE
            WHEN bpr_opt.opt_kategorie = 'SONST' THEN `project.dataset.concat2`(bpr_opt.pds_description, bpr_opt.cntrct_id)
            ELSE `project.dataset.concat2r`(bpr_opt.pds_description, bpr_opt.cntrct_id)
          END AS pds_des2,
          CASE
            WHEN bpr_opt.opt_kategorie = 'GPRS' THEN `project.dataset.concat3`(bpr_opt.pds_description, bpr_opt.cntrct_id)
            ELSE `project.dataset.concat3r`(bpr_opt.pds_description, bpr_opt.cntrct_id)
          END AS pds_des3
        FROM (
          SELECT
            bpr_id,
            cntrct_id,
            pds_description,
            opt_kategorie
          FROM `project.dataset.sof_ta_bpr_opt_filter`
          ORDER BY cntrct_id, pds_description -- Explicit sort for LEAD function consistency
        ) AS bpr_opt
      )
      WHERE lagi > cntrct_id OR lagi = -1;

      -- Get count of records processed (written to sof_ta_tarifoption)
      SET v_records = (SELECT COUNT(*) FROM `project.dataset.sof_ta_tarifoption`);

    EXCEPTION WHEN ERROR THEN
      SET v_err_msg = FORMAT('Error during SQL script execution: %s', @@error.message);
      RAISE USING MESSAGE v_err_msg;
    END;

    -- Log successful completion
    INSERT INTO `project.dataset.job_audit_log` (log_time, job_kennung, entry_nr, message_type, message, stichtag, status)
    VALUES (CURRENT_TIMESTAMP(), p_jobkennung, CAST(p_eintragsnr AS INT64), 'INFO', FORMAT('SQL script executed successfully. %d records processed.', v_records), v_stichtag_date, 'SUCCESS');

  EXCEPTION WHEN ERROR THEN
    SET v_err_msg = FORMAT('Failed in sp_k_ausd_bp_ta_tarifoption: %s', @@error.message);
    INSERT INTO `project.dataset.job_audit_log` (log_time, job_kennung, entry_nr, message_type, message, stichtag, status)
    VALUES (CURRENT_TIMESTAMP(), p_jobkennung, CAST(p_eintragsnr AS INT64), 'ERROR', v_err_msg, v_stichtag_date, 'FAILED');
    RAISE USING MESSAGE v_err_msg;
  END;
END;