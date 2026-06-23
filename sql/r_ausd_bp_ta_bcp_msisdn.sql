-- Migrated from KornShell script: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_msisdn.ksh
-- Target BigQuery Stored Procedure for job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_msisdn.ksh

-- This stored procedure orchestrates the data processing for PoolBasisprodukt.
-- It handles parameter validation, date derivation, execution of core SQL logic,
-- record counting, and optional post-processing.

CREATE OR REPLACE PROCEDURE `my-gcp-project.my_dataset.r_ausd_bp_ta_bcp_msisdn`(
  p_JobKennung STRING,
  p_EintragsNr STRING,
  p_Stichtag STRING, -- Expected format 'DDMMYYYY'
  p_wiederanlaufWert STRING
)
BEGIN
  -- Declare variables
  DECLARE v_TabName STRING DEFAULT 'PoolBasisprodukt';
  DECLARE v_records INT64;
  DECLARE v_datum_heute DATE;
  DECLARE v_datum_gestern DATE;
  DECLARE v_stichtag_date DATE;
  DECLARE v_restart_value STRING;

  -- 1. Parameter Validation (replacing pruefeParameterGesetzt and DWDate_Datum_Check)
  IF p_JobKennung IS NULL OR p_JobKennung = '' THEN
    RAISE USING MESSAGE = 'FEHLER: JobKennung parameter is required.';
  END IF;

  IF p_EintragsNr IS NULL OR p_EintragsNr = '' THEN
    RAISE USING MESSAGE = 'FEHLER: EintragsNr parameter is required.';
  END IF;

  IF p_Stichtag IS NULL OR p_Stichtag = '' THEN
    RAISE USING MESSAGE = 'FEHLER: Stichtag parameter is required.';
  END IF;

  -- Validate Stichtag format 'DDMMYYYY'
  SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);
  IF v_stichtag_date IS NULL THEN
    RAISE USING MESSAGE = FORMAT('FEHLER: Stichtag "%s" is not in required DDMMYYYY format.', p_Stichtag);
  END IF;

  -- Initialize p_wiederanlaufWert if not set
  SET v_restart_value = COALESCE(p_wiederanlaufWert, '0');

  -- 2. Date Derivation (replacing gestern.ksh logic)
  SET v_datum_heute = CURRENT_DATE();
  SET v_datum_gestern = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

  -- Log parameters for debugging/auditing
  SELECT FORMAT('INFO: Parameters - JobKennung: %s, EintragsNr: %s, Stichtag: %s (%s), WiederanlaufWert: %s',
                p_JobKennung, p_EintragsNr, p_Stichtag, FORMAT_DATE('%Y-%m-%d', v_stichtag_date), v_restart_value) AS log_message;
  SELECT FORMAT('INFO: Derived Dates - Heute: %s, Gestern: %s',
                FORMAT_DATE('%Y-%m-%d', v_datum_heute), FORMAT_DATE('%Y-%m-%d', v_datum_gestern)) AS log_message;


  -- 3. Execute core SQL logic (migrated from d_ausd_bp_ta_bcp_msisdn.sql)
  -- This section was previously defined in d_ausd_bp_ta_bcp_msisdn_bq.sql and is now embedded here.

  -- Step01: Truncate the target table
  TRUNCATE TABLE `my-gcp-project.my_dataset.sof_ta_bcp_msisdn`;

  -- Step11c: Anreicherung der Daten mit den MSISDN des BCP-Vertrages.
  INSERT INTO `my-gcp-project.my_dataset.sof_ta_bcp_msisdn`
  (
    CNTRCT_ID,
    BPR_ID,
    CNTRCT_ID_REF,
    TN_TEL_MSISDN
  )
  SELECT
    DISTINCT
    bp.cntrct_id,
    bp.bpr_id,
    bp.cntrct_id_ref,
    rn.tn_tel_msisdn
  FROM
    `my-gcp-project.my_dataset.sof_ta_bpr_bcp` AS bp
  INNER JOIN
    `my-gcp-project.my_dataset.sof_ta_rn_vertrag` AS rn
  ON
    bp.cntrct_id_ref = rn.cntrct_id;

  -- 4. Record Counting
  -- The original ksh script read the record count from a temporary file.
  -- Here, we count all records inserted into the target table by the main SQL logic.
  SET v_records = (SELECT COUNT(*) FROM `my-gcp-project.my_dataset.sof_ta_bcp_msisdn`);

  SELECT FORMAT('INFO: Data processing completed. %d records processed for %s.', v_records, v_TabName) AS log_message;

  -- 5. Optional Post-processing (migrated from commented sed, sort, join commands)
  -- This section is enabled based on the design document's assumption of reactivation.

  -- For the post-processing section, the exact schema of the intermediate files (cibasis_data24.dat, etc.)
  -- is not fully defined in the design document or source.
  -- This implementation makes assumptions about column names (col1, col2_bp, col3_msisdn, col2_ref, col2_fax)
  -- and their derivation from `my-gcp-project.my_dataset.sof_ta_bcp_msisdn` based on the join keys
  -- and output fields of the original 'join' commands.
  -- In a real migration, this section would require precise mapping to source data fields.

  -- Create temporary tables for intermediate steps (sed and sort -u equivalents)
  CREATE TEMP TABLE tmp_cibasis_data24_dat AS
  SELECT DISTINCT
    REPLACE(CAST(CNTRCT_ID AS STRING), ' ', '') AS col1, -- Assuming CNTRCT_ID as primary join key
    REPLACE(CAST(BPR_ID AS STRING), ' ', '') AS col2_bp,
    REPLACE(CAST(TN_TEL_MSISDN AS STRING), ' ', '') AS col3_msisdn
  FROM
    `my-gcp-project.my_dataset.sof_ta_bcp_msisdn`
  ORDER BY
    col1;

  CREATE TEMP TABLE tmp_cibasis_data96_dat AS
  SELECT DISTINCT
    REPLACE(CAST(CNTRCT_ID AS STRING), ' ', '') AS col1, -- Assuming CNTRCT_ID as primary join key
    REPLACE(CAST(CNTRCT_ID_REF AS STRING), ' ', '') AS col2_ref -- Placeholder for another relevant column
  FROM
    `my-gcp-project.my_dataset.sof_ta_bcp_msisdn`
  ORDER BY
    col1;

  CREATE TEMP TABLE tmp_cibasis_fax_dat AS
  SELECT DISTINCT
    REPLACE(CAST(CNTRCT_ID AS STRING), ' ', '') AS col1, -- Assuming CNTRCT_ID as primary join key
    REPLACE(CAST(BPR_ID AS STRING), ' ', '') AS col2_fax -- Placeholder for a fax-related column
  FROM
    `my-gcp-project.my_dataset.sof_ta_bcp_msisdn`
  ORDER BY
    col1;

  -- Join 1: cibasis_data24.dat (file1) and cibasis_data96.dat (file2) to cibasis_24_96.tmp
  -- Original: join -j1 1 -j2 1 -o 2.1,1.2,2.2 -a 2 -t ';' cibasis_data24.dat cibasis_data96.dat > cibasis_24_96.tmp
  -- -a 2 means preserve unmatched from file2, which implies a RIGHT JOIN (file1 is left table, file2 is right table)
  CREATE TEMP TABLE tmp_cibasis_24_96_tmp AS
  SELECT
    t96.col1,        -- Output 2.1 (file2's col1)
    t24.col2_bp,     -- Output 1.2 (file1's col2)
    t96.col2_ref     -- Output 2.2 (file2's col2)
  FROM
    tmp_cibasis_data24_dat AS t24
  RIGHT JOIN
    tmp_cibasis_data96_dat AS t96
  ON
    t24.col1 = t96.col1;

  -- Join 2: cibasis_24_96.tmp (file1) and cibasis_fax.dat (file2) to cibasisprodukt.csv
  -- Original: join -j1 1 -j2 1 -o 1.1,1.2,1.3,2.2 -a 1 -t ';' cibasis_24_96.tmp cibasis_fax.dat > cibasisprodukt.csv
  -- -a 1 means preserve unmatched from file1, which implies a LEFT JOIN (file1 is left table, file2 is right table)
  CREATE OR REPLACE TABLE `my-gcp-project.my_dataset.cibasisprodukt_csv` AS -- Target table for final output
  SELECT
    t_24_96.col1,         -- Output 1.1 (file1's col1)
    t_24_96.col2_bp,      -- Output 1.2 (file1's col2)
    t_24_96.col3_msisdn,  -- Output 1.3 (file1's col3)
    tfax.col2_fax         -- Output 2.2 (file2's col2)
  FROM
    tmp_cibasis_24_96_tmp AS t_24_96
  LEFT JOIN
    tmp_cibasis_fax_dat AS tfax
  ON
    t_24_96.col1 = tfax.col1;

  SELECT 'INFO: Post-processing to cibasisprodukt_csv completed.' AS log_message;

  -- 6. Optional Job Tracking (migrated from commented FOSJobErzeugeEintrag)
  -- If re-activated, this would involve inserting into a job tracking table.
  -- Example:
  -- INSERT INTO `my-gcp-project.my_dataset.job_tracking_table`
  -- (job_id, table_name, status, stichtag_date, process_date, record_count, notes)
  -- VALUES (p_JobKennung, v_TabName, 'A', v_stichtag_date, CURRENT_DATE(), v_records, 'Initialbefuellung');
  -- SELECT 'INFO: Job tracking entry created (if enabled).' AS log_message;

END;