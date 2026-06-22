# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_iccid_einzeln.ksh

## 1. Purpose & Scope
This job, `k_ausd_bp_ta_iccid_einzeln.ksh`, serves as a control script for a data processing task. Its primary purpose is to prepare and validate input parameters (job identifier, entry number, key date, and an optional restart value), ensure correct date formatting, derive "today" and "yesterday" dates, and then orchestrate the execution of an underlying SQL script, `d_ausd_bp_ta_iccid_einzeln.sql`. The SQL script performs the core business logic, which involves reading from source tables `DWTK_MELDUNGEN` and `SOF$TA_BPR_BASIS` and writing processed data into the `SOF$TA_ICCID_EINZELN` table. The shell script also captures the record count from the SQL execution via a temporary file.

The scope of this migration is to re-implement this entire workflow in Google Cloud's BigQuery, translating the shell scripting logic into a BigQuery Stored Procedure or equivalent orchestration, and the Oracle SQL into BigQuery SQL.

## 2. Source Inventory
| File Name | Technology | Category | Tool | Summary | Complexity Tier | Migration Flags | Migration Bucket |
| :---------------------------------------------------------------------- | :--------- | :------- | :-------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | :-------------- | :-------------- | :--------------- |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_iccid_einzeln.ksh` | Shell      | shell    | KornShell | This KornShell script acts as a control script, preparing parameters, performing validation, and orchestrating the execution of an SQL script (d_ausd_bp_ta_iccid_einzeln.sql) for data processing. | medium          | `[]`            | semi_auto        |
| `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_iccid_einzeln.sql` | SQL        | (inferred) | (inferred) | Performs data extraction and transformation, reading from `DWTK_MELDUNGEN`, `SOF$TA_BPR_BASIS` and writing to `SOF$TA_ICCID_EINZELN`. | (inferred)      | (inferred)      | (inferred)       |

## 3. Target Architecture
The migrated solution will primarily reside in Google BigQuery.

*   **Orchestration**: The shell script's control flow, parameter handling, and date derivations will be re-implemented as a BigQuery Stored Procedure or an orchestrated job using Cloud Composer (Apache Airflow) or Google Cloud Workflows. The BigQuery Stored Procedure is preferred for closer logical integration.
*   **Data Processing**: The SQL logic from `d_ausd_bp_ta_iccid_einzeln.sql` will be converted to BigQuery SQL and embedded directly within the BigQuery Stored Procedure or executed as a scheduled query.
*   **Source Tables**: The current Oracle tables `DWTK_MELDUNGEN` and `SOF$TA_BPR_BASIS` will be migrated to BigQuery tables, likely under a `_RAW` or `_STG` dataset initially, then transformed into a curated dataset (`project.dataset.dwtk_meldungen`, `project.dataset.sof_ta_bpr_basis`).
*   **Target Table**: The current Oracle table `SOF$TA_ICCID_EINZELN` will be migrated to a BigQuery table (`project.dataset.sof_ta_iccid_einzeln`).
*   **Logging/Error Handling**: Error messages and run status (currently to console/temp file) will be redirected to a dedicated BigQuery logging table (`project.dataset.job_error_log`, `project.dataset.job_run_result`).
*   **Temporary Files**: The temporary file (`$DW_DIR_UTL/bert_k_ausd_bp_ta_iccid_einzeln.tmp`) used for record count will be replaced by direct BigQuery table operations or procedure output variables.

## 4. Data Flow & Lineage
The original data flow is:
1.  `k_ausd_bp_ta_iccid_einzeln.ksh` (Shell script) starts execution.
2.  Parameters `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert` are parsed.
3.  Environment scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`) are sourced for utility functions and date derivation.
4.  Parameter validation and date format checks are performed.
5.  The `starteSQLSkript` function (from `h_alis_sqlplus.ksh`) is called, executing `d_ausd_bp_ta_iccid_einzeln.sql`.
6.  `d_ausd_bp_ta_iccid_einzeln.sql` reads from `DWTK_MELDUNGEN` and `SOF$TA_BPR_BASIS`.
7.  `d_ausd_bp_ta_iccid_einzeln.sql` truncates and inserts data into `SOF$TA_ICCID_EINZELN`.
8.  `d_ausd_bp_ta_iccid_einzeln.sql` uses `PACKAGE:DWPA_UTIL_SKRIPT` for `runstatement`.
9.  The shell script then reads the result (record count) from `$DW_DIR_UTL/bert_k_ausd_bp_ta_iccid_einzeln.tmp`.

The migrated data flow in BigQuery will be:
1.  A BigQuery Stored Procedure, `project.dataset.d_ausd_bp_ta_iccid_einzeln_proc`, is invoked, accepting parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`).
2.  Within the stored procedure, parameter validation, date format checks, and date derivations will occur.
3.  The core SQL logic from `d_ausd_bp_ta_iccid_einzeln.sql` will be integrated, reading from BigQuery tables `project.dataset.dwtk_meldungen` and `project.dataset.sof_ta_bpr_basis`.
4.  The procedure will truncate and insert data into `project.dataset.sof_ta_iccid_einzeln`.
5.  Logging and error reporting will write to `project.dataset.job_error_log` and `project.dataset.job_run_result`.
6.  The record count will be obtained directly via `SELECT COUNT(*)` from the target table or the `INSERT` statement result.

## 5. Transformation Logic

**Shell Script (`k_ausd_bp_ta_iccid_einzeln.ksh`) Logic to BigQuery:**

*   **Parameter Parsing**: `getopts` will be replaced by explicit `IN` parameters in the BigQuery Stored Procedure.
*   **Parameter Validation**: Shell `if` conditions will be translated to BigQuery SQL `IF` statements and `ASSERT` clauses. Missing parameters or invalid date formats will trigger errors logged to `job_error_log`.
*   **Date Derivations**:
    *   `DWDate_Datum_Check $p_Stichtag 'DDMMYYYY'` will use `SAFE.PARSE_DATE('%d%m%Y', p_Stichtag)` to validate the date format.
    *   `gestern.ksh` (for `p_datum_heute`, `p_datum_gestern`) will be replaced by `CURRENT_DATE()` and `DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)` functions in BigQuery.
*   **External Script Calls**: Utilities like `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh` will be either:
    *   Replaced by native BigQuery SQL functions/statements.
    *   Integrated as part of the stored procedure's logic.
    *   Removed if their functionality is no longer needed in BigQuery.
*   **SQL Execution Orchestration**: The `starteSQLSkript` wrapper will be removed. The transformed BigQuery SQL for `d_ausd_bp_ta_iccid_einzeln.sql` will be directly embedded and executed within the BigQuery Stored Procedure.
*   **Temporary File Handling**: The usage of `$tmpFile` (read via `cat`) will be replaced by direct SQL operations (e.g., `SELECT COUNT(*) FROM ...`) or output variables of the BigQuery Stored Procedure.

**SQL Script (`d_ausd_bp_ta_iccid_einzeln.sql`) Logic to BigQuery SQL:**

*   **Oracle Specific Syntax**:
    *   `DEFINE v_carmen = "@pcrs1"`: If `@pcrs1` refers to a database link or specific Oracle connection, this needs to be re-evaluated. If it's a static value, it can be a constant in BigQuery.
    *   `COLUMN s_datum new_value v_datum noprint`: This is an SQL*Plus command for variable substitution. In BigQuery, `v_datum` will be a `DECLARE`d variable, set by `SELECT`ing the result directly.
    *   `NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101')`: `NVL` becomes `COALESCE`, `TO_CHAR` becomes `FORMAT_DATE('%Y%m%d', ...)`, `MAX(m.timecreated)` is standard.
    *   `WHENEVER SQLERROR CONTINUE/EXIT FAILURE`: Error handling will be implemented using BigQuery's `EXCEPTION` block in stored procedures or handled at the orchestration layer.
    *   `set timing on`: BigQuery console provides timing automatically.
    *   `TRUNCATE TABLE ... REUSE STORAGE`: Becomes `TRUNCATE TABLE project.dataset.table_name`.
    *   `isbert_schema.DWPA_UTIL_SKRIPT.runstatement`: This package call needs to be investigated. If it executes dynamic SQL, the dynamic SQL itself must be migrated. If it's a fixed operation, it will be translated.
    *   `INSERT INTO ... SELECT ...`: Standard SQL, will be directly translatable to BigQuery SQL, adjusting for any Oracle-specific functions.
    *   `TO_DATE('&v_datum','YYYYMMDD')`: Becomes `PARSE_DATE('%Y%m%d', v_datum_variable)` where `v_datum_variable` is a declared variable.
    *   `decode (bp.bpr_id*100 + bp.slave_number, 384801, bp.iccid, null)`: Oracle's `DECODE` function will be converted to BigQuery's `CASE` statement.
    *   `COMMIT;`: BigQuery operations are typically atomic per statement; explicit `COMMIT` is not used in this context.
    *   `spool`: Used for outputting SQL results to a file. This output will be either logged directly to BigQuery logging tables or returned as a result set from the stored procedure.
    *   `exit success`: Handled by successful completion of the BigQuery Stored Procedure.
*   **Input Table Conversion**: `isbert_schema.dwtk_meldungen` becomes `project.dataset.dwtk_meldungen`. `sof$ta_bpr_basis` becomes `project.dataset.sof_ta_bpr_basis`.
*   **Output Table Conversion**: `sof$ta_iccid_einzeln` becomes `project.dataset.sof_ta_iccid_einzeln`.

## 6. External Dependencies
*   **Oracle Database**: Both source tables (`DWTK_MELDUNGEN`, `SOF$TA_BPR_BASIS`) and the target table (`SOF$TA_ICCID_EINZELN`) currently reside in an Oracle database (inferred from SQL syntax and naming conventions like `ISBERT_SCHEMA`). These will be migrated to BigQuery tables.
*   **Shell Utilities/Environment**:
    *   `$HOME/.dw_init`, `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`: These custom shell scripts and environment settings provide functionality like error handling, date checking, parameter parsing, SQL execution wrappers, and date derivation. They will be replaced by native BigQuery SQL scripting constructs, standard BigQuery functions, or dedicated Cloud Functions/Composer tasks if complex logic cannot be directly embedded.
    *   `PACKAGE:DWPA_UTIL_SKRIPT`: This Oracle package needs to be analyzed. Its functions will either be reimplemented as BigQuery UDFs or part of the stored procedure, or replaced by equivalent BigQuery features.

## 7. Unresolved / Risks
*   **Dynamic SQL in `DWPA_UTIL_SKRIPT`**: The exact functionality of `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` needs clarification. If it executes complex dynamic SQL, each potential dynamic statement needs to be identified and migrated.
*   **`v_TabName` and `p_wiederanlaufWert` Usage**: The shell script sets `v_TabName='PoolBasisprodukt'` and `p_wiederanlaufWert`, but their usage is not visible within the provided shell script logic. It is assumed they are consumed by the `starteSQLSkript` function or the SQL script. This needs to be confirmed and their usage in BigQuery replicated, potentially as arguments to the main BigQuery Stored Procedure.
*   **`gestern.ksh` Logic**: While `CURRENT_DATE()` and `DATE_SUB` can replace simple "yesterday" logic, if `gestern.ksh` involves more complex business rules for date derivation, those rules need to be extracted and implemented in BigQuery.
*   **Trace/Spool Files**: The `start ../trace.sql.cfg` and `spool` commands in the SQL script indicate logging to files. This will be replaced by BigQuery native logging mechanisms (Cloud Logging) and possibly BigQuery logging tables.
*   **Schema Mapping**: Detailed schema mapping from Oracle (`DWTK_MELDUNGEN`, `SOF$TA_BPR_BASIS`, `SOF$TA_ICCID_EINZELN`) to BigQuery table definitions is required. Data types and column names may need adjustment.
*   **No Direct Linkage in Lineage**: The absence of a direct `INVOKES` edge from the `.ksh` to the `.sql` file in `lineage_edges` indicates that the analysis tool didn't capture this dynamic invocation. The design must account for this by manually linking these components based on the code analysis.

## 8. Build Plan

The migration will be implemented as a BigQuery Stored Procedure with associated DDL for the tables.

1.  **DDL for Target Tables**:
    *   Create BigQuery table `project.dataset.dwtk_meldungen` (mirroring Oracle `DWTK_MELDUNGEN`).
    *   Create BigQuery table `project.dataset.sof_ta_bpr_basis` (mirroring Oracle `SOF$TA_BPR_BASIS`).
    *   Create BigQuery table `project.dataset.sof_ta_iccid_einzeln` (mirroring Oracle `SOF$TA_ICCID_EINZELN`).
    *   Create BigQuery table `project.dataset.job_error_log` for error messages (`job_kennung`, `eintragsnr`, `stichtag`, `err_nr`, `err_arg`, `created_at`).
    *   Create BigQuery table `project.dataset.job_run_result` for run metadata and record counts (`job_kennung`, `eintragsnr`, `stichtag`, `tab_name`, `datum_heute`, `datum_gestern`, `restart_value`, `record_count`, `created_at`).

2.  **SQL for BigQuery Stored Procedure**:
    *   Translate the combined logic of `k_ausd_bp_ta_iccid_einzeln.ksh` and `d_ausd_bp_ta_iccid_einzeln.sql` into a single BigQuery Stored Procedure: `project.dataset.d_ausd_bp_ta_iccid_einzeln_proc`.
    *   The procedure will accept parameters `p_JobKennung STRING`, `p_EintragsNr STRING`, `p_Stichtag STRING`, `p_wiederanlaufWert INT64`.
    *   Implement parameter validation and date checks using `IF` and `SAFE.PARSE_DATE`.
    *   Integrate the Oracle SQL `INSERT ... SELECT ...` statement, converting `DECODE` to `CASE` and other Oracle-specific functions to BigQuery equivalents.
    *   Replace `TRUNCATE TABLE` with BigQuery DDL/DML.
    *   Replace `v_datum` substitution with a `DECLARE`d variable.
    *   Implement logging to `project.dataset.job_error_log` and `project.dataset.job_run_result`.
    *   The `DWPA_UTIL_SKRIPT.runstatement` call needs to be replaced with its equivalent BigQuery SQL if possible, or noted for manual re-implementation if complex.

3.  **Orchestration (Optional but Recommended)**:
    *   If using Cloud Composer, create an Airflow DAG to call the BigQuery Stored Procedure, handling parameters and scheduling.

4.  **Unit and Integration Tests**:
    *   Develop test cases to verify parameter validation, date logic, and the correctness of data transformations and inserts.

**Example Build Output (BigQuery SQL for Stored Procedure):**

```sql
-- BigQuery Stored Procedure for k_ausd_bp_ta_iccid_einzeln.ksh and d_ausd_bp_ta_iccid_einzeln.sql
-- Language: BQSQL

CREATE OR REPLACE PROCEDURE `project.dataset.d_ausd_bp_ta_iccid_einzeln_proc`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING,
  IN p_Stichtag STRING,
  IN p_wiederanlaufWert INT64
)
BEGIN
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';
  DECLARE v_TabName STRING DEFAULT 'PoolBasisprodukt'; -- Unused in migrated logic, keep for reference
  DECLARE v_datum_heute DATE;
  DECLARE v_datum_gestern DATE;
  DECLARE v_records STRING DEFAULT '';
  DECLARE v_stichtag_date DATE;
  DECLARE v_max_timecreated_datum STRING;

  -- Parameter validation
  IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
    SET ErrNr = 193; SET ErrArg = 'Jobkennung';
  END IF;

  IF ErrNr = 0 AND (p_Stichtag IS NULL OR TRIM(p_Stichtag) = '') THEN
    SET ErrNr = 193; SET ErrArg = 'Stichtag';
  END IF;

  IF ErrNr = 0 AND (p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '') THEN
    SET ErrNr = 193; SET ErrArg = 'EintragsNr';
  END IF;

  IF ErrNr != 0 THEN
    INSERT INTO `project.dataset.job_error_log`
    (job_kennung, eintragsnr, stichtag, err_nr, err_arg, created_at)
    VALUES
    (p_JobKennung, p_EintragsNr, p_Stichtag, ErrNr, ErrArg, CURRENT_TIMESTAMP());
    SELECT FORMAT('FEHLER: 0 E %d %s', ErrNr, ErrArg) AS message;
    RETURN; -- Exit procedure
  END IF;

  -- Date validation for DDMMYYYY
  SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);

  IF v_stichtag_date IS NULL THEN
    SET ErrNr = 193; SET ErrArg = 'Stichtag ungültig - Format DDMMYYYY erwartet';
    INSERT INTO `project.dataset.job_error_log`
    (job_kennung, eintragsnr, stichtag, err_nr, err_arg, created_at)
    VALUES
    (p_JobKennung, p_EintragsNr, p_Stichtag, ErrNr, ErrArg, CURRENT_TIMESTAMP());
    SELECT FORMAT('FEHLER: 0 E %d %s', ErrNr, ErrArg) AS message;
    RETURN; -- Exit procedure
  END IF;

  -- Derive today and yesterday dates
  SET v_datum_heute = CURRENT_DATE();
  SET v_datum_gestern = DATE_SUB(v_datum_heute, INTERVAL 1 DAY);

  -- Determine v_max_timecreated_datum (equivalent to Oracle's v_datum from dwtk_meldungen)
  SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
  INTO v_max_timecreated_datum
  FROM `project.dataset.dwtk_meldungen` AS m
  WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';

  -- Default restart value
  IF p_wiederanlaufWert IS NULL THEN
    SET p_wiederanlaufWert = 0;
  END IF;

  -- Truncate target table
  TRUNCATE TABLE `project.dataset.sof_ta_iccid_einzeln`;

  -- Core logic: INSERT into target table
  INSERT INTO `project.dataset.sof_ta_iccid_einzeln`
  ( CNTRCT_ID,
    TN_ICCID, TN_IMSI_MCC, TN_IMSI_MNC, TN_IMSI_HLR, TN_IMSI_SI, TN_STATUS, TN_VALID_TO, TN_E_ID, TN_CARD_TYPE_NAME,
    TC_ICCID, TC_IMSI_MCC, TC_IMSI_MNC, TC_IMSI_HLR, TC_IMSI_SI, TC_STATUS, TC_VALID_TO, TC_E_ID, TC_CARD_TYPE_NAME,
    TB_ICCID, TB_IMSI_MCC, TB_IMSI_MNC, TB_IMSI_HLR, TB_IMSI_SI, TB_STATUS, TB_VALID_TO, TB_E_ID, TB_CARD_TYPE_NAME,
    MS1_ICCID, MS1_IMSI_MCC, MS1_IMSI_MNC, MS1_IMSI_HLR, MS1_IMSI_SI, MS1_STATUS, MS1_VALID_TO, MS1_E_ID, MS1_CARD_TYPE_NAME,
    MS2_ICCID, MS2_IMSI_MCC, MS2_IMSI_MNC, MS2_IMSI_HLR, MS2_IMSI_SI, MS2_STATUS, MS2_VALID_TO, MS2_E_ID, MS2_CARD_TYPE_NAME,
    MS3_ICCID, MS3_IMSI_MCC, MS3_IMSI_MNC, MS3_IMSI_HLR, MS3_IMSI_SI, MS3_STATUS, MS3_VALID_TO, MS3_E_ID, MS3_CARD_TYPE_NAME,
    MS4_ICCID, MS4_IMSI_MCC, MS4_IMSI_MNC, MS4_IMSI_HLR, MS4_IMSI_SI, MS4_STATUS, MS4_VALID_TO, MS4_E_ID, MS4_CARD_TYPE_NAME,
    MS5_ICCID, MS5_IMSI_MCC, MS5_IMSI_MNC, MS5_IMSI_HLR, MS5_IMSI_SI, MS5_STATUS, MS5_VALID_TO, MS5_E_ID, MS5_CARD_TYPE_NAME,
    MS6_ICCID, MS6_IMSI_MCC, MS6_IMSI_MNC, MS6_IMSI_HLR, MS6_IMSI_SI, MS6_STATUS, MS6_VALID_TO, MS6_E_ID, MS6_CARD_TYPE_NAME,
    MS7_ICCID, MS7_IMSI_MCC, MS7_IMSI_MNC, MS7_IMSI_HLR, MS7_IMSI_SI, MS7_STATUS, MS7_VALID_TO, MS7_E_ID, MS7_CARD_TYPE_NAME,
    MS8_ICCID, MS8_IMSI_MCC, MS8_IMSI_MNC, MS8_IMSI_HLR, MS8_IMSI_SI, MS8_STATUS, MS8_VALID_TO, MS8_E_ID, MS8_CARD_TYPE_NAME,
    MS9_ICCID, MS9_IMSI_MCC, MS9_IMSI_MNC, MS9_IMSI_HLR, MS9_IMSI_SI, MS9_STATUS, MS9_VALID_TO, MS9_E_ID, MS9_CARD_TYPE_NAME,
    MS10_ICCID, MS10_IMSI_MCC, MS10_IMSI_MNC, MS10_IMSI_HLR, MS10_IMSI_SI, MS10_STATUS, MS10_VALID_TO, MS10_E_ID, MS10_CARD_TYPE_NAME
  )
  SELECT
        bp.cntrct_id,
        CASE WHEN bp.bpr_id = 31 THEN iccid ELSE NULL END AS TN_ICCID,
        CASE WHEN bp.bpr_id = 31 THEN imsi_mcc ELSE NULL END AS TN_IMSI_MCC,
        CASE WHEN bp.bpr_id = 31 THEN imsi_mnc ELSE NULL END AS TN_IMSI_MNC,
        CASE WHEN bp.bpr_id = 31 THEN imsi_hlr ELSE NULL END AS TN_IMSI_HLR,
        CASE WHEN bp.bpr_id = 31 THEN imsi_si ELSE NULL END AS TN_IMSI_SI,
        CASE WHEN bp.bpr_id = 31 THEN CASE WHEN bp.valid_to <= PARSE_DATE('%Y%m%d', v_max_timecreated_datum) THEN 'L' ELSE 'A' END ELSE NULL END AS TN_STATUS,
        CASE WHEN bp.bpr_id = 31 THEN bp.valid_to ELSE NULL END AS TN_VALID_TO,
        CASE WHEN bp.bpr_id = 31 THEN E_ID ELSE NULL END AS TN_E_ID,
        CASE WHEN bp.bpr_id = 31 THEN CARD_TYPE_NAME ELSE NULL END AS TN_CARD_TYPE_NAME,
        CASE WHEN bp.bpr_id = 2759 THEN iccid ELSE NULL END AS TC_ICCID,
        CASE WHEN bp.bpr_id = 2759 THEN imsi_mcc ELSE NULL END AS TC_IMSI_MCC,
        CASE WHEN bp.bpr_id = 2759 THEN imsi_mnc ELSE NULL END AS TC_IMSI_MNC,
        CASE WHEN bp.bpr_id = 2759 THEN imsi_hlr ELSE NULL END AS TC_IMSI_HLR,
        CASE WHEN bp.bpr_id = 2759 THEN imsi_si ELSE NULL END AS TC_IMSI_SI,
        CASE WHEN bp.bpr_id = 2759 THEN CASE WHEN bp.valid_to <= PARSE_DATE('%Y%m%d', v_max_timecreated_datum) THEN 'L' ELSE 'A' END ELSE NULL END AS TC_STATUS,
        CASE WHEN bp.bpr_id = 2759 THEN bp.valid_to ELSE NULL END AS TC_VALID_TO,
        CASE WHEN bp.bpr_id = 2759 THEN E_ID ELSE NULL END AS TC_E_ID,
        CASE WHEN bp.bpr_id = 2759 THEN CARD_TYPE_NAME ELSE NULL END AS TC_CARD_TYPE_NAME,
        CASE WHEN bp.bpr_id = 2800 THEN iccid ELSE NULL END AS TB_ICCID,
        CASE WHEN bp.bpr_id = 2800 THEN imsi_mcc ELSE NULL END AS TB_IMSI_MCC,
        CASE WHEN bp.bpr_id = 2800 THEN imsi_mnc ELSE NULL END AS TB_IMSI_MNC,
        CASE WHEN bp.bpr_id = 2800 THEN imsi_hlr ELSE NULL END AS TB_IMSI_HLR,
        CASE WHEN bp.bpr_id = 2800 THEN imsi_si ELSE NULL END AS TB_IMSI_SI,
        CASE WHEN bp.bpr_id = 2800 THEN CASE WHEN bp.valid_to <= PARSE_DATE('%Y%m%d', v_max_timecreated_datum) THEN 'L' ELSE 'A' END ELSE NULL END AS TB_STATUS,
        CASE WHEN bp.bpr_id = 2800 THEN bp.valid_to ELSE NULL END AS TB_VALID_TO,
        CASE WHEN bp.bpr_id = 2800 THEN E_ID ELSE NULL END AS TB_E_ID,
        CASE WHEN bp.bpr_id = 2800 THEN CARD_TYPE_NAME ELSE NULL END AS TB_CARD_TYPE_NAME,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384801 THEN bp.iccid ELSE NULL END AS MS1_ICCID,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384801 THEN bp.imsi_mcc ELSE NULL END AS MS1_IMSI_MCC,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384801 THEN bp.imsi_mnc ELSE NULL END AS MS1_IMSI_MNC,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384801 THEN bp.imsi_hlr ELSE NULL END AS MS1_IMSI_HLR,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384801 THEN bp.imsi_si ELSE NULL END AS MS1_IMSI_SI,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384801 THEN IF(bp.valid_to > PARSE_DATE('%Y%m%d', v_max_timecreated_datum), 'A', 'L') ELSE NULL END AS MS1_STATUS,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384801 THEN bp.valid_to ELSE NULL END AS MS1_VALID_TO,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384801 THEN bp.E_ID ELSE NULL END AS MS1_E_ID,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384801 THEN bp.CARD_TYPE_NAME ELSE NULL END AS MS1_CARD_TYPE_NAME,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384802 THEN bp.iccid ELSE NULL END AS MS2_ICCID,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384802 THEN bp.imsi_mcc ELSE NULL END AS MS2_IMSI_MCC,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384802 THEN bp.imsi_mnc ELSE NULL END AS MS2_IMSI_MNC,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384802 THEN bp.imsi_hlr ELSE NULL END AS MS2_IMSI_HLR,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384802 THEN bp.imsi_si ELSE NULL END AS MS2_IMSI_SI,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384802 THEN IF(bp.valid_to > PARSE_DATE('%Y%m%d', v_max_timecreated_datum), 'A', 'L') ELSE NULL END AS MS2_STATUS,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384802 THEN bp.valid_to ELSE NULL END AS MS2_VALID_TO,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384802 THEN bp.E_ID ELSE NULL END AS MS2_E_ID,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384802 THEN bp.CARD_TYPE_NAME ELSE NULL END AS MS2_CARD_TYPE_NAME,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384803 THEN bp.iccid ELSE NULL END AS MS3_ICCID,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384803 THEN bp.imsi_mcc ELSE NULL END AS MS3_IMSI_MCC,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384803 THEN bp.imsi_mnc ELSE NULL END AS MS3_IMSI_MNC,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384803 THEN bp.imsi_hlr ELSE NULL END AS MS3_IMSI_HLR,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384803 THEN bp.imsi_si ELSE NULL END AS MS3_IMSI_SI,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384803 THEN IF(bp.valid_to > PARSE_DATE('%Y%m%d', v_max_timecreated_datum), 'A', 'L') ELSE NULL END AS MS3_STATUS,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384803 THEN bp.valid_to ELSE NULL END AS MS3_VALID_TO,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384803 THEN bp.E_ID ELSE NULL END AS MS3_E_ID,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384803 THEN bp.CARD_TYPE_NAME ELSE NULL END AS MS3_CARD_TYPE_NAME,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384804 THEN bp.iccid ELSE NULL END AS MS4_ICCID,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384804 THEN bp.imsi_mcc ELSE NULL END AS MS4_IMSI_MCC,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384804 THEN bp.imsi_mnc ELSE NULL END AS MS4_IMSI_MNC,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384804 THEN bp.imsi_hlr ELSE NULL END AS MS4_IMSI_HLR,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384804 THEN bp.imsi_si ELSE NULL END AS MS4_IMSI_SI,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384804 THEN IF(bp.valid_to > PARSE_DATE('%Y%m%d', v_max_timecreated_datum), 'A', 'L') ELSE NULL END AS MS4_STATUS,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384804 THEN bp.valid_to ELSE NULL END AS MS4_VALID_TO,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384804 THEN bp.E_ID ELSE NULL END AS MS4_E_ID,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384804 THEN bp.CARD_TYPE_NAME ELSE NULL END AS MS4_CARD_TYPE_NAME,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384805 THEN bp.iccid ELSE NULL END AS MS5_ICCID,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384805 THEN bp.imsi_mcc ELSE NULL END AS MS5_IMSI_MCC,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384805 THEN bp.imsi_mnc ELSE NULL END AS MS5_IMSI_MNC,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384805 THEN bp.imsi_hlr ELSE NULL END AS MS5_IMSI_HLR,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384805 THEN bp.imsi_si ELSE NULL END AS MS5_IMSI_SI,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384805 THEN IF(bp.valid_to > PARSE_DATE('%Y%m%d', v_max_timecreated_datum), 'A', 'L') ELSE NULL END AS MS5_STATUS,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384805 THEN bp.valid_to ELSE NULL END AS MS5_VALID_TO,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384805 THEN bp.E_ID ELSE NULL END AS MS5_E_ID,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384805 THEN bp.CARD_TYPE_NAME ELSE NULL END AS MS5_CARD_TYPE_NAME,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384806 THEN bp.iccid ELSE NULL END AS MS6_ICCID,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384806 THEN bp.imsi_mcc ELSE NULL END AS MS6_IMSI_MCC,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384806 THEN bp.imsi_mnc ELSE NULL END AS MS6_IMSI_MNC,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384806 THEN bp.imsi_hlr ELSE NULL END AS MS6_IMSI_HLR,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384806 THEN bp.imsi_si ELSE NULL END AS MS6_IMSI_SI,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384806 THEN IF(bp.valid_to > PARSE_DATE('%Y%m%d', v_max_timecreated_datum), 'A', 'L') ELSE NULL END AS MS6_STATUS,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384806 THEN bp.valid_to ELSE NULL END AS MS6_VALID_TO,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384806 THEN bp.E_ID ELSE NULL END AS MS6_E_ID,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384806 THEN bp.CARD_TYPE_NAME ELSE NULL END AS MS6_CARD_TYPE_NAME,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384807 THEN bp.iccid ELSE NULL END AS MS7_ICCID,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384807 THEN bp.imsi_mcc ELSE NULL END AS MS7_IMSI_MCC,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384807 THEN bp.imsi_mnc ELSE NULL END AS MS7_IMSI_MNC,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384807 THEN bp.imsi_hlr ELSE NULL END AS MS7_IMSI_HLR,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384807 THEN bp.imsi_si ELSE NULL END AS MS7_IMSI_SI,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384807 THEN IF(bp.valid_to > PARSE_DATE('%Y%m%d', v_max_timecreated_datum), 'A', 'L') ELSE NULL END AS MS7_STATUS,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384807 THEN bp.valid_to ELSE NULL END AS MS7_VALID_TO,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384807 THEN bp.E_ID ELSE NULL END AS MS7_E_ID,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384807 THEN bp.CARD_TYPE_NAME ELSE NULL END AS MS7_CARD_TYPE_NAME,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384808 THEN bp.iccid ELSE NULL END AS MS8_ICCID,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384808 THEN bp.imsi_mcc ELSE NULL END AS MS8_IMSI_MCC,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384808 THEN bp.imsi_mnc ELSE NULL END AS MS8_IMSI_MNC,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384808 THEN bp.imsi_hlr ELSE NULL END AS MS8_IMSI_HLR,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384808 THEN bp.imsi_si ELSE NULL END AS MS8_IMSI_SI,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384808 THEN IF(bp.valid_to > PARSE_DATE('%Y%m%d', v_max_timecreated_datum), 'A', 'L') ELSE NULL END AS MS8_STATUS,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384808 THEN bp.valid_to ELSE NULL END AS MS8_VALID_TO,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384808 THEN bp.E_ID ELSE NULL END AS MS8_E_ID,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384808 THEN bp.CARD_TYPE_NAME ELSE NULL END AS MS8_CARD_TYPE_NAME,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384809 THEN bp.iccid ELSE NULL END AS MS9_ICCID,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384809 THEN bp.imsi_mcc ELSE NULL END AS MS9_IMSI_MCC,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384809 THEN bp.imsi_mnc ELSE NULL END AS MS9_IMSI_MNC,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384809 THEN bp.imsi_hlr ELSE NULL END AS MS9_IMSI_HLR,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384809 THEN bp.imsi_si ELSE NULL END AS MS9_IMSI_SI,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384809 THEN IF(bp.valid_to > PARSE_DATE('%Y%m%d', v_max_timecreated_datum), 'A', 'L') ELSE NULL END AS MS9_STATUS,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384809 THEN bp.valid_to ELSE NULL END AS MS9_VALID_TO,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384809 THEN bp.E_ID ELSE NULL END AS MS9_E_ID,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384809 THEN bp.CARD_TYPE_NAME ELSE NULL END AS MS9_CARD_TYPE_NAME,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384810 THEN bp.iccid ELSE NULL END AS MS10_ICCID,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384810 THEN bp.imsi_mcc ELSE NULL END AS MS10_IMSI_MCC,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384810 THEN bp.imsi_mnc ELSE NULL END AS MS10_IMSI_MNC,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384810 THEN bp.imsi_hlr ELSE NULL END AS MS10_IMSI_HLR,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384810 THEN bp.imsi_si ELSE NULL END AS MS10_IMSI_SI,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384810 THEN IF(bp.valid_to > PARSE_DATE('%Y%m%d', v_max_timecreated_datum), 'A', 'L') ELSE NULL END AS MS10_STATUS,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384810 THEN bp.valid_to ELSE NULL END AS MS10_VALID_TO,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384810 THEN bp.E_ID ELSE NULL END AS MS10_E_ID,
        CASE WHEN bp.bpr_id*100 + bp.slave_number = 384810 THEN bp.CARD_TYPE_NAME ELSE NULL END AS MS10_CARD_TYPE_NAME
  FROM    `project.dataset.sof_ta_bpr_basis` AS bp
  WHERE   bp.bpr_id IN (31, 2759, 2800, 3848);

  -- Log run results
  INSERT INTO `project.dataset.job_run_result`
  (job_kennung, eintragsnr, stichtag, tab_name, datum_heute, datum_gestern, restart_value, record_count, created_at)
  VALUES
  (p_JobKennung, p_EintragsNr, p_Stichtag, v_TabName, v_datum_heute, v_datum_gestern, p_wiederanlaufWert, (SELECT CAST(COUNT(*) AS STRING) FROM `project.dataset.sof_ta_iccid_einzeln`), CURRENT_TIMESTAMP());

  SELECT '---------- ENDE Datenverarbeitung ----------' AS message;

END;
```