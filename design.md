# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_carmen.ksh

## 1. Purpose & Scope

This job, `k_ausd_bp_ta_apn_carmen.ksh`, is a KornShell control script designed to orchestrate the execution of a data preparation process. Its primary purpose is to validate input parameters (job identifier, entry number, key date, and an optional restart value), perform date validation, and then execute an external SQL script (`d_ausd_bp_ta_apn_carmen.sql`) to carry out the core data transformation. The script also handles error reporting and, based on commented-out sections, was intended for job logging and possibly post-processing of output files (cleanup, sorting, joining). The overall objective is to process data related to `PoolBasisprodukt`.

## 2. Source Inventory

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_carmen.ksh`
    *   **Technology:** KornShell (`.ksh`)
    *   **Tier:** Not provided in `file_complexity` table; assumed to be `simple` due to orchestration nature.
    *   **Automation Bucket:** `semi_auto`
    *   **Purpose:** Orchestration, parameter validation, SQL script execution.

## 3. Target Architecture

The target architecture in Google BigQuery will involve:

*   **BigQuery Stored Procedures:** The core orchestration logic of the KornShell script, including parameter validation, date validation, and calling the main data processing logic, will be migrated into a BigQuery Stored Procedure. This procedure will manage variables, conditionals, and error handling.
*   **BigQuery Tables:**
    *   **Source Tables:** The tables read by the external SQL script (`d_ausd_bp_ta_apn_carmen.sql`) will be migrated or ingested into BigQuery.
    *   **Target Table:** The `PoolBasisprodukt` table (or its BigQuery equivalent) will be the final destination for processed data.
    *   **Logging Tables:** Dedicated BigQuery tables for job error logs (`job_error_log`) and job control/activity logs (`job_control_log`) will replace the shell script's `DWMSG_MeldeFehler` and the commented `FOSJobErzeugeEintrag` functionality.
    *   **Staging Tables:** For the commented file manipulation logic (`sed`, `sort`, `join`), staging tables in BigQuery will be used to process intermediate data.
*   **BigQuery SQL:** The logic contained within the external SQL script (`d_ausd_bp_ta_apn_carmen.sql`) will be converted to BigQuery SQL, potentially embedded within the stored procedure or as separate BigQuery SQL scripts executed by the stored procedure.
*   **Orchestration (Optional):** If complex scheduling or external system integrations are required beyond BigQuery's native scheduling, Google Cloud Composer (Apache Airflow) or Google Cloud Workflows could be used to trigger the BigQuery Stored Procedure.

## 4. Data Flow & Lineage

The current script orchestrates a data flow that can be re-engineered into BigQuery:

1.  **Parameter Input:** The BigQuery Stored Procedure will accept `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, and `p_wiederanlaufWert` as direct input parameters.
2.  **Environment Initialization:** Replaced by BigQuery's intrinsic environment (e.g., `CURRENT_DATE()`, `DATE_SUB()`) and explicit declaration of variables within the stored procedure. Configuration can be managed via stored procedure parameters or a dedicated BigQuery configuration table.
3.  **Validation:**
    *   Parameter presence checks will be implemented using `IF` statements and `ASSERT` in BigQuery SQL.
    *   Date format validation (`DDMMYYYY`) will use BigQuery's `SAFE.PARSE_DATE()` function.
    *   Errors will be logged to `project.dataset.job_error_log` and potentially raise exceptions using `SIGNAL SQLSTATE`.
4.  **Date Derivation:** The functionality of `gestern.ksh` (deriving yesterday's date) will be replaced by BigQuery SQL functions like `CURRENT_DATE()` and `DATE_SUB()`.
5.  **Core Data Processing:** The execution of `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_bp_ta_apn_carmen.sql` will be replaced by direct execution of its translated BigQuery SQL logic. This SQL will perform data extraction, transformation, and loading into the target `PoolBasisprodukt` table within BigQuery.
6.  **Record Counting:** The `tmpFile` mechanism for counting records will be replaced by a `SELECT COUNT(*)` statement storing the result in a BigQuery variable within the stored procedure.
7.  **Job Logging:** The commented `FOSJobErzeugeEintrag` functionality will be implemented as an `INSERT` statement into a `project.dataset.job_control_log` BigQuery table, capturing job execution metadata and record counts.
8.  **Commented File Post-processing:** The `sed`, `sort`, `join` operations, if determined to be active or required, will be translated into BigQuery SQL transformations using `REGEXP_REPLACE`, `SELECT DISTINCT`, `ORDER BY`, and `JOIN` operations on staging tables.

**Execution Order:**
The stored procedure will encapsulate the entire flow, ensuring sequential execution of validation, data derivation, core SQL logic, and logging steps.

## 5. Transformation Logic

The KornShell script itself primarily acts as an orchestrator. The key transformations occur within the external SQL script `d_ausd_bp_ta_apn_carmen.sql` (not provided in this scope) and the commented file processing logic.

**KornShell Constructs to BigQuery SQL Mapping:**

*   **Environment Variables (`$HOME`, `$BERT_DIR_ROOT`, `$DW_DIR_UTL`):**
    *   **Legacy:** Environment variables used for paths and configuration.
    *   **Target:** Replaced by stored procedure parameters, BigQuery scripting variables, or values from a BigQuery configuration table.
*   **Parameter Parsing (`getopts`):**
    *   **Legacy:** Used to parse command-line arguments.
    *   **Target:** Direct input parameters to the BigQuery Stored Procedure.
*   **Conditionals (`if [ ! $ErrNr -eq 0 ]`, `if [[ -z "$p_wiederanlaufWert" ]]`):**
    *   **Legacy:** Shell `if` statements for control flow.
    *   **Target:** BigQuery SQL `IF ... THEN ... END IF;` constructs, `CASE WHEN ... THEN ... END`, or `ASSERT` for validation.
*   **Utility Script Calls (`. ${BERT_DIR_ROOT}/.../*.ksh`):**
    *   **Legacy:** Sourcing external shell scripts for common functions (error messaging, date checks, parameter parsing, SQL execution).
    *   **Target:** Equivalent BigQuery SQL functions (`SAFE.PARSE_DATE`, `CURRENT_TIMESTAMP`), `ASSERT` statements, or `INSERT` into logging tables for error handling. The `starteSQLSkript` wrapper will be replaced by direct embedded BigQuery SQL execution.
*   **External Command Execution (`set \`gestern.ksh\``, `cat $tmpFile`):**
    *   **Legacy:** Executing a script to get dates, reading a temporary file for record counts.
    *   **Target:** `CURRENT_DATE()`, `DATE_SUB()` for date derivation. `SELECT COUNT(*)` for record counts stored in a variable.
*   **Commented File Processing (`sed`, `sort`, `join`):**
    *   **Legacy:** Operations on flat files.
    *   **Target:** BigQuery SQL `REGEXP_REPLACE`, `SELECT DISTINCT`, `ORDER BY`, `JOIN` operations on temporary or staging tables.

**Example BQ SQL Pseudocode for Control Logic:**

```sql
-- BigQuery Stored Procedure: control logic equivalent
CREATE OR REPLACE PROCEDURE `project.dataset.k_ausd_bp_ta_apn_carmen`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING,
  IN p_Stichtag STRING,
  IN p_wiederanlaufWert INT64
)
BEGIN
  DECLARE v_TabName STRING DEFAULT 'PoolBasisprodukt';
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_wiederanlaufWert INT64 DEFAULT 0;
  DECLARE v_datum_heute DATE DEFAULT CURRENT_DATE();
  DECLARE v_datum_gestern DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
  DECLARE v_stichtag_date DATE;
  DECLARE v_err_msg STRING DEFAULT '';
  DECLARE v_err_nr INT64 DEFAULT 0;

  -- Parameter validation
  IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
    SET v_err_nr = 1;
    SET v_err_msg = 'Jobkennung fehlt';
  END IF;
  -- ... (other parameter validations) ...

  IF v_err_nr <> 0 THEN
    -- Log error
    INSERT INTO `project.dataset.job_error_log` (job_name, entry_nr, error_nr, error_msg, created_at)
    VALUES (p_JobKennung, p_EintragsNr, v_err_nr, v_err_msg, CURRENT_TIMESTAMP());
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_err_msg;
  END IF;

  -- Date validation for DDMMYYYY
  SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);
  IF v_stichtag_date IS NULL THEN
    -- Log error
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Stichtag hat ungueltiges Format DDMMYYYY';
  END IF;

  -- Restart value initialization
  SET v_wiederanlaufWert = IFNULL(p_wiederanlaufWert, 0);

  -- Execute core SQL logic equivalent to external SQL script
  -- The content of d_ausd_bp_ta_apn_carmen.sql will be translated and embedded here
  -- Example:
  CREATE TEMP TABLE tmp_result AS
  SELECT *
  FROM `project.dataset.source_table`
  WHERE process_date = v_stichtag_date
    AND restart_value >= v_wiederanlaufWert;

  SET v_records = (SELECT COUNT(*) FROM tmp_result);

  INSERT INTO `project.dataset.PoolBasisprodukt_target`
  SELECT * FROM tmp_result;

  -- Job logging equivalent to FOSJobErzeugeEintrag
  INSERT INTO `project.dataset.job_control_log`
  (tab_name, status_a, status_i, stichtag_from, stichtag_to, job_type, active_flag, record_count, comment_text, job_name, entry_nr, created_at)
  VALUES
  (v_TabName, 'A', 'I', v_stichtag_date, v_stichtag_date, 'J', 'N', v_records, 'Initialbefuellung', p_JobKennung, p_EintragsNr, CURRENT_TIMESTAMP());

END;
```

## 6. External Dependencies

| Original System/Dependency | How it's Replaced in BigQuery |
| :------------------------- | :----------------------------------------------------------------------------------------------------------------------- |
| `sqlplus` (via `h_alis_sqlplus.ksh`) | Direct execution of translated SQL within BigQuery environment (e.g., embedded in stored procedure) |
| `$HOME/.dw_init` (environment sourcing) | Stored Procedure parameters, BigQuery variables, or a BigQuery configuration table |
| `f_alis_msgerr.ksh` (error concept) | BigQuery error logging table (`job_error_log`) and `SIGNAL SQLSTATE` for exceptions |
| `h_alis_date.ksh` (date check) | BigQuery SQL functions like `SAFE.PARSE_DATE()` |
| `h_alis_parameter.ksh` (parameter parsing) | BigQuery Stored Procedure input parameters and `IF`/`ASSERT` statements |
| `gestern.ksh` (date derivation) | BigQuery SQL functions `CURRENT_DATE()` and `DATE_SUB()` |
| `d_ausd_bp_ta_apn_carmen.sql` (external SQL script) | Translated BigQuery SQL embedded within or called by the stored procedure |
| Temporary files (`.tmp`, `.dat`, `.sed`, `.csv`) | BigQuery temporary tables, staging tables, or direct BigQuery table operations |
| `FOSJobDeaktivate`, `FOSJobErzeugeEintrag` (Job management functions, commented) | Dedicated BigQuery job control/log table (`job_control_log`) managed by stored procedures |
| `sed`, `sort`, `join` (file utilities, commented) | BigQuery SQL string functions (`REGEXP_REPLACE`), analytical functions, `SELECT DISTINCT`, `ORDER BY`, and `JOIN` operations |

## 7. Unresolved / Risks

*   **Complexity of `d_ausd_bp_ta_apn_carmen.sql`:** The actual data transformation logic resides in `d_ausd_bp_ta_apn_carmen.sql`, which was not available for analysis. The success of this migration heavily depends on the complexity and convertibility of this SQL script to BigQuery SQL. If it contains highly procedural or database-specific (e.g., Oracle PL/SQL) constructs, this will increase the manual effort.
*   **Missing `file_complexity` data:** The `file_complexity` table did not return any rows for this file. This means the detailed complexity signals and migration flags are unknown, which could hide potential challenges.
*   **Dynamic SQL in `d_ausd_bp_ta_apn_carmen.sql`:** If `d_ausd_bp_ta_apn_carmen.sql` generates SQL dynamically, this would require careful re-engineering to BigQuery's dynamic SQL capabilities or alternative approaches.
*   **External System Calls in `d_ausd_bp_ta_apn_carmen.sql`:** If the SQL script itself calls external systems (e.g., other databases, APIs), these will need to be identified and handled. The `lineage_assembled_jobs` record indicated no `external_systems` for the job, but this might not cover dependencies within nested SQL.
*   **Job Purpose Clarity:** The `file_purpose` for the ksh script was not populated in the `file_analysis` table, and the `purpose_note` was generic. While the tool inferred the purpose, a clearer understanding from documentation would be beneficial.
*   **Orchestration Beyond BigQuery:** While the core logic can be migrated to a BigQuery Stored Procedure, if this job is part of a larger workflow requiring complex scheduling, dependency management, or integration with other non-BigQuery components, a Cloud Composer (Airflow) or Cloud Workflows solution might be necessary.

## 8. Build Plan

The migration will be implemented as follows:

1.  **Translate `d_ausd_bp_ta_apn_carmen.sql` to BigQuery SQL (if available):**
    *   **Language:** BigQuery SQL
    *   **Output:** `d_ausd_bp_ta_apn_carmen_bq.sql` (or embedded in SP). This is a critical prerequisite.

2.  **Create BigQuery Error Logging Table:**
    *   **Language:** BigQuery DDL
    *   **Output:** `job_error_log.sql`
    *   **Description:** Table to record job execution errors.

3.  **Create BigQuery Job Control Log Table:**
    *   **Language:** BigQuery DDL
    *   **Output:** `job_control_log.sql`
    *   **Description:** Table to record job execution history and metrics.

4.  **Create BigQuery Stored Procedure for Orchestration:**
    *   **Language:** BigQuery SQL (Scripting)
    *   **Output:** `sp_k_ausd_bp_ta_apn_carmen.sql`
    *   **Description:** This procedure will contain the translated logic of the `k_ausd_bp_ta_apn_carmen.ksh` script, including parameter validation, date derivation, execution of the core data processing SQL (from step 1), and logging to the job control table.

5.  **Develop BigQuery Staging Tables (if commented file logic is active):**
    *   **Language:** BigQuery DDL
    *   **Output:** `cibasis_data24_staging.sql`, `cibasis_data96_staging.sql`, `cibasis_fax_staging.sql`
    *   **Description:** If the commented `sed/sort/join` block represents active business logic, these tables will be created to stage the data for BigQuery SQL transformations.

6.  **Translate Commented File Processing Logic (if active):**
    *   **Language:** BigQuery SQL
    *   **Output:** `cibasis_postprocessing_bq.sql` (or embedded in SP)
    *   **Description:** BigQuery SQL queries to replicate the functionality of the `sed`, `sort`, `join` commands on the staging tables.

7.  **Create BigQuery Target Table for `PoolBasisprodukt`:**
    *   **Language:** BigQuery DDL
    *   **Output:** `PoolBasisprodukt_target.sql`
    *   **Description:** The final destination table for the processed data.

8.  **Orchestration Layer (if needed):**
    *   **Language:** Python (for Cloud Composer/Airflow DAG) or YAML (for Cloud Workflows)
    *   **Output:** `dag_k_ausd_bp_ta_apn_carmen.py` (or `workflow_k_ausd_bp_ta_apn_carmen.yaml`)
    *   **Description:** If the job requires external scheduling or complex dependency management, a Cloud Composer DAG or Cloud Workflows definition will be created to invoke the BigQuery Stored Procedure.