# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_action_assoc.ksh

## 1. Purpose & Scope
This KornShell script, `k_ausd_v_ta_action_assoc.ksh`, serves as a control script for the `r_ausd_vertrag.ksh` process. Its primary business purpose is to orchestrate a data preparation job related to the `ta_action_assoc` entity. This involves managing job execution, calling a core SQL script (`d_ausd_v_ta_action_assoc.sql`), and recording job status and processed record counts. The script handles parameter parsing, basic error checking, and interaction with a job management table (likely to ignore or deactivate active jobs).

The scope of this migration is to translate the orchestration logic and parameter handling of this KornShell script into a BigQuery-compatible solution, likely a BigQuery Stored Procedure, while preserving its core functionality. The actual data transformation logic residing in the SQL script (`d_ausd_v_ta_action_assoc.sql`) will need a separate migration effort, but this document will focus on the shell script's wrapper functionality.

## 2. Source Inventory

**File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_action_assoc.ksh`
*   **Technology:** Shell Script (KornShell)
*   **Purpose:** Local ETL orchestration
*   **Category:** `shell`
*   **Tool:** `KornShell`
*   **Complexity Tier:** `medium`
*   **Automation Bucket:** `semi_auto`
*   **Migration Flags:** `[]`
*   **Complexity Signals:** (None detected)

## 3. Target Architecture
The target platform is BigQuery. The migration will primarily involve translating the shell script's control flow, parameter handling, and job management logic into a BigQuery Stored Procedure.

*   **BigQuery Stored Procedure:** A BigQuery Stored Procedure, e.g., `sp_ausd_v_ta_action_assoc`, will encapsulate the script's logic. It will accept input parameters equivalent to the shell script's command-line arguments.
*   **BigQuery Tables:**
    *   **Job Control Table:** A BigQuery table (e.g., `project.dataset.job_table`) will be used to manage job statuses (active/inactive), replacing the implicit job management logic in the shell script.
    *   **Logging Table:** A BigQuery table (e.g., `project.dataset.error_log`) will capture error messages and execution details, replacing shell `echo`/`print` statements and custom error handling.
    *   **Audit Table:** A BigQuery table (e.g., `project.dataset.job_log`) to store job start/end times and record counts, replacing the temporary file mechanism.
*   **Migrated SQL Logic:** The business logic from `d_ausd_v_ta_action_assoc.sql` will be translated into BigQuery SQL and either inlined within the stored procedure, called as a separate stored procedure, or executed as a series of SQL statements within the main procedure.

## 4. Data Flow & Lineage
The original script's lineage indicated no direct `READS` or `WRITES` edges, suggesting its primary role is orchestration.

**Original Data Flow:**
1.  **Start:** `k_ausd_v_ta_action_assoc.ksh` is invoked with `p_JobKennung` and `p_EintragsNr`.
2.  **Initialization:** Sources environment variables (`. $HOME/.dw_init`) and helper scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`).
3.  **Parameter Parsing:** Uses `getopts` to parse `j` (JobKennung) and `f` (EintragsNr).
4.  **Parameter Validation:** Calls `pruefeParameterGesetzt` and exits with an error code if required parameters are missing.
5.  **SQL Execution:** Calls `starteSQLSkript` which, in turn, executes `${BERT_DIR_ROOT}/aufbereitung/sql/d_ausd_v_ta_action_assoc.sql` with provided parameters. This step likely involves interaction with a database (e.g., Oracle via SQL*Plus) and generates a temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_action_assoc_$$.tmp`) with record counts.
6.  **Record Count:** Reads `v_records` from the temporary file.
7.  **End:** Prints a completion message.

**Target BigQuery Data Flow:**
1.  **Invocation:** BigQuery Stored Procedure `sp_ausd_v_ta_action_assoc` is called with `p_JobKennung` and `p_EintragsNr` parameters. This could be triggered by Cloud Composer, Cloud Functions, or manually.
2.  **Parameter Validation:** Internal `IF` statements and BigQuery scripting variables handle validation. Errors are logged to `error_log` and raise exceptions.
3.  **Job Start Logging:** Inserts a `STARTED` record into `project.dataset.job_log`.
4.  **Deactivate Old Jobs:** An `UPDATE` statement modifies `project.dataset.job_table` to deactivate old active jobs.
5.  **Execute Business Logic:** The translated BigQuery SQL logic (from `d_ausd_v_ta_action_assoc.sql`) is executed. This might involve `INSERT`, `UPDATE`, `MERGE` into target tables.
6.  **Record Count:** BigQuery scripting variables (`DECLARE v_records INT64;`) and `SELECT COUNT(*)` capture the number of processed records.
7.  **Job End Logging:** Inserts a `FINISHED` record into `project.dataset.job_log` including the processed record count.
8.  **Completion:** `SELECT` statements provide completion messages.

## 5. Transformation Logic
The transformation logic from the original shell script to BigQuery Stored Procedure can be summarized as:

*   **Environment Variables:** Shell environment variables (`$HOME`, `$BERT_DIR_ROOT`, `$DW_DIR_UTL`) will be replaced by:
    *   BigQuery Stored Procedure input parameters.
    *   BigQuery dataset constants.
    *   Configuration tables in BigQuery.
*   **Sourced Scripts:** Helper scripts (`. ${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`, etc.) will have their functionality rewritten as:
    *   BigQuery Stored Procedures for common functions (e.g., parameter validation, error logging).
    *   Direct BigQuery scripting (`IF`/`THEN`/`ELSE`) for control flow.
*   **Parameter Parsing (`getopts`):** The need for explicit parameter parsing will be eliminated by using BigQuery Stored Procedure input parameters directly.
*   **Conditional Logic (`if`, `case`):** Translated to BigQuery's `IF ... THEN ... END IF;` statements.
*   **Temporary Files (`tmpFile`, `cat`):** Replaced by BigQuery scripting variables (`DECLARE`) or direct inserts into audit/log tables.
*   **SQL Execution Wrapper (`starteSQLSkript`):** This function will be replaced by a direct invocation of the migrated BigQuery SQL logic, possibly another BigQuery Stored Procedure if `d_ausd_v_ta_action_assoc.sql` is migrated as such.
*   **Error Handling (`DWMSG_MeldeFehler`, `exit`):** Replaced by `INSERT` statements into an `error_log` table and `RAISE USING MESSAGE` for procedure termination.
*   **Job Management:** `UPDATE` statements against a dedicated `job_table` in BigQuery will handle activation/deactivation.

**Example BigQuery SQL Pseudocode (from CM MCP tool):**
```sql
CREATE OR REPLACE PROCEDURE `project.dataset.sp_ausd_v_ta_action_assoc`(
  p_JobKennung STRING,
  p_EintragsNr STRING
)
BEGIN
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';
  DECLARE v_TabName STRING DEFAULT 'ta_action_assoc';
  DECLARE v_records INT64 DEFAULT 0;

  -- Parameter validation (based on pruefeParameterGesetzt)
  IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
    SET ErrNr = 1;
    SET ErrArg = 'Jobkennung';
  END IF;

  IF ErrNr = 0 AND (p_EintragsNr IS NULL OR TRIM(p_EintragsNr) = '') THEN
    SET ErrNr = 1;
    SET ErrArg = 'EintragsNr';
  END IF;

  IF ErrNr != 0 THEN
    -- Log error (based on DWMSG_MeldeFehler)
    INSERT INTO `project.dataset.error_log`
      (error_ts, error_code, error_arg, procedure_name, message)
    VALUES
      (CURRENT_TIMESTAMP(), ErrNr, ErrArg, 'sp_ausd_v_ta_action_assoc', 'Bitte ueber Rahmenscript aufrufen');

    RAISE USING MESSAGE = CONCAT('FEHLER: 0 E ', CAST(ErrNr AS STRING), ' ', ErrArg);
  END IF;

  -- Job start logging
  INSERT INTO `project.dataset.job_log`
    (job_kennung, eintrags_nr, tab_name, status, start_ts)
  VALUES
    (p_JobKennung, p_EintragsNr, v_TabName, 'STARTED', CURRENT_TIMESTAMP());

  -- Deactivate old active jobs (based on script's purpose)
  UPDATE `project.dataset.job_table`
  SET active_flag = FALSE,
      end_ts = CURRENT_TIMESTAMP()
  WHERE job_kennung = p_JobKennung
    AND active_flag = TRUE
    AND eintrags_nr != p_EintragsNr;

  -- Execute migrated SQL logic from d_ausd_v_ta_action_assoc.sql
  -- Placeholder for translated business SQL
  -- Example: CALL `project.dataset.sp_d_ausd_v_ta_action_assoc`(p_JobKennung, p_EintragsNr);

  -- Example record count
  SET v_records = (SELECT COUNT(*) FROM `project.dataset.some_target_table_after_sql_execution`);

  -- Persist result count
  INSERT INTO `project.dataset.job_log`
    (job_kennung, eintrags_nr, tab_name, status, record_count, end_ts)
  VALUES
    (p_JobKennung, p_EintragsNr, v_TabName, 'FINISHED', v_records, CURRENT_TIMESTAMP());

  SELECT ' ---------- ENDE Datenverarbeitung ---------- ' AS message;
  SELECT v_records AS processed_records;
END;
```

## 6. External Dependencies
The original script has the following external dependencies and their proposed replacements:

*   **Shell Environment Initialization (`. $HOME/.dw_init`):**
    *   **Replacement:** Configuration in BigQuery (dataset constants, dedicated config tables) or passed as parameters to the BigQuery Stored Procedure. For orchestration, environment variables in Cloud Composer/Cloud Run can store equivalent values.
*   **Sourced Utility Scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_sqlplus.ksh`):**
    *   **Replacement:** Functionality of these scripts will be re-implemented as BigQuery Stored Procedures, `UDF`s, or BigQuery scripting blocks.
*   **External SQL File (`d_ausd_v_ta_action_assoc.sql`):**
    *   **Replacement:** This SQL file contains the core business logic and will need to be migrated separately to BigQuery SQL. It could become part of the main stored procedure, a separate stored procedure, or a series of SQL statements.
*   **Temporary File (`$DW_DIR_UTL/bert_k_ausd_v_ta_action_assoc_$$.tmp`):**
    *   **Replacement:** BigQuery scripting variables (`DECLARE`) for temporary data or direct inserts into audit/log tables for persistence.
*   **`getopts` command:**
    *   **Replacement:** BigQuery Stored Procedure parameters directly.
*   **Underlying Database (implied by SQL*Plus calls in `h_alis_sqlplus.ksh`):**
    *   **Replacement:** BigQuery, with all SQL statements translated to BigQuery Standard SQL.

## 7. Unresolved / Risks
*   **Core SQL Logic:** The most significant unresolved item is the content and migration of `d_ausd_v_ta_action_assoc.sql`. This document only covers the shell wrapper; the SQL script's logic for `ta_action_assoc` is critical and must be analyzed and migrated to BigQuery Standard SQL. This may involve Teradata to BigQuery SQL conversion if it's a Teradata SQL.
*   **Job Control Table Schema:** The exact schema and existing data within the legacy `job_table` and its `active_flag` logic are unknown and require detailed analysis for faithful BigQuery re-implementation.
*   **Error Logging Details:** The full functionality of `DWMSG_MeldeFehler` and the nuances of the `ErrNr` codes are not fully detailed and may require deeper investigation.
*   **`pruefeParameterGesetzt` details:** The exact implementation of this function in `h_alis_parameter.ksh` is not fully known and may require a more detailed rewrite than a simple NULL/empty check for BigQuery parameters.
*   **Orchestration Context:** If `k_ausd_v_ta_action_assoc.ksh` is part of a larger job schedule (e.g., cron, UC4), the overarching orchestration strategy (e.g., Cloud Composer, Cloud Run) needs to be designed to invoke the new BigQuery Stored Procedure.
*   **Data Consistency:** Ensuring atomicity and transactional integrity during job status updates and data processing in the new BigQuery environment needs careful consideration, especially if the original system had specific transactional guarantees.

## 8. Build Plan

1.  **Define Target BigQuery Schemas:**
    *   Create `project.dataset.job_table` (including `job_kennung`, `eintrags_nr`, `active_flag`, `start_ts`, `end_ts`).
    *   Create `project.dataset.error_log` (including `error_ts`, `error_code`, `error_arg`, `procedure_name`, `message`).
    *   Create `project.dataset.job_log` (including `job_kennung`, `eintrags_nr`, `tab_name`, `status`, `record_count`, `start_ts`, `end_ts`).
2.  **Migrate `d_ausd_v_ta_action_assoc.sql`:**
    *   Analyze the SQL content for dependencies, specific database functions, and logic.
    *   Translate to BigQuery Standard SQL. This might result in a new BigQuery Stored Procedure (e.g., `sp_d_ausd_v_ta_action_assoc`) or a set of executable SQL statements.
    *   **Language:** BigQuery SQL
3.  **Create BigQuery Stored Procedure `sp_ausd_v_ta_action_assoc`:**
    *   Implement parameter validation using `IF` statements.
    *   Implement job status updates (`INSERT`/`UPDATE` on `job_table`, `job_log`).
    *   Integrate the migrated logic from `d_ausd_v_ta_action_assoc.sql` (either by calling another procedure or inlining the SQL).
    *   Implement error logging (`INSERT` into `error_log`, `RAISE`).
    *   **Language:** BigQuery SQL
4.  **Develop Orchestration (if applicable):**
    *   If part of a larger workflow, create a Cloud Composer DAG or Cloud Run job to invoke `sp_ausd_v_ta_action_assoc` with appropriate parameters.
    *   **Language:** Python (for Cloud Composer) or relevant runtime for Cloud Run.
5.  **Testing:**
    *   Unit test `sp_ausd_v_ta_action_assoc` with various parameter combinations, including error cases.
    *   Integration test with the migrated `d_ausd_v_ta_action_assoc` logic.
    *   End-to-end testing within the new orchestration framework.