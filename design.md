# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `h_alis_sqlplus.ksh`. The script's primary purpose is to provide helper routines for executing Oracle SQL*Plus scripts. It performs validation checks for script existence and readability, logs invocation details, and then executes the specified SQL*Plus script, returning its exit code. The scope of this migration is to re-implement this functionality within the Google Cloud Platform, targeting BigQuery for data processing and potentially Cloud Functions/Workflows for orchestration, as direct `sqlplus` execution is not supported in BigQuery.

## 2. Source Inventory
The job consists of a single KornShell script.
- **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh`
  - **Technology:** KornShell
  - **Summary:** This KornShell script provides helper routines for executing SQL*Plus scripts, including validation for script existence and basic error handling.
  - **Tier:** Not available (No data from `file_complexity` table).
  - **Automation Bucket:** Not available (No data from `automation_rate` table).

## 3. Target Architecture
The migrated solution will primarily leverage BigQuery for the core logic and potentially Cloud Functions or Cloud Workflows for orchestration of the script execution.

- **BigQuery Stored Procedure:** The `starteSQLSkript` function will be re-implemented as a BigQuery stored procedure, encapsulating the parameter validation, logging, and dynamic SQL execution logic.
- **Script Registry Table (BigQuery):** A BigQuery table (`project.dataset.script_registry`) will store metadata about the SQL scripts that `starteSQLSkript` is intended to invoke. This will include the script name, its BigQuery SQL content (if translated), and a flag for readability/availability.
- **Error Logging Table (BigQuery):** A separate BigQuery table (`project.dataset.error_log`) will capture error messages, replacing the `DWMSG_MeldeFehler` function.
- **Invocation Log Table (BigQuery):** An invocation log table (`project.dataset.invocation_log`) could be used to record details of each `starteSQLSkript` execution, mirroring the `echo` statements.
- **Orchestration (Cloud Functions/Workflows/Composer):** If the original SQL*Plus scripts become BigQuery scripts, an external orchestrator like Cloud Functions or Cloud Workflows could be used to trigger the BigQuery stored procedure, especially if there are external dependencies or complex sequencing.

## 4. Data Flow & Lineage
The original script does not process data directly but acts as an orchestrator for SQL*Plus scripts. In the target architecture, the flow will be:

1. **Invocation:** An external orchestrator (e.g., Cloud Function, Cloud Workflow, or a calling BigQuery script/procedure) invokes the BigQuery stored procedure `project.dataset.starteSQLSkript`.
2. **Parameter Validation:** The stored procedure validates the input parameters (`p_Eintragsnr`, `p_Skript`, `p_Params`).
3. **Script Availability Check:** The procedure queries the `project.dataset.script_registry` table to verify the existence and readability of the target script.
4. **Logging:** Informational messages (like "Rufe SQL*PLUS auf mit folgenden Einstellungen") and error messages are written to BigQuery logging tables (`invocation_log`, `error_log`).
5. **Dynamic SQL Execution:** The stored procedure dynamically executes the BigQuery SQL content associated with `p_Skript`, retrieved from the `script_registry` table, using `EXECUTE IMMEDIATE`.
6. **Error Handling & Return:** Errors during execution are caught, logged, and an appropriate return code is propagated.

There are no direct data inputs or outputs from this specific script to/from other tables, as it primarily manages the execution of other SQL scripts.

## 5. Transformation Logic
The transformation logic for `h_alis_sqlplus.ksh` will primarily involve re-implementing the shell script's control flow and utility functions into BigQuery SQL (for the `starteSQLSkript` procedure) and potentially Python (for external orchestration).

**Original Script Logic:**
- **Parameter capture and shifting:** Uses positional parameters (`$1`, `$2`, `shift`).
- **Parameter validation:** Checks for empty parameters (`-z`).
- **File readability check:** Uses `[ ! -r $p_Skript ]`.
- **Informational output:** Uses `echo` for messages.
- **SQL*Plus invocation:** `sqlplus ${DW_ORAUSER} @$p_Skript $* </dev/null`.
- **Error handling:** Captures `sqlplus` exit code (`$?`) and returns it. Calls external `DWMSG_MeldeFehler` function.
- **Environment variables:** Uses `DW_ORAUSER`, `ModulName`, `ModulVersion`.

**Target BigQuery SQL Logic (`project.dataset.starteSQLSkript`):**
- **Parameters:** Will accept `p_Eintragsnr STRING`, `p_Skript STRING`, `p_Params ARRAY<STRING>`.
- **Variable Declaration:** `DECLARE` statements for `errcode`, `ModulName`, `ModulVersion`.
- **Parameter Validation:** `IF p_Eintragsnr IS NULL OR p_Skript IS NULL ... THEN`
  - Error reporting via `CALL project.dataset.DWMSG_MeldeFehler(...)`.
- **Script Readability Check:** Replaced by a query against the `script_registry` table:
  ```sql
  IF NOT EXISTS (SELECT 1 FROM `project.dataset.script_registry` WHERE script_name = p_Skript AND is_readable = TRUE) THEN
    CALL `project.dataset.DWMSG_MeldeFehler`(...);
    RETURN;
  END IF;
  ```
- **Informational Output:** Replaced by `SELECT` statements (or inserts into a logging table).
- **SQL*Plus Invocation:** This is the core transformation. The `sqlplus` command is replaced by:
  ```sql
  BEGIN
    EXECUTE IMMEDIATE (SELECT script_sql FROM `project.dataset.script_registry` WHERE script_name = p_Skript LIMIT 1);
    SET errcode = 0;
  EXCEPTION WHEN ERROR THEN
    SET errcode = 1; -- Capture BigQuery execution error
  END;
  ```
- **Environment Variables:** Will be passed as parameters to the stored procedure or defined as constants within it. `DW_ORAUSER` needs to be handled contextually in BigQuery (e.g., as part of the service account permissions, or as a parameter if dynamic user context is needed).

## 6. External Dependencies
The original script has the following external dependencies, and their proposed replacements are:

- **`sqlplus` executable:** This is a fundamental dependency for interacting with Oracle databases.
  - **Replacement:** Direct execution of SQL*Plus is not possible in BigQuery. The BigQuery stored procedure will instead execute BigQuery SQL dynamically (`EXECUTE IMMEDIATE`). If the original SQL*Plus scripts contain Oracle-specific SQL, they must be translated to BigQuery SQL dialect. If they interact with an Oracle database, a separate mechanism (e.g., Cloud Data Fusion, database migration services, or direct JDBC/ODBC connection from an external orchestrator) would be needed. The current design assumes the invoked SQL scripts are migrated to BigQuery SQL.
- **`DWMSG_MeldeFehler` function:** An external shell function used for error reporting.
  - **Replacement:** This will be replaced by a BigQuery stored procedure (`project.dataset.DWMSG_MeldeFehler`) that inserts error details into a designated BigQuery error logging table (`project.dataset.error_log`).
- **Oracle Database (`DW_ORAUSER`):** The `sqlplus` command implies an interaction with an Oracle database.
  - **Replacement:** If the underlying SQL scripts are migrated to BigQuery, the Oracle database dependency is removed. If not, then a hybrid approach might be necessary where an external orchestrator (like Cloud Functions or Cloud Run) invokes `sqlplus` remotely or connects to Oracle. For a full BigQuery migration, the `DW_ORAUSER` will become irrelevant as BigQuery permissions are managed via IAM.
- **File System (for script readability check):** The script checks for the existence and readability of the SQL script file.
  - **Replacement:** This will be replaced by querying a BigQuery `script_registry` table for metadata about the script, including its content and availability status.

## 7. Unresolved / Risks
- **SQL*Plus Script Translation:** The most significant unresolved item is the content of the SQL*Plus scripts that `h_alis_sqlplus.ksh` would normally invoke. This design assumes these underlying scripts will be translated to BigQuery SQL and stored in the `script_registry` table. If these scripts are complex or highly Oracle-specific (e.g., PL/SQL), their migration effort will be substantial and is outside the scope of this particular design document.
- **Parameter Handling for Invoked Scripts:** The original script passes `all remaining parameters` to `sqlplus`. The BigQuery `EXECUTE IMMEDIATE` approach needs to carefully reconstruct these parameters for the dynamically executed SQL. The proposed `p_Params ARRAY<STRING>` offers a way to pass these, but the invoked SQL script needs to be designed to handle array parameters or dynamically bind them.
- **Error Code Mapping:** While the design preserves the return of `196` and `201` for internal errors, the `sqlplus` exit codes are generic. The BigQuery `EXCEPTION WHEN ERROR` clause provides a generic error status, but fine-grained mapping of specific Oracle/SQL*Plus error codes to BigQuery error handling might be complex if needed.
- **`file_complexity` and `automation_rate` data:** The absence of data for these tables for the source file indicates a gap in the analysis. This prevents a quantitative assessment of the file's migration complexity and automation potential.

## 8. Build Plan
1. **Define BigQuery Dataset and Tables:**
   - Create the target BigQuery dataset (e.g., `project.dataset`).
   - Create the `script_registry` table:
     ```sql
     CREATE TABLE `project.dataset.script_registry` (
       script_name STRING,
       script_sql STRING,
       is_readable BOOLEAN,
       original_source_path STRING -- Optional: to link back to original file
     );
     ```
   - Create the `error_log` table:
     ```sql
     CREATE TABLE `project.dataset.error_log` (
       entry_number STRING,
       severity STRING,
       error_code INT64,
       message STRING,
       timestamp TIMESTAMP
     );
     ```
   - (Optional) Create `invocation_log` table for auditing/debugging.
2. **Migrate `DWMSG_MeldeFehler`:**
   - Create the `project.dataset.DWMSG_MeldeFehler` stored procedure in BigQuery (BQSQL).
3. **Migrate `h_alis_sqlplus.ksh` function:**
   - Create the `project.dataset.starteSQLSkript` stored procedure in BigQuery (BQSQL) as per the pseudocode provided in the design analysis.
4. **Populate `script_registry`:**
   - Translate all SQL*Plus scripts that `h_alis_sqlplus.ksh` would invoke into BigQuery SQL.
   - Insert these translated scripts and their metadata (name, BQ SQL content, readability) into the `project.dataset.script_registry` table. This is a critical step and needs a separate sub-project for each invoked script.
5. **Develop Orchestration Layer (if needed):**
   - If `starteSQLSkript` needs to be triggered by external events or integrate with other systems, develop Cloud Functions or Cloud Workflows (Python or other supported languages) to call the BigQuery stored procedure.
6. **Testing:** Thoroughly test the BigQuery stored procedure and its integration with the `script_registry` and error logging. Test with valid and invalid script names, and scripts that succeed and fail.