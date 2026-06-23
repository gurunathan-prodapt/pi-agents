# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh

## 1. Purpose & Scope

The `h_alis_sqlplus.ksh` script is a KornShell utility designed to act as a wrapper for executing Oracle SQL*Plus scripts. Its primary function is to encapsulate common logic such as validating input parameters, ensuring the target SQL script file exists and is readable, and providing basic error handling before invoking `sqlplus`. It logs the invocation details and returns the exit code of the `sqlplus` command. This script does not contain core business logic or data transformations itself; rather, it facilitates the execution of other SQL scripts that likely perform such operations. Its business purpose is to standardize and make robust the execution of SQL*Plus routines within a broader ETL or data processing workflow.

The scope of this migration design is to replace the functionality of this KornShell utility script with equivalent components within the Google Cloud / BigQuery ecosystem. The target platform is BigQuery.

## 2. Source Inventory

The migration involves a single source file:

*   **File Name**: `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh`
*   **Technology**: KornShell
*   **Summary**: This KornShell script provides helper routines for executing SQL*Plus scripts, including validation for script existence and basic error handling.
*   **Complexity Tier**: medium
*   **Automation Bucket**: semi_auto
*   **Migration Flags**: None identified.

## 3. Target Architecture

The target architecture for this utility script will leverage BigQuery's capabilities for stored procedures and potentially Cloud Composer (Airflow) for orchestration, along with BigQuery tables for metadata and logging.

*   **BigQuery Stored Procedure**: The core logic of `starteSQLSkript` (parameter validation, logging, and invoking the SQL logic) will be implemented as a BigQuery SQL stored procedure. This procedure will accept parameters equivalent to the original shell script's arguments.
*   **SQL Script Registry Table**: To replace the filesystem check for script existence and readability, a BigQuery table (e.g., `project.dataset.sql_script_registry`) will store metadata about the migrated SQL scripts, including their names and status (e.g., `is_readable`).
*   **Error/Execution Log Tables**: Dedicated BigQuery tables (e.g., `project.dataset.error_log`, `project.dataset.execution_log`) will capture error messages and execution details, replacing the shell's `echo` statements and the `DWMSG_MeldeFehler` function.
*   **Migrated SQL Logic**: The actual SQL*Plus scripts that `h_alis_sqlplus.ksh` would originally execute will be migrated to BigQuery SQL stored procedures or standard BigQuery SQL scripts. The `starteSQLSkript` equivalent procedure will `CALL` these migrated BigQuery SQL procedures.
*   **Orchestration (Optional/Context Dependent)**: If this utility script was part of a larger workflow orchestrated by tools like UC4 (as hinted by other `lineage_edges`), Cloud Composer (Airflow) would be the suitable replacement to schedule and manage the execution of these BigQuery stored procedures and other BigQuery jobs.

## 4. Data Flow & Lineage

The original `h_alis_sqlplus.ksh` script is a utility wrapper; it doesn't directly process data but orchestrates the execution of other SQL*Plus scripts that do.

**Original Flow:**
Other scripts/processes (e.g., UC4 jobs) -> INVOKES `h_alis_sqlplus.ksh` with parameters (entry number, SQL script name, script arguments) -> `h_alis_sqlplus.ksh` validates and logs -> `h_alis_sqlplus.ksh` INVOKES `sqlplus` with specific Oracle user, SQL script, and arguments -> SQL*Plus executes against Oracle database (READS/WRITES TABLEs) -> `h_alis_sqlplus.ksh` captures and returns exit code.

The `lineage_edges` show many SQL files reading from and writing to tables (e.g., `TABLE:RPT$TA_S_D1_VERTRAG`, `TABLE:DWH$VI_C_VERTRAG`). These represent the underlying data interactions that the original `h_alis_sqlplus.ksh` orchestrated. For example, files like `vobs/dw_source/isdwh/exporter/apt/sql/d_exis_apt_bestandsdaten.sql` are invoked by other scripts (not directly by `h_alis_sqlplus.ksh` in the provided lineage for this job, but this is the general pattern), and these SQL files perform the actual data operations.

**Target Flow (BigQuery):**
Orchestration (e.g., Cloud Composer DAG) -> CALLS `project.dataset.starteSQLSkript` (BigQuery Stored Procedure) with parameters -> `project.dataset.starteSQLSkript` validates parameters against `project.dataset.sql_script_registry` and logs to `project.dataset.execution_log`/`error_log` -> `project.dataset.starteSQLSkript` CALLS the migrated BigQuery stored procedure (e.g., `project.dataset.migrated_bestandsdaten_proc`) that contains the translated logic of the original `d_exis_apt_bestandsdaten.sql` -> Migrated BigQuery stored procedure READS/WRITES data from/to BigQuery tables.

## 5. Transformation Logic

The KornShell script's logic will be transformed into BigQuery SQL (for the stored procedure and associated tables) and potentially Python (for orchestration).

**Original KornShell (`h_alis_sqlplus.ksh`):**

*   **Function Definition**: `starteSQLSkript() { ... }`
*   **Parameter Validation**: `if [ -z "$p_Eintragsnr" -o -z "$p_Skript" ]`
*   **File Readability Check**: `if [ ! -r $p_Skript ]`
*   **Error Reporting**: `DWMSG_MeldeFehler $p_Eintragsnr E 196 "..."`
*   **Logging**: `echo "Rufe SQL*PLUS auf..."`, `echo "Sql*Plus-Skript : $p_Skript"`, `echo "Skript-Parameter: $*"`
*   **SQL*Plus Execution**: `sqlplus ${DW_ORAUSER} @$p_Skript $* </dev/null`
*   **Error Code Capture**: `errcode=$?`
*   **Shell Error Control**: `set +e`, `set -e`

**Target BigQuery SQL (Stored Procedure `project.dataset.starteSQLSkript`):**

*   **Procedure Definition**: `CREATE OR REPLACE PROCEDURE project.dataset.starteSQLSkript(p_Eintragsnr STRING, p_Skript STRING, p_Params ARRAY<STRING>)`
*   **Parameter Validation**: `IF p_Eintragsnr IS NULL OR p_Eintragsnr = '' OR p_Skript IS NULL OR p_Skript = '' THEN RAISE USING MESSAGE = '196'; END IF;`
*   **Script Existence/Readability Check**: Replaced by a lookup in a BigQuery metadata table:
    ```sql
    IF NOT EXISTS (SELECT 1 FROM `project.dataset.sql_script_registry` WHERE script_name = p_Skript AND is_readable = TRUE) THEN
      RAISE USING MESSAGE = '201';
    END IF;
    ```
*   **Error Reporting**: Replaced by `INSERT` statements into an error log table and `RAISE` to signal errors:
    ```sql
    INSERT INTO `project.dataset.error_log` (entry_nr, severity, error_code, message, module_name, module_version, created_at) VALUES (...);
    RAISE USING MESSAGE = '196';
    ```
*   **Logging**: Replaced by `INSERT` statements into an execution log table:
    ```sql
    INSERT INTO `project.dataset.execution_log` (module_name, module_version, entry_nr, script_name, script_params, log_message, created_at) VALUES (...);
    ```
*   **SQL*Plus Execution**: This is the core transformation. The original SQL*Plus script (`$p_Skript`) must be translated into a BigQuery SQL stored procedure. The `starteSQLSkript` procedure will then `CALL` this migrated BigQuery procedure. For example: `CALL `project.dataset.migrated_sql_script_proc`(p_Eintragsnr, p_Skript, p_Params);`
*   **Error Code Capture**: BigQuery stored procedures don't explicitly "return" an error code in the same way a shell script does. Error conditions will be handled using `RAISE` statements and can be captured by the calling orchestration layer. An `OUT` parameter or logging to an audit table could also be used to signify success/failure.
*   **Shell Error Control (`set +e`/`-e`)**: BigQuery scripting handles errors through `BEGIN...EXCEPTION...END` blocks, which manage execution flow on error.

## 6. External Dependencies

The original `h_alis_sqlplus.ksh` script has the following external dependencies:

*   **Oracle SQL*Plus Client**: This is implicitly invoked by the script to execute SQL against an Oracle database.
    *   **Replacement in BigQuery**: The Oracle SQL*Plus client will be replaced by BigQuery's native SQL engine. The SQL within the original SQL*Plus scripts will be translated into BigQuery-compatible SQL and deployed as BigQuery stored procedures or scheduled queries.
*   **Oracle Database**: The `sqlplus` command connects to an Oracle database (implied by `DW_ORAUSER` and the nature of SQL*Plus).
    *   **Replacement in BigQuery**: The Oracle database will be replaced by BigQuery datasets and tables. Data will be migrated from Oracle to BigQuery.
*   **`DWMSG_MeldeFehler` function**: An external shell function used for error reporting.
    *   **Replacement in BigQuery**: This will be replaced by `INSERT` statements into a BigQuery `error_log` table, centralizing error reporting within the BigQuery environment.
*   **Filesystem**: The script checks for the existence and readability of SQL script files on the local filesystem.
    *   **Replacement in BigQuery**: This will be replaced by a BigQuery `sql_script_registry` metadata table that stores information about available BigQuery stored procedures or scripts.

## 7. Unresolved / Risks

*   **SQL*Plus Specific Features**: Any Oracle-specific SQL, PL/SQL blocks, or SQL*Plus commands (e.g., `SET SERVEROUTPUT ON`, `DEFINE`) within the invoked SQL scripts will need careful manual review and translation to their BigQuery equivalents. This is a general risk for SQL migrations and not specific to the `h_alis_sqlplus.ksh` wrapper itself.
*   **Dynamic SQL Generation**: If the `sqlplus` command line involves complex, dynamically generated SQL or script names, the migration will need to ensure this dynamic behavior is correctly replicated in BigQuery stored procedures or through Python scripting. The current script shows `sqlplus ... @$p_Skript $*`, which implies `p_Skript` is a direct file path and `$*` are direct parameters.
*   **Orchestration Context**: The `lineage_edges` output suggests a broader UC4 orchestration environment. The specific triggers or scheduling mechanisms that called `h_alis_sqlplus.ksh` will need to be identified and migrated (e.g., to Cloud Composer DAGs).
*   **`DW_ORAUSER`**: The value of this environment variable and how it's managed will need to be translated into appropriate BigQuery connection configurations or service account usage.

## 8. Build Plan

The migration will involve the following steps and generated components:

1.  **Create BigQuery `error_log` Table**:
    *   **Language**: BigQuery SQL (DDL)
    *   **Purpose**: Centralized logging for errors.
2.  **Create BigQuery `execution_log` Table**:
    *   **Language**: BigQuery SQL (DDL)
    *   **Purpose**: Centralized logging for script execution details.
3.  **Create BigQuery `sql_script_registry` Table**:
    *   **Language**: BigQuery SQL (DDL + DML for initial population)
    *   **Purpose**: Store metadata about migrated BigQuery SQL scripts/procedures, replacing filesystem checks.
4.  **Generate BigQuery Stored Procedure for `starteSQLSkript`**:
    *   **Language**: BigQuery SQL (Stored Procedure DDL)
    *   **Purpose**: Implement the parameter validation, logging, and dynamic invocation logic of the original KornShell script.
    *   **File Name (Example)**: `bq_starte_sql_skript_proc.sql`
5.  **Migrate Oracle SQL*Plus Scripts**:
    *   **Language**: BigQuery SQL (Stored Procedure DDL or SQL scripts)
    *   **Purpose**: Translate each SQL*Plus script previously invoked by `h_alis_sqlplus.ksh` into a BigQuery-compatible stored procedure or SQL script.
    *   **File Names (Examples)**: `bq_d_exis_apt_bestandsdaten_proc.sql`, `bq_d_exis_apt_nna_daten_proc.sql`, etc. (one for each SQL file identified in the `lineage_edges` that this script would orchestrate, or by examining the `$p_Skript` inputs in the calling contexts).
6.  **Develop Cloud Composer DAG (if applicable)**:
    *   **Language**: Python
    *   **Purpose**: To orchestrate the execution of the new BigQuery stored procedures, replacing the original UC4 jobs or shell call mechanisms.
    *   **File Name (Example)**: `airflow_main_dag.py`