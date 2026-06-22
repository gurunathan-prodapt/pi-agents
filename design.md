# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemeen/is/util/bin/h_alis_sqlplus.ksh

## 1. Purpose & Scope
This migration job focuses on `h_alis_sqlplus.ksh`, a KornShell utility script (`ModulName="alis_sqlplus"`, `ModulVersion="V1.1.3"`). Its primary purpose is to provide a robust wrapper for executing SQL*Plus scripts. The script handles parameter validation, ensures the target SQL script file exists and is readable, logs execution details, and then invokes `sqlplus` with an Oracle user and passes any additional parameters to the SQL script. Error handling is integrated through calls to an external function `DWMSG_MeldeFehler` and by capturing the SQL*Plus exit code.

The scope of this migration covers transforming the KornShell script's logic and its orchestration capabilities to the Google Cloud Platform, specifically utilizing BigQuery stored procedures and associated services for execution. This document does not cover the migration of the actual SQL*Plus scripts that `h_alis_sqlplus.ksh` is designed to execute; those are considered separate migration units.

## 2. Source Inventory
The job `5af228f1` consists of a single source file:

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh`
    *   **Technology:** KornShell (`shell`)
    *   **Purpose:** Orchestration / Utility
    *   **Complexity Tier:** medium
    *   **Automation Bucket:** semi_auto

## 3. Target Architecture
The `h_alis_sqlplus.ksh` script will be re-engineered as a BigQuery Stored Procedure, leveraging BigQuery's SQL scripting capabilities for control flow, parameter handling, and error management.

*   **Core Logic:** A BigQuery Stored Procedure, `project.dataset.starteSQLSkript`, will encapsulate the script's core functionality (parameter validation, logic, error handling).
*   **Script Invocation:** The `sqlplus` invocation will be replaced by dynamic BigQuery SQL execution using `EXECUTE IMMEDIATE`. This implies that the SQL*Plus scripts (which are external to this job's component files) would need to be migrated to BigQuery-compatible SQL scripts or stored procedures themselves and registered in a configuration table.
*   **Metadata/Configuration:** A BigQuery table, `project.dataset.sql_script_registry`, will store metadata about the migratable SQL scripts, including their BigQuery-compatible SQL content (`script_sql`) and a flag indicating their readiness (`is_readable`).
*   **Logging:** The current `echo` statements and calls to `DWMSG_MeldeFehler` will be replaced by `SELECT` statements for immediate output or `INSERT` statements into a BigQuery logging table (e.g., `project.dataset.migration_log`) for persistent storage. An equivalent BigQuery Stored Procedure, `project.dataset.DWMSG_MeldeFehler`, would handle the error logging.
*   **Orchestration:** Cloud Composer (Apache Airflow) or Cloud Workflows can be used to orchestrate the execution of this BigQuery Stored Procedure, especially if `h_alis_sqlplus.ksh` was itself invoked by a larger scheduling system like UC4.

## 4. Data Flow & Lineage
The original `h_alis_sqlplus.ksh` script acts as a wrapper; its direct data flow within itself is minimal. Its primary "data flow" role is to facilitate the execution of other SQL scripts.

**Legacy Flow (Conceptual):**
`Caller` -> `h_alis_sqlplus.ksh` (provides parameters) -> `sqlplus` (uses `DW_ORAUSER`, executes `p_Skript` with `params`) -> `Oracle Database` (reads/writes tables)

**Target BigQuery Flow:**
`Orchestration Layer (e.g., Cloud Composer)` -> `CALL project.dataset.starteSQLSkript(p_Eintragsnr, p_Skript, p_Parameter)`
`project.dataset.starteSQLSkript` procedure:
1.  Validates `p_Eintragsnr` and `p_Skript` parameters.
2.  Checks `project.dataset.sql_script_registry` for `p_Skript`'s existence and readability.
3.  Logs execution details (equivalent of `echo`).
4.  `EXECUTE IMMEDIATE` the BigQuery SQL content (`script_sql`) retrieved from `sql_script_registry` for `p_Skript`, passing `p_Parameter` as dynamic SQL variables or equivalent.
5.  Captures execution status/errors.
6.  Logs errors via `CALL project.dataset.DWMSG_MeldeFehler`.
7.  Returns a status code.

Lineage for this specific job (`h_alis_sqlplus.ksh`) is primarily about its invocation of other SQL scripts. The provided `lineage_edges` show many SQL scripts (e.g., `d_exis_apt_bestandsdaten.sql`, `d_exis_apt_nna_daten.sql`) reading from various Oracle tables like `RPT$TA_S_D1_VERTRAG`, `SOF$TA_BPR_OPTIONEN`, `DWH$VI_C_VERTRAG`, etc. These SQL scripts would be the `p_Skript` argument to `starteSQLSkript`. The `lineage_edges` also suggest higher-level UC4 jobs (e.g., `DW.BERT_ABLAUFSTEUERUNG.xml`) invoke other jobs/scripts, some of which might use this `h_alis_sqlplus.ksh` wrapper.

## 5. Transformation Logic
The transformation will convert the KornShell constructs into BigQuery SQL scripting:

*   **Variable Declarations:** Shell variables (`ModulName`, `ModulVersion`, `p_Eintragsnr`, `p_Skript`, `errcode`) will become `DECLARE`d variables within the BigQuery Stored Procedure.
*   **Parameter Handling:** Shell positional parameters (`$1`, `$2`, `$*`) will map to BigQuery Stored Procedure input parameters. The `shift 2` operation will be handled by accepting an `ARRAY<STRING>` for remaining parameters.
*   **Conditional Logic:** The `if [ -z ... ]` and `if [ ! -r ... ]` checks will be translated to BigQuery `IF ... THEN ... ELSE ... END IF` statements, using `IS NULL OR = ''` for empty string checks.
*   **File Existence Check:** The `[ -r $p_Skript ]` (file readability) will be replaced by querying the `sql_script_registry` table to verify if the script `p_Skript` is registered and marked as runnable.
*   **`sqlplus` Invocation:** This is the core transformation. Instead of calling an external binary, the BigQuery Stored Procedure will dynamically execute the BigQuery-migrated SQL content of the specified script (`p_Skript`) using `EXECUTE IMMEDIATE`.
*   **Error Handling:** The `set +e` / `set -e` pattern will be replaced by a BigQuery `BEGIN ... EXCEPTION WHEN ERROR THEN ... END` block around the `EXECUTE IMMEDIATE` statement to safely capture execution errors.
*   **Logging:** `echo` commands will be converted to `SELECT 'message' AS message;` or `INSERT INTO migration_log (...) VALUES (...)` statements. Calls to `DWMSG_MeldeFehler` will be `CALL`s to an equivalent BigQuery Stored Procedure for error logging.
*   **Return Code:** The shell script's `return $errcode` will map to the BigQuery Stored Procedure implicitly returning the `errcode` variable as its final output or by using `SELECT errcode AS return_code;` within the procedure.

## 6. External Dependencies
The original script has the following external dependencies and their proposed BigQuery replacements:

*   **`sqlplus` executable:** Replaced by BigQuery's native SQL execution engine, specifically `EXECUTE IMMEDIATE` for dynamic SQL.
*   **Oracle Database:** The underlying database that `sqlplus` connects to will be migrated to BigQuery. The SQL scripts executed by `h_alis_sqlplus.ksh` would read/write from tables in BigQuery datasets.
*   **`DW_ORAUSER` environment variable:** The Oracle user might be hardcoded or resolved from environment variables in the legacy system. In BigQuery, this will be implicitly handled by the service account executing the BigQuery job, or it can be a configurable parameter passed to the stored procedure if needed for auditing or multi-tenancy.
*   **`DWMSG_MeldeFehler` function:** This external error reporting function will be replaced by a BigQuery Stored Procedure, `project.dataset.DWMSG_MeldeFehler`, which will log errors to a BigQuery logging table.
*   **SQL script files (e.g., `$p_Skript`) on local filesystem:** The content of these scripts will be migrated to BigQuery-compatible SQL and stored within a BigQuery configuration table (`sql_script_registry`) or as separate BigQuery Stored Procedures. The "file existence" check will become a lookup in this registry table.

## 7. Unresolved / Risks
*   **SQL*Plus Script Content:** The actual SQL logic within the SQL*Plus scripts invoked by `h_alis_sqlplus.ksh` was not part of this job's component files. These scripts will require separate migration efforts to convert Oracle SQL to BigQuery SQL, including data type mapping, function equivalents, and potential performance optimizations. This is the biggest dependency and potential risk.
*   **`DWMSG_MeldeFehler` Implementation:** The exact implementation of `DWMSG_MeldeFehler` is unknown. Its functionality needs to be reverse-engineered and replicated as a BigQuery Stored Procedure.
*   **`p_Parameter` Usage:** The way `p_Parameter` are used within the invoked SQL*Plus scripts is unknown. This needs careful analysis during the migration of those individual SQL scripts to ensure proper parameterization in BigQuery dynamic SQL or stored procedures.
*   **Legacy Scheduler Integration:** If this `h_alis_sqlplus.ksh` job was part of a larger UC4 workflow, the migration of that workflow to Cloud Composer (Airflow) will need to correctly invoke the new BigQuery Stored Procedure and handle its return codes.
*   **`semi_auto` Bucket:** The `semi_auto` migration bucket indicates that some manual intervention or complex re-engineering will be required. This primarily relates to the external SQL scripts and the integration with the broader environment.

## 8. Build Plan
The build plan focuses on creating the BigQuery Stored Procedure and necessary supporting BigQuery objects.

1.  **Define `project.dataset.DWMSG_MeldeFehler` (BigQuery Stored Procedure, BQSQL):**
    *   Create a BigQuery Stored Procedure to replicate the error logging functionality of the original `DWMSG_MeldeFehler` shell function. It should accept parameters equivalent to the original function (e.g., `p_Eintragsnr`, `error_type`, `error_code`, `message`).
    *   This procedure will likely `INSERT` error details into a `project.dataset.migration_log` table.

2.  **Define `project.dataset.sql_script_registry` (BigQuery Table, DDL):**
    *   Create a BigQuery table to store metadata about the SQL scripts that `starteSQLSkript` will execute.
    *   **Schema:**
        *   `script_name` (STRING, PRIMARY KEY): e.g., 'path/to/script.sql'
        *   `script_sql` (STRING): The BigQuery-compatible SQL content of the script.
        *   `is_readable` (BOOL): Flag indicating if the script is ready for execution.
        *   `last_updated` (TIMESTAMP)

3.  **Define `project.dataset.starteSQLSkript` (BigQuery Stored Procedure, BQSQL):**
    *   Create the main BigQuery Stored Procedure based on the pseudocode provided by the CM MCP tool.

    ```sql
    CREATE OR REPLACE PROCEDURE `project.dataset.starteSQLSkript`(
      p_Eintragsnr STRING,
      p_Skript STRING,
      p_Parameter ARRAY<STRING>
    )
    BEGIN
      DECLARE Modul_Name STRING DEFAULT 'alis_sqlplus';
      DECLARE Modul_Version STRING DEFAULT 'V1.1.3';
      DECLARE errcode INT64 DEFAULT 0;
      DECLARE p_Parameter_String STRING DEFAULT '';
      DECLARE script_exists BOOL DEFAULT FALSE;
      DECLARE sql_to_execute STRING;

      SET p_Parameter_String = ARRAY_TO_STRING(p_Parameter, ' ');

      IF p_Eintragsnr IS NULL OR p_Eintragsnr = '' OR p_Skript IS NULL OR p_Skript = '' THEN
        CALL `project.dataset.DWMSG_MeldeFehler`(
          p_Eintragsnr,
          'E',
          196,
          CONCAT(Modul_Name, ' ', Modul_Version, ' starteSQLSkript')
        );
        SELECT 196 AS return_code;
        LEAVE;
      END IF;

      SET script_exists = (
        SELECT COUNT(1) > 0
        FROM `project.dataset.sql_script_registry`
        WHERE script_name = p_Skript
          AND is_readable = TRUE
      );

      IF NOT script_exists THEN
        CALL `project.dataset.DWMSG_MeldeFehler`(
          p_Eintragsnr,
          'E',
          201,
          p_Skript
        );
        SELECT 201 AS return_code;
        LEAVE;
      END IF;

      SELECT 'Rufe SQL*PLUS auf mit folgenden Einstellungen' AS message;
      SELECT CONCAT('Sql*Plus-Skript : ', p_Skript) AS message;
      SELECT CONCAT('Skript-Parameter: ', p_Parameter_String) AS message;

      -- Retrieve the BigQuery SQL content
      SET sql_to_execute = (
        SELECT script_sql
        FROM `project.dataset.sql_script_registry`
        WHERE script_name = p_Skript
        LIMIT 1
      );

      BEGIN
        -- Execute the migrated BigQuery SQL logic
        EXECUTE IMMEDIATE sql_to_execute;

        SET errcode = 0;
      EXCEPTION WHEN ERROR THEN
        SET errcode = 1;
        -- Log the actual error that occurred during EXECUTE IMMEDIATE
        CALL `project.dataset.DWMSG_MeldeFehler`(
          p_Eintragsnr,
          'E',
          -1, -- Placeholder for dynamic SQL execution error
          CONCAT('Error executing script ', p_Skript, ': ', @@error.message)
        );
      END;

      SELECT errcode AS return_code;
    END;
    ```

4.  **Populate `project.dataset.sql_script_registry` (Data Insertion, BQSQL):**
    *   As individual SQL*Plus scripts are migrated to BigQuery SQL, insert their content and metadata into this registry table.

5.  **Develop Orchestration (e.g., Cloud Composer DAG, Python):**
    *   Create a Cloud Composer DAG (if part of a larger workflow) to call `project.dataset.starteSQLSkript`.
    *   The DAG would pass the required `p_Eintragsnr`, `p_Skript`, and `p_Parameter` as arguments to the BigQuery Stored Procedure call.