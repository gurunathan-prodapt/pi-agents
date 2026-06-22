```markdown
# Migration Notes: `h_alis_sqlplus.ksh`

## 1. Summary

This migration job re-engineers the `h_alis_sqlplus.ksh` KornShell utility script, which served as a wrapper for executing SQL*Plus scripts on an Oracle database. The migration targets Google Cloud Platform, specifically BigQuery.

The original script's core logic, including parameter validation, script existence checks, and error handling, has been transformed into a BigQuery Stored Procedure named `project.dataset.starteSQLSkript`. The invocation of `sqlplus` has been replaced by dynamic BigQuery SQL execution using `EXECUTE IMMEDIATE`. The external SQL*Plus script files are now managed as BigQuery-compatible SQL content stored in a BigQuery configuration table (`project.dataset.sql_script_registry`). Error logging, previously handled by `DWMSG_MeldeFehler`, is now managed by an equivalent BigQuery Stored Procedure.

This migration focuses on the wrapper's functionality; the actual SQL*Plus scripts it used to execute are considered separate migration units and must be converted to BigQuery SQL and registered in the `sql_script_registry` table.

## 2. Generated Artifacts

The migration process has generated the following BigQuery SQL files:

*   **`bqsql/project/dataset/DWMSG_MeldeFehler.sql`**
    *   **Role:** Defines a BigQuery Stored Procedure (`project.dataset.DWMSG_MeldeFehler`) that replicates the error logging functionality of the original `DWMSG_MeldeFehler` shell function. It inserts error details into a `project.dataset.migration_log` table. This provides a centralized and persistent logging mechanism within BigQuery.
*   **`bqsql/project/dataset/sql_script_registry_ddl.sql`**
    *   **Role:** Defines the Data Definition Language (DDL) for the `project.dataset.sql_script_registry` BigQuery table. This table serves as the central repository for metadata and the BigQuery-compatible SQL content of the scripts that `starteSQLSkript` will execute. It replaces the file system-based storage and readability checks of the original KornShell script.
*   **`bqsql/project/dataset/starteSQLSkript.sql`**
    *   **Role:** Defines the main BigQuery Stored Procedure (`project.dataset.starteSQLSkript`). This is the re-engineered version of the `h_alis_sqlplus.ksh` script. It handles parameter validation, checks for script existence and readability in `sql_script_registry`, logs execution details, and dynamically executes the BigQuery SQL content of the specified script using `EXECUTE IMMEDIATE`. It also incorporates error handling and calls `DWMSG_MeldeFehler` for logging.
*   **`bqsql/project/dataset/sql_script_registry_sample_data.sql`**
    *   **Role:** Provides sample `INSERT` statements for populating the `project.dataset.sql_script_registry` table. This file is a template to guide the manual process of registering migrated SQL scripts. It demonstrates how to add `script_name`, `script_sql`, `is_readable` status, and `last_updated` timestamps for each script.

## 3. Key Design Decisions

*   **BigQuery Stored Procedure for Core Logic:** The entire orchestration logic of `h_alis_sqlplus.ksh` (parameter validation, conditional checks, error handling) was translated into a BigQuery Stored Procedure (`starteSQLSkript`). This leverages BigQuery's native SQL scripting capabilities, providing a serverless, scalable, and managed execution environment, eliminating the need for external compute instances for script execution.
*   **`EXECUTE IMMEDIATE` for Dynamic SQL Execution:** The `sqlplus` invocation, which executed external SQL files, was replaced by `EXECUTE IMMEDIATE` within BigQuery. This allows `starteSQLSkript` to dynamically execute BigQuery-compatible SQL content retrieved from a configuration table, maintaining the dynamic nature of the original wrapper.
*   **`sql_script_registry` Table for Script Management:** Instead of relying on a file system for SQL script storage and readability checks, a BigQuery table (`sql_script_registry`) was introduced. This centralizes script content and metadata, simplifies management, and allows for programmatic checks of script existence and readiness (`is_readable`). This also decouples the script content from the execution logic.
*   **Dedicated BigQuery Stored Procedure for Error Logging:** The `DWMSG_MeldeFehler` function was re-implemented as a BigQuery Stored Procedure (`project.dataset.DWMSG_MeldeFehler`). This ensures consistent, structured, and persistent error logging directly within BigQuery, facilitating monitoring and debugging. It writes to a `migration_log` table, which can be easily queried and integrated with GCP logging services.
*   **Parameter Handling via `ARRAY<STRING>`:** The original KornShell script accepted positional parameters. In BigQuery, these are mapped to explicit stored procedure parameters, with the variable number of additional parameters (`$*` in shell) being handled by an `ARRAY<STRING>`. This provides type safety and clarity in parameter passing.
*   **Trade-off: External SQL Script Migration:** A significant design decision was to *not* include the migration of the actual SQL*Plus script content within this job. This keeps the scope focused on the wrapper's functionality but introduces a critical dependency: the `sql_script_registry` must be populated with correctly migrated BigQuery SQL before `starteSQLSkript` can function end-to-end. This was deemed necessary due to the `semi_auto` complexity tier and the diverse nature of potential SQL*Plus scripts.

## 4. Manual Steps Before Go-Live

Before `project.dataset.starteSQLSkript` can be used in a production environment, several manual steps are required:

1.  **Create BigQuery Dataset:** Ensure the target BigQuery dataset (`project.dataset`) exists. If not, create it:
    ```sql
    CREATE SCHEMA `project.dataset` OPTIONS(location='US'); -- Adjust location as needed
    ```
2.  **Create `migration_log` Table:** The `DWMSG_MeldeFehler` procedure relies on a `project.dataset.migration_log` table. Create this table with an appropriate schema:
    ```sql
    CREATE OR REPLACE TABLE `project.dataset.migration_log` (
      log_timestamp TIMESTAMP NOT NULL,
      job_id STRING,
      log_level STRING,
      error_code INT64,
      message STRING
    )
    PARTITION BY DATE(log_timestamp)
    CLUSTER BY job_id, log_level;
    ```
3.  **Deploy Generated Artifacts:**
    *   Execute `bqsql/project/dataset/DWMSG_MeldeFehler.sql` to create the error logging procedure.
    *   Execute `bqsql/project/dataset/sql_script_registry_ddl.sql` to create the script registry table.
    *   Execute `bqsql/project/dataset/starteSQLSkript.sql` to create the main orchestration procedure.
4.  **Migrate and Populate `sql_script_registry`:**
    *   **Crucial Step:** Identify all SQL*Plus scripts that were previously executed by `h_alis_sqlplus.ksh`.
    *   For each identified SQL*Plus script:
        *   Manually convert its Oracle SQL syntax to BigQuery-compatible SQL. This includes data type mapping, function equivalents, and query structure adjustments.
        *   Determine how parameters (`p_Parameter`) passed to `h_alis_sqlplus.ksh` were used within the SQL*Plus script. If the BigQuery-migrated script requires parameters, the `script_sql` content in `sql_script_registry` must be designed to handle them (e.g., by using `DECLARE` variables and `EXECUTE IMMEDIATE` with `USING` clauses, or by calling another BigQuery Stored Procedure that accepts parameters). The current `starteSQLSkript` implementation passes `p_Parameter` as an `ARRAY<STRING>` but does not directly inject them into the `script_sql` content. This will require careful consideration during the individual script migrations.
        *   Insert the `script_name` (e.g., the original file path), the converted `script_sql`, and set `is_readable` to `TRUE` into the `project.dataset.sql_script_registry` table. Use `bqsql/project/dataset/sql_script_registry_sample_data.sql` as a guide.
5.  **IAM Permissions:** Ensure the Google Cloud service account or user executing the `starteSQLSkript` procedure has the necessary BigQuery permissions:
    *   `bigquery.jobs.create` (to run queries and procedures)
    *   `bigquery.tables.getData` (to read from `sql_script_registry`)
    *   `bigquery.tables.updateData` (to insert into `migration_log`)
    *   `bigquery.routines.call` (to call `DWMSG_MeldeFehler` and `starteSQLSkript` itself)
    *   Permissions to read/write from any tables accessed by the dynamically executed `script_sql`.
6.  **Orchestration Integration:** If `h_alis_sqlplus.ksh` was part of a larger scheduling system (e.g., UC4), integrate the new `starteSQLSkript` procedure into the new orchestration layer (e.g., Cloud Composer/Airflow DAGs). This involves creating tasks that call the BigQuery Stored Procedure and handle its `return_code`.

## 5. Known Gaps & Unresolved References

*   **SQL*Plus Script Content Migration:** The most significant gap is the actual content of the SQL*Plus scripts that `h_alis_sqlplus.ksh` used to execute. These scripts were not part of this job's component files and require separate, dedicated migration efforts to convert Oracle SQL to BigQuery SQL. This includes handling data type differences, function equivalences, and potential architectural changes.
*   **`p_Parameter` Usage within Dynamic SQL:** The current `starteSQLSkript` procedure passes `p_Parameter` as an `ARRAY<STRING>` but does not explicitly inject these parameters into the `script_sql` content retrieved from `sql_script_registry`. If the migrated BigQuery SQL scripts require these parameters, the `script_sql` itself must be designed to accept them (e.g., as part of a `CALL` to another procedure, or by constructing dynamic SQL within `script_sql` that uses `EXECUTE IMMEDIATE` with `USING` clauses). This requires careful analysis during the migration of individual SQL scripts.
*   **`DWMSG_MeldeFehler` Full Fidelity:** While a BigQuery Stored Procedure for `DWMSG_MeldeFehler` has been created, its exact original implementation details (e.g., what specific information it logged, where it logged it, any side effects) are unknown. The current implementation logs to a `migration_log` table, which is a reasonable approximation, but a full fidelity replication might require more details.
*   **Legacy Scheduler Integration:** The migration of the broader scheduling system (e.g., UC4) that invoked `h_alis_sqlplus.ksh` is outside the scope of this document. The new orchestration layer (e.g., Cloud Composer) must be configured to correctly call `starteSQLSkript` and interpret its `return_code`.
*   **Error Code Mapping:** The `DWMSG_MeldeFehler` call for dynamic SQL execution errors uses `-1` as a placeholder. Ideally, specific BigQuery error codes or more descriptive error messages should be captured and logged for better diagnostics.

## 6. Validation

To validate the migrated `starteSQLSkript` procedure, follow these steps:

1.  **Prerequisites:**
    *   Ensure all generated BigQuery objects (`DWMSG_MeldeFehler` procedure, `sql_script_registry` table, `starteSQLSkript` procedure, `migration_log` table) are deployed.
    *   Populate `sql_script_registry` with at least one valid, BigQuery-compatible SQL script (e.g., a simple `SELECT 1;` or a script that queries a test table) and one invalid/non-existent script entry.
2.  **Test Cases:**

    *   **Case 1: Valid Script Execution (Success)**
        *   **Action:** Call `starteSQLSkript` with a valid `p_Skript` that exists in `sql_script_registry` and has `is_readable = TRUE`.
        ```sql
        CALL `project.dataset.starteSQLSkript`('TEST_JOB_001', 'path/to/first_migrated_script.sql', ['param1', 'param2']);
        ```
        *   **Expected Outcome:**
            *   The call should complete successfully.
            *   The output should include informational messages (e.g., "Rufe SQL*PLUS auf...", "Skript-Pfad...", "Skript-Parameter...").
            *   The final `SELECT errcode AS return_code;` should return `0`.
            *   The `script_sql` content of `path/to/first_migrated_script.sql` should have been executed.
            *   No error entries should be found in `project.dataset.migration_log` for `TEST_JOB_001`.

    *   **Case 2: Missing `p_Eintragsnr` or `p_Skript` (Validation Error)**
        *   **Action:** Call `starteSQLSkript` with `NULL` or empty values for `p_Eintragsnr` or `p_Skript`.
        ```sql
        CALL `project.dataset.starteSQLSkript`(NULL, 'path/to/some_script.sql', []);
        CALL `project.dataset.starteSQLSkript`('TEST_JOB_002', '', []);
        ```
        *   **Expected Outcome:**
            *   The call should terminate early.
            *   The final `SELECT errcode AS return_code;` should return `196`.
            *   An entry should be found in `project.dataset.migration_log` for `TEST_JOB_002` (or `NULL` for the first call) with `log_level='E'`, `error_code=196`, and a message indicating missing parameters.

    *   **Case 3: Script Not Found or Not Readable (Validation Error)**
        *   **Action:** Call `starteSQLSkript` with a `p_Skript` that is not in `sql_script_registry` or has `is_readable = FALSE`.
        ```sql
        CALL `project.dataset.starteSQLSkript`('TEST_JOB_003', 'non_existent_script.sql', []);
        CALL `project.dataset.starteSQLSkript`('TEST_JOB_004', 'path/to/yet_another_script.sql', []); -- Assuming 'yet_another_script.sql' has is_readable=FALSE
        ```
        *   **Expected Outcome:**
            *   The call should terminate early.
            *   The final `SELECT errcode AS return_code;` should return `201`.
            *   An entry should be found in `project.dataset.migration_log` for `TEST_JOB_003` or `TEST_JOB_004` with `log_level='E'`, `error_code=201`, and a message indicating the script was not found or not readable.

    *   **Case 4: Error During Dynamic SQL Execution (Runtime Error)**
        *   **Action:** Update `sql_script_registry` to include a script with intentionally malformed BigQuery SQL (e.g., `SELECT * FROM non_existent_table;`). Then call `starteSQLSkript` with this script.
        ```sql
        -- First, update registry (manual step or via SQL)
        INSERT INTO `project.dataset.sql_script_registry` (script_name, script_sql, is_readable, last_updated)
        VALUES ('path/to/error_script.sql', 'SELECT * FROM `project.dataset.non_existent_table`;', TRUE, CURRENT_TIMESTAMP());

        -- Then, call the procedure
        CALL `project.dataset.starteSQLSkript`('TEST_JOB_005', 'path/to/error_script.sql', []);
        ```
        *   **Expected Outcome:**
            *   The call should complete, but the `EXECUTE IMMEDIATE` block should catch an error.
            *   The final `SELECT errcode AS return_code;` should return `1`.
            *   An entry should be found in `project.dataset.migration_log` for `TEST_JOB_005` with `log_level='E'`, `error_code=-1`, and a message detailing the BigQuery execution error (e.g., "Not found: Table project.dataset.non_existent_table").

3.  **"Passing" Criteria:**
    *   All test cases execute as expected, returning the correct `return_code`.
    *   Informational messages are displayed correctly.
    *   Error conditions are correctly logged in `project.dataset.migration_log` with appropriate `log_level`, `error_code`, and `message`.
    *   No unexpected BigQuery errors occur during the execution of `starteSQLSkript` itself.

## 7. Rollback Procedure

In case of issues during or after the deployment of the migrated `h_alis_sqlplus.ksh` functionality, the following rollback procedure can be followed:

1.  **Stop New Invocations:** Immediately halt any new scheduled or manual invocations of `project.dataset.starteSQLSkript` from your orchestration layer (e.g., pause Cloud Composer DAGs, disable Cloud Workflows).
2.  **Revert Orchestration:** If the orchestration layer (e.g., Cloud Composer) was modified to call `starteSQLSkript`, revert these changes to point back to the original `h_alis_sqlplus.ksh` script or its legacy scheduling mechanism.
3.  **Delete BigQuery Stored Procedures:** Remove the newly deployed BigQuery Stored Procedures:
    ```sql
    DROP PROCEDURE IF EXISTS `project.dataset.starteSQLSkript`;
    DROP PROCEDURE IF EXISTS `project.dataset.DWMSG_MeldeFehler`;
    ```
4.  **Delete BigQuery Tables (Optional, but Recommended for Cleanliness):**
    *   If the `sql_script_registry` table was only used for this migration and contains no other critical data, delete it.
    *   The `migration_log` table might be shared by other migrations. If it's specific to this migration, delete it. Otherwise, consider truncating relevant entries.
    ```sql
    DROP TABLE IF EXISTS `project.dataset.sql_script_registry`;
    -- DROP TABLE IF EXISTS `project.dataset.migration_log`; -- Only if not shared
    ```
5.  **Restore Legacy Environment:** Ensure the original `h_alis_sqlplus.ksh` script and its dependencies (Oracle database, `sqlplus` executable, `DWMSG_MeldeFehler` function, original SQL script files) are fully operational and accessible to the legacy scheduling system.
6.  **Verify Legacy Operations:** Run a few test jobs using the original `h_alis_sqlplus.ksh` to confirm that the rollback was successful and the legacy system is functioning as expected.
7.  **Analyze and Rectify:** Investigate the root cause of the rollback. This might involve re-evaluating the BigQuery SQL conversion of the individual SQL scripts, adjusting IAM permissions, or refining the logic within `starteSQLSkript` or `DWMSG_MeldeFehler`. Once the issues are resolved, a new migration attempt can be planned.