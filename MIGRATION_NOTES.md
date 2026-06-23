```markdown
# MIGRATION_NOTES.md

## 1. Summary

The KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh`, which serves as a helper for executing Oracle SQL*Plus scripts, has been migrated.

The migration targets Google Cloud Platform, specifically:
*   **BigQuery Stored Procedures** for the core logic of validating parameters, checking script availability, and executing dynamic SQL.
*   **BigQuery Tables** for storing script metadata (`script_registry`) and logging errors (`error_log`).
*   **Cloud Functions/Workflows/Composer** as potential orchestration layers, replacing the shell script's direct execution context.

The primary goal was to re-implement the script's functionality in a cloud-native, serverless environment, moving away from direct `sqlplus` execution which is not supported in BigQuery.

## 2. Generated artifacts

The migration process generated the following BigQuery SQL artifacts:

*   **`project.dataset.script_registry.sql`**:
    *   **Role**: Defines a BigQuery table (`project.dataset.script_registry`) to store metadata about the SQL scripts that `starteSQLSkript` is intended to invoke. This includes the script's name, its BigQuery SQL content (after translation from Oracle SQL*Plus), and a flag indicating its readability/availability. This table replaces the file system checks (`[ ! -r $p_Skript ]`) of the original KornShell script.
*   **`project.dataset.error_log.sql`**:
    *   **Role**: Defines a BigQuery table (`project.dataset.error_log`) to capture error messages generated during the execution of the `starteSQLSkript` procedure or its invoked scripts. This table replaces the functionality of the `DWMSG_MeldeFehler` shell function.
*   **`project.dataset.DWMSG_MeldeFehler.sql`**:
    *   **Role**: Defines a BigQuery stored procedure (`project.dataset.DWMSG_MeldeFehler`) that inserts error details into the `project.dataset.error_log` table. This procedure directly replaces the external `DWMSG_MeldeFehler` shell function.
*   **`project.dataset.starteSQLSkript.sql`**:
    *   **Role**: Defines the main BigQuery stored procedure (`project.dataset.starteSQLSkript`) that encapsulates the core logic of the original `h_alis_sqlplus.ksh` script. It handles parameter validation, checks script availability against `script_registry`, logs informational and error messages, and dynamically executes the BigQuery SQL content of the specified script using `EXECUTE IMMEDIATE`. This is the direct replacement for the KornShell script.

## 3. Key design decisions

*   **Replacement of `sqlplus` with BigQuery Dynamic SQL**: The most fundamental decision was to replace the `sqlplus` executable with BigQuery's `EXECUTE IMMEDIATE` statement. This was necessary because direct Oracle `sqlplus` execution is not supported within BigQuery. This implies that all underlying SQL*Plus scripts must be translated to BigQuery SQL and stored as content in the `script_registry` table.
*   **Metadata-driven Script Management**: Instead of relying on a file system for script existence and readability checks, a BigQuery table (`script_registry`) was introduced. This centralizes script management, allows for versioning (if extended), and aligns with BigQuery's data-centric approach.
*   **BigQuery Native Logging**: The original script's `echo` statements and calls to `DWMSG_MeldeFehler` were replaced with inserts into dedicated BigQuery logging tables (`error_log`) via a BigQuery stored procedure (`DWMSG_MeldeFehler`). This provides structured, queryable logs within the BigQuery ecosystem.
*   **Orchestration Layer**: Recognizing that BigQuery stored procedures are typically invoked, an external orchestration layer (Cloud Functions, Workflows, or Composer) was identified as a potential necessity. This allows for triggering the `starteSQLSkript` procedure based on events, schedules, or as part of larger data pipelines.
*   **Parameter Handling**: The original script's flexible parameter passing (`$*`) to `sqlplus` was re-designed to use an `ARRAY<STRING>` parameter (`p_Params`) in the BigQuery stored procedure. This provides a structured way to pass arguments, though it requires the invoked BigQuery SQL scripts to be designed to consume these array parameters.
*   **Trade-off: SQL*Plus Script Translation Complexity**: The design assumes that the underlying SQL*Plus scripts will be translated to BigQuery SQL. This is a significant effort and a major trade-off. If these scripts contain complex PL/SQL or Oracle-specific features, their translation can be non-trivial and is considered outside the scope of this specific migration.

## 4. Manual steps before go-live

Before the migrated solution can be fully operational, the following manual steps are required:

1.  **BigQuery Dataset Creation**: Ensure the target BigQuery dataset (e.g., `project.dataset`) exists. If not, create it.
    ```bash
    bq mk --location=US project:dataset
    ```
    (Replace `US` with your desired region).
2.  **Deploy Generated Artifacts**: Execute the generated `.sql` files in BigQuery to create the tables and stored procedures.
    ```bash
    bq query --use_legacy_sql=false < project.dataset.script_registry.sql
    bq query --use_legacy_sql=false < project.dataset.error_log.sql
    bq query --use_legacy_sql=false < project.dataset.DWMSG_MeldeFehler.sql
    bq query --use_legacy_sql=false < project.dataset.starteSQLSkript.sql
    ```
3.  **Translate and Populate `script_registry`**:
    *   **Crucial Step**: All original SQL*Plus scripts that `h_alis_sqlplus.ksh` would have invoked *must* be translated into BigQuery SQL.
    *   Insert these translated BigQuery SQL scripts, along with their original names and a `TRUE` for `is_readable`, into the `project.dataset.script_registry` table.
    *   Example:
        ```sql
        INSERT INTO `project.dataset.script_registry` (script_name, script_sql, is_readable, original_source_path)
        VALUES
          ('my_first_sqlplus_script.sql', 'SELECT * FROM `my_bq_project.my_bq_dataset.my_table`;', TRUE, 'path/to/original/my_first_sqlplus_script.sql'),
          ('another_script.sql', 'CALL `my_bq_project.my_bq_dataset.my_other_procedure`("param1");', TRUE, 'path/to/original/another_script.sql');
        ```
4.  **IAM Permissions**:
    *   The service account or user invoking `project.dataset.starteSQLSkript` must have the necessary BigQuery roles:
        *   `BigQuery Data Editor` on `project.dataset` (or more granular permissions like `bigquery.tables.getData`, `bigquery.tables.insertData` for logs, `bigquery.routines.call` for procedures).
        *   `BigQuery Job User` to run queries and procedures.
    *   Ensure the service account has permissions to execute any underlying BigQuery SQL statements that the invoked scripts (`script_sql` in `script_registry`) might contain (e.g., `bigquery.tables.update`, `bigquery.tables.create`, etc., on the relevant tables/datasets).
5.  **Orchestration Setup (if applicable)**:
    *   If using Cloud Functions, Cloud Workflows, or Composer, set up the triggers, schedules, and code to call the `project.dataset.starteSQLSkript` procedure. This might involve using the BigQuery client libraries in Python, Node.js, etc.
    *   Ensure any secrets (e.g., API keys if connecting to external systems from orchestration) are securely managed (e.g., using Secret Manager).

## 5. Known gaps & unresolved references

*   **SQL*Plus Script Translation**: The most significant gap is the actual translation of the original Oracle SQL*Plus scripts into BigQuery SQL. This migration design *assumes* this translation will occur and that the translated scripts will be stored in `script_registry`. The complexity and effort for this translation are highly dependent on the original script content (e.g., use of PL/SQL, Oracle-specific functions, DDL, DML). This is a substantial "B4" (Before Go-Live) item.
*   **Parameter Handling for Invoked Scripts**: While `p_Params ARRAY<STRING>` is provided, the invoked BigQuery SQL scripts must be designed to correctly interpret and use these parameters. This might require dynamic SQL within the invoked scripts themselves or a more sophisticated parameter binding mechanism.
*   **Fine-grained Error Code Mapping**: The BigQuery `EXCEPTION WHEN ERROR` provides a generic error status. If the original `sqlplus` scripts relied on specific Oracle error codes for detailed error handling, a more complex mapping or custom error handling logic would be needed within the invoked BigQuery SQL scripts.
*   **`file_complexity` and `automation_rate` data**: The absence of this data for the source script means a quantitative assessment of its migration complexity and automation potential was not possible. This is an analytical gap.
*   **`DW_ORAUSER` Context**: The original script used `DW_ORAUSER`. In BigQuery, user context is managed via IAM. If the invoked BigQuery SQL scripts need to operate under different user contexts or impersonate users, this would require advanced IAM configurations or a redesign of the invoked scripts to accept user-specific parameters.

## 6. Validation

To validate the migrated `starteSQLSkript` procedure, perform the following tests:

1.  **Successful Script Execution**:
    *   **Setup**: Insert a simple, valid BigQuery SQL script into `script_registry` (e.g., `SELECT 1;` or `CREATE TEMPORARY TABLE my_temp_table AS SELECT 'test';`).
    *   **Execution**: Call `CALL project.dataset.starteSQLSkript('entry1', 'my_success_script.sql', []);`.
    *   **Expected Outcome**: The procedure should complete without error. No entries should appear in `project.dataset.error_log` related to this invocation. If the invoked script performs DML/DDL, verify its effects.
2.  **Script Not Found/Not Readable**:
    *   **Execution**: Call `CALL project.dataset.starteSQLSkript('entry2', 'non_existent_script.sql', []);`.
    *   **Expected Outcome**: An entry should appear in `project.dataset.error_log` with `entry_number = 'entry2'`, `severity = 'ERROR'`, `error_code = 201`, and a message indicating the script was not found or not readable.
3.  **Invalid Parameters (NULL `p_Skript`)**:
    *   **Execution**: Call `CALL project.dataset.starteSQLSkript('entry3', NULL, []);`.
    *   **Expected Outcome**: An entry should appear in `project.dataset.error_log` with `entry_number = 'entry3'`, `severity = 'ERROR'`, `error_code = 196`, and a message indicating `Skriptname ist NULL`.
4.  **Invoked Script Fails**:
    *   **Setup**: Insert a BigQuery SQL script into `script_registry` that is syntactically incorrect or will cause a runtime error (e.g., `SELECT 1 / 0;` or `SELECT * FROM non_existent_table;`).
    *   **Execution**: Call `CALL project.dataset.starteSQLSkript('entry4', 'my_failing_script.sql', []);`.
    *   **Expected Outcome**: An entry should appear in `project.dataset.error_log` with `entry_number = 'entry4'`, `severity = 'ERROR'`, `error_code = 1` (or a more specific BigQuery error code if mapped), and a message detailing the BigQuery execution error.
5.  **Parameter Passing**:
    *   **Setup**: Insert a BigQuery SQL script into `script_registry` that uses parameters (e.g., `CREATE TEMPORARY TABLE my_param_table AS SELECT @param1 AS col1, @param2 AS col2;`).
    *   **Execution**: Call `CALL project.dataset.starteSQLSkript('entry5', 'my_param_script.sql', ['--param1=value1', '--param2=value2']);`. (Note: The invoked script needs to parse these array elements or use them directly).
    *   **Expected Outcome**: The invoked script should execute successfully, and its logic should correctly interpret the passed parameters.

**"Passing" means**:
*   The `project.dataset.starteSQLSkript` procedure completes without unhandled exceptions.
*   For successful invocations, no errors are logged in `project.dataset.error_log`.
*   For expected error conditions, the correct error codes and messages are logged in `project.dataset.error_log`.
*   The underlying BigQuery SQL scripts invoked via `EXECUTE IMMEDIATE` perform their intended actions correctly.

## 7. Rollback procedure

In case of issues during or after the migration, the following steps can be taken to roll back to the original state:

1.  **Halt New Invocations**: Stop any new calls to the `project.dataset.starteSQLSkript` procedure. This includes disabling any Cloud Functions, Cloud Workflows, or Composer DAGs that trigger it.
2.  **Revert Orchestration**: If any orchestration layers (e.g., shell scripts, schedulers) were modified to call the BigQuery procedure, revert them to call the original `h_alis_sqlplus.ksh` script.
3.  **Drop BigQuery Stored Procedures**:
    ```sql
    DROP PROCEDURE IF EXISTS `project.dataset.starteSQLSkript`;
    DROP PROCEDURE IF EXISTS `project.dataset.DWMSG_MeldeFehler`;
    ```
4.  **Drop BigQuery Tables**:
    ```sql
    DROP TABLE IF EXISTS `project.dataset.script_registry`;
    DROP TABLE IF EXISTS `project.dataset.error_log`;
    -- If invocation_log was created:
    -- DROP TABLE IF EXISTS `project.dataset.invocation_log`;
    ```
5.  **Verify Original Script Functionality**: Ensure the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/h_alis_sqlplus.ksh` script is still present, executable, and functions as expected in its original environment.
6.  **Data Recovery (if applicable)**: If any data was modified or created by the *invoked* BigQuery SQL scripts during the migration attempt, assess the impact and perform data recovery as per the data's specific recovery plan. The `h_alis_sqlplus.ksh` itself does not modify data, but the scripts it calls might.

This rollback procedure effectively removes the migrated components from BigQuery and restores the system to its state before the migration attempt.