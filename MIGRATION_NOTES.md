```markdown
# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the KornShell utility script `h_alis_sqlplus.ksh` from its original Unix/Oracle environment to Google Cloud Platform, specifically leveraging BigQuery. The original script served as a wrapper for executing Oracle SQL*Plus scripts, handling parameter validation, script existence checks, and basic error logging.

The migration replaces this shell script's functionality with a BigQuery SQL stored procedure (`starteSQLSkript`) that orchestrates the execution of other BigQuery stored procedures (which are the migrated versions of the original SQL*Plus scripts). Auxiliary BigQuery tables are introduced for logging and managing metadata about the migrated SQL scripts.

**Target Platform:** Google Cloud Platform (BigQuery for data processing and orchestration logic, potentially Cloud Composer for workflow management).

## 2. Generated Artifacts

The migration process generates the following BigQuery SQL files and their corresponding BigQuery objects:

*   **`your_bq_dataset.error_log.sql`**
    *   **Role:** Defines the Data Definition Language (DDL) for the `error_log` BigQuery table. This table centralizes error reporting, replacing the `DWMSG_MeldeFehler` function and shell `echo` statements for error messages in the original script. It captures details like entry number, severity, error code, message, and module information.
*   **`your_bq_dataset.execution_log.sql`**
    *   **Role:** Defines the DDL for the `execution_log` BigQuery table. This table stores detailed logs of script executions, including module name, version, entry number, script name, parameters, and log messages. It replaces the `echo` statements used for logging execution flow in the original KornShell script.
*   **`your_bq_dataset.sql_script_registry.sql`**
    *   **Role:** Defines the DDL for the `sql_script_registry` BigQuery table and provides example Data Manipulation Language (DML) for initial population. This table acts as a metadata store for all migrated BigQuery stored procedures, replacing the filesystem-based script existence and readability checks (`if [ ! -r $p_Skript ]`) of the original shell script. It links original script names to their new BigQuery procedure names.
*   **`bq_starte_sql_skript_proc.sql`**
    *   **Role:** Defines the BigQuery SQL stored procedure `your_gcp_project.your_bq_dataset.starteSQLSkript`. This is the core migrated component, directly replacing the `starteSQLSkript` function in `h_alis_sqlplus.ksh`. It performs parameter validation, checks the `sql_script_registry` for script existence and readability, logs execution and errors, and dynamically `CALL`s the appropriate migrated BigQuery stored procedure.
*   **`your_bq_dataset.migrated_d_exis_apt_bestandsdaten.sql`**
    *   **Role:** This is a **placeholder** BigQuery SQL stored procedure. It represents one of the many SQL*Plus scripts that `h_alis_sqlplus.ksh` would have originally invoked (e.g., `vobs/dw_source/isdwh/exporter/apt/sql/d_exis_apt_bestandsdaten.sql`). Its purpose is to demonstrate how individual SQL*Plus scripts are migrated into BigQuery stored procedures. The actual SQL logic within this procedure needs to be manually translated from its Oracle SQL*Plus equivalent.

## 3. Key Design Decisions

*   **BigQuery Stored Procedures for Wrapper Logic:** The decision to implement the `h_alis_sqlplus.ksh` wrapper logic as a BigQuery stored procedure (`starteSQLSkript`) was made to keep the orchestration and execution within the BigQuery ecosystem. This leverages BigQuery's native capabilities for procedural logic, error handling, and logging, reducing external dependencies and simplifying the overall architecture.
*   **Metadata Tables for Script Management and Logging:**
    *   **`sql_script_registry`:** Replacing filesystem checks with a BigQuery table allows for centralized management of migrated scripts, their status, and their corresponding BigQuery procedure names. This provides a more robust, auditable, and scalable solution compared to file-based checks.
    *   **`error_log` and `execution_log`:** Centralizing logging in BigQuery tables provides a structured, queryable, and persistent record of all executions and errors. This significantly improves observability, debugging, and auditing capabilities compared to scattered shell `echo` statements and external error functions.
*   **Dynamic `CALL` for Script Execution:** Instead of directly executing SQL files, the `starteSQLSkript` procedure dynamically `CALL`s other BigQuery stored procedures. This maintains the original wrapper's flexibility to invoke different "scripts" while adhering to BigQuery's procedural execution model.
*   **Error Handling with `RAISE` and `EXCEPTION WHEN ERROR`:** BigQuery's `RAISE` and `EXCEPTION WHEN ERROR` constructs are used to manage error flow, replacing the shell's `set -e` and explicit error code checks. This provides a more structured and robust error management mechanism.
*   **Trade-off: Manual SQL Translation:** The most significant trade-off is the necessity for manual translation of the actual Oracle SQL*Plus scripts into BigQuery SQL. While the wrapper logic is automated, the core business logic within the invoked SQL files requires careful review and adaptation due to differences in SQL dialects, functions, and procedural constructs between Oracle and BigQuery. This is a common challenge in database migrations.

## 4. Manual Steps Before Go-Live

Before the migrated components can be used in a production environment, the following manual steps are required:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (`your_bq_dataset` in the generated code) exists within `your_gcp_project`. If not, create it.
2.  **IAM Permissions:**
    *   Configure appropriate Identity and Access Management (IAM) roles for the service account that will execute the `starteSQLSkript` procedure. This service account needs:
        *   `BigQuery Data Editor` on `your_bq_dataset` to write to `error_log` and `execution_log`.
        *   `BigQuery Data Viewer` on `your_bq_dataset` to read from `sql_script_registry`.
        *   `BigQuery Job User` to execute stored procedures.
        *   Permissions to read/write to any BigQuery tables that the *migrated SQL procedures* (e.g., `migrated_d_exis_apt_bestandsdaten`) interact with.
3.  **Populate `sql_script_registry` Table:**
    *   The `sql_script_registry` table must be fully populated with entries for *every* original SQL*Plus script that `h_alis_sqlplus.ksh` might have invoked. Each entry must accurately map the original script path to its corresponding BigQuery stored procedure name.
    *   Verify `is_readable` flag is set correctly for each entry.
4.  **Translate and Deploy All SQL*Plus Scripts:**
    *   **Crucially, each Oracle SQL*Plus script (`.sql` file) that was previously executed by `h_alis_sqlplus.ksh` must be manually translated into a BigQuery SQL stored procedure.** This involves:
        *   Converting Oracle-specific SQL syntax, functions, and data types to BigQuery equivalents.
        *   Refactoring PL/SQL blocks into BigQuery scripting.
        *   Handling SQL*Plus specific commands (e.g., `SET`, `DEFINE`) appropriately.
    *   Deploy these translated BigQuery stored procedures to `your_bq_dataset`.
    *   Ensure the parameters for these migrated procedures are compatible with how `starteSQLSkript` passes them (currently as `ARRAY<STRING>`). Adjust the `CALL` statement within `starteSQLSkript` if individual parameters are needed.
5.  **Orchestration Integration:**
    *   If the original `h_alis_sqlplus.ksh` was part of a larger workflow (e.g., scheduled by UC4), the new `starteSQLSkript` procedure needs to be integrated into the new orchestration system (e.g., Cloud Composer/Airflow DAGs, Cloud Scheduler, Dataform). This involves creating new tasks that `CALL` `your_gcp_project.your_bq_dataset.starteSQLSkript` with the appropriate parameters.
6.  **Parameter Mapping Review:**
    *   Carefully review how parameters (`p_Params ARRAY<STRING>`) are passed from `starteSQLSkript` to the individual migrated procedures. The example `migrated_d_exis_apt_bestandsdaten` assumes `p_params[OFFSET(0)]` is the entry number. This assumption needs to be validated and potentially adjusted for each migrated script based on its original parameter usage.

## 5. Known Gaps & Unresolved References

*   **Full SQL*Plus Script Translation (B4 Item):** The generated `migrated_d_exis_apt_bestandsdaten.sql` is a placeholder. The complete and accurate translation of all original Oracle SQL*Plus scripts into BigQuery SQL stored procedures is a significant manual effort and is the primary remaining task. This is a "B4" (BigQuery-specific) item requiring detailed analysis of each source SQL file.
*   **Dynamic SQL Complexity:** If any original SQL*Plus scripts involved highly dynamic SQL generation or complex conditional execution paths that are not easily mapped to BigQuery stored procedures, these might require more advanced BigQuery scripting or even Python-based Cloud Functions/Dataflow jobs for full replication. The current `starteSQLSkript` assumes a direct `CALL` to a static procedure name.
*   **Orchestration Context (UC4 Replacement):** The design document mentions UC4. The specific mechanisms and schedules by which UC4 (or any other scheduler) invoked `h_alis_sqlplus.ksh` need to be fully understood and replicated in a GCP orchestration tool like Cloud Composer. This is outside the scope of this specific script's migration but is critical for the overall workflow.
*   **`DW_ORAUSER` Equivalent:** The `DW_ORAUSER` environment variable in the original script specified the Oracle user. In BigQuery, this is replaced by the service account executing the BigQuery job. The specific service account and its permissions need to be carefully managed and configured.
*   **Parameter Interpretation in Migrated Procedures:** The `starteSQLSkript` passes all parameters as an `ARRAY<STRING>`. Each migrated procedure (e.g., `migrated_d_exis_apt_bestandsdaten`) must correctly parse and use these array elements. This requires consistent parameter ordering and type handling across all migrated scripts.
*   **Error Code Mapping:** The original `DWMSG_MeldeFehler` likely used specific error codes. While `error_log` captures `BQ.EXCEPTION_ERROR_CODE()`, a mapping from original custom error codes to new BigQuery-specific or custom BigQuery error codes might be necessary for consistency.

## 6. Validation

Validation ensures that the migrated `starteSQLSkript` and its invoked procedures function correctly and produce the expected outcomes.

1.  **Unit Test `starteSQLSkript` Procedure:**
    *   **How to run:** Execute `CALL your_gcp_project.your_bq_dataset.starteSQLSkript(...)` directly from the BigQuery console or a client tool.
    *   **Test Cases:**
        *   **Missing Parameters:** Call with `NULL` or empty `p_Eintragsnr` or `p_Skript`.
            *   **Passing:** The call should `RAISE` an exception with error code '196', and an entry should appear in `error_log` with severity 'E'.
        *   **Script Not Found:** Call with a `p_Skript` value not present in `sql_script_registry`.
            *   **Passing:** The call should `RAISE` an exception with error code '200', and an entry should appear in `error_log` with severity 'E'.
        *   **Script Not Readable:** Call with a `p_Skript` value present in `sql_script_registry` but with `is_readable = FALSE`.
            *   **Passing:** The call should `RAISE` an exception with error code '201', and an entry should appear in `error_log` with severity 'E'.
        *   **Successful Invocation:** Call with valid `p_Eintragsnr`, `p_Skript` (pointing to a *successfully migrated* placeholder procedure like `migrated_d_exis_apt_bestandsdaten`), and `p_Params`.
            *   **Passing:** The call should complete without `RAISE`ing an exception. An entry should appear in `execution_log` for the start and successful completion of `starteSQLSkript`, and also for the invocation of the target procedure.
2.  **Validate Logging:**
    *   **How to run:** After each test case above, query `your_gcp_project.your_bq_dataset.error_log` and `your_gcp_project.your_bq_dataset.execution_log`.
    *   **Passing:**
        *   Error scenarios should have corresponding entries in `error_log` with correct `entry_nr`, `severity`, `error_code`, and `message`.
        *   Successful scenarios should have entries in `execution_log` for both `starteSQLSkript` and the invoked target procedure, reflecting the correct `script_name`, `script_params`, and `log_message`.
3.  **Validate Migrated SQL Procedures:**
    *   **How to run:** Once the placeholder `migrated_d_exis_apt_bestandsdaten` (and all other migrated procedures) contain the actual translated BigQuery SQL, execute them both directly and via `starteSQLSkript`.
    *   **Passing:**
        *   The procedure should execute without BigQuery SQL errors.
        *   Data transformations (INSERTs, UPDATEs, DELETEs) should produce the exact same results as the original Oracle SQL*Plus script when run against equivalent datasets. This requires comparing target table contents.
        *   Performance should be comparable or improved.
4.  **End-to-End Workflow Validation:**
    *   **How to run:** If using Cloud Composer, trigger the DAG that orchestrates the `starteSQLSkript` calls.
    *   **Passing:** The entire workflow should complete successfully, with all expected data transformations occurring, and all logs (BigQuery `error_log`, `execution_log`, and Composer task logs) showing successful execution.

## 7. Rollback Procedure

In case of critical issues during or after go-live, the following rollback procedure can be executed to revert to the original system:

1.  **Halt New System Orchestration:**
    *   Immediately pause or disable any new orchestration mechanisms (e.g., Cloud Composer DAGs, Cloud Scheduler jobs) that invoke the BigQuery `starteSQLSkript` procedure.
2.  **Revert to Original Orchestration:**
    *   Re-enable or restart the original orchestration jobs (e.g., UC4 jobs) that were responsible for invoking `h_alis_sqlplus.ksh`.
3.  **Verify Original System Functionality:**
    *   Monitor the original system to ensure it has resumed normal operations and is processing data as expected.
4.  **Data Consistency Check (if applicable):**
    *   If any data was written or modified by the new BigQuery system before rollback, assess the impact. Depending on the nature of the data and the duration of the new system's operation, a data reconciliation or restoration from backup might be necessary for the target BigQuery tables. For this specific utility script, direct data modification is done by the *invoked* scripts, so the impact depends on their migration status.
5.  **Clean Up Migrated BigQuery Objects (Optional, Post-Rollback Analysis):**
    *   Once the original system is stable, the newly created BigQuery tables (`error_log`, `execution_log`, `sql_script_registry`) and stored procedures (`starteSQLSkript`, `migrated_d_exis_apt_bestandsdaten`, etc.) can be deleted from `your_gcp_project.your_bq_dataset`. This step should only be performed after a successful rollback and thorough analysis of the issues that triggered the rollback.

**Note:** This rollback procedure assumes that the original `h_alis_sqlplus.ksh` script and its dependencies (Oracle database, SQL*Plus client) remain operational and untouched during the migration process, allowing for a direct switch-back.