# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_aurd_rechstan.ksh` and its associated SQL script `d_aurd_rechstan.sql`.

The original `k_aurd_rechstan.ksh` script served as an ETL orchestrator, responsible for:
- Parsing and validating command-line parameters.
- Performing date calculations and validations.
- Executing the core data processing logic contained within `d_aurd_rechstan.sql` via SQL*Plus.
- Capturing processed record counts.
- (Intended) Logging job status and metrics to a job management system.

The migration targets Google Cloud Platform (GCP), specifically:
- **BigQuery Stored Procedures:** To encapsulate the orchestration logic, parameter handling, validation, and the core data processing (migrated from `d_aurd_rechstan.sql`).
- **BigQuery Tables:** For job monitoring (`job_table`), error logging (`error_log`), and the final data destination (`RKopfStan`).
- **Cloud Composer (Airflow):** For higher-level scheduling, dependency management, and invocation of the BigQuery Stored Procedure.

The primary goal was to modernize the ETL process, leverage cloud-native services for scalability and maintainability, and eliminate dependencies on legacy shell scripting and SQL*Plus.

## 2. Generated Artifacts

The migration produced the following files:

1.  **`my_dataset/job_table_ddl.sql`**
    *   **Role:** BigQuery DDL (Data Definition Language) script. This creates or updates the `job_table` in BigQuery, which is designed to store metadata, status, and metrics for all ETL job runs. It replaces the implicit job tracking functionality (e.g., `FOSJobErzeugeEintrag`) from the original KornShell script.
2.  **`my_dataset/error_log_table_ddl.sql`**
    *   **Role:** BigQuery DDL script. This creates or updates the `error_log` table in BigQuery. This table serves as a centralized repository for capturing detailed error messages and stack traces from BigQuery Stored Procedure executions, replacing the `f_alis_msgerr.ksh` and `DWMSG_MeldeFehler` mechanisms.
3.  **`my_dataset/target_table_ddl.sql`**
    *   **Role:** BigQuery DDL script. This creates or updates the `RKopfStan` table in BigQuery. This is the ultimate target table where the processed data, originally populated by `d_aurd_rechstan.sql`, will reside. Note that the schema provided is a placeholder as the original DDL for `RKopfStan` was not available.
4.  **`my_dataset/r_aurd_rechstan_sp.sql`**
    *   **Role:** BigQuery Stored Procedure definition. This is the core migrated artifact. It encapsulates the entire logic of the original `k_aurd_rechstan.ksh` (parameter parsing, validation, date handling, job logging) and the data processing logic from `d_aurd_rechstan.sql` (converted to BigQuery SQL). It handles error management and updates the `job_table` and `error_log` tables.
5.  **`orchestration_dag.py`**
    *   **Role:** Python script defining an Apache Airflow DAG (Directed Acyclic Graph) for Cloud Composer. This DAG is responsible for scheduling and invoking the `r_aurd_rechstan` BigQuery Stored Procedure. It replaces the external scheduling mechanism that would have triggered `k_aurd_rechstan.ksh` and provides robust orchestration capabilities.

## 3. Key Design Decisions

### Chosen Approach: BigQuery Stored Procedure + Cloud Composer

*   **Consolidation and Modernization:** The decision to migrate to a BigQuery Stored Procedure consolidates the orchestration logic (from `k_aurd_rechstan.ksh`) and the core data processing logic (from `d_aurd_rechstan.sql`) into a single, cohesive unit written in BigQuery SQL. This eliminates dependencies on legacy KornShell scripts, SQL*Plus, and various helper scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `gestern.ksh`, etc.).
*   **Leveraging Cloud-Native Capabilities:** BigQuery Stored Procedures offer native scalability, performance, and integration within the GCP ecosystem. This approach fully utilizes BigQuery's capabilities for data processing, rather than treating it merely as a data store.
*   **Robust Orchestration:** Cloud Composer (Airflow) provides enterprise-grade scheduling, monitoring, and dependency management. It allows for easy integration into larger data pipelines, offering better visibility and control compared to traditional cron jobs or proprietary schedulers.
*   **Standardized Error Handling and Logging:** By using BigQuery's `RAISE USING MESSAGE` and dedicated `error_log` and `job_table` BigQuery tables, error handling and job status tracking become standardized, auditable, and easily queryable within the data warehouse environment.

### Notable Trade-offs

*   **Increased Initial Complexity:** A single KornShell script is replaced by multiple artifacts (DDLs, Stored Procedure, Airflow DAG). While this improves maintainability and scalability in the long run, it represents a higher initial development and deployment overhead for what was a relatively simple shell script.
*   **Learning Curve:** Developers familiar with KornShell and SQL*Plus will need to adapt to BigQuery Scripting syntax, BigQuery's SQL dialect, and Airflow concepts.
*   **Placeholder Schemas:** Due to the unavailability of the original DDL for `RKopfStan` and the source tables for `d_aurd_rechstan.sql`, placeholder schemas were used. This requires manual verification and potential adjustment of the `target_table_ddl.sql` and the `MERGE` statement within the stored procedure.
*   **Assumptions on `d_aurd_rechstan.sql`:** The migration assumes that the SQL logic within `d_aurd_rechstan.sql` is directly translatable to BigQuery SQL. If it contained complex procedural logic (e.g., PL/SQL specific features, cursors, loops) not easily mapped to BigQuery's declarative or scripting capabilities, further refactoring would be required.
*   **Commented-out Code Interpretation:** The original script had commented-out calls to `FOSJobErzeugeEintrag` and `FOSJobDeaktivate`. The migration design assumes these were intended functionalities and has implemented equivalent job tracking in the `job_table`. If these were intentionally disabled, the corresponding `INSERT` and `UPDATE` statements in the stored procedure could be removed.

## 4. Manual Steps Before Go-Live

Before the migrated solution can be fully operational, the following manual steps must be performed:

1.  **GCP Project and Dataset Setup:**
    *   Ensure a GCP project (`my_project`) is active and billing is enabled.
    *   Create the BigQuery dataset (`my_dataset`) where all tables and the stored procedure will reside.
        ```bash
        bq mk --dataset my_project:my_dataset
        ```
2.  **IAM Permissions:**
    *   **BigQuery Service Account:** A service account (e.g., used by Cloud Composer or other orchestrators) must have the following BigQuery roles:
        *   `BigQuery Data Editor` (for `my_dataset`) to write to `job_table`, `error_log`, and `RKopfStan`.
        *   `BigQuery Job User` to execute the stored procedure.
        *   `BigQuery Data Viewer` (for `my_dataset`) to read from `source_rechstan_data` and `RKopfStan` (for record counting).
    *   **Cloud Composer Service Account:** If using Cloud Composer, its service account will need the above BigQuery roles, plus standard Composer roles.
3.  **Deploy BigQuery DDLs:**
    *   Execute the DDL scripts to create the necessary tables.
    *   **`my_dataset/job_table_ddl.sql`**:
        ```bash
        bq query --use_legacy_sql=false < my_dataset/job_table_ddl.sql
        ```
    *   **`my_dataset/error_log_table_ddl.sql`**:
        ```bash
        bq query --use_legacy_sql=false < my_dataset/error_log_table_ddl.sql
        ```
    *   **`my_dataset/target_table_ddl.sql`**:
        *   **CRITICAL:** Review and update the placeholder schema for `RKopfStan` to accurately reflect the original table structure.
        ```bash
        bq query --use_legacy_sql=false < my_dataset/target_table_ddl.sql
        ```
4.  **Deploy BigQuery Stored Procedure:**
    *   Execute the stored procedure definition script.
    *   **`my_dataset/r_aurd_rechstan_sp.sql`**:
        *   **CRITICAL:** Review and update the placeholder `MERGE` statement within the stored procedure to accurately reflect the logic from `d_aurd_rechstan.sql` and the actual source table (`source_rechstan_data`) and target table (`RKopfStan`) schemas.
        ```bash
        bq query --use_legacy_sql=false < my_dataset/r_aurd_rechstan_sp.sql
        ```
5.  **Source Data Availability:**
    *   Ensure the source table (`my_project.my_dataset.source_rechstan_data` in the placeholder code) exists in BigQuery and is populated with the necessary input data for the `r_aurd_rechstan` process. Its schema must align with the `MERGE` statement in the stored procedure.
6.  **Deploy Cloud Composer DAG (if applicable):**
    *   Upload `orchestration_dag.py` to the DAGs folder of your Cloud Composer environment.
    *   Verify the DAG appears in the Airflow UI and is unpaused.
    *   Adjust the `schedule_interval` and parameter values (e.g., `p_job_kennung`, `p_eintrags_nr`, `p_wiederanlauf_wert`) in `orchestration_dag.py` as per operational requirements.

## 5. Known Gaps & Unresolved References

1.  **`d_aurd_rechstan.sql` Content:** The actual SQL logic from `d_aurd_rechstan.sql` was not available during the design phase. The `r_aurd_rechstan_sp.sql` contains a placeholder `MERGE` statement. This is a **critical gap** that requires the original `d_aurd_rechstan.sql` to be fully translated and integrated into the BigQuery Stored Procedure.
2.  **`RKopfStan` and Source Table Schemas:** The DDL for `RKopfStan` (`target_table_ddl.sql`) and the reference to `source_rechstan_data` in the stored procedure are placeholders. The correct schemas for these tables must be identified and implemented.
3.  **Commented-out Job Logging:** The original `k_aurd_rechstan.ksh` had commented-out calls for job logging (`FOSJobErzeugeEintrag`, `FOSJobDeaktivate`). The migration assumes these were intended and has implemented equivalent functionality in the `job_table`. Confirmation is needed if this functionality should indeed be active in the target environment.
4.  **Error Code Standardization:** The original script used `ErrNr` and `ErrArg`. While BigQuery's `RAISE USING MESSAGE` and `error_log` table provide robust error reporting, a specific mapping or standardization of error codes from the legacy system to the new BigQuery error logging standard might be required for consistency.
5.  **Wider Orchestration Context (B4 Item):** If `k_aurd_rechstan.ksh` was part of a larger, more complex job stream (e.g., invoked by a central scheduler like UC4), the `orchestration_dag.py` provided is a standalone example. The integration of this DAG into the broader enterprise scheduling framework (e.g., as a sub-DAG or part of a larger Cloud Composer workflow) is a follow-up item.
6.  **`p_wiederanlauf_wert` Logic:** The `p_wiederanlauf_wert` parameter is passed to the stored procedure. The placeholder `MERGE` statement does not explicitly use this value. The actual logic from `d_aurd_rechstan.sql` that utilized this parameter (e.g., for restartability or incremental loads) must be incorporated into the BigQuery Stored Procedure.

## 6. Validation

To validate the successful migration and functionality of the `r_aurd_rechstan` process:

1.  **Manual BigQuery Stored Procedure Execution:**
    *   Execute the stored procedure directly in BigQuery to test its core logic and error handling.
    *   **Command Example:**
        ```sql
        CALL `my_project.my_dataset.r_aurd_rechstan`(
            p_job_kennung => 'TEST_JOB_MANUAL',
            p_eintrags_nr => '9999',
            p_stichtag => '01012023', -- Use a specific date for testing
            p_wiederanlauf_wert => 0
        );
        ```
    *   **Passing Criteria:**
        *   The `CALL` statement completes without error.
        *   Verify `my_project.my_dataset.job_table` contains a new entry for `TEST_JOB_MANUAL` with `status = 'SUCCESS'` and `processed_records` reflecting the expected count for `01012023`.
        *   Verify `my_project.my_dataset.RKopfStan` contains the expected processed data for `stichtag_date = '2023-01-01'`.
        *   Verify `my_project.my_dataset.error_log` contains no entries related to this successful run.
    *   **Error Testing:** Intentionally pass invalid parameters (e.g., `p_stichtag => 'INVALID_DATE'`) and verify that the stored procedure raises an error and logs it correctly in `error_log`, with `job_table` showing `status = 'FAILED'`.

2.  **Cloud Composer DAG Execution (if applicable):**
    *   Trigger the `r_aurd_rechstan_orchestration` DAG in the Airflow UI (or wait for its scheduled run).
    *   **Passing Criteria:**
        *   The DAG run completes successfully in the Airflow UI (all tasks turn green).
        *   The `execute_r_aurd_rechstan_sp` task logs show successful execution.
        *   Verify `my_project.my_dataset.job_table` contains a new entry for the corresponding DAG run (e.g., `p_job_kennung = 'DAILY_RECHSTAN_JOB'`) with `status = 'SUCCESS'` and correct `processed_records`.
        *   Verify `my_project.my_dataset.RKopfStan` contains the expected processed data for the `p_stichtag` passed by the DAG (e.g., `execution_date`).
        *   Verify `my_project.my_dataset.error_log` contains no entries related to this successful run.

## 7. Rollback Procedure

In case of critical issues or if the migration needs to be reverted, follow these steps:

1.  **Stop New Executions:**
    *   If using Cloud Composer, pause or delete the `r_aurd_rechstan_orchestration` DAG in the Airflow UI to prevent further executions.
    *   Ensure no manual calls to the BigQuery Stored Procedure are made.
2.  **Revert Data (if necessary):**
    *   If the `RKopfStan` table was modified by the new BigQuery process and the data is incorrect or corrupted, restore it to its state before the migration. This might involve:
        *   Restoring `RKopfStan` from a BigQuery snapshot or backup.
        *   Deleting records inserted by the new process (e.g., `DELETE FROM my_project.my_dataset.RKopfStan WHERE stichtag_date >= 'migration_start_date';`).
        *   **CRITICAL:** This step requires careful planning and execution to avoid data loss.
3.  **Delete BigQuery Stored Procedure:**
    ```sql
    DROP PROCEDURE IF EXISTS `my_project.my_dataset.r_aurd_rechstan`;
    ```
4.  **Delete BigQuery Tables (Optional, if newly created and not shared):**
    *   If `job_table` and `error_log` were created solely for this migration and are not used by other processes, they can be deleted.
    *   If `RKopfStan` was newly created and not used by other processes, it can be deleted.
    ```bash
    bq rm -f my_project:my_dataset.job_table
    bq rm -f my_project:my_dataset.error_log
    bq rm -f my_project:my_dataset.RKopfStan
    ```
5.  **Undeploy Cloud Composer DAG (if applicable):**
    *   Delete `orchestration_dag.py` from the DAGs folder of your Cloud Composer environment.
6.  **Reactivate Original Process:**
    *   Re-enable or restart the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_aurd_rechstan.ksh` script and its associated scheduling mechanism.
    *   Verify that the original process is running as expected and populating `RKopfStan` correctly.