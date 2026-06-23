# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the `r_ausd_austausch.ksh` job, identified by `run_id: 5af228f1-3847-4cc6-9310-ed82ed19407c` and `seed_name: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_austausch.ksh`. The job's original purpose was to generate a "snapshot extract of the base table for the BERT Report" from an Oracle database using KornShell scripts and Oracle SQL/PL/SQL.

The entire workflow, including parameter handling, date calculations, complex data transformations, and table updates, has been re-implemented and migrated to **Google Cloud BigQuery**. The shell-based orchestration and Oracle SQL logic have been translated into BigQuery SQL stored procedures, leveraging BigQuery's native capabilities for data storage, transformation, and error handling.

## 2. Generated Artifacts

The migration process has generated the following BigQuery SQL files:

*   **`sql/ddl/source_tables.sql`**:
    *   **Role**: Contains Data Definition Language (DDL) statements to create the BigQuery tables corresponding to the original Oracle source tables (e.g., `sof_ta_p_rech_empf`, `sof_ta_p_vertrag`). These tables are expected to be populated with data ingested from the legacy Oracle system.
*   **`sql/ddl/target_tables.sql`**:
    *   **Role**: Contains DDL statements to create the BigQuery tables that serve as the final reporting targets (e.g., `rpt_ta_s_d1_rech_empf`, `rpt_ta_s_d1_vertrag`). These tables will store the transformed data.
*   **`sql/ddl/job_control_logging_tables.sql`**:
    *   **Role**: Contains DDL statements to create administrative tables (`job_control`, `job_log`) within BigQuery. These tables are used by the migrated stored procedures for logging job execution status, parameters, and any encountered messages or errors.
*   **`sql/stored_procedures/d_ausd_austausch_sp.sql`**:
    *   **Role**: Contains the BigQuery SQL stored procedure (`D_AUSD_AUSTAUSCH_SP`) that encapsulates the core data transformation logic. This procedure is a direct translation of the `d_ausd_austausch.sql` Oracle script, including complex `INSERT ... SELECT` operations, `CASE` statements, and handling of temporary data.
*   **`sql/stored_procedures/bert_austausch_ksh_sp.sql`**:
    *   **Role**: Contains the main BigQuery SQL stored procedure (`BERT_AUSTAUSCH_KSH_SP`) that acts as the orchestration layer. This procedure replaces the functionality of `r_ausd_austausch.ksh` and `k_ausd_austausch.ksh`, handling parameter parsing, date determination, logging, and invoking the `D_AUSD_AUSTAUSCH_SP` for data transformation.

## 3. Key Design Decisions

The following key design decisions were made during the migration:

*   **BigQuery Native Implementation**: The entire workflow was re-implemented using BigQuery's native SQL capabilities, including stored procedures for orchestration and data transformation. This eliminates dependencies on external shell scripts and Oracle-specific environments, leveraging BigQuery's scalability and performance.
*   **Stored Procedures for Modularity**: The original KornShell orchestration (`r_ausd_austausch.ksh`, `k_ausd_austausch.ksh`) and the core Oracle SQL (`d_ausd_austausch.sql`) were separated into two distinct BigQuery stored procedures:
    *   `BERT_AUSTAUSCH_KSH_SP` for overall job control, parameter handling, and logging.
    *   `D_AUSD_AUSTAUSCH_SP` for the complex data transformation logic.
    This promotes modularity, reusability, and easier debugging.
*   **Translation of Oracle-Specific Constructs**:
    *   Oracle `DECODE` functions were translated to BigQuery `CASE` statements.
    *   Oracle `NVL` functions were translated to BigQuery `IFNULL` or `COALESCE`.
    *   Oracle implicit joins (comma-separated tables in `FROM` clause with join conditions in `WHERE`) were explicitly converted to BigQuery `INNER JOIN` or `LEFT JOIN` syntax, ensuring clarity and correctness.
    *   Oracle date functions and the logic from `gestern.ksh` were replaced with BigQuery's native date and time functions (`CURRENT_DATE()`, `FORMAT_DATE`, `DATE_SUB`, etc.).
*   **BigQuery-Idiomatic Table Update Strategy**: The "new table and swap" pattern used in Oracle (e.g., `TRUNCATE`, `RENAME`) was primarily replaced with BigQuery's `MERGE` statement. This approach allows for atomic, incremental updates or upserts, providing better performance and data consistency compared to a full `CREATE OR REPLACE TABLE` for every run, especially for restartability. For full refreshes, `CREATE OR REPLACE TABLE` is used where appropriate.
*   **Removal of Oracle Index Management**: BigQuery does not use traditional B-tree indexes. All `CREATE INDEX`, `ALTER INDEX`, `DROP INDEX`, and `ANALYZE TABLE` statements from the Oracle script were removed. BigQuery manages query performance through partitioning, clustering, and its columnar storage engine. Appropriate partitioning and clustering keys were defined in the DDL for target tables to optimize query performance.
*   **Integrated Logging and Error Handling**: Custom BigQuery tables (`job_control`, `job_log`) were introduced to centralize logging and job status tracking. This replaces the legacy shell script's file-based logging and provides a structured, queryable log history. Error handling within stored procedures utilizes BigQuery's `RAISE ERROR` for explicit failure conditions.
*   **Refactoring Temporary Tables**: Oracle temporary tables (`sof$ta_rechdef`, `sof$ta_kd_kto`) were refactored into Common Table Expressions (CTEs) within the BigQuery transformation stored procedure. This improves readability, optimizes query execution plans, and avoids the overhead of creating and dropping physical temporary tables.

**Notable Trade-offs:**

*   **Loss of Explicit Index Control**: While BigQuery's automatic performance management is powerful, the explicit control over index creation and tuning present in Oracle is no longer available. Performance relies heavily on proper partitioning, clustering, and query optimization.
*   **Complexity of `MERGE` vs. `CREATE OR REPLACE`**: While `MERGE` is powerful, its implementation can be more complex than a simple `CREATE OR REPLACE TABLE` for full refreshes. The choice was made to prioritize atomicity and incremental update capability where the original logic implied it.
*   **Translation Nuances**: Converting highly specific Oracle PL/SQL constructs or the exact behavior of the `isbert_schema.dwpa_util_skript.runstatement` utility required careful analysis to ensure equivalent BigQuery SQL behavior, especially regarding transactional integrity and locking.

## 4. Manual Steps Before Go-Live

Before the migrated job can be put into production, the following manual steps are required:

1.  **BigQuery Project and Dataset Creation**:
    *   Ensure the Google Cloud Project (`project`) exists.
    *   Create the following BigQuery datasets:
        *   `project.source_dataset`: To host the migrated Oracle source tables.
        *   `project.reporting_dataset`: To host the final reporting tables.
        *   `project.admin_dataset`: To host the `job_control` and `job_log` tables.
    *   These datasets should be created in the appropriate geographic region.

2.  **Deploy DDL for Tables**:
    *   Execute the DDL scripts:
        *   `sql/ddl/source_tables.sql`
        *   `sql/ddl/target_tables.sql`
        *   `sql/ddl/job_control_logging_tables.sql`
    *   This will create all necessary tables in their respective datasets.

3.  **Initial Data Ingestion for Source Tables**:
    *   The `project.source_dataset` tables (e.g., `sof_ta_p_rech_empf`, `sof_ta_p_vertrag`, etc.) must be populated with current data from the legacy Oracle system. This can be achieved using:
        *   Google Cloud Database Migration Service (DMS).
        *   Batch exports from Oracle and loads into BigQuery (e.g., via Cloud Storage).
        *   Change Data Capture (CDC) solutions for ongoing synchronization.

4.  **Deploy Stored Procedures**:
    *   Execute the stored procedure creation scripts:
        *   `sql/stored_procedures/d_ausd_austausch_sp.sql`
        *   `sql/stored_procedures/bert_austausch_ksh_sp.sql`
    *   These procedures should be created in a designated dataset, typically `project.reporting_dataset` or a dedicated `project.procedures_dataset`.

5.  **IAM Permissions Configuration**:
    *   A dedicated Google Cloud Service Account should be created for running this job.
    *   Grant the Service Account the following IAM roles:
        *   `BigQuery Data Editor` on `project.source_dataset`, `project.reporting_dataset`, and `project.admin_dataset`.
        *   `BigQuery Job User` on the project to allow it to run BigQuery jobs.
        *   If the procedures are in a separate dataset, `BigQuery Data Viewer` on that dataset.

6.  **Scheduling Configuration**:
    *   The `BERT_AUSTAUSCH_KSH_SP` stored procedure needs to be scheduled for execution. Recommended options include:
        *   **Cloud Composer (Apache Airflow)**: For complex scheduling, dependency management, and integration with other workflows. A DAG would be created to call the stored procedure.
        *   **Cloud Scheduler + Cloud Function/Workflows**: A Cloud Scheduler job can trigger a Cloud Function or Cloud Workflow, which in turn executes the BigQuery stored procedure.
        *   **BigQuery Scheduled Queries**: For simpler, recurring executions directly within BigQuery.
    *   Configure the schedule to match the original job's frequency and timing.

7.  **Secrets Management (if applicable)**:
    *   If any sensitive parameters (e.g., API keys, external system credentials) are introduced or required by the BigQuery procedures, ensure they are securely managed using Google Cloud Secret Manager.

## 5. Known Gaps & Unresolved References

The following items were identified during the migration as potential gaps, risks, or areas requiring further follow-up:

*   **Oracle `isbert_schema.dwpa_util_skript.runstatement` Behavior**: The exact behavior and side effects (e.g., locking, transaction management) of this Oracle utility package, which executes DDL operations, were not fully detailed. The migration assumes it's a wrapper for standard DDL. Any specific, non-standard behavior might require further investigation and BigQuery-specific handling.
*   **Performance Optimization Post-Migration**: While BigQuery is performant, the `d_ausd_austausch.sql` script contains very large `SELECT` statements with numerous joins and intricate `CASE` logic. Initial translation might not yield optimal performance. Post-migration, careful review and optimization (e.g., fine-tuning partitioning/clustering, optimizing join order, re-evaluating CTE usage) will be crucial.
*   **`(+)` Oracle Outer Join Syntax Conversion**: The conversion of Oracle's proprietary `(+)` outer join syntax to BigQuery's explicit `LEFT JOIN` requires meticulous verification to ensure identical data retrieval, especially in queries with multiple outer joins.
*   **Data Type Mapping and Explicit Casting**: While a best effort was made, implicit data type conversions in Oracle might behave differently in BigQuery. A detailed data type mapping exercise and potential explicit casting within the BigQuery SQL may be necessary to prevent data truncation or unexpected behavior.
*   **Oracle `WHENEVER SQLERROR` Handling**: The precise error handling logic of Oracle's `WHENEVER SQLERROR CONTINUE/EXIT FAILURE` directives needs to be fully understood and mapped to BigQuery's `BEGIN...EXCEPTION...END` blocks to ensure equivalent robustness and error reporting.
*   **Oracle `parallel (table, N)` Hints**: These Oracle-specific hints for parallel execution have been removed. BigQuery automatically manages parallelism, but the impact of this removal on specific query execution plans should be monitored.
*   **`TRUNCATE REUSE STORAGE` Equivalence**: BigQuery's `TRUNCATE TABLE` does not have a `REUSE STORAGE` equivalent. This is generally not an issue as BigQuery manages storage automatically, but it's a difference in behavior.
*   **Unused/Commented-Out Code**: The original `d_ausd_austausch.sql` contained commented-out sections (`--AL??`) and blocks (e.g., `INSERT INTO SOF$TA_K_BERT_DATENSTAND`) that appeared to be deprecated or for specific testing. These were generally omitted from the BigQuery migration. Confirmation that these sections are indeed no longer required is recommended.

## 6. Validation

Validation of the migrated job involves comparing its output and behavior against the legacy Oracle system.

**How to Run Tests:**

1.  **Prepare Test Data**: Ensure the `project.source_dataset` in BigQuery contains a representative snapshot of data from the Oracle source tables, ideally for a specific `p_stichtag` (reference date) that was previously processed by the legacy job.
2.  **Execute the Migrated Job**:
    *   Call the main orchestration stored procedure:
        ```sql
        CALL `project.reporting_dataset.BERT_AUSTAUSCH_KSH_SP`(
            p_stichtag => 'YYYY-MM-DD', -- e.g., '2023-10-26'
            p_wiederanlaufWert => 0 -- or a specific restart value for incremental testing
        );
        ```
    *   Monitor the execution in the BigQuery UI or via `job_control` and `job_log` tables.
3.  **Execute the Legacy Job**: Run the original `r_ausd_austausch.ksh` job in the Oracle environment for the *exact same* `p_stichtag` and `p_wiederanlaufWert` used in BigQuery.
4.  **Compare Results**:
    *   **Row Counts**: For each target reporting table (e.g., `rpt_ta_s_d1_rech_empf`, `rpt_ta_s_d1_vertrag`), compare the total row count in BigQuery with the corresponding table in Oracle.
    *   **Key Metrics**: For numerical columns, calculate sums, averages, min/max values in both BigQuery and Oracle and compare them.
    *   **Data Sample Comparison**: Select a random sample of records (e.g., 100-1000 rows) from each target table in BigQuery and Oracle. Compare all column values for these records to ensure exact matches. Pay special attention to `CASE` logic, date conversions, and string manipulations.
    *   **Error Logs**: Review the `project.admin_dataset.job_log` table in BigQuery for any errors or warnings. Compare with the log output of the legacy job.
    *   **Performance**: Compare the execution time of the BigQuery job with the legacy Oracle job.

**What "Passing" Means:**

*   **Successful Execution**: The `BERT_AUSTAUSCH_KSH_SP` completes without errors, and the `job_control` table shows a `status` of 'SUCCEEDED' for the corresponding `run_id`.
*   **Data Consistency**:
    *   Row counts for all target reporting tables in BigQuery exactly match those in the Oracle system for the same input parameters.
    *   Key aggregate metrics (sums, averages, etc.) for numerical columns in BigQuery exactly match those in Oracle.
    *   A statistically significant sample of individual records shows exact value matches across all columns between BigQuery and Oracle.
*   **Functional Equivalence**: All business logic, including complex `CASE` statements, date calculations, and join conditions, produces identical results.
*   **Logging Accuracy**: The `job_log` table accurately records job progress, parameters, and any informational messages or warnings.
*   **Performance**: The BigQuery job completes within acceptable performance thresholds, ideally matching or exceeding the performance of the legacy Oracle job.

## 7. Rollback Procedure

In the event of critical issues or unexpected behavior after go-live, the following rollback procedure should be followed:

1.  **Immediate Halt of New Job**:
    *   Disable or delete the BigQuery job's scheduler (e.g., pause the Cloud Composer DAG, disable the Cloud Scheduler job).
    *   If the job is currently running, attempt to cancel the BigQuery job execution.

2.  **Reactivate Legacy Job**:
    *   Re-enable and restart the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_austausch.ksh` job in the legacy Oracle environment. Ensure it runs with the correct parameters and schedule.

3.  **Data Rollback (if necessary)**:
    *   **For Target Reporting Tables (`project.reporting_dataset.*`)**:
        *   If the BigQuery job has corrupted or incorrectly updated the target reporting tables, the simplest approach is to `TRUNCATE TABLE` the affected BigQuery tables.
        *   Then, allow the re-activated legacy Oracle job to run and populate the Oracle reporting tables.
        *   Subsequently, re-ingest the correct data from the Oracle reporting tables back into the BigQuery `project.reporting_dataset` tables (e.g., via a batch load or a one-time data transfer).
        *   Alternatively, if backups of the BigQuery target tables exist (e.g., via BigQuery time travel or snapshots), restore them to a known good state.
    *   **For Source Tables (`project.source_dataset.*`)**: These tables are typically read-only for this job. If they were somehow affected, they would need to be re-synced from the Oracle source system.

4.  **Investigation and Remediation**:
    *   Analyze the `project.admin_dataset.job_log` and BigQuery job history for the failed BigQuery job to identify the root cause of the issue.
    *   Address the identified bugs, performance bottlenecks, or configuration errors in the BigQuery stored procedures or environment.

5.  **Re-validation**:
    *   Once the issues are resolved, repeat the full validation process (Section 6) in a non-production environment before attempting another go-live.