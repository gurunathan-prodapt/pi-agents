# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `k_ausd_v_ta_p_vertrag.ksh`, which orchestrates the execution of a SQL script (`d_ausd_v_ta_p_vertrag.sql`) for processing contract data. The legacy script handled parameter parsing, job status management, error reporting, and SQL script invocation.

The migration re-implements this functionality within the Google Cloud Platform, specifically leveraging:
*   **Google BigQuery** for all data storage (tables), data transformation (SQL), and orchestration logic (Stored Procedures).
*   **Google Cloud Composer (Apache Airflow)** or **Cloud Workflows** for scheduling and triggering the BigQuery processes.

The primary goal was to modernize the existing shell-based orchestration and SQL execution into a fully cloud-native, scalable, and manageable BigQuery solution.

## 2. Generated Artifacts

The migration process generated the following key artifacts in the target BigQuery environment and Google Cloud Platform:

*   **`project.dataset.r_ausd_vertrag` (BigQuery Stored Procedure):**
    *   **Role:** This is the main orchestration procedure. It replaces the `k_ausd_v_ta_p_vertrag.ksh` KornShell script, handling parameter validation, job status updates in `project.dataset.job_table`, error logging to `project.dataset.error_log`, and invoking the data transformation logic.
*   **`project.dataset.d_ausd_v_ta_p_vertrag` (BigQuery Stored Procedure):**
    *   **Role:** This procedure encapsulates the core data transformation logic, replacing the `d_ausd_v_ta_p_vertrag.sql` script. It reads from source BigQuery tables and writes to target BigQuery tables, with all SQL converted to BigQuery Standard SQL.
*   **`project.dataset.job_table` (BigQuery Table):**
    *   **Role:** A control table used for managing and tracking the execution status of jobs, including `r_ausd_vertrag`. It replaces any implicit job tracking or legacy job management tables.
*   **`project.dataset.error_log` (BigQuery Table):**
    *   **Role:** A dedicated table for capturing and storing error messages and execution failures, replacing the shell script's `DWMSG_MeldeFehler` mechanism.
*   **Migrated BigQuery Tables:**
    *   **Role:** BigQuery representations of the legacy Oracle tables: `project.dataset.DWTK_MELDUNGEN`, `project.dataset.SOF_TA_VERTRAG_TMP`, `project.dataset.SOF_TA_P_VERTRAG`, `project.dataset.VIA`. These serve as the source and target for the `d_ausd_v_ta_p_vertrag` procedure.
*   **Cloud Composer DAG / Cloud Workflow Definition:**
    *   **Role:** The scheduling mechanism responsible for triggering the `project.dataset.r_ausd_vertrag` BigQuery Stored Procedure at the required intervals, replacing the legacy cron-based scheduling.
*   **BigQuery UDFs (Optional, as needed):**
    *   **Role:** If complex, reusable logic from the legacy helper scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) was identified, specific BigQuery User-Defined Functions might have been created to encapsulate this logic.

## 3. Key Design Decisions

The migration strategy was guided by the following key design decisions:

*   **Consolidation into BigQuery Stored Procedures:** The orchestration logic (from `k_ausd_v_ta_p_vertrag.ksh`) and the data transformation logic (from `d_ausd_v_ta_p_vertrag.sql`) were both re-implemented as BigQuery Stored Procedures. This centralizes the entire job's logic within BigQuery, leveraging its native capabilities for scripting, data manipulation, and execution, thereby eliminating external shell script dependencies.
*   **Native BigQuery Job and Error Management:** Instead of relying on filesystem-based temporary files or external logging mechanisms, dedicated BigQuery tables (`job_table`, `error_log`) were created. This provides a scalable, queryable, and integrated solution for monitoring job status and errors directly within the BigQuery ecosystem.
*   **Elimination of Filesystem Dependencies:** The use of temporary files for record counting and reliance on environment variables for paths in the legacy shell script were replaced by BigQuery scripting variables (`DECLARE`, `SET`) and direct queries on BigQuery tables. This removes external filesystem dependencies and simplifies deployment.
*   **Standardized GCP Scheduling:** The legacy scheduling (likely cron or similar) was replaced by Google Cloud Composer (Airflow) or Cloud Workflows. This aligns with GCP best practices for orchestrating data pipelines, offering robust scheduling, monitoring, and retry capabilities.
*   **Direct BigQuery SQL Conversion:** The core data transformation SQL was directly converted to BigQuery Standard SQL. This minimizes the introduction of new technologies and leverages BigQuery's performance for analytical workloads. Legacy Oracle package calls were either inlined, converted to BigQuery UDFs, or re-implemented as separate BigQuery Stored Procedures.
*   **Trade-offs:**
    *   **SQL Dialect Conversion Complexity:** Converting from Oracle SQL to BigQuery Standard SQL required careful mapping of data types, functions, and syntax, which can be a non-trivial effort depending on the complexity of the original SQL.
    *   **Loss of Direct OS Interaction:** The migration to BigQuery Stored Procedures means losing direct operating system interaction capabilities (e.g., `getopts`, `cat`, `eval`). This was a deliberate choice to move towards a more managed and cloud-native environment, with equivalent functionality re-implemented using BigQuery's scripting features.
    *   **Dependency on BigQuery Ecosystem:** The entire solution is now tightly coupled with BigQuery, which is beneficial for performance and scalability within GCP but might require specific BigQuery expertise for maintenance.

## 4. Manual Steps Before Go-Live

Before the migrated job can be put into production, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (`project.dataset`) exists. If not, create it:
        ```bash
        bq mk --location=US project:dataset
        ```
2.  **BigQuery Table Creation:**
    *   Create the `job_table` and `error_log` tables with the specified schemas:
        ```sql
        -- job_table
        CREATE TABLE `project.dataset.job_table` (
          job_kennung STRING,
          eintrags_nr STRING,
          tab_name STRING,
          status STRING,
          created_at TIMESTAMP,
          updated_at TIMESTAMP
        );

        -- error_log
        CREATE TABLE `project.dataset.error_log` (
          error_timestamp TIMESTAMP,
          procedure_name STRING,
          error_message STRING,
          job_kennung STRING,
          eintrags_nr STRING
        );
        ```
    *   Ensure all source and target BigQuery tables (e.g., `DWTK_MELDUNGEN`, `SOF_TA_VERTRAG_TMP`, `SOF_TA_P_VERTRAG`, `VIA`) are created with their correct schemas and partitioning/clustering strategies.
3.  **IAM Permissions:**
    *   Grant the necessary IAM roles to the service account that will execute the Cloud Composer DAG or Cloud Workflow. This typically includes:
        *   `BigQuery Data Editor` (for writing to `job_table`, `error_log`, and target data tables).
        *   `BigQuery Data Viewer` (for reading from source data tables).
        *   `BigQuery Job User` (for running BigQuery jobs/stored procedures).
4.  **Data Ingestion Pipeline Activation:**
    *   Verify that the data ingestion pipeline responsible for populating the BigQuery source tables (e.g., `DWTK_MELDUNGEN`, `SOF_TA_VERTRAG_TMP`) from the legacy Oracle system is active and providing up-to-date data.
5.  **Cloud Composer/Workflows Deployment:**
    *   Deploy the Cloud Composer DAG or Cloud Workflow definition that triggers `project.dataset.r_ausd_vertrag`.
    *   Ensure the DAG/Workflow is enabled and scheduled according to the required frequency.
6.  **Parameter Configuration:**
    *   If any parameters for the BigQuery Stored Procedure (`p_JobKennung`, `p_EintragsNr`) are dynamic or environment-dependent, ensure they are correctly configured within the Composer DAG or Workflow.

## 5. Known Gaps & Unresolved References

The following items were identified as potential gaps or areas requiring further investigation/refinement:

*   **Complexity of `d_ausd_v_ta_p_vertrag.sql`:** The detailed content of the original SQL script was not available during the design phase. The effort for BigQuery SQL conversion and package migration (`DWPA_UTIL_SKRIPT`, `PV`) is an estimate and may require significant analysis and refactoring once the full SQL content is known.
*   **Logic within Sourced Helper Scripts:** The full functionality of `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, and `h_alis_sqlplus.ksh` was not fully analyzed. While common utilities are assumed to be replaced by BigQuery standard functions, any complex or business-specific logic within these scripts would require dedicated migration to BigQuery UDFs or Stored Procedures. The `starteSQLSkript` function in `h_alis_sqlplus.ksh` in particular might contain complex error handling or connection logic that needs careful re-implementation.
*   **"Deactivate older active jobs" Logic:** The exact business rules and implementation details for deactivating older active jobs were implicitly handled by the legacy shell script. This logic needs to be fully understood and precisely replicated in BigQuery SQL, likely involving `UPDATE` statements on the `job_table` based on status and timestamp comparisons.
*   **`v_TabName='ta_p_vertrag'` Usage:** The full context and usage of this variable within the legacy script were not entirely clear from the provided fragment. Its role in job management or logging needs to be fully mapped and implemented in the BigQuery Stored Procedure.
*   **Data Volume and Performance:** While BigQuery is highly scalable, the performance characteristics of the migrated SQL for large data volumes (e.g., `SOF$TA_VERTRAG_TMP`) need thorough testing and optimization post-migration. This includes evaluating partitioning, clustering, and query optimization strategies.

## 6. Validation

Validation of the migrated job involves running the new BigQuery Stored Procedure and verifying its behavior and output against the legacy system.

**How to Run Tests:**

1.  **Manual Execution (for initial testing):**
    *   Execute the main orchestration stored procedure directly in the BigQuery console:
        ```sql
        CALL `project.dataset.r_ausd_vertrag`('YOUR_JOB_KENNUNG', 'YOUR_EINTRAGS_NR');
        ```
2.  **Scheduled Execution (for integrated testing):**
    *   Trigger the Cloud Composer DAG or Cloud Workflow manually, or allow it to run on its scheduled interval.

**What "Passing" Means:**

A successful validation run meets the following criteria:

*   **Successful Procedure Execution:** The `project.dataset.r_ausd_vertrag` BigQuery Stored Procedure completes without raising unhandled exceptions or errors.
*   **Correct Job Status Updates:**
    *   Verify that `project.dataset.job_table` is updated correctly, showing the job as `ACTIVE` at the start and `COMPLETED` (or equivalent success status) at the end.
    *   Confirm that the "deactivation of older active jobs" logic functions as expected, updating previous entries in `job_table` if applicable.
*   **No Error Log Entries:** Check `project.dataset.error_log` to ensure no error messages were recorded during the execution.
*   **Data Integrity and Accuracy:**
    *   **Record Counts:** Compare the number of records processed and inserted/updated in the target tables (`project.dataset.SOF_TA_P_VERTRAG`, `project.dataset.VIA`) with the corresponding counts from the legacy system's output.
    *   **Sample Data Verification:** Select a representative sample of records from the target tables and compare their values against the expected output from the legacy system for the same input data.
    *   **Idempotency (if applicable):** If the job is designed to be idempotent, verify that running it multiple times with the same input yields the same final state without unintended side effects.
*   **Performance:** The execution time of the BigQuery Stored Procedure should be within acceptable limits, ideally matching or improving upon the legacy system's performance.

## 7. Rollback Procedure

In the event of critical issues or unexpected behavior with the migrated job, the following rollback procedure can be initiated to revert to the legacy system:

1.  **Deactivate New Scheduling:**
    *   Immediately disable or pause the Cloud Composer DAG or Cloud Workflow responsible for triggering `project.dataset.r_ausd_vertrag`. This prevents any further execution of the migrated job.
2.  **Reactivate Legacy Scheduling:**
    *   Re-enable the original scheduling mechanism (e.g., cron job, legacy scheduler) for `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_vertrag.ksh`.
3.  **Verify Legacy System Operation:**
    *   Confirm that the legacy `k_ausd_v_ta_p_vertrag.ksh` script is running as expected and interacting with its original source and target systems (e.g., Oracle database).
4.  **Data State Assessment (Critical Step):**
    *   **Impact on Target Tables:** If the migrated BigQuery job writes to target tables that are also consumed by other systems, or if the migration involved a "cut-over" where BigQuery became the new authoritative source, a more complex data rollback strategy might be required.
        *   If the BigQuery target tables (`project.dataset.SOF_TA_P_VERTRAG`, `project.dataset.VIA`) are *distinct* from the legacy Oracle target tables, then no data rollback is typically needed for the target tables themselves, as the legacy system will simply continue writing to its original targets.
        *   If the BigQuery tables were intended to *replace* the legacy tables and downstream systems were switched to BigQuery, then those downstream systems must be reverted to consume from the legacy Oracle tables.
    *   **Data Consistency:** Ensure that the state of the data in the legacy system is consistent and can resume processing without issues. This might involve restoring a backup of the legacy target tables if the migrated job made irreversible changes to shared data.
5.  **Monitor Legacy System:**
    *   Closely monitor the re-activated legacy job to ensure it functions correctly and processes data as expected.

**Note:** This rollback procedure assumes that the legacy environment and its data sources/targets remain operational and accessible throughout the migration period.