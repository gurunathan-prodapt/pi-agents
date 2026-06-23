# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_barrier_zusgf.ksh` to Google BigQuery.

The original script served as a control mechanism, responsible for:
*   Ignoring already active jobs.
*   Invoking an SQL script (`d_ausd_v_ta_barrier_zusgf.sql`) for core data processing.
*   Registering job execution entries and deactivating older active jobs.
*   Handling parameter parsing, validation, and retrieving a record count from a temporary file.

The script has been migrated to a BigQuery-native solution, primarily consisting of two BigQuery Stored Procedures:
*   `project.dataset.proc_ausd_v_ta_barrier_zusgf`: The main control procedure, handling job management, parameter validation, and orchestration.
*   `project.dataset.proc_d_ausd_v_ta_barrier_zusgf`: Encapsulating the core data transformation logic previously found in `d_ausd_v_ta_barrier_zusgf.sql`.

This migration eliminates the dependency on KornShell, `sqlplus`, and filesystem-based temporary files, leveraging BigQuery's scalability and native SQL capabilities.

## 2. Generated Artifacts

The migration process generated the following BigQuery DDL (Data Definition Language) scripts and expects the creation of BigQuery Stored Procedures:

*   **`bigquery/ddl/job_error_log.sql`**
    *   **Role:** Defines the `job_error_log` table in BigQuery. This table will store detailed error messages and context whenever an issue occurs during the execution of the migrated job, replacing the shell script's error logging mechanisms.
*   **`bigquery/ddl/job_table.sql`**
    *   **Role:** Defines the `job_table` in BigQuery. This table is central to managing the state of jobs, including activation, deactivation, and status tracking, mirroring the job control logic of the original KornShell script.
*   **`bigquery/ddl/dwtk_meldungen.sql`**
    *   **Role:** Defines the `dwtk_meldungen` table in BigQuery. This table is inferred as a potential output or logging table from the original SQL script's context, likely used for storing messages or alerts.
*   **`bigquery/ddl/ta_barrier.sql`**
    *   **Role:** Defines the `ta_barrier` table in BigQuery. This table is inferred as a primary input data source for the `proc_d_ausd_v_ta_barrier_zusgf` stored procedure, based on the naming convention and purpose.
*   **`bigquery/ddl/ta_barrier_zusgf.sql`**
    *   **Role:** Defines the `ta_barrier_zusgf` table in BigQuery. This is the target table where the final processed data from `proc_d_ausd_v_ta_barrier_zusgf` will be stored. It represents the main output of the data transformation.
*   **`bigquery/ddl/job_run_log.sql`**
    *   **Role:** Defines the `job_run_log` table in BigQuery. This table records successful job executions, including start/end times, processed record counts, and overall status, replacing the original script's method of tracking job completion and metrics.
*   **`bigquery/stored_procedures/proc_d_ausd_v_ta_barrier_zusgf.sql` (Expected)**
    *   **Role:** This stored procedure will contain the migrated core data transformation logic from the original `d_ausd_v_ta_barrier_zusgf.sql` file. It performs the actual ETL operations.
*   **`bigquery/stored_procedures/proc_ausd_v_ta_barrier_zusgf.sql` (Expected)**
    *   **Role:** This is the main orchestrating stored procedure. It encapsulates the control flow, parameter validation, job status management (using `job_table`), invocation of `proc_d_ausd_v_ta_barrier_zusgf`, and logging of run details and record counts (to `job_run_log`).

## 3. Key Design Decisions

The migration strategy focused on translating the KornShell script's orchestration and data processing logic into BigQuery-native constructs.

*   **KornShell to BigQuery Stored Procedures:** The primary decision was to replace the shell script with BigQuery Stored Procedures. This allows the entire workflow to run natively within BigQuery, eliminating external dependencies like KornShell interpreters, `sqlplus`, and filesystem operations. It leverages BigQuery's performance and scalability for both control flow and data manipulation.
*   **Separation of Control and Data Logic:** The original script's pattern of a control shell script invoking a separate SQL script was maintained. `proc_ausd_v_ta_barrier_zusgf` handles the orchestration and job management, while `proc_d_ausd_v_ta_barrier_zusgf` focuses solely on the data transformation. This promotes modularity, reusability, and easier debugging.
*   **BigQuery Tables for Job Management:** Instead of implicit job control mechanisms or temporary files, dedicated BigQuery tables (`job_table`, `job_run_log`, `job_error_log`) are used to manage job status, log execution details, and record errors. This provides a centralized, queryable, and persistent record of job activities.
*   **Native BigQuery Parameter Handling:** Command-line parameter parsing (`getopts`) in the KornShell script is replaced by explicit `IN` parameters in the BigQuery Stored Procedures, providing clear input definitions and type safety.
*   **Record Count Retrieval:** The original method of writing a record count to a temporary file and then reading it back is replaced by a direct `SELECT COUNT(*)` query on the target BigQuery table (`ta_barrier_zusgf`) after the data processing is complete. This is more efficient and BigQuery-native.
*   **BigQuery-Native Error Handling:** Shell-based error handling and messaging utilities are replaced by BigQuery's `RAISE` statements for immediate termination and logging to the `job_error_log` table for persistent error tracking.
*   **Trade-offs:**
    *   **Increased SQL Complexity:** Implementing control flow (IF/THEN, loops, variable assignments) directly in BigQuery SQL can be more verbose and less intuitive than in a scripting language like KornShell.
    *   **BigQuery-Specific Knowledge:** The solution requires familiarity with BigQuery SQL, stored procedures, and DDL, which might be a new skill set for teams accustomed to shell scripting and traditional RDBMS.
    *   **Loss of Direct Filesystem Interaction:** While generally a benefit in cloud environments, any complex file-based operations (e.g., reading configuration files, complex report generation) would require alternative BigQuery-native solutions (e.g., Cloud Storage, UDFs).

## 4. Manual Steps Before Go-Live

Before the migrated job can be put into production, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (`project.dataset` as referenced in the DDLs) exists. If not, create it:
        ```sql
        CREATE SCHEMA IF NOT EXISTS `project.dataset`;
        ```
2.  **Table Creation:**
    *   Execute all generated DDL scripts to create the necessary tables in the target BigQuery dataset:
        *   `bigquery/ddl/job_error_log.sql`
        *   `bigquery/ddl/job_table.sql`
        *   `bigquery/ddl/dwtk_meldungen.sql`
        *   `bigquery/ddl/ta_barrier.sql`
        *   `bigquery/ddl/ta_barrier_zusgf.sql`
        *   `bigquery/ddl/job_run_log.sql`
    *   **Note:** The schemas for `dwtk_meldungen`, `ta_barrier`, and `ta_barrier_zusgf` are inferred. These must be thoroughly reviewed and adjusted to precisely match the source system's schema and the actual output of the `d_ausd_v_ta_barrier_zusgf.sql` logic.
3.  **Stored Procedure Deployment:**
    *   Deploy the two BigQuery Stored Procedures:
        *   `proc_d_ausd_v_ta_barrier_zusgf` (containing the migrated core SQL logic).
        *   `proc_ausd_v_ta_barrier_zusgf` (the main control procedure).
    *   These procedures will be created using `CREATE OR REPLACE PROCEDURE` statements.
4.  **IAM Permissions:**
    *   Ensure the Google Cloud service account or user identity that will execute the BigQuery Stored Procedures has the necessary IAM roles:
        *   `BigQuery Data Editor` (roles/bigquery.dataEditor) on the target dataset (`project.dataset`) to create, update, and delete data in the tables.
        *   `BigQuery Job User` (roles/bigquery.jobUser) to run BigQuery jobs (including stored procedures).
        *   Potentially `BigQuery Metadata Viewer` (roles/bigquery.metadataViewer) if the procedures need to inspect table schemas.
5.  **Orchestration Setup (if applicable):**
    *   If using an orchestration tool like Cloud Composer (Airflow) or Cloud Run to trigger `proc_ausd_v_ta_barrier_zusgf`, set up the corresponding DAG or service. This includes configuring the BigQuery connection and passing the required parameters (`p_JobKennung`, `p_EintragsNr`).
6.  **Data Ingestion for Source Tables:**
    *   Ensure that the source data for `ta_barrier` (and any other inferred input tables for `proc_d_ausd_v_ta_barrier_zusgf`) is correctly ingested and available in BigQuery before the job runs.

## 5. Known Gaps & Unresolved References

The following items have been identified as requiring further investigation or are currently unresolved:

*   **Complexity of `d_ausd_v_ta_barrier_zusgf.sql`:** The actual SQL logic within the original `d_ausd_v_ta_barrier_zusgf.sql` script is not provided in detail. Its complexity, use of specific database features (e.g., Oracle PL/SQL, vendor-specific functions), and exact data sources/targets are critical for accurately migrating `proc_d_ausd_v_ta_barrier_zusgf`. This requires a dedicated, in-depth analysis and migration effort.
*   **Job Table Schema Details:** The exact schema and logic for the `job_table` and `dwtk_meldungen` (if it's part of job management) are inferred. These need to be thoroughly reviewed against the legacy system's implementation to ensure accurate replication of job control behavior.
*   **`r_ausd_vertrag.ksh` Relationship:** The design document states `k_ausd_v_ta_barrier_zusgf.ksh` is a control script *for* `r_ausd_vertrag.ksh`. The precise nature of this relationship and its implications for the overall workflow (e.g., is `k_ausd_v_ta_barrier_zusgf.ksh` called by `r_ausd_vertrag.ksh`, or does it manage `r_ausd_vertrag.ksh`'s execution?) need to be fully understood to ensure the migrated BigQuery solution fits into the broader ecosystem.
*   **`DW_DIR_UTL` and `BERT_DIR_ROOT` Variables:** The original script uses these environment variables for pathing. While temporary file usage has been replaced, any other implicit uses of these variables (e.g., for configuration files, logging directories) need to be identified and mapped to BigQuery-native equivalents (e.g., BigQuery tables, Cloud Storage, parameters).
*   **`target_result_table` Schema Confirmation:** The DDL for `ta_barrier_zusgf` is an inferred schema. It must be rigorously validated against the actual output structure of the legacy `d_ausd_v_ta_barrier_zusgf.sql` to prevent data truncation or type mismatches.
*   **Source Data for `ta_barrier` and `dwtk_meldungen`:** The migration path for the data populating these inferred input tables needs to be defined. Are they already in BigQuery, or do they require separate ingestion pipelines?

## 6. Validation

Validation of the migrated job involves several steps to ensure functional equivalence and performance.

### How to Run Tests:

1.  **Unit Testing `proc_d_ausd_v_ta_barrier_zusgf`:**
    *   Load representative sample data into the inferred input tables (`ta_barrier`, etc.).
    *   Execute `CALL project.dataset.proc_d_ausd_v_ta_barrier_zusgf();` (assuming no parameters for this sub-procedure, or with dummy parameters if required).
    *   Query the `ta_barrier_zusgf` table to inspect the output.
2.  **Integration Testing `proc_ausd_v_ta_barrier_zusgf`:**
    *   Execute the main control procedure: `CALL project.dataset.proc_ausd_v_ta_barrier_zusgf('TEST_JOB', '123');` (using appropriate test parameters).
    *   Monitor the `job_table`, `job_run_log`, and `job_error_log` tables for correct entries and status updates.
    *   Verify the final data in `ta_barrier_zusgf`.
3.  **Comparative Data Validation:**
    *   Run the legacy `k_ausd_v_ta_barrier_zusgf.ksh` job with a specific set of input data.
    *   Run the migrated `proc_ausd_v_ta_barrier_zusgf` with the *exact same* input data.
    *   Compare:
        *   Record counts in the target tables (`ta_barrier_zusgf`).
        *   Checksums or hash values of the target tables.
        *   Random sample rows from both legacy and BigQuery target tables for data accuracy.
4.  **Error Handling Validation:**
    *   Test scenarios where parameters are missing or invalid.
    *   Test scenarios where `proc_d_ausd_v_ta_barrier_zusgf` might fail (e.g., due to data issues).
    *   Verify that errors are correctly logged in `job_error_log` and the main procedure handles them gracefully (e.g., marking job as FAILED in `job_table`).
5.  **Performance Testing:**
    *   Run the migrated job with production-scale data volumes.
    *   Compare execution times with the legacy job.
    *   Analyze BigQuery job statistics for slot usage and byte processing.

### What "Passing" Means:

A successful migration validation implies the following:

*   **Functional Equivalence:** The `proc_ausd_v_ta_barrier_zusgf` and `proc_d_ausd_v_ta_barrier_zusgf` procedures produce identical results (data content and record counts) in `ta_barrier_zusgf` as the legacy system for the same inputs.
*   **Correct Job State Management:**
    *   `job_table` accurately reflects the current status of the job (e.g., 'ACTIVE', 'COMPLETED').
    *   `job_run_log` contains a 'SUCCESS' entry with the correct `record_count` and timestamps for successful runs.
    *   `job_error_log` contains relevant error details for failed runs, and the `job_table` reflects a 'FAILED' status.
*   **Parameter Handling:** The procedures correctly parse and validate input parameters, rejecting invalid ones and logging errors.
*   **Performance:** The BigQuery job completes within acceptable timeframes, ideally matching or improving upon legacy performance, and operates within defined cost boundaries.
*   **No Unexpected Errors:** No unhandled exceptions or unexpected errors occur during execution.

## 7. Rollback Procedure

In the event of critical issues detected after go-live, the following rollback procedure should be followed:

1.  **Immediate Halt of New Executions:**
    *   Stop any orchestration (e.g., Cloud Composer DAGs, Cloud Run services) that triggers `proc_ausd_v_ta_barrier_zusgf`.
    *   If manual triggers are in place, communicate broadly to cease execution.
2.  **Revert Orchestration to Legacy (if applicable):**
    *   If an orchestration layer was updated to point to the BigQuery job, revert it to trigger the original `k_ausd_v_ta_barrier_zusgf.ksh` script.
3.  **Data Rollback (if necessary):**
    *   If the BigQuery job has written data to `ta_barrier_zusgf` or other target tables, and this data is deemed incorrect or corrupted:
        *   **Option A (Truncate/Delete):** If the data can be safely re-generated by the legacy system, truncate or delete the affected partitions/data from `ta_barrier_zusgf` and any other tables modified by the BigQuery job.
            ```sql
            TRUNCATE TABLE `project.dataset.ta_barrier_zusgf`;
            -- Or for partitioned tables:
            DELETE FROM `project.dataset.ta_barrier_zusgf` WHERE _PARTITIONTIME = 'YYYY-MM-DD';
            ```
        *   **Option B (Restore from Snapshot/Backup):** If BigQuery table snapshots or backups were taken prior to the problematic run, restore the tables to their previous state.
4.  **Disable/Delete BigQuery Artifacts:**
    *   Consider disabling or deleting the BigQuery Stored Procedures (`proc_ausd_v_ta_barrier_zusgf`, `proc_d_ausd_v_ta_barrier_zusgf`) to prevent accidental execution.
    *   The DDL-created tables (`job_error_log`, `job_table`, `job_run_log`, etc.) can be retained for post-mortem analysis or deleted if no longer needed.
5.  **Re-enable Legacy Job:**
    *   Ensure the original `k_ausd_v_ta_barrier_zusgf.ksh` job and its dependencies are fully operational and can resume processing without issues.
6.  **Post-Rollback Analysis:**
    *   Conduct a thorough investigation into the root cause of the rollback to address the issues before attempting re-migration or re-deployment.