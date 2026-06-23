# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `k_ausd_bp_ta_bpr_instance.ksh` and its dependent SQL script `d_ausd_bp_ta_bpr_instance.sql`. The original scripts orchestrated a data preparation process, including parameter validation, date checks, and data manipulation for a 'PoolBasisprodukt'.

The migration targets Google Cloud's BigQuery platform. The orchestration logic has been translated into a BigQuery Stored Procedure, the core data manipulation logic into another BigQuery Stored Procedure, and the scheduling mechanism is designed to leverage Cloud Composer (Apache Airflow). Logging has been centralized into dedicated BigQuery tables.

## 2. Generated artifacts

The following files were generated as part of this migration:

*   **`ddl/job_error_log.sql`**
    *   **Role:** Defines the Data Definition Language (DDL) for the `job_error_log` BigQuery table. This table is used to capture and store detailed error messages and codes during the execution of the migrated BigQuery stored procedures, replacing the legacy `f_alis_msgerr.ksh` and direct print statements.
*   **`ddl/job_run_log.sql`**
    *   **Role:** Defines the DDL for the `job_run_log` BigQuery table. This table records successful job execution details, including job name, ID, business date, record counts, and status, replacing temporary files and commented-out legacy logging.
*   **`procedures/d_ausd_bp_ta_bpr_instance.sql`**
    *   **Role:** Contains the BigQuery Stored Procedure `d_ausd_bp_ta_bpr_instance`. This procedure is the direct migration of the core data manipulation logic found in the original `d_ausd_bp_ta_bpr_instance.sql` file. It performs data truncation and insertion into the target table (`sof_ta_bpr_instance`) based on specific business rules.
*   **`procedures/r_ausd_bp_ta_bpr_instance.sql`**
    *   **Role:** Contains the BigQuery Stored Procedure `r_ausd_bp_ta_bpr_instance`. This is the main orchestration procedure, directly replacing the `k_ausd_bp_ta_bpr_instance.ksh` KornShell script. It handles parameter validation, date derivation, calls the `d_ausd_bp_ta_bpr_instance` procedure, and logs execution details and errors to the respective BigQuery logging tables.
*   **`orchestration/k_ausd_bp_ta_bpr_instance_dag.py`**
    *   **Role:** An Apache Airflow DAG (Directed Acyclic Graph) script. This Python file defines the workflow for scheduling and executing the `r_ausd_bp_ta_bpr_instance` BigQuery stored procedure. It demonstrates how to pass parameters dynamically and trigger the BigQuery job, replacing the legacy job control system.
*   **`tests/test_r_ausd_bp_ta_bpr_instance.sql`**
    *   **Role:** A BigQuery SQL script designed for testing the migrated `r_ausd_bp_ta_bpr_instance` stored procedure. It includes example calls for successful execution, invalid parameter handling, and missing parameter scenarios, along with verification queries against the logging tables.

## 3. Key design decisions

The migration strategy involved several key design decisions to leverage BigQuery's capabilities and modernize the data processing pipeline:

*   **Orchestration Logic Migration to BigQuery Stored Procedures:**
    *   **Why:** The original KornShell script's primary role was orchestration (parameter validation, date handling, SQL execution). Migrating this directly into a BigQuery Stored Procedure (`r_ausd_bp_ta_bpr_instance`) centralizes the logic within the data platform, reducing external dependencies and simplifying deployment. BigQuery's scripting capabilities provide a robust environment for procedural logic.
    *   **Trade-offs:** This approach ties the orchestration logic tightly to BigQuery, potentially reducing portability to other data platforms. Debugging complex BigQuery procedures can sometimes be more challenging than shell scripts.
*   **Core SQL Logic Encapsulation:**
    *   **Why:** The `d_ausd_bp_ta_bpr_instance.sql` content was encapsulated into a separate BigQuery Stored Procedure (`d_ausd_bp_ta_bpr_instance`). This promotes modularity, reusability, and allows for independent testing and optimization of the core data manipulation.
    *   **Trade-offs:** Requires careful dialect conversion from the original SQL (e.g., Oracle) to BigQuery SQL, which might introduce subtle behavioral changes if not thoroughly tested.
*   **Centralized Logging to BigQuery Tables:**
    *   **Why:** Replaced disparate logging mechanisms (temporary files, `f_alis_msgerr.ksh`, print statements) with structured BigQuery tables (`job_error_log`, `job_run_log`). This provides a scalable, queryable, and centralized repository for job execution metadata and errors, significantly improving monitoring and troubleshooting capabilities.
    *   **Trade-offs:** Requires initial DDL setup for the logging tables.
*   **Native BigQuery Date and Parameter Handling:**
    *   **Why:** Replaced external shell scripts (`gestern.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`) with BigQuery's built-in functions (`CURRENT_DATE()`, `DATE_SUB()`, `PARSE_DATE()`) and SQL scripting constructs (`IF` statements, `IS NULL`). This eliminates external dependencies, simplifies the environment, and leverages BigQuery's optimized functions.
    *   **Trade-offs:** Requires careful translation of date formats and validation rules to BigQuery SQL.
*   **Cloud Composer (Airflow) for Scheduling:**
    *   **Why:** The legacy job control system (implied by `FOS-Jobverwaltung` comments) was replaced with a modern, scalable, and robust orchestration platform like Cloud Composer. This provides advanced scheduling, dependency management, monitoring, and alerting capabilities.
    *   **Trade-offs:** Introduces a new component (Airflow) with its own operational overhead and learning curve.

## 4. Manual steps before go-live

Before the migrated solution can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Create the target BigQuery dataset, e.g., `your_project_id.your_dataset_id`, where all migrated tables and procedures will reside.
    *   **Command Example:** `bq mk --dataset --default_location=US your_project_id:your_dataset_id`
2.  **BigQuery Table Creation (DDL Deployment):**
    *   Execute the DDL scripts for the logging tables:
        *   `ddl/job_error_log.sql`
        *   `ddl/job_run_log.sql`
    *   Execute DDL for the target data table `sof_ta_bpr_instance` and any source tables (`cds_ta_cntrct`, `pds_ta_bpri_com`) if they don't already exist in BigQuery. Ensure their schemas match the expected structure for the `d_ausd_bp_ta_bpr_instance` procedure.
    *   **Placeholder Replacement:** Replace `your_project_id.your_dataset_id` with the actual project and dataset IDs in all DDL files before execution.
3.  **BigQuery Stored Procedure Deployment:**
    *   Deploy the `procedures/d_ausd_bp_ta_bpr_instance.sql` and `procedures/r_ausd_bp_ta_bpr_instance.sql` scripts to create the stored procedures in the target BigQuery dataset.
    *   **Placeholder Replacement:** Replace `your_project_id.your_dataset_id` with the actual project and dataset IDs in all procedure files before deployment.
4.  **IAM / Permissions Configuration:**
    *   Ensure the Google Cloud service account used by Cloud Composer (or any other scheduler/user invoking the procedures) has the necessary BigQuery roles:
        *   `BigQuery Data Editor` on `your_project_id.your_dataset_id` to create/update tables and execute procedures.
        *   `BigQuery Job User` to run BigQuery jobs.
5.  **Source Data Ingestion:**
    *   Ensure that the source tables (`cds_ta_cntrct`, `pds_ta_bpri_com`) are populated with the necessary data in BigQuery. This might involve separate data ingestion pipelines (e.g., Dataflow, Cloud Storage transfers, BigQuery Data Transfer Service).
6.  **Cloud Composer (Airflow) DAG Deployment:**
    *   Deploy the `orchestration/k_ausd_bp_ta_bpr_instance_dag.py` file to your Cloud Composer environment's DAGs folder.
    *   **Configuration:** Update `BIGQUERY_PROJECT_ID` and `BIGQUERY_DATASET_ID` variables within the DAG file to match your environment.
    *   **Scheduling:** Verify the `schedule_interval` in the DAG matches the required execution frequency.
7.  **Parameter Review for Airflow DAG:**
    *   Review the parameters passed to the `r_ausd_bp_ta_bpr_instance` procedure within the Airflow DAG (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`). Adjust macros or static values as needed for your specific scheduling requirements. Pay close attention to the `p_Stichtag` format (`DDMMYYYY`).

## 5. Known gaps & unresolved references

*   **Complexity of `d_ausd_bp_ta_bpr_instance.sql`:** The migration of `d_ausd_bp_ta_bpr_instance.sql` assumes a relatively straightforward translation from its original SQL dialect (likely Oracle) to BigQuery SQL. If the original script contains highly complex procedural logic, proprietary functions, or intricate cursor operations, further manual refinement and testing may be required. This was flagged as a primary risk in the design document.
*   **Commented-out Post-processing Logic:** The original KornShell script contained commented-out sections involving `sed`, `sort`, and `join` operations. This migration does *not* include the translation of this logic. If these operations are required for the current business process, they represent a significant gap and must be translated into BigQuery SQL (e.g., using `REPLACE`, `DISTINCT`, `ORDER BY`, and `JOIN` clauses on BigQuery tables) and integrated into the `r_ausd_bp_ta_bpr_instance` or a new procedure.
*   **`PoolBasisprodukt` vs `sof_ta_bpr_instance`:** The design document refers to `PoolBasisprodukt` as the target data pool, while the generated code uses `sof_ta_bpr_instance`. Clarification is needed to confirm the exact target table name and ensure consistency.
*   **`v_datum` Derivation in `d_ausd_bp_ta_bpr_instance.sql`:** The generated `d_ausd_bp_ta_bpr_instance` procedure notes that the original Oracle script used `isbert_schema.dwtk_meldungen` to determine `v_datum`. The migrated procedure currently uses `p_Stichtag` directly. If the original complex date derivation logic is critical, it needs to be re-implemented in BigQuery.
*   **Target Table Partitioning/Clustering:** The DDL for `sof_ta_bpr_instance` is not provided, and its partitioning or clustering strategy is not defined. For optimal BigQuery performance and cost efficiency, the target table should be appropriately partitioned (e.g., by `business_date` or `insert_at`) and/or clustered.
*   **Record Count `WHERE` Clause:** In `r_ausd_bp_ta_bpr_instance`, the `WHERE some_date_column = v_stichtag_date` clause for record counting is commented out. This needs to be uncommented and adjusted to reflect the actual date column in `sof_ta_bpr_instance` that corresponds to `v_stichtag_date` to ensure accurate record counts for the specific business date.
*   **Legacy Job Control System Integration:** While Cloud Composer replaces the scheduling aspect, any deeper integration with the `FOS-Jobverwaltung` (e.g., specific status updates, inter-job communication beyond simple success/failure) would need further analysis and implementation within the Airflow DAG or BigQuery procedures.

## 6. Validation

To validate the migrated solution, follow these steps:

1.  **Prerequisites:**
    *   Ensure all manual steps from Section 4 are completed, including dataset, table, and procedure deployments, and source data ingestion.
    *   Ensure the `sof_ta_bpr_instance` table exists and is empty before testing, or has a known state for incremental tests.

2.  **Execute Test Script:**
    *   Open the `tests/test_r_ausd_bp_ta_bpr_instance.sql` file.
    *   **Placeholder Replacement:** Replace `your_project_id.your_dataset_id` with your actual project and dataset IDs.
    *   Execute the script in the BigQuery console or via a BigQuery client tool.

3.  **Verification Steps and "Passing" Criteria:**

    *   **Test Case 1: Successful Execution (Valid Parameters)**
        *   **Action:** The script calls `r_ausd_bp_ta_bpr_instance` with `test_stichtag_valid`.
        *   **Passing Means:**
            *   The `job_run_log` table contains a new entry for `r_ausd_bp_ta_bpr_instance` with `status = 'SUCCESS'`, `job_id = 'TEST_JOB_001'`, `entry_nr = '001'`, and `business_date` matching `test_stichtag_valid` (e.g., `2023-12-25`).
            *   The `record_count` in `job_run_log` accurately reflects the number of rows inserted into `sof_ta_bpr_instance` for the given `business_date`.
            *   The `sof_ta_bpr_instance` table contains the expected data based on the source tables (`cds_ta_cntrct`, `pds_ta_bpri_com`) and the logic in `d_ausd_bp_ta_bpr_instance`.
            *   No new entries are found in `job_error_log` for this run.

    *   **Test Case 2: Invalid `Stichtag` Format**
        *   **Action:** The script calls `r_ausd_bp_ta_bpr_instance` with `test_stichtag_invalid`.
        *   **Passing Means:**
            *   The `job_error_log` table contains a new entry for `r_ausd_bp_ta_bpr_instance` with `error_code = 193` and `error_arg` indicating the invalid format.
            *   No new entry is found in `job_run_log` for this specific call.
            *   The `sof_ta_bpr_instance` table remains unchanged (no data inserted/modified by this call).

    *   **Test Case 3: Missing `JobKennung` (Parameter Validation Error)**
        *   **Action:** The script calls `r_ausd_bp_ta_bpr_instance` with `p_JobKennung = NULL`.
        *   **Passing Means:**
            *   The `job_error_log` table contains a new entry for `r_ausd_bp_ta_bpr_instance` with `error_code = 1` (or the configured code for missing `Jobkennung`) and `error_arg = 'Jobkennung'`.
            *   No new entry is found in `job_run_log` for this specific call.
            *   The `sof_ta_bpr_instance` table remains unchanged.

    *   **Airflow DAG Validation:**
        *   Trigger the `k_ausd_bp_ta_bpr_instance_migration_dag` in Cloud Composer.
        *   **Passing Means:**
            *   The DAG run completes successfully in the Airflow UI.
            *   The `call_r_ausd_bp_ta_bpr_instance` task completes successfully.
            *   The `job_run_log` table shows a successful entry corresponding to the DAG run's parameters.
            *   The `sof_ta_bpr_instance` table is populated as expected.

## 7. Rollback procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be executed to revert to the original KornShell-based solution:

1.  **Stop New Migrated Runs:**
    *   **Cloud Composer:** Pause or delete the `k_ausd_bp_ta_bpr_instance_migration_dag` in the Airflow UI to prevent further executions of the BigQuery stored procedure.
    *   **Manual Triggers:** Ensure any manual triggers or external systems calling the BigQuery stored procedure are stopped.
2.  **Reactivate Original Script:**
    *   Ensure the original `k_ausd_bp_ta_bpr_instance.ksh` script and its dependencies are available and configured to run in the legacy environment.
    *   Re-enable the original scheduling mechanism for `k_ausd_bp_ta_bpr_instance.ksh`.
3.  **Clean Up BigQuery Artifacts (Optional, but Recommended):**
    *   **Delete BigQuery Stored Procedures:**
        ```sql
        DROP PROCEDURE IF EXISTS `your_project_id.your_dataset_id.r_ausd_bp_ta_bpr_instance`;
        DROP PROCEDURE IF EXISTS `your_project_id.your_dataset_id.d_ausd_bp_ta_bpr_instance`;
        ```
    *   **Delete BigQuery Logging Tables:**
        ```sql
        DROP TABLE IF EXISTS `your_project_id.your_dataset_id.job_error_log`;
        DROP TABLE IF EXISTS `your_project_id.your_dataset_id.job_run_log`;
        ```
    *   **Delete Target Data Table (if created solely for migration):**
        *   **CAUTION:** Only perform this step if `sof_ta_bpr_instance` was created specifically for this migration and does not contain any production data that needs to be preserved.
        ```sql
        DROP TABLE IF EXISTS `your_project_id.your_dataset_id.sof_ta_bpr_instance`;
        ```
4.  **Data Reconciliation (if necessary):**
    *   If any data was written to `sof_ta_bpr_instance` by the migrated process, and this data needs to be reconciled with the legacy system or removed, perform appropriate data cleanup or backfill operations.
    *   Verify that the legacy system's target data (e.g., `PoolBasisprodukt`) is in a consistent state.

This rollback procedure ensures a clean reversion to the previous state, minimizing disruption.