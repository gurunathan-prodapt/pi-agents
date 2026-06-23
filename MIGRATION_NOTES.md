# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the legacy KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_p_basisprod.ksh`. The original script orchestrated a data preparation process, including parameter validation, date handling, and execution of a core SQL script (`d_ausd_bp_ta_p_basisprod.sql`) to populate a `PoolBasisprodukt` table.

The migration targets Google Cloud Platform, specifically leveraging **BigQuery Stored Procedures** for orchestration and control flow, and **BigQuery SQL** for data transformation. An **Apache Airflow DAG (Cloud Composer)** is provided as an example for external scheduling and invocation. The goal is to replace the shell-based execution environment with a cloud-native, scalable, and managed solution.

## 2. Generated artifacts

The migration process has generated the following files:

*   **`ddl/error_log.sql`**
    *   **Role**: Defines the Data Definition Language (DDL) for the `error_log` table in BigQuery. This table will capture detailed error messages, codes, and severity for any failures encountered during the execution of the migrated BigQuery Stored Procedure, replacing the legacy shell-based error logging utilities.
*   **`ddl/job_log.sql`**
    *   **Role**: Defines the DDL for the `job_log` table in BigQuery. This table will record the start, completion, and status of each job run, including the number of records processed, replacing the legacy FOS job management functions.
*   **`ddl/poolbasisprodukt_target.sql`**
    *   **Role**: Defines the DDL for the `PoolBasisprodukt_target` table in BigQuery. This is the target table where the transformed data will be loaded, corresponding to the output of the original `d_ausd_bp_ta_p_basisprod.sql` script. The schema is inferred from the SQL logic.
*   **`sql/d_ausd_bp_ta_p_basisprod.sql`**
    *   **Role**: Contains the BigQuery SQL translation of the core data transformation logic originally found in `d_ausd_bp_ta_p_basisprod.sql`. This script performs the main `TRUNCATE` and `INSERT` operations to populate the `PoolBasisprodukt_target` table from various source tables.
*   **`stored_procedures/r_ausd_bp_ta_p_basisprod.sql`**
    *   **Role**: Defines the BigQuery Stored Procedure `r_ausd_bp_ta_p_basisprod`. This is the central component of the migrated job. It encapsulates the original KornShell script's orchestration logic, including parameter validation, date calculations, error handling, and the execution of the core BigQuery SQL transformation.
*   **`dags/k_ausd_bp_ta_p_basisprod_dag.py`**
    *   **Role**: An example Apache Airflow DAG (for Cloud Composer) that demonstrates how to schedule and invoke the `r_ausd_bp_ta_p_basisprod` BigQuery Stored Procedure. It handles passing necessary parameters like `job_kennung`, `eintrags_nr`, `stichtag`, `job_id`, and `run_id`.

## 3. Key design decisions

*   **Orchestration Shift to BigQuery Stored Procedures**: The KornShell script's primary role was orchestration (parameter handling, date logic, SQL execution). This logic is now fully migrated into a BigQuery Stored Procedure.
    *   **Why**: This centralizes the job's logic within BigQuery, leveraging its native scripting capabilities, robust error handling (`EXCEPTION WHEN ERROR`), and direct access to BigQuery SQL. It eliminates the need for external shell environments and client-side SQL execution tools (like SQLPlus).
    *   **Trade-offs**: Requires re-writing shell logic into BigQuery SQL scripting, which can be verbose for complex control flows.
*   **Native BigQuery SQL for Data Processing**: The core SQL logic from `d_ausd_bp_ta_p_basisprod.sql` is translated directly into BigQuery SQL and embedded within the stored procedure.
    *   **Why**: Maximizes performance by executing transformations directly within BigQuery's highly optimized engine. It removes the overhead of external database connections and data transfer.
    *   **Trade-offs**: Requires careful translation of any Oracle-specific SQL constructs or functions to their BigQuery equivalents.
*   **Centralized Logging and Error Handling**: Legacy shell utilities for logging and error messaging are replaced by dedicated BigQuery `job_log` and `error_log` tables.
    *   **Why**: Provides a structured, queryable, and centralized repository for job metadata and errors. This integrates seamlessly with Google Cloud's monitoring and alerting services (e.g., Cloud Logging, Cloud Monitoring).
    *   **Trade-offs**: Requires defining and managing these log tables.
*   **Cloud Composer for Scheduling**: An Airflow DAG is provided for external scheduling.
    *   **Why**: Cloud Composer (managed Airflow) offers a robust, scalable, and feature-rich platform for orchestrating complex data pipelines, replacing legacy schedulers like FOS. It provides retry mechanisms, dependency management, and rich monitoring.
    *   **Trade-offs**: Introduces a new component (Airflow) to manage, though it's a managed service.
*   **Elimination of Temporary Files**: The use of temporary files (`tmpFile`) for record counts is replaced by BigQuery scripting variables (`@@row_count`).
    *   **Why**: Simplifies the architecture, removes file system dependencies, and improves efficiency by keeping all operations within the BigQuery environment.
*   **Date Function Modernization**: Shell-based date calculations are replaced by BigQuery's native date functions (`CURRENT_DATE()`, `DATE_SUB()`, `SAFE.PARSE_DATE()`).
    *   **Why**: Improves reliability, readability, and performance of date operations.
*   **Handling of Commented-Out Code**: The `sed`, `sort`, `join` operations, being commented out in the source, are explicitly *not* migrated.
    *   **Why**: Focuses migration effort on active code. If these become active requirements, they would necessitate a separate redesign (B4 item) using BigQuery SQL or other GCP services (e.g., Dataflow for complex file processing).

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps are required:

1.  **BigQuery Project and Dataset Setup**:
    *   Ensure the BigQuery project (`project`) and dataset (`dataset`) specified in the generated code exist. If not, create them.
2.  **Source Table DDLs and Data Ingestion**:
    *   The generated SQL (`sql/d_ausd_bp_ta_p_basisprod.sql`) assumes the existence of several source tables (e.g., `project.dataset.sof$ta_cntrct_dist`, `project.dataset.sof$ta_iccid_vertrag`, etc.). **The DDLs for these source tables are NOT generated by this migration and must be created manually.**
    *   **Crucially, data must be ingested into these source tables.** This typically involves migrating data from the legacy Oracle database (or other sources) into BigQuery. This step is outside the scope of this specific job migration but is a prerequisite.
3.  **Deploy DDLs**:
    *   Execute `ddl/error_log.sql`, `ddl/job_log.sql`, and `ddl/poolbasisprodukt_target.sql` in BigQuery to create the necessary logging and target tables.
4.  **Deploy BigQuery Stored Procedure**:
    *   Execute `stored_procedures/r_ausd_bp_ta_p_basisprod.sql` in BigQuery to create the stored procedure.
5.  **IAM Permissions**:
    *   Ensure the service account that will execute the BigQuery Stored Procedure (e.g., the Cloud Composer service account) has the necessary BigQuery roles:
        *   `BigQuery Data Editor` on `project.dataset` to write to `error_log`, `job_log`, and `PoolBasisprodukt_target`.
        *   `BigQuery Data Viewer` on `project.dataset` for all source tables (e.g., `sof$ta_cntrct_dist`).
        *   `BigQuery Job User` to run BigQuery jobs.
6.  **Cloud Composer / Airflow DAG Deployment**:
    *   Upload the `dags/k_ausd_bp_ta_p_basisprod_dag.py` file to your Cloud Composer environment's DAGs folder.
    *   Configure the `BIGQUERY_PROJECT` and `BIGQUERY_DATASET` variables within the DAG to match your environment.
    *   Define the desired `schedule_interval` for the DAG.
7.  **Parameter Configuration**:
    *   Review and adjust the default values for `p_job_kennung`, `p_eintrags_nr`, and `p_wiederanlauf_wert` in the Airflow DAG or when directly calling the stored procedure. The `p_stichtag` parameter is dynamically generated from the Airflow execution date.

## 5. Known gaps & unresolved references

*   **Actual `d_ausd_bp_ta_p_basisprod.sql` Content**: The provided `sql/d_ausd_bp_ta_p_basisprod.sql` is a *placeholder* based on a hypothetical schema and join logic. The actual content of the original `d_ausd_bp_ta_p_basisprod.sql` file was not available during analysis. **This is the most critical gap.** The generated SQL must be thoroughly reviewed and validated against the original SQL script to ensure functional equivalence and data integrity. Any Oracle-specific functions or complex logic will need careful BigQuery translation.
*   **Source Table Schemas**: The DDLs for the source tables (e.g., `sof$ta_cntrct_dist`, `sof$ta_iccid_vertrag`) are assumed based on column names in the generated SQL. Their precise schema (data types, nullability, partitioning, clustering) from the legacy system needs to be accurately replicated in BigQuery.
*   **`p_wiederanlaufWert` Logic**: The `p_wiederanlauf_wert` parameter is passed to the stored procedure but its specific usage or impact on the core SQL logic (e.g., for restartability or incremental processing) is not explicitly defined in the provided design document or generated code. Its role needs to be clarified and implemented if it affects the data transformation.
*   **Commented-Out File Processing**: The original script contained commented-out `sed`, `sort`, `join` commands. As per design, these are not migrated. If these operations ever become active requirements, they would require a significant redesign (B4 migration bucket) using BigQuery SQL, Cloud Storage, and potentially Dataflow for complex file manipulations.
*   **Implicit Utility Script Logic**: While the purpose of utility scripts like `f_alis_msgerr.ksh` and `h_alis_date.ksh` is understood, their exact internal logic (e.g., specific error codes, date formats beyond `DDMMYYYY`) has been replaced by BigQuery native equivalents. Any subtle behaviors of these utilities might need further investigation if discrepancies arise.
*   **`semi_auto` Migration Bucket**: The job was categorized as `semi_auto`, indicating that manual review, refinement, and potentially some re-engineering are expected, especially concerning the core SQL logic and any nuanced behaviors of the original shell script.

## 6. Validation

Validation ensures the migrated job functions correctly and produces equivalent results to the legacy system.

1.  **Functional Test (BigQuery Stored Procedure)**:
    *   **How to run**:
        *   Manually call the stored procedure in BigQuery:
            ```sql
            CALL `project.dataset.r_ausd_bp_ta_p_basisprod`(
                p_job_kennung => 'TEST_JOB',
                p_eintrags_nr => '1',
                p_stichtag => '01012023', -- Use a specific test date
                p_wiederanlauf_wert => 0,
                p_job_id => 'MANUAL_TEST',
                p_run_id => GENERATE_UUID()
            );
            ```
        *   Trigger the Airflow DAG `k_ausd_bp_ta_p_basisprod_workflow` in Cloud Composer.
    *   **"Passing" means**:
        *   The BigQuery job for the stored procedure completes successfully without error.
        *   A `SUCCESS` entry is recorded in `project.dataset.job_log` for the corresponding `job_id` and `run_id`.
        *   No new entries appear in `project.dataset.error_log` for the run.
        *   The `record_count` in `project.dataset.job_log` matches the expected number of records processed by the legacy system for the same input date.
2.  **Data Validation**:
    *   **How to run**:
        *   Execute the legacy `k_ausd_bp_ta_p_basisprod.ksh` script for a specific `Stichtag` on the legacy system.
        *   Execute the migrated BigQuery Stored Procedure for the *same* `Stichtag`.
        *   Extract the output data from both the legacy `PoolBasisprodukt` table and the BigQuery `project.dataset.PoolBasisprodukt_target` table.
        *   Perform a row-by-row and column-by-column comparison of the two datasets. Tools like `dbt_utils.rec_diff` or custom SQL queries can be used for this.
    *   **"Passing" means**:
        *   The number of rows in the BigQuery target table is identical to the legacy output.
        *   All corresponding columns have identical values. Any expected differences (e.g., due to data type changes like `NUMBER` to `FLOAT64` or `STRING` to `DATE` parsing) should be documented and verified as acceptable.
3.  **Performance Validation**:
    *   **How to run**: Compare the execution time and resource consumption (CPU, I/O) of the migrated BigQuery job against the legacy script.
    *   **"Passing" means**: The BigQuery job completes within acceptable timeframes, ideally faster or comparable to the legacy system, and resource usage is efficient.
4.  **Parameter Edge Case Testing**:
    *   Test the stored procedure with missing parameters, invalid date formats for `p_stichtag`, and other edge cases to ensure robust error handling and logging.

## 7. Rollback procedure

In case of issues during or after go-live, the following rollback procedure can be executed:

1.  **Stop New Runs**: Immediately pause or disable the Airflow DAG `k_ausd_bp_ta_p_basisprod_workflow` in Cloud Composer to prevent any further execution of the migrated job.
2.  **Re-enable Legacy Job**: Re-activate the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_p_basisprod.ksh` script on the legacy system.
3.  **Data Recovery (if necessary)**:
    *   If the `project.dataset.PoolBasisprodukt_target` table was overwritten or truncated by the migrated job, and its data is critical for downstream processes, consider restoring it from a previous backup or re-running the legacy job to populate a separate, temporary target table.
    *   If the legacy job writes to the *same* target table as the migrated job, ensure that the legacy job is configured to write to a different, temporary target table or that the `PoolBasisprodukt_target` table can be safely truncated and reloaded by the legacy process.
4.  **Archive/Delete Migrated Components**:
    *   (Optional, for clean-up) Delete or archive the BigQuery Stored Procedure `project.dataset.r_ausd_bp_ta_p_basisprod`.
    *   (Optional, for clean-up) Delete or archive the BigQuery tables `project.dataset.error_log`, `project.dataset.job_log`, and `project.dataset.PoolBasisprodukt_target` if they are no longer needed or if a clean re-deployment is planned.
    *   (Optional, for clean-up) Remove the `dags/k_ausd_bp_ta_p_basisprod_dag.py` file from the Cloud Composer DAGs folder.
5.  **Root Cause Analysis**: Investigate the reason for the rollback and address the identified issues before attempting re-migration or re-deployment.