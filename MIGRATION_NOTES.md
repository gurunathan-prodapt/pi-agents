# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `k_ausd_bp_ta_bpr_basis_his.ksh`, which orchestrates the processing of data for the `PoolBasisprodukt` table via an external SQL script (`d_ausd_bp_ta_bpr_basis_his.sql`).

The job has been re-platformed from a legacy Unix/KornShell environment to Google Cloud Platform (GCP). The target architecture leverages:
*   **Google Cloud Composer (Apache Airflow)** for workflow orchestration and scheduling.
*   **Google BigQuery** for data processing, housing the migrated SQL logic within a Stored Procedure, and for logging job execution details.

## 2. Generated Artifacts

The migration process has produced the following artifacts:

*   **`sql/r_ausd_bp_ta_bpr_basis_his.sql`**
    *   **Role:** This file defines a BigQuery Stored Procedure named `project.dataset.r_ausd_bp_ta_bpr_basis_his`. It encapsulates the core data processing logic originally found in `d_ausd_bp_ta_bpr_basis_his.sql`, along with parameter validation and job logging. It performs a `TRUNCATE` and `INSERT` operation into the `project.dataset.sof$ta_bpr_basis_his` table.
    *   **Dependencies:** Requires the `project.dataset.job_log` table to exist for logging. Reads from `isbert_schema.cds$ta_cntrct` and `isbert_schema.pds$ta_bpri_com`.

*   **`sql/job_log_ddl.sql`**
    *   **Role:** This file contains the Data Definition Language (DDL) statement to create the `project.dataset.job_log` table in BigQuery. This table is used by the `r_ausd_bp_ta_bpr_basis_his` stored procedure to record execution status, error details, and processed record counts, replacing the legacy job management system integration.

*   **`dags/k_ausd_bp_ta_bpr_basis_his_dag.py`**
    *   **Role:** This is an Apache Airflow DAG (Directed Acyclic Graph) written in Python. It serves as the orchestrator for the migrated job. It defines a single task that calls the `project.dataset.r_ausd_bp_ta_bpr_basis_his` BigQuery Stored Procedure, passing runtime parameters. This DAG replaces the original KornShell script's role in environment setup, parameter parsing, and SQL execution.

## 3. Key Design Decisions

*   **BigQuery Stored Procedure for Core Logic:** The primary SQL logic from `d_ausd_bp_ta_bpr_basis_his.sql` was migrated into a BigQuery Stored Procedure.
    *   **Rationale:** This centralizes the data transformation logic within the data warehouse, leveraging BigQuery's performance and scalability. It allows for direct execution of SQL, parameterization, and robust error handling within the database context. It also simplifies the orchestration layer by reducing it to a single procedure call.
    *   **Trade-offs:** Requires refactoring of potentially proprietary SQL syntax to BigQuery Standard SQL. Debugging might shift from shell-level to BigQuery SQL-level.

*   **Cloud Composer (Airflow) for Orchestration:** Apache Airflow on Cloud Composer was chosen to manage the workflow.
    *   **Rationale:** Airflow provides a robust, scalable, and cloud-native solution for scheduling, monitoring, and managing complex data pipelines. It replaces the shell script's role in environment setup, parameter handling, and sequential execution, offering better visibility, retry mechanisms, and dependency management.
    *   **Trade-offs:** Introduces a new technology stack (Python/Airflow) and requires familiarity with Airflow concepts.

*   **Centralized BigQuery `job_log` Table:** A dedicated BigQuery table (`project.dataset.job_log`) was created for logging job execution details.
    *   **Rationale:** This replaces the original script's commented-out legacy job management system integration and temporary file-based record counting. It provides a structured, queryable, and centralized audit trail for all job runs, integrating seamlessly with BigQuery's data processing.

*   **Native BigQuery/Python for Utilities:** All shell-based utilities (e.g., `gestern.ksh`, `h_alis_date.ksh`, parameter validation) were replaced with native BigQuery SQL functions (e.g., `CURRENT_DATE()`, `PARSE_DATE()`) within the stored procedure or Python logic within the Airflow DAG.
    *   **Rationale:** Eliminates external script dependencies, simplifies the environment, and leverages the capabilities of the target platform.

*   **Parameter Handling:** Parameters are now passed from the Airflow DAG to the BigQuery Stored Procedure.
    *   **Rationale:** This provides a clear interface for job execution, allowing for dynamic parameterization via Airflow's UI, API, or scheduled triggers.

## 4. Manual Steps Before Go-Live

Before the migrated job can be run in production, the following manual steps must be completed:

1.  **GCP Project and BigQuery Dataset Setup:**
    *   Ensure a GCP project is active and billing is enabled.
    *   Create the target BigQuery dataset (e.g., `project.dataset`) where the `sof$ta_bpr_basis_his` table, `job_log` table, and stored procedure will reside.
    *   Identify and configure the BigQuery dataset for source tables (e.g., `isbert_schema`) or ensure appropriate external table definitions are in place.

2.  **IAM Permissions:**
    *   Grant the Cloud Composer service account (typically `service-<project-number>@cloudcomposer.gserviceaccount.com`) the necessary BigQuery roles:
        *   `BigQuery Data Editor` on the target dataset (`project.dataset`) to create/update tables and execute stored procedures.
        *   `BigQuery Data Viewer` on the source datasets (e.g., `isbert_schema`) to read data.

3.  **Airflow Connection:**
    *   Ensure the `google_cloud_default` connection is configured correctly in your Airflow environment (Cloud Composer). This connection is used by the `BigQueryExecuteQueryOperator`.

4.  **Deploy BigQuery `job_log` Table:**
    *   Execute the DDL from `sql/job_log_ddl.sql` in BigQuery to create the `project.dataset.job_log` table.
    ```bash
    bq query --use_legacy_sql=false --project_id=<your-gcp-project-id> \
      "$(cat sql/job_log_ddl.sql)"
    ```

5.  **Deploy BigQuery Stored Procedure:**
    *   Execute the DDL from `sql/r_ausd_bp_ta_bpr_basis_his.sql` in BigQuery to create the stored procedure.
    *   **IMPORTANT:** Replace `project.dataset` with your actual project ID and dataset name in the `CREATE OR REPLACE PROCEDURE` statement and all table references within the SQL.
    *   **IMPORTANT:** Replace `isbert_schema` with the actual dataset name where your source tables (`cds$ta_cntrct`, `pds$ta_bpri_com`) reside.
    ```bash
    bq query --use_legacy_sql=false --project_id=<your-gcp-project-id> \
      "$(cat sql/r_ausd_bp_ta_bpr_basis_his.sql)"
    ```

6.  **Deploy Airflow DAG:**
    *   Upload `dags/k_ausd_bp_ta_bpr_basis_his_dag.py` to the DAGs folder of your Cloud Composer environment.
    *   **IMPORTANT:** Edit the DAG file (`k_ausd_bp_ta_bpr_basis_his_dag.py`) to replace `your_gcp_project_id` with your actual GCP project ID and `us-central1` with your BigQuery dataset's actual `location` (e.g., `europe-west1`).

7.  **Scheduling:**
    *   The DAG is currently configured with `schedule=None`, meaning it will not run automatically. It is designed for manual triggers or external scheduling. If automated scheduling is required, update the `schedule` parameter in the DAG definition.

## 5. Known Gaps & Unresolved References

*   **Target Table Name Discrepancy:** The design document mentioned `PoolBasisprodukt` as the target table, but the generated BigQuery Stored Procedure (`r_ausd_bp_ta_bpr_basis_his.sql`) targets `project.dataset.sof$ta_bpr_basis_his`. This needs to be confirmed as the correct target table name for the migrated process.
*   **Source Table Schema:** The generated stored procedure assumes source tables `cds$ta_cntrct` and `pds$ta_bpri_com` exist within a BigQuery dataset named `isbert_schema`. This `isbert_schema` placeholder must be replaced with the actual BigQuery dataset where these source tables reside.
*   **`d_ausd_bp_ta_bpr_basis_his.sql` Content Review:** While the content of `d_ausd_bp_ta_bpr_basis_his.sql` was not available during the initial design analysis, its logic has been incorporated into the generated BigQuery Stored Procedure. A thorough review of the migrated SQL within `r_ausd_bp_ta_bpr_basis_his.sql` is crucial to ensure functional equivalence and optimal BigQuery performance.
*   **Legacy `starteSQLSkript` Implementation:** The exact logic within the original `starteSQLSkript` function was unknown. The migration assumes it primarily handled SQL execution and parameter passing. If it contained other complex logic (e.g., specific error handling, pre/post-processing), that logic would need to be identified and migrated separately.
*   **Commented-out Logic:** The original KornShell script contained significant commented-out sections related to `FOSJob` functions and file post-processing (`sed`, `sort`, `join`). This logic has *not* been migrated, assuming it is no longer active or required. If any of this logic needs to be reactivated, it will require additional migration effort.
*   **`dw_init` Contents:** The specific environment variables and configurations defined in the `$HOME/.dw_init` file were not analyzed. Any critical environment settings previously sourced from this file must be identified and configured as Airflow environment variables or BigQuery constants.
*   **Error Code Semantics:** The original script used specific error codes (e.g., 192, 193). While the BigQuery stored procedure includes error logging, the exact mapping of these legacy error codes to BigQuery's `ERROR_CODE()` or custom codes might be required if upstream systems depend on specific error values.

## 6. Validation

To validate the successful migration and execution of the job:

1.  **Trigger the Airflow DAG:**
    *   Navigate to the Airflow UI in Cloud Composer.
    *   Find the `k_ausd_bp_ta_bpr_basis_his_dag` DAG.
    *   Click the "Trigger DAG" button. You can optionally provide custom parameters for `p_job_kennung`, `p_eintrags_nr`, `p_stichtag`, and `p_wiederanlauf_wert`. For `p_stichtag`, ensure it's in `YYYY-MM-DD` format.

2.  **Monitor DAG Execution:**
    *   Observe the DAG run in the Airflow UI. The `call_basis_his_procedure` task should transition to a "success" state.
    *   Check the task logs for any BigQuery errors or warnings.

3.  **Verify BigQuery `job_log` Entry:**
    *   Query the `project.dataset.job_log` table in BigQuery.
    *   Look for a new entry with `job_name` matching `p_job_kennung` (default: `BERT_BASIS_HIS`) and `status` as 'SUCCESS'.
    *   Verify `records_processed` reflects the expected number of rows inserted.
    *   In case of failure, check for a 'FAILED' status, `error_nr`, and `error_arg` for details.

4.  **Verify Target Data:**
    *   Query the target table `project.dataset.sof$ta_bpr_basis_his` in BigQuery.
    *   Confirm that data has been inserted as expected and that the record count matches `records_processed` from the `job_log` table.
    *   Perform data quality checks on a sample of the inserted records to ensure transformations are correct.

**"Passing" Criteria:**
*   The `k_ausd_bp_ta_bpr_basis_his_dag` DAG completes successfully in Airflow.
*   A 'SUCCESS' entry is recorded in the `project.dataset.job_log` table for the corresponding job run, with a non-zero `records_processed` count (unless no data is expected).
*   The `project.dataset.sof$ta_bpr_basis_his` table contains the expected processed data, and its row count matches the `records_processed` value from the `job_log`.
*   No unexpected errors or warnings are observed in Airflow task logs or BigQuery job history.

## 7. Rollback Procedure

In case of issues or a decision to revert, follow these steps:

1.  **Disable/Delete Airflow DAG:**
    *   In the Airflow UI, toggle the `k_ausd_bp_ta_bpr_basis_his_dag` to "Off" to prevent further scheduled or manual runs.
    *   Optionally, delete the DAG file from the Composer DAGs folder.

2.  **Revert to Original Execution:**
    *   Resume execution of the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis_his.ksh` script in the legacy environment. Ensure its dependencies are intact and functional.

3.  **Clean Up BigQuery Objects (Optional, if migration is fully abandoned):**
    *   **Drop the BigQuery Stored Procedure:**
        ```sql
        DROP PROCEDURE IF EXISTS project.dataset.r_ausd_bp_ta_bpr_basis_his;
        ```
    *   **Drop the `job_log` table:**
        ```sql
        DROP TABLE IF EXISTS project.dataset.job_log;
        ```
    *   **Clear or Drop the target table data:** Depending on the impact, you might need to `TRUNCATE` or `DROP` the `project.dataset.sof$ta_bpr_basis_his` table if it was populated incorrectly by the migrated job.
        ```sql
        TRUNCATE TABLE project.dataset.sof$ta_bpr_basis_his;
        -- OR
        DROP TABLE IF EXISTS project.dataset.sof$ta_bpr_basis_his;
        ```
    *   **Note:** Exercise caution when dropping tables, especially if they contain data from other processes.