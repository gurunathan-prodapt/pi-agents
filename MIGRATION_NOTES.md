# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the legacy KornShell script `k_ausd_bp_ta_bpr_basis_his.ksh` and its associated core SQL logic (`d_ausd_bp_ta_bpr_basis_his.sql`). The original script served as an orchestration wrapper for a data preparation job related to `PoolBasisprodukt`, including parameter validation, environment setup, SQL execution, and potential file post-processing.

The job has been migrated to **Google Cloud Platform (GCP)**, leveraging:
*   **Google Cloud Composer (Apache Airflow)** for orchestration, scheduling, and parameter management.
*   **Google BigQuery** for all data storage, core data transformation logic (via Stored Procedures), and handling of file-based post-processing.

## 2. Generated Artifacts

The migration process generated the following files:

*   **`dags/k_ausd_bp_ta_bpr_basis_his_dag.py`**
    *   **Role**: This is the main Airflow DAG (Directed Acyclic Graph) that orchestrates the entire job. It replaces the KornShell script's control flow, parameter validation, and execution management. It defines tasks for parameter preparation, logging, executing the core BigQuery Stored Procedure, and optionally executing the post-processing BigQuery Stored Procedure.
*   **`bigquery/stored_procedures/sof.d_ausd_bp_ta_bpr_basis_his.sql`**
    *   **Role**: This BigQuery Stored Procedure encapsulates the core data transformation logic originally found in `d_ausd_bp_ta_bpr_basis_his.sql`. It is responsible for truncating and reloading the `sof.ta_bpr_basis_his` table based on the provided `p_process_date` and data from `cds.ta_cntrct` and `pds.ta_bpri_com`. It includes BigQuery-native error handling.
*   **`bigquery/ddl/cds.ta_cntrct.sql`**
    *   **Role**: Data Definition Language (DDL) script for creating the `cds.ta_cntrct` table in BigQuery. This table is a source for the core data transformation.
*   **`bigquery/ddl/pds.ta_bpri_com.sql`**
    *   **Role**: DDL script for creating the `pds.ta_bpri_com` table in BigQuery. This table is also a source for the core data transformation.
*   **`bigquery/ddl/sof.ta_bpr_basis_his.sql`**
    *   **Role**: DDL script for creating the `sof.ta_bpr_basis_his` table in BigQuery. This is the target table where the processed data is stored.
*   **`bigquery/ddl/isbert_dataset.job_control.sql`**
    *   **Role**: DDL script for creating a `job_control` table in BigQuery. This table is intended for logging job execution status, start/end times, processed rows, and error messages, replacing the legacy `FOSJobErzeugeEintrag` and temporary file-based logging.
*   **`bigquery/stored_procedures/isbert_dataset.post_process_cibasis_data.sql`**
    *   **Role**: This BigQuery Stored Procedure re-implements the commented-out `sed`, `sort`, and `join` operations from the original KornShell script. It processes data from assumed external tables (`cibasis_data24_ext`, `cibasis_data96_ext`, `cibasis_fax_ext`) and produces a final output table `isbert_dataset.cibasisprodukt_csv` using BigQuery SQL functions and temporary tables.

## 3. Key Design Decisions

*   **Orchestration Re-platforming (KornShell to Airflow)**:
    *   **Why**: Airflow provides a robust, scalable, and cloud-native solution for workflow orchestration, offering superior scheduling, monitoring, logging, and error handling capabilities compared to a custom KornShell script. It integrates seamlessly with other GCP services.
    *   **Trade-offs**: Requires familiarity with Python and Airflow concepts; initial setup and maintenance of an Airflow environment.
*   **Core Logic Migration (SQL*Plus to BigQuery Stored Procedure)**:
    *   **Why**: Migrating the `d_ausd_bp_ta_bpr_basis_his.sql` logic into a BigQuery Stored Procedure eliminates the dependency on Oracle SQL*Plus, allowing the core data transformation to execute natively within BigQuery. This leverages BigQuery's performance, scalability, and cost-efficiency for large-scale data processing.
    *   **Trade-offs**: Requires translation of Oracle-specific SQL syntax (e.g., `NVL` to `IFNULL`, `TO_DATE` to `PARSE_DATE`, string concatenation `||` to `CONCAT`), removal of Oracle hints, and adaptation of error handling.
*   **Parameter Handling and Validation (Shell Script to Airflow/BigQuery)**:
    *   **Why**: Input parameters (`JobKennung`, `EintragsNr`, `Stichtag`, `wiederanlaufWert`) are now managed by Airflow DAG parameters and validated within a dedicated Airflow task (`validate_and_prepare_params`). Date format validation is performed in Python, and parameters are passed to the BigQuery Stored Procedure. This centralizes parameter management and validation within the orchestration layer.
    *   **Trade-offs**: Requires explicit mapping of shell arguments to Airflow DAG parameters and BigQuery procedure inputs.
*   **Date Utilities (Custom Shell Scripts to BigQuery/Python Native Functions)**:
    *   **Why**: Legacy date utility scripts (`h_alis_date.ksh`, `gestern.ksh`) are replaced by native Python `datetime` functions within Airflow and BigQuery's built-in date functions (`CURRENT_DATE()`, `DATE_SUB()`). This simplifies the solution by removing external script dependencies.
*   **Logging and Control (Temporary Files/FOSJobErzeugeEintrag to BigQuery Control Table)**:
    *   **Why**: Job execution logs, record counts, and status updates are now directed to a dedicated BigQuery `job_control` table. This provides a centralized, queryable, and persistent record of job executions, replacing ephemeral temporary files and integrating with a modern data warehousing approach.
    *   **Trade-offs**: Requires defining a schema for the control table and implementing explicit `INSERT` statements in the Airflow DAG.
*   **File Post-processing (Shell Commands to BigQuery Stored Procedure)**:
    *   **Why**: The commented-out `sed`, `sort`, and `join` operations are re-implemented as a BigQuery Stored Procedure. This keeps all data processing within BigQuery, avoiding the need for external file systems, shell execution, or complex data transfers. It leverages BigQuery's SQL capabilities for string manipulation, deduplication, and joining.
    *   **Trade-offs**: Translating complex shell commands (especially `join` with `-o` and `-a` options) to BigQuery SQL can be intricate and requires careful understanding of the original logic and assumptions about data format (e.g., delimiters, field positions). It also assumes the source flat files are ingested into BigQuery as external or staging tables.
*   **Data Storage (Mixed to BigQuery)**:
    *   **Why**: All source, intermediate, and target data are consolidated within BigQuery. This provides a unified, high-performance, and scalable data platform, simplifying data governance and access.

## 4. Manual Steps Before Go-Live

Before deploying and running the migrated job, the following manual steps are required:

1.  **GCP Project ID Configuration**:
    *   Replace all instances of `your-gcp-project` in the generated SQL and Python files with your actual GCP Project ID.
2.  **BigQuery Dataset Creation**:
    *   Ensure the following BigQuery datasets exist in your GCP project. If not, create them:
        *   `sof`
        *   `cds`
        *   `pds`
        *   `isbert_dataset`
3.  **BigQuery Table DDL Execution**:
    *   Execute the DDL scripts to create the necessary tables:
        *   `bigquery/ddl/cds.ta_cntrct.sql`
        *   `bigquery/ddl/pds.ta_bpri_com.sql`
        *   `bigquery/ddl/sof.ta_bpr_basis_his.sql`
        *   `bigquery/ddl/isbert_dataset.job_control.sql`
    *   **Important**: Ensure `cds.ta_cntrct` and `pds.ta_bpri_com` are populated with relevant source data.
4.  **BigQuery Stored Procedure Deployment**:
    *   Deploy the BigQuery Stored Procedures by executing their respective SQL files:
        *   `bigquery/stored_procedures/sof.d_ausd_bp_ta_bpr_basis_his.sql`
        *   `bigquery/stored_procedures/isbert_dataset.post_process_cibasis_data.sql`
5.  **IAM Permissions**:
    *   The Service Account used by your Airflow environment (e.g., the Composer environment's service account) must have the necessary BigQuery permissions. This typically includes:
        *   `BigQuery Data Editor` on the `sof`, `cds`, `pds`, and `isbert_dataset` datasets.
        *   `BigQuery Job User` to run queries and procedures.
6.  **Airflow GCP Connection**:
    *   Verify that the `google_cloud_default` connection is correctly configured in your Airflow environment. This connection is used by the `BigQueryExecuteStoredProcedureOperator`.
7.  **Airflow DAG Deployment**:
    *   Upload `dags/k_ausd_bp_ta_bpr_basis_his_dag.py` to your Airflow DAGs folder.
8.  **Data Ingestion for Post-Processing (if required)**:
    *   If the commented-out file post-processing logic is indeed required, ensure that the source flat files (`cibasis_data24.dat`, `cibasis_data96.dat`, `cibasis_fax.dat`) are ingested into BigQuery. This typically involves:
        *   Uploading them to a Cloud Storage bucket.
        *   Creating BigQuery external tables (e.g., `isbert_dataset.cibasis_data24_ext`) pointing to these files, ensuring the schema (e.g., a single `line` column) matches the procedure's expectations.
9.  **Configure Airflow DAG Scheduling**:
    *   The DAG is currently set to `schedule=None`. Configure the appropriate schedule (e.g., `@daily`, `cron` expression) for production execution.
10. **Activate Logging in Airflow DAG**:
    *   Uncomment and configure the `log_job_start` and `log_job_end` tasks in `k_ausd_bp_ta_bpr_basis_his_dag.py` to write actual job status and metrics to the `isbert_dataset.job_control` table. This requires populating the `sql` parameters within the `BigQueryExecuteQueryOperator` calls.
11. **Implement Procedure Output Handling**:
    *   The `capture_and_log_results` task in the Airflow DAG currently has placeholder logic for reading the BigQuery Stored Procedure's output. This task needs to be fully implemented to query the BigQuery `job_control` table (where the procedure logs its status) or another mechanism to retrieve the `status` and `rows_inserted` values from the `d_ausd_bp_ta_bpr_basis_his` procedure.

## 5. Known Gaps & Unresolved References

*   **`wiederanlaufWert` Parameter Usage**: The `p_wiederanlaufWert` parameter is passed by the Airflow DAG but is not currently utilized within the `sof.d_ausd_bp_ta_bpr_basis_his` BigQuery Stored Procedure. The procedure performs a `TRUNCATE` and `INSERT`, implying a full reload rather than a restartable process. If restart logic is required, the BigQuery Stored Procedure needs to be modified to incorporate this parameter and implement the corresponding logic (e.g., conditional deletion, incremental processing).
*   **Commented-out File Post-processing Requirement**: The original KornShell script had significant commented-out sections for `sed`, `sort`, and `join` operations. While `isbert_dataset.post_process_cibasis_data.sql` has been generated to handle this, it needs to be confirmed whether this functionality is actually required in the target state. If not, the `post_process_cibasis_files` task in the Airflow DAG should be removed or conditionally disabled.
*   **Full `d_ausd_bp_ta_bpr_basis_his.sql` Content**: The migration design document noted that the full content of the original `d_ausd_bp_ta_bpr_basis_his.sql` was unknown. The generated BigQuery Stored Procedure is based on the provided SQL snippet. If the original SQL contained additional logic not present in the snippet, it represents a gap that needs to be addressed.
*   **Complexity Tier**: The complexity tier of the original KornShell script was undetermined. This might impact future effort estimations for similar migrations.
*   **Custom Utility Script Logic**: While common functionalities of `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, and `gestern.ksh` have been replaced by native Airflow/BigQuery features, any highly specific or complex custom logic within these original scripts would need further review to ensure accurate replication.
*   **`cibasis_data*.dat` Ingestion**: The `post_process_cibasis_data` procedure assumes the existence of BigQuery external tables for `cibasis_data24.dat`, `cibasis_data96.dat`, and `cibasis_fax.dat`. The actual mechanism for ingesting these files into Cloud Storage and then exposing them as BigQuery external tables (or loading them into native tables) needs to be established and maintained.
*   **Airflow `capture_and_log_results` Task Implementation**: This task currently contains placeholder logic for retrieving the status and row counts from the BigQuery Stored Procedure. A concrete implementation is required to query the `isbert_dataset.job_control` table or another mechanism to accurately capture the procedure's outcome.

## 6. Validation

To validate the successful migration and functionality of the new job:

1.  **Deploy and Trigger the DAG**:
    *   Ensure the `k_ausd_bp_ta_bpr_basis_his` DAG is deployed to your Airflow environment.
    *   Trigger the DAG manually from the Airflow UI.
    *   Test with various parameters:
        *   Valid `Stichtag` (e.g., `DDMMYYYY` format).
        *   Invalid `Stichtag` (e.g., `YYYY-MM-DD`, malformed date).
        *   Missing required parameters (`JobKennung`, `EintragsNr`, `Stichtag`).
2.  **Monitor Airflow Logs**:
    *   Observe the task logs in the Airflow UI for any errors or unexpected behavior.
3.  **Verify BigQuery Stored Procedure Execution**:
    *   Check the BigQuery job history for the execution of `sof.d_ausd_bp_ta_bpr_basis_his` and `isbert_dataset.post_process_cibasis_data` (if enabled).
    *   Review the job details for any errors or warnings.
4.  **Data Validation**:
    *   **Core Data**: Query the target table `your-gcp-project.sof.ta_bpr_basis_his`.
        *   **Passing Criteria**: The table should be populated with the expected data, matching the logic of the original `d_ausd_bp_ta_bpr_basis_his.sql`. Row counts and data content should align with the legacy system's output for the same `Stichtag`.
    *   **Post-processing Data (if enabled)**: Query `your-gcp-project.isbert_dataset.cibasisprodukt_csv`.
        *   **Passing Criteria**: The output table should contain data transformed according to the `sed`, `sort`, and `join` logic, matching the expected output of the legacy file processing.
5.  **Control Table Verification**:
    *   Query `your-gcp-project.isbert_dataset.job_control`.
    *   **Passing Criteria**: There should be new entries corresponding to each DAG run, with correct `job_name`, `start_time`, `end_time`, `status` (e.g., 'SUCCESS' or 'FAILED'), `process_date`, and `rows_processed`.
6.  **Error Handling Validation**:
    *   **Passing Criteria**: When an invalid `Stichtag` is provided, the `validate_and_prepare_params` task should fail with an `AirflowException` and a clear error message. The `job_control` table should reflect a 'FAILED' status for this run.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be executed:

1.  **Pause/Delete Airflow DAG**:
    *   Immediately pause or delete the `k_ausd_bp_ta_bpr_basis_his` DAG in the Airflow UI to prevent further executions.
2.  **Re-enable Legacy Job**:
    *   Re-enable the original `k_ausd_bp_ta_bpr_basis_his.ksh` KornShell script in the legacy environment.
3.  **BigQuery Data Reversion**:
    *   **Target Table (`sof.ta_bpr_basis_his`)**:
        *   If the data in `sof.ta_bpr_basis_his` is corrupted or incorrect, truncate the table:
            ```sql
            TRUNCATE TABLE `your-gcp-project.sof.ta_bpr_basis_his`;
            ```
        *   If a backup of the table existed before the migration, restore it.
    *   **Post-processing Output (`isbert_dataset.cibasisprodukt_csv`)**:
        *   If this table was created/modified incorrectly, truncate or drop it:
            ```sql
            TRUNCATE TABLE `your-gcp-project.isbert_dataset.cibasisprodukt_csv`;
            -- OR
            DROP TABLE `your-gcp-project.isbert_dataset.cibasisprodukt_csv`;
            ```
4.  **BigQuery Stored Procedure Reversion**:
    *   If there were previous versions of the `sof.d_ausd_bp_ta_bpr_basis_his` or `isbert_dataset.post_process_cibasis_data` procedures, revert them. Otherwise, the new procedures can remain as they do not actively run without the DAG.
5.  **Monitor Legacy Job**:
    *   Verify that the re-enabled legacy job runs successfully and produces the correct output.
6.  **Post-Rollback Analysis**:
    *   Investigate the root cause of the rollback to address issues before attempting re-migration.