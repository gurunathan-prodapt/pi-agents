# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell orchestration script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis_his.ksh`. This script was responsible for parsing runtime parameters, validating dates, deriving "today" and "yesterday" dates, and orchestrating the execution of a core SQL script (`d_ausd_bp_ta_bpr_basis_his.sql`) to populate the `PoolBasisprodukt` table. It also included basic error handling and record counting.

The job has been migrated to Google Cloud Platform (GCP), utilizing:
*   **Google BigQuery** for data processing, storage, and hosting the transformed SQL logic as Stored Procedures.
*   **Cloud Composer (Apache Airflow)** for workflow orchestration, scheduling, and parameter management.

## 2. Generated artifacts

The migration process generated the following artifacts:

*   **`bigquery/ddl/pool_basisprodukt.sql`**
    *   **Role:** Defines the Data Definition Language (DDL) for the `PoolBasisprodukt` table in BigQuery. This table is the primary target for the data processed by the core SQL logic.
*   **`bigquery/procedures/d_ausd_bp_ta_bpr_basis_his.sql`**
    *   **Role:** A BigQuery Stored Procedure that encapsulates the core data transformation and loading logic. This procedure is the direct migration of the original `d_ausd_bp_ta_bpr_basis_his.sql` script, performing the actual ETL operations to populate `PoolBasisprodukt`.
*   **`bigquery/ddl/audit_error_log.sql`**
    *   **Role:** Defines the DDL for a BigQuery table (`error_log`) dedicated to capturing detailed error messages and context during job execution. This replaces the legacy `f_alis_msgerr.ksh` functionality.
*   **`bigquery/ddl/audit_job_audit.sql`**
    *   **Role:** Defines the DDL for a BigQuery table (`job_audit`) used to log the start, completion, status, and key metrics (like processed records) of each job run. This replaces implicit job tracking and commented-out FOS job management.
*   **`bigquery/procedures/r_ausd_bp_ta_bpr_basis_his.sql`**
    *   **Role:** A BigQuery Stored Procedure that serves as the primary orchestrator for the migrated job. It replaces the original `k_ausd_bp_ta_bpr_basis_his.ksh` script by handling parameter validation, date derivation, calling the `d_ausd_bp_ta_bpr_basis_his` procedure, and logging audit/error information to the respective BigQuery tables.
*   **`airflow/dags/k_ausd_bp_ta_bpr_basis_his_dag.py`**
    *   **Role:** An Apache Airflow DAG definition written in Python. This DAG is responsible for scheduling the job, passing runtime parameters, and triggering the execution of the `r_ausd_bp_ta_bpr_basis_his` BigQuery Stored Procedure.
*   **`bigquery/sql/optional_file_processing.sql`**
    *   **Role:** Contains placeholder BigQuery SQL code for the commented-out file processing logic (involving `sed`, `sort`, `join` operations on `.dat` files) found in the original KornShell script. This artifact is provided for potential future activation, requiring prior ingestion of the source files into BigQuery.

## 3. Key design decisions

The migration involved several key design decisions to transition from a KornShell-based legacy system to a GCP-native architecture:

*   **Orchestration Replatforming to Airflow and BigQuery Stored Procedures**:
    *   The original KornShell script's role as an orchestrator (parameter parsing, date derivation, SQL execution) was split. The core orchestration logic was refactored into a BigQuery Stored Procedure (`r_ausd_bp_ta_bpr_basis_his`), which is then scheduled and triggered by an Apache Airflow DAG. This leverages Airflow for robust scheduling, dependency management, and monitoring, while keeping data-centric orchestration logic close to the data in BigQuery.
*   **Data Processing Migration to BigQuery Stored Procedures**:
    *   The main data transformation logic, originally contained within `d_ausd_bp_ta_bpr_basis_his.sql` (executed via SQL*Plus), was migrated into a dedicated BigQuery Stored Procedure (`d_ausd_bp_ta_bpr_basis_his`). This fully utilizes BigQuery's scalable, serverless, and high-performance SQL engine for data manipulation.
*   **Native BigQuery Constructs for Shell Logic**:
    *   Shell script functionalities such as parameter validation (`getopts`, `pruefeParameterGesetzt`), date calculations (`h_alis_date.ksh`, `gestern.ksh`), and temporary file handling were replaced with native BigQuery SQL constructs. This includes `IF` statements, `ASSERT` for validation, `SAFE.PARSE_DATE`, `CURRENT_DATE()`, `DATE_SUB()`, and `DECLARE`d variables for intermediate values.
*   **Centralized Logging and Auditing**:
    *   The ad-hoc error handling (`f_alis_msgerr.ksh`) and any implicit/commented-out job tracking (e.g., FOS functions) were replaced by structured BigQuery audit tables (`project.audit.error_log` and `project.audit.job_audit`). This provides a consistent, queryable, and scalable mechanism for monitoring job execution, debugging errors, and auditing historical runs.
*   **Handling Commented-out Logic**:
    *   The original script contained commented-out `sed`, `sort`, `join` operations on external data files. Instead of migrating this unused code directly, a separate, optional BigQuery SQL script (`optional_file_processing.sql`) was generated. This allows for a deliberate decision on whether to activate this functionality in the future, requiring a clear strategy for data ingestion of the source files into BigQuery if chosen.

**Notable Trade-offs:**

*   **Increased SQL Complexity**: Consolidating orchestration and data processing logic into BigQuery Stored Procedures can lead to more complex and verbose SQL code compared to simpler DML scripts. However, this is a trade-off for improved maintainability, version control, and leveraging BigQuery's native features for robust data pipelines.
*   **New Infrastructure Dependency**: Introducing Cloud Composer (Airflow) adds a new layer of infrastructure to manage. While this provides superior scheduling, dependency management, and observability compared to a standalone shell script, it requires operational expertise in Airflow.
*   **Data Ingestion for Optional Files**: If the commented-out file processing logic is activated, it introduces a new dependency on ingesting those raw `.dat` files into BigQuery, which was not a direct part of the original script's execution flow. This requires defining and implementing a separate ingestion pipeline.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **GCP Project and Billing Setup**:
    *   Ensure a Google Cloud Project is provisioned and has billing enabled.
2.  **BigQuery Datasets Creation**:
    *   Create the primary BigQuery dataset for tables and procedures (e.g., `your-gcp-project.isbert_data`). This will be referenced as `project.dataset`.
    *   Create a separate BigQuery dataset for audit and error logs (e.g., `your-gcp-project.audit_logs`). This will be referenced as `project.audit`.
3.  **BigQuery Table and Procedure Deployment**:
    *   Execute the DDL scripts to create the necessary tables:
        *   `bigquery/ddl/pool_basisprodukt.sql`
        *   `bigquery/ddl/audit_error_log.sql`
        *   `bigquery/ddl/audit_job_audit.sql`
    *   Deploy the BigQuery Stored Procedures:
        *   `bigquery/procedures/d_ausd_bp_ta_bpr_basis_his.sql`
        *   `bigquery/procedures/r_ausd_bp_ta_bpr_basis_his.sql`
    *   Ensure all `project.dataset` and `project.audit` placeholders in the generated SQL are replaced with your actual dataset IDs.
4.  **IAM Permissions Configuration**:
    *   The Google Cloud Service Account used by your Cloud Composer environment (Airflow) must be granted appropriate permissions:
        *   `BigQuery Data Editor` role (or more granular permissions like `bigquery.tables.create`, `bigquery.tables.updateData`, `bigquery.routines.create`, `bigquery.routines.update`, `bigquery.jobs.create`) on both `project.dataset` and `project.audit`.
        *   `BigQuery Job User` role to run BigQuery jobs.
5.  **Cloud Composer Environment Setup**:
    *   Verify that a Cloud Composer environment is provisioned, healthy, and accessible.
    *   Ensure the `google_cloud_default` Airflow connection is configured correctly to use the appropriate GCP project and service account.
6.  **Airflow DAG Deployment and Configuration**:
    *   Upload the `airflow/dags/k_ausd_bp_ta_bpr_basis_his_dag.py` file to the DAGs folder of your Cloud Composer environment.
    *   **Update Placeholders**: Modify the `project_id` and `dataset_id` placeholders within the DAG file to match your actual GCP project ID and BigQuery dataset ID.
    *   **Define Schedule**: Set the desired `schedule` for the DAG in `k_ausd_bp_ta_bpr_basis_his_dag.py` (e.g., `schedule='0 0 * * *'` for daily execution).
7.  **Optional File Processing (if activated)**:
    *   If the commented-out `sed`, `sort`, `join` logic is to be activated:
        *   Implement a data ingestion pipeline to load the source `.dat` and `.csv` files (e.g., `cibasis_data24.dat`, `cibasis_data96.dat`, `cibasis_fax.dat`) into BigQuery tables (e.g., `project.dataset.cibasis_data24_raw`, etc.). This might involve Cloud Storage loads, external tables, or other ingestion methods.
        *   Deploy the relevant parts of `bigquery/sql/optional_file_processing.sql` as BigQuery tables, views, or additional procedures, and integrate their execution into the Airflow DAG or `r_ausd_bp_ta_bpr_basis_his` procedure.
8.  **Secrets Management (if applicable)**:
    *   If any sensitive parameters or environment-specific configurations are required beyond what's passed via Airflow parameters, consider using GCP Secret Manager and integrating it with Airflow.

## 5. Known gaps & unresolved references

The following items have been identified as known gaps, requiring further attention or decisions:

*   **Core SQL Script (`d_ausd_bp_ta_bpr_basis_his.sql`) Detailed Migration**: The generated `bigquery/procedures/d_ausd_bp_ta_bpr_basis_his.sql` is currently a placeholder. The actual, detailed migration of the original SQL script's logic into BigQuery SQL is a critical prerequisite and must be completed. This includes defining the precise schema for `PoolBasisprodukt` and any intermediate tables based on the source SQL.
*   **Commented-out Logic Activation Decision**: A definitive decision is required on whether the commented-out `sed`, `sort`, `join` operations from the original KornShell script should be reactivated in the target environment. If so, the `bigquery/sql/optional_file_processing.sql` needs to be fully implemented, and a data ingestion strategy for the source `.dat` files must be defined and executed.
*   **FOS Job Management Replacement**: The original script referenced `FOSJobDeaktivate` and `FOSJobErzeugeEintrag`, indicating integration with a legacy job management system. While the `job_audit` table provides basic auditing, a comprehensive replacement for the FOS system (if it's a critical enterprise job scheduler/monitor) might require further integration with Cloud Monitoring, Cloud Logging, or custom GCP solutions to replicate full functionality.
*   **Error Handling Detail**: The `f_alis_msgerr.ksh` script likely provided specific error codes and detailed messages. The BigQuery error logging (`error_log` table) captures general messages, but a more granular mapping of legacy error codes/types might be beneficial for operational consistency and debugging.
*   **Character Encoding**: The comment `Andre Lbbers` in the original script hints at potential character encoding issues (e.g., Latin-1 to UTF-8). This needs to be verified during data ingestion and processing in BigQuery to ensure data integrity.
*   **Record Count `WHERE` Clause Refinement**: The `TODO` in `bigquery/procedures/r_ausd_bp_ta_bpr_basis_his.sql` regarding the `v_records` count needs refinement. The `WHERE` clause for `SELECT COUNT(*)` must accurately identify records processed by the *current run* of `d_ausd_bp_ta_bpr_basis_his`. This may require adding a `job_id` or `run_timestamp` column to the `PoolBasisprodukt` table to uniquely identify records inserted by a specific execution.
*   **`PoolBasisprodukt` Schema Definition**: The DDL for `PoolBasisprodukt` in `bigquery/ddl/pool_basisprodukt.sql` is currently a placeholder. The actual, complete schema (column names, data types, nullability) must be derived from a thorough analysis of the `d_ausd_bp_ta_bpr_basis_his.sql` script's output.

## 6. Validation

Validation ensures the migrated job functions correctly, produces accurate results, and meets performance expectations.

**How to Run Tests:**

1.  **Trigger Airflow DAG**:
    *   Navigate to the Airflow UI for your Cloud Composer environment.
    *   Find the `k_ausd_bp_ta_bpr_basis_his_orchestration` DAG.
    *   Manually trigger the DAG, providing test parameters (e.g., `job_kennung`, `eintrags_nr`, `stichtag` in `DDMMYYYY` format, `wiederanlauf_wert`). Use parameters that mimic typical production runs and also edge cases (e.g., invalid `stichtag`).
2.  **Monitor Airflow Task Execution**:
    *   Observe the `execute_bigquery_orchestrator_sp` task in the Airflow UI's Graph View or Grid View. It should transition through `running` and eventually to `success`.
3.  **Verify BigQuery Stored Procedure Execution**:
    *   In the BigQuery UI, navigate to "SQL workspace" -> "Query history".
    *   Confirm that the `r_ausd_bp_ta_bpr_basis_his` and `d_ausd_bp_ta_bpr_basis_his` procedures were executed without errors. Review job details for any warnings or unexpected behavior.
4.  **Data Verification**:
    *   Query the target table `project.dataset.PoolBasisprodukt` to ensure that data has been inserted correctly.
    *   Compare the inserted data (count, content, data types, formats) against the expected output from the legacy system for the same input parameters.
    *   If the optional file processing was activated, query `project.dataset.cibasisprodukt` (or equivalent output tables) to verify the transformed data.
5.  **Audit Log Check**:
    *   Query `project.audit.job_audit` to confirm a `COMPLETED` entry for the triggered job run. Verify `start_time`, `end_time`, `duration_seconds`, and `processed_records` are accurate.
    *   For error test cases, query `project.audit.error_log` to ensure error details are captured correctly.

**What "Passing" Means:**

*   **Successful Execution**: The Airflow DAG `k_ausd_bp_ta_bpr_basis_his_orchestration` completes successfully without any task failures for all valid test cases.
*   **Data Integrity**: The data in `project.dataset.PoolBasisprodukt` (and any other target tables) is accurate, complete, and matches the expected output of the legacy system for a given set of input parameters. All data types and formats are correct.
*   **Audit Trail**: The `project.audit.job_audit` table contains a `COMPLETED` entry for each successful job run, with `processed_records` matching the actual count of records processed.
*   **Error Handling**: For invalid input parameters, the Airflow task fails, and a corresponding error entry is logged in `project.audit.error_log` with relevant details.
*   **Performance**: The execution time of the migrated job in BigQuery/Airflow is comparable to or better than the legacy KornShell script.

## 7. Rollback procedure

In the event of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated to revert to the legacy system:

1.  **Immediate Action**:
    *   **Deactivate Airflow DAG**: Pause or delete the `k_ausd_bp_ta_bpr_basis_his_orchestration` DAG in the Airflow UI to prevent any further executions of the migrated job.
    *   **Reactivate Legacy Script**: Re-enable and restart the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis_his.ksh` script in the legacy environment. Ensure its scheduler (e.g., cron) is reactivated.
2.  **Data Rollback (if necessary)**:
    *   If the `PoolBasisprodukt` table in BigQuery was directly modified or truncated by the migrated job, and these changes are undesirable, restore the table from a pre-migration backup.
    *   If the migration involved writing to a *new* BigQuery table, no data rollback is typically needed for the target table itself, as the legacy system operates on its own data store.
3.  **Cleanup (optional, but recommended for a clean rollback)**:
    *   **Drop BigQuery Procedures**: Drop the `project.dataset.r_ausd_bp_ta_bpr_basis_his` and `project.dataset.d_ausd_bp_ta_bpr_basis_his` stored procedures from BigQuery.
    *   **Drop BigQuery Tables**: Drop the `project.dataset.PoolBasisprodukt` table (if it was created solely for the migration) and the audit tables (`project.audit.error_log`, `project.audit.job_audit`).
    *   **Remove Airflow DAG**: Delete the `k_ausd_bp_ta_bpr_basis_his_orchestration.py` file from the Cloud Composer DAGs folder.
    *   **Remove Optional File Processing Assets**: If `optional_file_processing.sql` was deployed, remove any associated BigQuery tables, views, or procedures.
4.  **Verification**:
    *   Confirm that the legacy job is running as expected in the original environment and producing correct results.
    *   Verify that no new BigQuery jobs related to the migrated process are being triggered.