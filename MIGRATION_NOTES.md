# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `k_ausd_bp_ta_bpr_instance.ksh`, which orchestrated an SQL data preparation job. The original script handled environment setup, parameter parsing and validation, date derivation, and the execution of a core SQL transformation (`d_ausd_bp_ta_bpr_instance.sql`).

The job has been migrated to the Google Cloud Platform (GCP), leveraging:
*   **BigQuery** for all data storage and SQL-based transformations.
*   **Cloud Composer (Airflow)** for orchestration, replacing the KornShell script's control flow.

The migration translates the shell script's logic into a Python-based Airflow DAG and the core SQL logic into BigQuery Stored Procedures, ensuring a cloud-native, scalable, and maintainable solution.

## 2. Generated artifacts

The migration produced the following artifacts:

*   **`bigquery/ddl/pool_basisprodukt.sql`**
    *   **Role**: Defines the BigQuery Data Definition Language (DDL) for the `PoolBasisprodukt` table. This table serves as the final destination for the processed basis product instance data, replacing its legacy counterpart (likely in Oracle). It includes a `processing_date` column for partitioning.
*   **`bigquery/ddl/sof_ta_bpr_instance_staging.sql`**
    *   **Role**: Defines the BigQuery DDL for a staging table named `sof_ta_bpr_instance_staging`. This table acts as an intermediate storage area during the data transformation process, mirroring the function of a similar staging table in the original SQL logic.
*   **`bigquery/procedures/d_ausd_bp_ta_bpr_instance_core.sql`**
    *   **Role**: A BigQuery Stored Procedure that encapsulates the core data transformation logic originally found in `d_ausd_bp_ta_bpr_instance.sql`. It performs data truncation, insertion into the staging table, and then inserts the processed data into the final `PoolBasisprodukt` table. It also returns the count of processed records.
*   **`bigquery/procedures/r_ausd_bp_ta_bpr_instance.sql`**
    *   **Role**: A BigQuery Orchestration Stored Procedure that replaces the high-level control flow of the original `k_ausd_bp_ta_bpr_instance.ksh` script. It handles parameter validation, date derivation (replacing `gestern.ksh`), and calls the `d_ausd_bp_ta_bpr_instance_core` procedure. It also exposes the `records_processed` count as an `OUT` parameter.
*   **`airflow/dags/k_ausd_bp_ta_bpr_instance_dag.py`**
    *   **Role**: An Apache Airflow DAG (Directed Acyclic Graph) written in Python. This DAG is the primary orchestrator on GCP, replacing the KornShell script entirely. It defines the workflow, parses input parameters, and executes the `r_ausd_bp_ta_bpr_instance` BigQuery Stored Procedure. It's designed for deployment on Cloud Composer.

## 3. Key design decisions

The following key design decisions were made during the migration:

*   **Cloud-Native Data Processing with BigQuery**: BigQuery was chosen as the target data platform due to its serverless architecture, scalability, cost-effectiveness for large datasets, and native SQL capabilities. This eliminates the need for managing traditional database instances.
*   **Orchestration with Cloud Composer (Airflow)**: Airflow, via Cloud Composer, was selected to replace the KornShell script's orchestration logic. Airflow provides robust scheduling, dependency management, monitoring, and logging capabilities, which are superior to custom shell scripting for complex workflows.
*   **Encapsulation of SQL Logic in BigQuery Stored Procedures**: The core SQL transformation logic (`d_ausd_bp_ta_bpr_instance.sql`) was translated into a BigQuery Stored Procedure (`d_ausd_bp_ta_bpr_instance_core`). This promotes modularity, reusability, and allows BigQuery to optimize the execution of the entire SQL block. A higher-level orchestration procedure (`r_ausd_bp_ta_bpr_instance`) was created to handle parameter validation and date derivation, centralizing the job's logic within BigQuery.
*   **Parameter Handling and Date Derivation**:
    *   **Original**: KornShell `getopts` for parameters, sourcing helper scripts (`h_alis_parameter.ksh`, `gestern.ksh`) for validation and date derivation.
    *   **Migrated**: Airflow DAG parameters provide a structured way to pass inputs. These are then passed to the BigQuery orchestration procedure (`r_ausd_bp_ta_bpr_instance`), which performs date parsing (`PARSE_DATE`) and derivation (`CURRENT_DATE()`, `DATE_SUB()`) directly within BigQuery SQL. This reduces external dependencies and keeps logic closer to the data.
*   **Replacement of Temporary Files**: The original script used a temporary file (`bert_k_ausd_bp_ta_bpr_instance.tmp`) to store processed record counts. In the migrated solution, this is replaced by an `OUT` parameter (`processed_records`) from the BigQuery Stored Procedures, providing a cleaner and more integrated way to return execution metrics.
*   **Trade-offs**:
    *   **Increased Component Count**: The solution now involves multiple components (Airflow DAG, BigQuery DDLs, BigQuery Stored Procedures) compared to a single shell script and SQL file. This increases initial setup complexity.
    *   **Learning Curve**: Requires familiarity with GCP services (BigQuery, Cloud Composer) and Airflow concepts.
    *   **Enhanced Maintainability and Scalability**: Despite initial complexity, the cloud-native approach offers significantly better maintainability, observability, and scalability for future growth and integration with other GCP services.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **GCP Project and BigQuery Dataset Setup**:
    *   Ensure the GCP project `my_gcp_project` exists.
    *   Create the BigQuery dataset `my_bq_dataset` within `my_gcp_project`.
2.  **Source Table Creation and Data Ingestion**:
    *   Create the BigQuery tables `cds_ta_cntrct` and `pds_ta_bpri_com` in `my_gcp_project.my_bq_dataset`. The DDL for these tables is *not* provided in the generated artifacts and must be derived from the original source system or existing schema definitions.
    *   Ingest historical and ongoing data into `cds_ta_cntrct` and `pds_ta_bpri_com` from the legacy source system. This is a critical prerequisite for the job to function.
3.  **Deploy BigQuery DDLs and Stored Procedures**:
    *   Execute `bigquery/ddl/pool_basisprodukt.sql` to create the `PoolBasisprodukt` table.
    *   Execute `bigquery/ddl/sof_ta_bpr_instance_staging.sql` to create the `sof_ta_bpr_instance_staging` table.
    *   Execute `bigquery/procedures/d_ausd_bp_ta_bpr_instance_core.sql` to create the core transformation stored procedure.
    *   Execute `bigquery/procedures/r_ausd_bp_ta_bpr_instance.sql` to create the orchestration stored procedure.
4.  **Cloud Composer Environment Setup**:
    *   Provision a Cloud Composer environment (if not already available).
    *   Ensure the Airflow environment has the necessary BigQuery provider package installed.
5.  **IAM and Permissions**:
    *   The Service Account associated with the Cloud Composer environment must have the following BigQuery roles:
        *   `BigQuery Data Editor` (to write to `PoolBasisprodukt` and `sof_ta_bpr_instance_staging`).
        *   `BigQuery Data Viewer` (to read from `cds_ta_cntrct` and `pds_ta_bpri_com`).
        *   `BigQuery Job User` (to run BigQuery jobs, including stored procedures).
    *   Ensure the Service Account has permissions to create/update BigQuery routines.
6.  **Deploy Airflow DAG**:
    *   Upload `airflow/dags/k_ausd_bp_ta_bpr_instance_dag.py` to the DAGs folder of your Cloud Composer environment.
7.  **Scheduling Configuration**:
    *   Review and configure the `schedule` parameter in `k_ausd_bp_ta_bpr_instance_dag.py` as required (e.g., `@daily`, `0 0 * * *`). Currently, it's set to `None` for manual triggering.
8.  **Secrets Management (if applicable)**:
    *   While not explicitly used in the generated code, if any external systems or sensitive configurations are introduced (e.g., for initial data ingestion), ensure they are securely managed using Secret Manager and accessed by the appropriate service accounts.

## 5. Known gaps & unresolved references

Based on the migration design document and generated code, the following items are flagged for follow-up or represent known gaps:

*   **Complexity of `d_ausd_bp_ta_bpr_instance.sql` Translation**: The provided `d_ausd_bp_ta_bpr_instance_core.sql` assumes a straightforward translation of the original SQL. If the original `d_ausd_bp_ta_bpr_instance.sql` contained highly complex, proprietary, or Oracle-specific SQL constructs (e.g., specific PL/SQL features, complex analytical functions not directly translatable), further refactoring or BigQuery-specific optimizations might be required.
*   **Source Table DDLs**: The DDLs for `cds_ta_cntrct` and `pds_ta_bpri_com` were not provided and are assumed to exist in BigQuery. Their exact schema and data types are critical for the `d_ausd_bp_ta_bpr_instance_core` procedure to function correctly.
*   **`starteSQLSkript` Function Details**: The original `starteSQLSkript` function's exact implementation (e.g., error handling, connection pooling, specific SQL*Plus commands) was not fully known. The migration assumes it was a simple wrapper for executing SQL. Any hidden complexities might require further investigation if unexpected behavior occurs.
*   **Job Tracking (`FOSJobErzeugeEintrag`, `FOSJobDeaktivate`)**: The original script contained commented-out calls to `FOSJobErzeugeEintrag` and `FOSJobDeaktivate`. If these functions become active or are critical for a wider job management system, a BigQuery-native equivalent (e.g., inserting into a job logging table, Cloud Function invocation) would need to be implemented.
*   **Error Numbering (`ErrNr=193`, `ErrNr=192`)**: The original script used specific error numbers. While BigQuery Stored Procedures can `RAISE` custom messages, a direct mapping or replication of the original error numbering scheme is not implemented. If downstream systems rely on these specific error codes, a new error handling strategy or mapping layer might be necessary.
*   **Character Encoding**: The original script's comment `Andre Lbbers` suggests potential character encoding issues. While BigQuery generally handles UTF-8 well, this should be verified during data ingestion and processing to ensure correct representation of all characters.
*   **`BigQueryExecuteStoredProcedureOperator` OUT Parameter Capture**: The Airflow operator `BigQueryExecuteStoredProcedureOperator` does not directly capture `OUT` parameters from BigQuery Stored Procedures into XComs. While the `r_ausd_bp_ta_bpr_instance` procedure returns `records_processed`, retrieving this value in a subsequent Airflow task would require a workaround (e.g., the SP writing to a temporary table, or a separate BigQuery query task to fetch the value). For this migration, it's assumed the SP's internal logging is sufficient.

## 6. Validation

To validate the successful migration and functionality of the `k_ausd_bp_ta_bpr_instance` job:

1.  **Trigger the Airflow DAG**:
    *   Access the Cloud Composer UI.
    *   Navigate to the `k_ausd_bp_ta_bpr_instance_dag`.
    *   Manually trigger the DAG, providing test parameters for `p_jobkennung`, `p_eintragsnr`, `p_stichtag` (e.g., `20230115`), and `p_wiederanlaufwert`.
2.  **Monitor DAG Execution**:
    *   Observe the DAG run in the Airflow UI. All tasks (`start`, `execute_bpr_instance_procedure`, `end`) should complete successfully (green status).
    *   Check the logs for the `execute_bpr_instance_procedure` task for any BigQuery errors or warnings.
3.  **Verify BigQuery Data**:
    *   **Staging Table**: Query `my_gcp_project.my_bq_dataset.sof_ta_bpr_instance_staging` for the `stichtag` used. Verify that data was inserted correctly and matches expectations based on the source data.
    *   **Target Table**: Query `my_gcp_project.my_bq_dataset.PoolBasisprodukt` for the `stichtag`. Verify that the processed data has been correctly inserted.
    *   **Record Count**: If possible, compare the `records_processed` count returned by the `r_ausd_bp_ta_bpr_instance` procedure (e.g., by inspecting BigQuery job logs or if a workaround for XCom capture is implemented) with the actual count of records inserted into `PoolBasisprodukt` for the given `stichtag`.
4.  **Passing Criteria**:
    *   The Airflow DAG `k_ausd_bp_ta_bpr_instance_dag` completes successfully without any task failures.
    *   No errors are reported in Cloud Logging for the BigQuery job execution.
    *   The `PoolBasisprodukt` table in BigQuery is populated with the expected data for the specified `stichtag`.
    *   The number of records processed and inserted matches the expected outcome from the original system or a known test case.
    *   Data quality checks on the `PoolBasisprodukt` table (e.g., null checks, data type conformity) pass.

## 7. Rollback procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Disable the Airflow DAG**:
    *   In the Cloud Composer UI, toggle off the `k_ausd_bp_ta_bpr_instance_dag` to prevent further automated executions.
2.  **Revert to Original Execution**:
    *   Resume execution of the original KornShell script `k_ausd_bp_ta_bpr_instance.ksh` on the legacy platform. Ensure all necessary environment variables and dependencies are correctly configured for the legacy system.
3.  **Data Cleanup (Optional but Recommended)**:
    *   If the migrated job has written incorrect or incomplete data to `my_gcp_project.my_bq_dataset.PoolBasisprodukt` for the affected `stichtag(s)`, delete this data from BigQuery to maintain data integrity.
        ```sql
        DELETE FROM my_gcp_project.my_bq_dataset.PoolBasisprodukt
        WHERE processing_date = 'YYYY-MM-DD'; -- Use the affected date(s)
        ```
    *   Similarly, clean up `my_gcp_project.my_bq_dataset.sof_ta_bpr_instance_staging` if necessary.
4.  **Investigate and Rectify**:
    *   Analyze the root cause of the failure in the migrated system. This may involve reviewing Airflow logs, BigQuery job history, and comparing data outputs between the legacy and migrated systems.
    *   Address any identified issues in the BigQuery DDLs, Stored Procedures, or the Airflow DAG.
5.  **Re-deploy and Re-validate**:
    *   Once fixes are implemented, re-deploy the updated artifacts.
    *   Repeat the validation steps (Section 6) to ensure the issues are resolved before attempting another go-live.