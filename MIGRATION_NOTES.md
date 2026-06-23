# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the `k_ausd_v_ta_cntrct_crs.ksh` KornShell job and its associated Oracle SQL script `d_ausd_v_ta_cntrct_crs.sql`. The original job orchestrated the extraction, filtering, and transformation of contract data (`ta_cntrct_crs`) from an Oracle source system into a staging table.

The migration targets Google Cloud Platform (GCP), leveraging BigQuery for data storage and transformation, and Cloud Composer (Apache Airflow) for job orchestration. The KornShell script's control logic and the Oracle SQL script's data processing logic have been translated into BigQuery Stored Procedures, while the scheduling and invocation are managed by an Airflow DAG.

## 2. Generated Artifacts

The following files were generated as part of this migration:

*   **`sql/ddl/project.isbert_schema.dwtk_meldungen.sql`**
    *   **Role:** BigQuery Data Definition Language (DDL) script to create the `dwtk_meldungen` table in the `project.isbert_schema` dataset. This table is a migrated version of the Oracle `DWTK_MELDUNGEN` table, used to determine the processing date (`v_datum`).
*   **`sql/ddl/project.source_cds.cds_ta_cntrct.sql`**
    *   **Role:** BigQuery DDL script to create the `cds_ta_cntrct` table in the `project.source_cds` dataset. This table is a migrated version of the Oracle `CDS$TA_CNTRCT` table, serving as the primary source for contract data.
*   **`sql/ddl/project.staging.sof_ta_cntrct_crs.sql`**
    *   **Role:** BigQuery DDL script to create the `sof_ta_cntrct_crs` table in the `project.staging` dataset. This is the target staging table where the processed contract data will be inserted.
*   **`sql/ddl/project.job_control.job_table.sql`**
    *   **Role:** BigQuery DDL script to create the `job_table` in the `project.job_control` dataset. This table is used to track the status and metadata of job executions, replacing the job tracking logic in the original KornShell script.
*   **`sql/ddl/project.job_control.error_log.sql`**
    *   **Role:** BigQuery DDL script to create the `error_log` table in the `project.job_control` dataset. This table centralizes error logging for the migrated job, replacing shell script error handling.
*   **`sql/ddl/project.job_control.job_result_log.sql`**
    *   **Role:** BigQuery DDL script to create the `job_result_log` table in the `project.job_control` dataset. This table stores execution results, such as record counts, for auditing and monitoring.
*   **`bigquery-sql/project.staging.d_ausd_v_ta_cntrct_crs.sql`**
    *   **Role:** BigQuery Stored Procedure that encapsulates the core data transformation logic. It translates the Oracle SQL script `d_ausd_v_ta_cntrct_crs.sql`, performing `TRUNCATE` and `INSERT INTO ... SELECT` operations on the contract data.
*   **`bigquery-sql/project.job_control.r_ausd_vertrag_control.sql`**
    *   **Role:** BigQuery Stored Procedure that serves as the main orchestration component. It translates the KornShell script `k_ausd_v_ta_cntrct_crs.ksh`, handling parameter parsing, job status management, date determination, error logging, and invoking the `d_ausd_v_ta_cntrct_crs` data transformation procedure.
*   **`airflow-dag/r_ausd_vertrag_dag.py`**
    *   **Role:** An Apache Airflow DAG (Directed Acyclic Graph) written in Python. This DAG is responsible for scheduling and triggering the `project.job_control.r_ausd_vertrag_control` BigQuery Stored Procedure, replacing the original KornShell script's role as the entry point for execution.

## 3. Key Design Decisions

*   **BigQuery as the Central Data Platform**: BigQuery was chosen for its serverless architecture, scalability, and high performance for analytical workloads. This decision replaces the Oracle database as the primary data store for this ETL process.
*   **BigQuery Stored Procedures for ETL Logic**: Both the KornShell orchestration logic and the Oracle SQL transformation logic are migrated into BigQuery Stored Procedures. This approach keeps the data processing close to the data, leverages BigQuery's native procedural capabilities, and simplifies maintenance by consolidating logic within the data warehouse.
*   **Cloud Composer (Apache Airflow) for Orchestration**: Cloud Composer provides a managed, robust, and scalable platform for scheduling and monitoring complex data workflows. It replaces the ad-hoc scheduling and execution management previously handled by KornShell scripts, offering better visibility, retry mechanisms, and integration with other GCP services.
*   **Dedicated Job Control Tables in BigQuery**: To maintain job tracking, error logging, and result logging capabilities, specific BigQuery tables (`job_table`, `error_log`, `job_result_log`) are introduced in a `job_control` dataset. This provides a structured, queryable, and centralized audit trail for job executions, replacing the file-based or implicit logging of the original shell script.
*   **Direct Translation of DDL/DML**: Oracle-specific SQL*Plus commands, hints (e.g., `PARALLEL`), and client-side utilities are removed or directly translated to their BigQuery equivalents (e.g., `TRUNCATE TABLE` instead of `DWPA_UTIL_SKRIPT.runstatement`). Date functions are adapted from Oracle to BigQuery syntax (e.g., `TO_DATE` to `PARSE_DATE` or direct `DATE` casting, `NVL` to `IFNULL`).
*   **External Data Ingestion for Source Tables**: The source Oracle tables (`DWTK_MELDUNGEN`, `CDS$TA_CNTRCT`) are treated as external dependencies requiring a separate ingestion pipeline into BigQuery (`project.isbert_schema.dwtk_meldungen`, `project.source_cds.cds_ta_cntrct`). This decouples the ingestion from the transformation job, allowing for flexible data loading strategies (e.g., batch, streaming).

**Notable Trade-offs:**

*   **Re-implementation of Shell Utilities**: Generic shell utilities (e.g., for error handling, date manipulation) had to be re-implemented using BigQuery's procedural language, which required manual effort to ensure functional parity.
*   **Upstream Data Ingestion Dependency**: The migration introduces a critical upstream dependency on a separate process to ingest data from the Oracle Carmen DB into BigQuery. This process needs to be robust and monitored independently.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **GCP Project Setup**:
    *   Ensure the target GCP project (referred to as `project` in the generated code) is created and configured.
2.  **BigQuery Dataset Creation**:
    *   Create the following BigQuery datasets within your GCP project:
        *   `project.isbert_schema`
        *   `project.source_cds`
        *   `project.staging`
        *   `project.job_control`
    *   These can be created via the BigQuery UI, `bq` command-line tool, or Terraform.
3.  **BigQuery Table Creation**:
    *   Execute the DDL scripts for all generated tables in their respective datasets:
        *   `sql/ddl/project.isbert_schema.dwtk_meldungen.sql`
        *   `sql/ddl/project.source_cds.cds_ta_cntrct.sql`
        *   `sql/ddl/project.staging.sof_ta_cntrct_crs.sql`
        *   `sql/ddl/project.job_control.job_table.sql`
        *   `sql/ddl/project.job_control.error_log.sql`
        *   `sql/ddl/project.job_control.job_result_log.sql`
4.  **BigQuery Stored Procedure Deployment**:
    *   Execute the BigQuery SQL scripts to create the stored procedures:
        *   `bigquery-sql/project.staging.d_ausd_v_ta_cntrct_crs.sql`
        *   `bigquery-sql/project.job_control.r_ausd_vertrag_control.sql`
5.  **Initial Data Ingestion**:
    *   **`project.isbert_schema.dwtk_meldungen`**: Set up a process to ingest historical and ongoing data from the Oracle `DWTK_MELDUNGEN` table into this BigQuery table.
    *   **`project.source_cds.cds_ta_cntrct`**: Set up a robust and scheduled process (e.g., using Dataflow, Fivetran, or a custom ingestion pipeline) to extract and load data from the Oracle Carmen DB's `CDS$TA_CNTRCT` table into this BigQuery table. This is a critical prerequisite for the job to function.
6.  **IAM Permissions**:
    *   Ensure the service account used by your Cloud Composer environment (or the user triggering the job) has the necessary BigQuery permissions:
        *   `BigQuery Data Editor` role (or equivalent granular permissions) on `project.isbert_schema`, `project.source_cds`, `project.staging`, and `project.job_control` datasets.
        *   Permissions to execute stored procedures (`bigquery.routines.call`).
7.  **Cloud Composer Environment Setup**:
    *   If not already present, deploy a Cloud Composer environment.
    *   Verify that the `google_cloud_default` Airflow connection is correctly configured to point to your GCP project.
8.  **Airflow DAG Deployment**:
    *   Upload the `airflow-dag/r_ausd_vertrag_dag.py` file to the DAGs folder of your Cloud Composer environment.
9.  **Parameter Configuration**:
    *   Edit `airflow-dag/r_ausd_vertrag_dag.py` to replace `'project'` with your actual GCP project ID.
    *   Adjust the `p_JobKennung` and `p_EintragsNr` parameters in the `BigQueryExecuteStoredProcedureOperator` to match your desired job identification.
10. **Scheduling**:
    *   Configure the `schedule_interval` in `airflow-dag/r_ausd_vertrag_dag.py` according to the business requirements for this job.

## 5. Known Gaps & Unresolved References

*   **Missing File Complexity Data**: The original complexity of the KornShell and SQL scripts could not be determined due to missing data. This might imply hidden complexities or edge cases not fully captured in the migration design.
*   **`VIA` Table Reference**: The original Oracle SQL script's lineage indicated a `WRITES_TABLE VIA` relationship. The nature and purpose of this `VIA` target table are unclear and were not explicitly addressed in the migration, as the SQL only explicitly inserts into `sof$ta_cntrct_crs`. Further investigation is required to determine if this represents a missing component or an implicit behavior not relevant to the BigQuery migration.
*   **Comprehensive `DWPA_UTIL_SKRIPT` Functionality**: Only the `TRUNCATE TABLE` functionality of the Oracle `DWPA_UTIL_SKRIPT` package was explicitly translated. If this package contains other critical functionalities used by the original job, they need to be identified and migrated.
*   **Upstream Invocation (`r_ausd_v_ta_cntrct_crs.ksh`)**: The original `k_ausd_v_ta_cntrct_crs.ksh` script is invoked by `r_ausd_v_ta_cntrct_crs.ksh`. The migration of this upstream caller, or its adaptation to invoke the new BigQuery-based workflow, needs to be considered for a complete end-to-end solution.
*   **Oracle-specific SQL Features**: While common Oracle SQL constructs have been translated, highly specific or complex Oracle functions or PL/SQL features not explicitly visible in the provided `d_ausd_v_ta_cntrct_crs.sql` might require additional manual review and BigQuery-specific equivalents.

## 6. Validation

Validation should cover both individual components and the end-to-end workflow.

### 6.1. Unit Testing BigQuery Stored Procedures

1.  **`project.staging.d_ausd_v_ta_cntrct_crs` (Data Transformation SP)**:
    *   **Setup**:
        *   Populate `project.isbert_schema.dwtk_meldungen` with a `timecreated` value for `BERT_DROP_TEMP_TABLE` to simulate `v_datum`.
        *   Populate `project.source_cds.cds_ta_cntrct` with a diverse set of test data, including records that should and should not meet the `WHERE` clause conditions (e.g., different `cntrct_st`, `redundant_owner_id`, `is_production`, and date ranges relative to `v_datum`).
    *   **Execution**: Call the stored procedure directly from the BigQuery console or client:
        ```sql
        CALL `project.staging.d_ausd_v_ta_cntrct_crs`('YYYY-MM-DD'); -- Replace YYYY-MM-DD with a relevant test date
        ```
    *   **Passing Criteria**:
        *   The `project.staging.sof_ta_cntrct_crs` table should be truncated and then populated.
        *   The count of records in `project.staging.sof_ta_cntrct_crs` should exactly match the expected count based on the test data and the `WHERE` clause logic.
        *   Verify the data types and values in `project.staging.sof_ta_cntrct_crs` are correct and accurately reflect the transformation from `project.source_cds.cds_ta_cntrct`.
2.  **`project.job_control.r_ausd_vertrag_control` (Orchestration SP)**:
    *   **Setup**:
        *   Ensure `project.isbert_schema.dwtk_meldungen` and `project.source_cds.cds_ta_cntrct` are populated with test data.
        *   Clear any existing records from `project.job_control.job_table`, `project.job_control.error_log`, and `project.job_control.job_result_log`.
    *   **Execution**: Call the stored procedure:
        ```sql
        CALL `project.job_control.r_ausd_vertrag_control`('TEST_JOB_KENNUNG', 'TEST_ENTRY_NR');
        ```
    *   **Passing Criteria (Successful Run)**:
        *   A record should be inserted into `project.job_control.job_table` with `status = 'COMPLETED'`, and appropriate `start_time`/`end_time`.
        *   A record should be inserted into `project.job_control.job_result_log` with a `record_count` matching the number of rows processed by `d_ausd_v_ta_cntrct_crs`.
        *   No errors should be logged in `project.job_control.error_log`.
    *   **Passing Criteria (Error Handling)**:
        *   Test scenarios that should cause an error (e.g., `dwtk_meldungen` table is empty or missing, leading to `v_datum` not being determined).
        *   Verify that `project.job_control.job_table` shows `status = 'FAILED'`.
        *   Verify that `project.job_control.error_log` contains the expected error message.

### 6.2. End-to-End Validation (Airflow DAG)

1.  **Execution**:
    *   Trigger the `r_ausd_vertrag_control_dag` from the Cloud Composer UI.
2.  **Passing Criteria**:
    *   The Airflow task `call_r_ausd_vertrag_control_sp` should complete successfully (green status).
    *   Verify the `job_table`, `error_log`, and `job_result_log` in BigQuery reflect a successful run, consistent with the stored procedure unit tests.
    *   **Data Reconciliation**: The most critical validation step. Compare the final data in `project.staging.sof_ta_cntrct_crs` with the output generated by the *original Oracle job* for the same input data and processing date. This ensures functional equivalence.

## 7. Rollback Procedure

In the event of critical issues or if the migrated job fails to meet requirements, the following rollback procedure can be initiated:

1.  **Halt New Workflow**:
    *   **Cloud Composer**: Pause or delete the `r_ausd_vertrag_control_dag` in the Cloud Composer UI to prevent any further execution of the migrated job.
2.  **Revert Orchestration**:
    *   **Legacy System**: Re-enable or restart the original `k_ausd_v_ta_cntrct_crs.ksh` script (and its upstream caller `r_ausd_v_ta_cntrct_crs.ksh`) in the legacy environment. Ensure its scheduler is reactivated.
3.  **Data State**:
    *   The `project.staging.sof_ta_cntrct_crs` table in BigQuery can be left as is or dropped, as it is a staging table and its state does not directly impact the original Oracle system.
    *   The `project.source_cds.cds_ta_cntrct` and `project.isbert_schema.dwtk_meldungen` tables in BigQuery are copies of the Oracle source data and do not affect the original Oracle tables.
4.  **Cleanup (Optional)**:
    *   If a full rollback is confirmed and the migrated components are no longer needed, the following BigQuery resources can be dropped:
        *   All tables in `project.isbert_schema`, `project.source_cds`, `project.staging`, and `project.job_control` datasets.
        *   The stored procedures `project.staging.d_ausd_v_ta_cntrct_crs` and `project.job_control.r_ausd_vertrag_control`.
        *   The `r_ausd_vertrag_control_dag.py` file can be removed from the Cloud Composer DAGs folder.
    *   The BigQuery datasets themselves can also be deleted if no other migrated jobs depend on them.