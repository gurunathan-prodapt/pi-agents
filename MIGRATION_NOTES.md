# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the legacy KornShell (ksh) script `k_ausd_bp_ta_cntrct_dist.ksh`. The original script served as an orchestration component, parsing parameters, validating inputs, and executing a core SQL script (`d_ausd_bp_ta_cntrct_dist.sql`) to process data.

The job has been migrated to Google Cloud Platform (GCP), leveraging **BigQuery** for all data storage and transformation logic, and **Cloud Composer (Airflow)** for robust scheduling and orchestration. The core functionality of the ksh script has been re-implemented as BigQuery Stored Procedures, and the execution is managed by an Airflow DAG.

## 2. Generated artifacts

The migration produced the following artifacts:

*   **`bigquery/ddl/pool_basisprodukt.sql`**
    *   **Role**: BigQuery Data Definition Language (DDL) script to create the target table `PoolBasisprodukt`. This table will store the final processed data, replacing the original target of `d_ausd_bp_ta_cntrct_dist.sql`. The schema is a placeholder and needs to be finalized based on the original `PoolBasisprodukt` definition.
*   **`bigquery/ddl/error_log.sql`**
    *   **Role**: BigQuery DDL script to create a dedicated table (`error_log`) for capturing and storing error messages and details from the BigQuery stored procedures. This replaces the legacy shell script's error logging mechanisms.
*   **`bigquery/ddl/job_log.sql`**
    *   **Role**: BigQuery DDL script to create a table (`job_log`) for tracking the execution status, start/end times, record counts, and other metadata for each run of the migrated job. This replaces any legacy job tracking or simple console output.
*   **`bigquery/stored_procedures/d_ausd_bp_ta_cntrct_dist_core.sql`**
    *   **Role**: A BigQuery Stored Procedure that encapsulates the core data transformation logic originally found in `d_ausd_bp_ta_cntrct_dist.sql`. It reads from source tables (assumed to be in BigQuery) and writes to the `PoolBasisprodukt` table. This procedure is designed for optimal performance within BigQuery.
*   **`bigquery/stored_procedures/r_ausd_bp_ta_cntrct_dist.sql`**
    *   **Role**: A BigQuery Stored Procedure that re-implements the orchestration logic of the original `k_ausd_bp_ta_cntrct_dist.ksh` script. It handles parameter parsing, validation, date derivation, calls the `d_ausd_bp_ta_cntrct_dist_core` procedure, captures record counts, and logs job status and errors to the `job_log` and `error_log` tables.
*   **`airflow/dags/k_ausd_bp_ta_cntrct_dist_dag.py`**
    *   **Role**: An Apache Airflow DAG (Directed Acyclic Graph) written in Python. This DAG is responsible for scheduling and orchestrating the execution of the `r_ausd_bp_ta_cntrct_dist` BigQuery stored procedure. It defines the workflow, passes parameters, and integrates with Cloud Composer's monitoring and alerting capabilities.

## 3. Key design decisions

*   **Cloud-Native Approach**: The decision was made to fully embrace GCP's cloud-native services. This provides benefits such as scalability, managed services, integrated monitoring, and reduced operational overhead compared to maintaining on-premise KornShell environments.
*   **BigQuery for Data Processing and Orchestration Logic**:
    *   **Why**: BigQuery offers a highly scalable, cost-effective, and performant data warehouse solution. By translating the core SQL and the orchestration logic into BigQuery Stored Procedures, we leverage BigQuery's native capabilities for data manipulation, validation, and procedural control. This keeps the data processing close to the data, minimizing data movement.
    *   **Trade-offs**: This approach requires re-writing shell scripting logic (e.g., parameter parsing, date manipulation, conditional logic, error handling) into BigQuery SQL scripting, which can be more verbose for complex orchestration tasks than a dedicated scripting language. However, the benefits of unified data and logic within BigQuery outweigh this.
*   **Cloud Composer (Airflow) for Workflow Orchestration**:
    *   **Why**: Airflow provides a robust, extensible, and widely adopted platform for defining, scheduling, and monitoring complex data pipelines. It replaces the basic scheduling and execution control of the legacy ksh script with advanced features like dependency management, retries, backfills, and a rich UI for operational visibility.
    *   **Trade-offs**: Introduces a new technology stack (Python, Airflow concepts) and requires managing an Airflow environment. The `k_ausd_bp_ta_cntrct_dist.ksh` script was a simple single-file execution, whereas the DAG introduces more components.
*   **Replacement of Utility Scripts**: Legacy utility ksh scripts (e.g., `.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`) have been replaced by BigQuery's built-in functions, SQL scripting constructs, and dedicated logging tables (`error_log`, `job_log`). This eliminates external shell dependencies and streamlines the execution within BigQuery.
*   **Elimination of Temporary Files**: The use of temporary files (e.g., `bert_k_ausd_bp_ta_cntrct_dist.tmp` for record counts) has been replaced by direct BigQuery table operations (e.g., `COUNT(*)` queries) and variable assignments within stored procedures, which is more efficient and cloud-native.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **GCP Project and BigQuery Dataset Setup**:
    *   Ensure the target GCP project (`your_project_id`) exists.
    *   Create the BigQuery dataset (`your_dataset_id`) where the tables and stored procedures will reside.
2.  **IAM Permissions**:
    *   The service account used by Cloud Composer (Airflow) must have sufficient permissions to:
        *   Execute BigQuery stored procedures (`bigquery.routines.call`).
        *   Read from and write to BigQuery tables (`bigquery.tables.getData`, `bigquery.tables.updateData`, `bigquery.tables.insertData`, `bigquery.tables.create`). This includes `PoolBasisprodukt`, `error_log`, `job_log`, and any source tables.
        *   Create/update BigQuery routines (`bigquery.routines.create`, `bigquery.routines.update`) if DDLs are deployed via Airflow.
    *   Ensure the user deploying the DDLs and stored procedures has `BigQuery Data Editor` or `BigQuery Admin` roles.
3.  **BigQuery DDL Deployment**:
    *   Execute the DDL scripts (`pool_basisprodukt.sql`, `error_log.sql`, `job_log.sql`) in BigQuery to create the necessary tables.
    *   **Crucially, the schema for `PoolBasisprodukt` in `bigquery/ddl/pool_basisprodukt.sql` is a placeholder.** It must be updated to accurately reflect the original schema of the `PoolBasisprodukt` table, including data types, nullability, and any partitioning/clustering strategies suitable for BigQuery.
4.  **BigQuery Stored Procedure Deployment**:
    *   Execute the stored procedure scripts (`d_ausd_bp_ta_cntrct_dist_core.sql`, `r_ausd_bp_ta_cntrct_dist.sql`) in BigQuery to create or replace the procedures.
5.  **Source Data Migration**:
    *   Ensure that all source tables referenced by the original `d_ausd_bp_ta_cntrct_dist.sql` script are migrated and available in BigQuery within the specified dataset (`your_dataset_id`) or accessible via appropriate external table configurations.
6.  **Airflow DAG Configuration**:
    *   Update the `PROJECT_ID` and `DATASET_ID` variables in `airflow/dags/k_ausd_bp_ta_cntrct_dist_dag.py` to match your GCP environment.
    *   Review and update the `BIGQUERY_CONN_ID` if a custom BigQuery connection is used in Airflow.
    *   Define the appropriate `schedule` for the DAG (e.g., `@daily`, `0 5 * * *`) as it is currently set to `None`.
    *   Review and adjust the parameter passing logic in the DAG, especially for `stichtag_raw` and `wiederanlauf_wert_raw`, to ensure they align with the desired dynamic behavior (e.g., using `execution_date` or Airflow Variables).
7.  **Secrets Management**:
    *   If the original `d_ausd_bp_ta_cntrct_dist.sql` or any other part of the legacy system used credentials or sensitive information, ensure these are securely managed in GCP (e.g., using Secret Manager) and accessed appropriately by the BigQuery procedures or Airflow DAG. (No explicit secrets were identified in the source script, but this is a general best practice).

## 5. Known gaps & unresolved references

*   **`PoolBasisprodukt` Schema Definition**: The DDL for `PoolBasisprodukt` is a placeholder. The actual schema (column names, data types, constraints, partitioning/clustering) must be derived from the original system and applied to `bigquery/ddl/pool_basisprodukt.sql`.
*   **Complexity of `d_ausd_bp_ta_cntrct_dist.sql` Translation**: The design assumes `d_ausd_bp_ta_cntrct_dist.sql` contains standard SQL easily translatable to BigQuery. If it contains complex procedural logic (e.g., PL/SQL specific features, cursors, loops) that are not directly supported or efficiently translated to BigQuery SQL scripting, further analysis and potential re-design (e.g., using Python-based data transformation in Cloud Dataflow/Dataproc) might be required.
*   **Commented-out Logic in Original Script**: The legacy ksh script contained commented-out `sed`, `sort`, `join` commands. It is currently assumed this logic is inactive and not required. This needs to be confirmed with business stakeholders. If these operations are required, they must be re-implemented using BigQuery SQL transformations.
*   **Error Reporting Integration**: While errors are logged to `error_log` and `job_log` tables, the integration with existing monitoring and alerting systems (e.g., Cloud Monitoring alerts, PagerDuty, Slack notifications) needs to be designed and implemented. The original `DWMSG_MeldeFehler` likely had specific reporting mechanisms.
*   **Job Tracking Metadata**: The `job_log` table captures basic job execution details. If the original system had more granular job tracking (e.g., `FOSJobErzeugeEintrag`), the `job_log` schema and the `r_ausd_bp_ta_cntrct_dist` procedure might need to be extended to capture all necessary metadata.
*   **Dynamic Parameter Handling in Airflow**: The Airflow DAG currently uses static or basic `ds_nodash` for `stichtag_raw`. For production, a more robust parameter management strategy (e.g., Airflow Variables, custom macros, or external configuration) should be implemented to handle various scenarios like backfills, specific date runs, or different `JobKennung` values.

## 6. Validation

Validation should cover functional equivalence, data integrity, and operational aspects.

1.  **BigQuery DDL Validation**:
    *   **How to run**: Execute the `bigquery/ddl/*.sql` files in the BigQuery console or via `bq` command-line tool.
    *   **Passing means**: All tables (`PoolBasisprodukt`, `error_log`, `job_log`) are created successfully with the correct schema, including data types, nullability, and any partitioning/clustering.
2.  **BigQuery Stored Procedure Validation**:
    *   **How to run**: Call `your_project_id.your_dataset_id.r_ausd_bp_ta_cntrct_dist` directly from the BigQuery console using various test parameters (valid, invalid dates, missing parameters).
    *   **Passing means**:
        *   The procedure executes successfully for valid inputs.
        *   The `PoolBasisprodukt` table is populated with expected data.
        *   The `job_log` table correctly records the start, end, status (`SUCCESS`), and `record_count`.
        *   Invalid inputs trigger appropriate error messages and log entries in `error_log` and `job_log` (status `FAILED`).
        *   The `d_ausd_bp_ta_cntrct_dist_core` procedure is called and completes successfully.
3.  **Airflow DAG Validation**:
    *   **How to run**: Upload `airflow/dags/k_ausd_bp_ta_cntrct_dist_dag.py` to Cloud Composer. Trigger the DAG manually from the Airflow UI.
    *   **Passing means**:
        *   The DAG runs successfully without Airflow-level errors.
        *   The `call_r_ausd_bp_ta_cntrct_dist` task completes successfully.
        *   The underlying BigQuery stored procedure (`r_ausd_bp_ta_cntrct_dist`) is invoked and completes as expected (as per SP validation above).
        *   Logs in Airflow UI show successful execution and any relevant output.
4.  **Data Validation**:
    *   **How to run**: Perform a data comparison between the `PoolBasisprodukt` table in BigQuery and the target table in the legacy system for a specific `Stichtag`. This can involve row counts, checksums, or detailed column-by-column comparisons.
    *   **Passing means**: The data in the BigQuery `PoolBasisprodukt` table is functionally identical to the data produced by the legacy system for the same input parameters.
5.  **Performance Testing**:
    *   **How to run**: Execute the DAG with production-like data volumes and monitor BigQuery query execution times and costs.
    *   **Passing means**: The job completes within acceptable timeframes and cost limits.

## 7. Rollback procedure

In case of critical issues during or after go-live, the following rollback procedure can be initiated:

1.  **Stop New Migrated Runs**:
    *   In the Cloud Composer Airflow UI, pause the `k_ausd_bp_ta_cntrct_dist_dag` to prevent any further executions of the migrated job.
2.  **Revert to Legacy Execution**:
    *   Re-enable or re-configure the original scheduling mechanism for the `k_ausd_bp_ta_cntrct_dist.ksh` script in the legacy environment.
    *   Verify that the legacy job can run successfully and produce the expected output.
3.  **Data Rollback (if necessary)**:
    *   If the `PoolBasisprodukt` table in BigQuery was modified by the migrated job and these modifications are deemed incorrect or corrupted, a data rollback might be necessary.
    *   **Option A (BigQuery Time Travel)**: If the issue is identified quickly (within the BigQuery time travel window, typically 7 days), the `PoolBasisprodukt` table can be restored to a previous state using BigQuery's time travel feature.
    *   **Option B (Backup/Snapshot)**: If a backup or snapshot of the `PoolBasisprodukt` table was taken before the migration or before the problematic run, restore the table from that backup.
    *   **Option C (Legacy Re-processing)**: If the legacy system can safely re-process the data for the affected `Stichtag` and overwrite the BigQuery table, this can be an option, but requires careful coordination.
    *   **Note**: The specific data rollback strategy depends heavily on the nature of the data, the impact of the error, and the available recovery points.
4.  **Troubleshoot and Re-plan**:
    *   Analyze the root cause of the failure in the migrated job using BigQuery logs, Airflow logs, and Cloud Monitoring.
    *   Address the identified issues, update the BigQuery stored procedures or Airflow DAG, and re-test thoroughly in a non-production environment before attempting another go-live.