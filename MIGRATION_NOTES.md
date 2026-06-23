# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_beschr.ksh`. This script, originally responsible for orchestrating the execution of a core SQL transformation (`d_ausd_bp_ta_bpr_beschr.sql`), parameter handling, date derivation, and job logging, has been migrated to Google Cloud Platform.

The target platform leverages:
*   **Google BigQuery** for data storage and processing, with the orchestration logic and core SQL transformation encapsulated in BigQuery Stored Procedures.
*   **Cloud Composer (Apache Airflow)** for scheduling and orchestrating the BigQuery components.

The migration aims to modernize the ETL process, improve scalability, and integrate with cloud-native monitoring and logging capabilities.

## 2. Generated Artifacts

The migration process generated the following files:

1.  **`sql/ddl/job_audit_table.sql`**
    *   **Role:** BigQuery DDL (Data Definition Language) script to create the `job_audit_table`. This table serves as a centralized audit log for job executions, replacing the implied `FOSJobErzeugeEintrag` functionality from the legacy system. It tracks job ID, entry number, reference date (`Stichtag`), start/end timestamps, status, record count, and error messages.

2.  **`sql/procedures/d_ausd_bp_ta_bpr_beschr_core.sql`**
    *   **Role:** BigQuery Stored Procedure placeholder. This file is intended to contain the translated core data transformation logic originally found in `d_ausd_bp_ta_bpr_beschr.sql`. It accepts `p_stichtag` and `p_wiederanlaufwert` as parameters. **Note: This is currently a placeholder and requires the actual business logic to be implemented.**

3.  **`sql/procedures/r_ausd_bp_ta_bpr_beschr.sql`**
    *   **Role:** BigQuery Stored Procedure that encapsulates the orchestration logic of the original `k_ausd_bp_ta_bpr_beschr.ksh`. This procedure handles:
        *   Parsing and validating input parameters (`job_kennung`, `eintrags_nr`, `stichtag_str`, `wiederanlauf_wert`).
        *   Validating the `stichtag_str` format.
        *   Deriving current and previous dates.
        *   Calling the core transformation procedure (`d_ausd_bp_ta_bpr_beschr_core`).
        *   Counting records in the target table.
        *   Logging job status and metrics to `job_audit_table`.
        *   Error handling and re-raising.

4.  **`dags/k_ausd_bp_ta_bpr_beschr_dag.py`**
    *   **Role:** An Apache Airflow DAG (Directed Acyclic Graph) written in Python. This DAG is responsible for:
        *   Scheduling the execution of the BigQuery Stored Procedure `r_ausd_bp_ta_bpr_beschr`.
        *   Passing parameters to the stored procedure, including dynamically generated `stichtag_str` from Airflow's `data_interval_start`.
        *   Defining the overall workflow for the job within Cloud Composer.

5.  **`sql/ddl/target_result_table.sql`**
    *   **Role:** BigQuery DDL script for the `target_result_table`. This is a placeholder for the table where the `d_ausd_bp_ta_bpr_beschr_core` procedure writes its primary output. It includes a `_DATA_DATE` column for partitioning and record counting. **Note: The actual schema for this table must be finalized based on the complete analysis of `d_ausd_bp_ta_bpr_beschr.sql`.**

6.  **`docs/parameter_mapping.md`**
    *   **Role:** Documentation detailing the mapping of legacy KornShell script parameters to the BigQuery Stored Procedure parameters and how they are configured within the Airflow DAG.

## 3. Key Design Decisions

*   **Orchestration Layer Shift**: The primary orchestration logic, previously in KornShell, was migrated to a BigQuery Stored Procedure (`r_ausd_bp_ta_bpr_beschr`) and scheduled by Cloud Composer.
    *   **Why**: Leverages BigQuery's native procedural capabilities for complex logic, keeps orchestration close to data, and utilizes Cloud Composer for robust, scalable, and managed scheduling, replacing custom shell scripting and external schedulers.
    *   **Trade-offs**: Introduces a new technology stack (BigQuery SQL, Airflow Python) and requires careful parameter mapping and environment configuration.

*   **Core SQL Transformation**: The core `d_ausd_bp_ta_bpr_beschr.sql` logic is intended to be migrated into a separate BigQuery Stored Procedure (`d_ausd_bp_ta_bpr_beschr_core`).
    *   **Why**: Encapsulates business logic, promotes modularity, allows for independent testing, and benefits from BigQuery's performance optimizations.
    *   **Trade-offs**: Requires a complete translation of Oracle SQL to BigQuery SQL, which may involve syntax and function differences.

*   **Replacement of Utility Scripts**: Legacy KornShell utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `gestern.ksh`, `h_alis_sqlplus.ksh`) are replaced by native BigQuery SQL functions (for date manipulation, parameter validation) or Airflow's built-in capabilities (for environment variables, scheduling).
    *   **Why**: Simplifies the architecture, reduces external dependencies, and leverages optimized, built-in cloud functionalities.
    *   **Trade-offs**: Requires understanding and mapping legacy utility logic to BigQuery/Airflow equivalents.

*   **Record Counting Mechanism**: The original method of writing record counts to a temporary file is replaced by a direct `COUNT(*)` query on the target BigQuery table within the orchestration stored procedure.
    *   **Why**: More reliable, eliminates file system dependencies, and integrates seamlessly with BigQuery's data model.

*   **Job Logging**: The commented `FOSJobErzeugeEintrag` functionality is replaced by explicit `INSERT` statements into a dedicated `job_audit_table` in BigQuery.
    *   **Why**: Provides a structured, queryable, and centralized audit trail for job executions, status, and errors, enhancing observability.

*   **Parameter Handling**: Command-line parameters are mapped to BigQuery Stored Procedure `IN` parameters and managed via Airflow DAG parameters and macros.
    *   **Why**: Standardized and robust way to pass configuration and dynamic values in a cloud environment.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps are required:

1.  **BigQuery Dataset Creation**: Ensure the target BigQuery dataset (`<dataset>`) exists in your GCP project (`<project_id>`).
    *   `bq mk --dataset <project_id>:<dataset>`

2.  **IAM & Permissions**:
    *   The Google Cloud service account used by Cloud Composer (Airflow) must have sufficient permissions to:
        *   Execute BigQuery jobs (`bigquery.jobs.create`).
        *   Create, read, write, and update data in BigQuery tables (`bigquery.tables.create`, `bigquery.tables.getData`, `bigquery.tables.updateData`, `bigquery.tables.delete`).
        *   Create and execute BigQuery stored procedures (`bigquery.routines.create`, `bigquery.routines.update`, `bigquery.routines.call`).
    *   Specifically, roles like `BigQuery Data Editor` and `BigQuery Job User` are typically required for the dataset.

3.  **Deploy BigQuery DDLs and Procedures**:
    *   Execute `sql/ddl/job_audit_table.sql` to create the audit table.
    *   **Crucially, finalize and execute `sql/ddl/target_result_table.sql` to define the schema of the target table.** This requires a full analysis of the original `d_ausd_bp_ta_bpr_beschr.sql`.
    *   **Implement and deploy the core logic into `sql/procedures/d_ausd_bp_ta_bpr_beschr_core.sql`.** This is the most significant manual step.
    *   Deploy `sql/procedures/r_ausd_bp_ta_bpr_beschr.sql`.
    *   Replace `<project_id>` and `<dataset>` placeholders in all SQL files with actual values.

4.  **Data Migration**:
    *   All source tables referenced by the original `d_ausd_bp_ta_bpr_beschr.sql` must be migrated or replicated to BigQuery. This is a prerequisite for `d_ausd_bp_ta_bpr_beschr_core` to function.

5.  **Airflow Configuration**:
    *   **GCP Connection**: Ensure the `google_cloud_default` connection (or a custom GCP connection) is properly configured in Airflow with the necessary service account key or permissions.
    *   **Variables**: Consider storing `GCP_PROJECT_ID` and `BQ_DATASET_ID` as Airflow Variables for easier management and security.
    *   **DAG Deployment**: Upload `dags/k_ausd_bp_ta_bpr_beschr_dag.py` to your Cloud Composer environment's DAGs folder.
    *   **Schedule**: Review and adjust the `schedule` parameter in the DAG to match the legacy job's execution frequency.
    *   **Parameters**: Review the default `params` in the DAG (`job_kennung`, `eintrags_nr`, `wiederanlauf_wert`) and adjust if they need to be dynamic or different from the defaults. Pay special attention to the `stichtag_str` macro and ensure it correctly derives the logical date for processing.

## 5. Known Gaps & Unresolved References

1.  **Core SQL Logic (`d_ausd_bp_ta_bpr_beschr_core.sql`)**: This is the most critical gap. The content of the original `d_ausd_bp_ta_bpr_beschr.sql` was not available for automated migration. The `d_ausd_bp_ta_bpr_beschr_core.sql` procedure is a placeholder and requires manual translation of the Oracle SQL logic to BigQuery SQL. This includes identifying source tables, target tables, and all transformation rules.
2.  **Target Table Schema (`target_result_table.sql`)**: The DDL for the target table is a placeholder. Its definitive schema depends entirely on the full analysis and implementation of `d_ausd_bp_ta_bpr_beschr_core.sql`.
3.  **Source Table Identification and Migration**: The specific Oracle source tables used by `d_ausd_bp_ta_bpr_beschr.sql` were not explicitly identified. These tables must be migrated or replicated to BigQuery before the core transformation can run.
4.  **Commented-out Legacy Logic**: The original `k_ausd_bp_ta_bpr_beschr.ksh` contained commented-out sections for `sed`, `sort`, `join` operations and `FOSJob` calls. This migration assumes these are inactive. If they become active requirements, their functionality will need to be translated to BigQuery SQL or Airflow tasks.
5.  **Dynamic `BERT_DIR_ROOT`**: The original script relied on `${BERT_DIR_ROOT}` for path resolution. In the cloud environment, this concept is replaced by explicit BigQuery dataset/table references and Airflow environment variables or parameters. Ensure all such references are correctly resolved.
6.  **Error Handling Granularity**: The `f_alis_msgerr.ksh` utility might have provided specific error codes or detailed logging. While the BigQuery procedure captures `@@error.message`, a detailed mapping of legacy error types to BigQuery error handling or custom logging might be required if specific error responses are critical.
7.  **`p_EintragsNr` and `p_wiederanlaufWert` Logic**: The migration provides default values for these parameters in the Airflow DAG. Review if these values need to be dynamically generated or managed based on specific business rules or restart logic from the legacy system.
8.  **Lineage Edges**: The original design document noted an absence of direct lineage edges, suggesting potential hidden dependencies not captured by automated analysis. Thorough testing and review are needed to uncover any such implicit dependencies.

## 6. Validation

Validation should cover both unit-level testing of BigQuery components and end-to-end integration testing via Airflow.

### How to Run Tests:

1.  **BigQuery Stored Procedure Unit Tests**:
    *   **`r_ausd_bp_ta_bpr_beschr`**: Execute the main orchestration procedure directly in the BigQuery console or via `bq query` command-line tool with various parameter combinations:
        *   Valid `job_kennung`, `eintrags_nr`, `stichtag_str` (e.g., `'01012023'`), `wiederanlauf_wert`.
        *   Invalid `stichtag_str` format (e.g., `'20230101'`).
        *   Missing required parameters (e.g., `job_kennung` as `NULL` or empty string).
        *   Test with a `stichtag` for which `d_ausd_bp_ta_bpr_beschr_core` is expected to produce data.
        *   Test with a `stichtag` for which `d_ausd_bp_ta_bpr_beschr_core` is expected to produce no data.
    *   **`d_ausd_bp_ta_bpr_beschr_core`**: Once implemented, test this procedure independently with various input data scenarios to ensure the core transformation logic is correct.

2.  **Airflow DAG Integration Tests**:
    *   **Manual Trigger**: Trigger the `k_ausd_bp_ta_bpr_beschr_dag` manually from the Airflow UI for a specific `data_interval_start` (logical date).
    *   **Scheduled Run**: Allow the DAG to run on its defined schedule.
    *   **Parameter Overrides**: Test overriding DAG parameters (e.g., `wiederanlauf_wert`) via the Airflow UI.

### What "Passing" Means:

*   **Successful Execution**:
    *   All BigQuery Stored Procedure calls complete without errors.
    *   The Airflow DAG runs to completion with a "success" status.
*   **Audit Logging**:
    *   The `job_audit_table` contains a new entry for each job run, with correct `job_id`, `entry_number`, `stichtag`, `start_timestamp`, `end_timestamp`, `status` ('SUCCESS' for successful runs, 'FAILED' for failures), `record_count`, and `error_message` (NULL for success).
*   **Data Integrity**:
    *   The `target_result_table` contains the expected data for the processed `Stichtag`.
    *   **Crucially, the `record_count` logged in `job_audit_table` matches the actual `COUNT(*)` of records inserted/updated in the `target_result_table` for that `Stichtag`.**
    *   Sample data validation: Select a few records for a given `Stichtag` from the legacy system and compare them with the corresponding records in the BigQuery `target_result_table` to ensure data accuracy.
*   **Error Handling**:
    *   When invalid parameters are provided (e.g., incorrect `stichtag_str`), the BigQuery procedure should `RAISE` an error, the `job_audit_table` should log a 'FAILED' status with an appropriate `error_message`, and the Airflow task should fail.
*   **Performance**:
    *   The BigQuery job completes within acceptable timeframes, comparable to or better than the legacy system.

## 7. Rollback Procedure

In case of issues or critical failures after deployment, the following rollback procedure should be followed:

1.  **Immediate Action (Pause New System)**:
    *   **Disable Airflow DAG**: In the Cloud Composer (Airflow) UI, toggle off the `k_ausd_bp_ta_bpr_beschr_dag` to prevent further executions.
    *   **Monitor Legacy System**: Ensure the legacy `k_ausd_bp_ta_bpr_beschr.ksh` job is still available and can be re-enabled.

2.  **Revert to Legacy System**:
    *   **Re-enable Legacy Job**: If the legacy job was disabled, re-enable its scheduling in the original environment (e.g., UC4).
    *   **Verify Legacy Execution**: Monitor the legacy job to ensure it runs successfully and produces correct output.

3.  **Data Consistency (if applicable)**:
    *   **Review BigQuery Data**: If the migrated job wrote data to `target_result_table` or other BigQuery tables, assess the impact.
        *   If the data is incorrect or incomplete, decide whether to delete the affected partitions/data in BigQuery or to allow the legacy system to overwrite/correct it in its own target.
        *   The `_DATA_DATE` column in `target_result_table` and the `stichtag` in `job_audit_table` can help identify affected data.
    *   **Audit Table**: The `job_audit_table` will retain logs of the failed BigQuery runs, which can be useful for post-mortem analysis.

4.  **Code Rollback (Optional, for clean-up or re-deployment)**:
    *   **Delete Airflow DAG**: Remove `dags/k_ausd_bp_ta_bpr_beschr_dag.py` from the Cloud Composer DAGs folder.
    *   **Delete BigQuery Procedures**: Drop the `r_ausd_bp_ta_bpr_beschr` and `d_ausd_bp_ta_bpr_beschr_core` stored procedures from BigQuery.
        *   `DROP PROCEDURE IF EXISTS <project_id>.<dataset>.r_ausd_bp_ta_bpr_beschr;`
        *   `DROP PROCEDURE IF EXISTS <project_id>.<dataset>.d_ausd_bp_ta_bpr_beschr_core;`
    *   **Delete BigQuery Tables**: If necessary, drop `job_audit_table` and `target_result_table`.
        *   `DROP TABLE IF EXISTS <project_id>.<dataset>.job_audit_table;`
        *   `DROP TABLE IF EXISTS <project_id>.<dataset>.target_result_table;`

This procedure ensures a quick return to a stable state using the proven legacy system while allowing for investigation and re-planning of the migration.