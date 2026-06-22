# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the `k_ausd_v_ta_acc_ref.ksh` KornShell script and its associated Oracle SQL processing logic. The original script served as an orchestration layer for processing data related to the `ta_acc_ref` table within an Oracle database environment.

The job has been re-platformed to **Google Cloud Platform (GCP)**. The orchestration logic is now managed by an **Apache Airflow DAG** running on **Cloud Composer**. The data processing, previously handled by Oracle SQL*Plus executing `d_ausd_v_ta_acc_ref.sql`, has been translated to **BigQuery Standard SQL** and is executed via Airflow's `BigQueryOperator`. The underlying data storage for `ta_acc_ref` and its dependencies has been migrated from Oracle to **Google BigQuery**.

## 2. Generated Artifacts

The migration process has resulted in the following key artifacts:

*   **`k_ausd_v_ta_acc_ref_dag.py`**:
    *   **Role**: This is the main Apache Airflow DAG file. It encapsulates the orchestration logic previously handled by `k_ausd_v_ta_acc_ref.ksh`, including parameter handling, validation, and sequencing of data processing tasks. It defines the tasks and their dependencies within the Cloud Composer environment.
    *   **Technology**: Python (Airflow DAG).

*   **`d_ausd_v_ta_acc_ref.bqsql`**:
    *   **Role**: This file contains the BigQuery Standard SQL translation of the original `d_ausd_v_ta_acc_ref.sql` Oracle script. It performs the core data transformation and manipulation logic on the `ta_acc_ref` table and its dependencies within BigQuery.
    *   **Technology**: BigQuery Standard SQL.

*   **`airflow_utils.py` (or similar Python package)**:
    *   **Role**: A Python module or package containing re-implemented functionalities from the original KornShell utility scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`). These utilities provide common functions for error handling, date manipulation, and parameter validation within the Airflow environment.
    *   **Technology**: Python.

*   **BigQuery DDLs for `isbert_ds.ta_acc_ref` and dependent tables**:
    *   **Role**: Data Definition Language (DDL) scripts for creating the `ta_acc_ref` table and any other source tables referenced by `d_ausd_v_ta_acc_ref.bqsql` within the `isbert_ds` BigQuery dataset. These define the schema, partitioning, and clustering (if applicable) for the BigQuery tables.
    *   **Technology**: BigQuery Standard SQL.

## 3. Key Design Decisions

The migration involved a significant re-platforming effort, categorized as B4 (Redesign). Key design decisions include:

*   **Cloud Composer for Orchestration**: The decision to use Cloud Composer (Airflow) was driven by the need for a cloud-native, scalable, and robust orchestration platform. It replaces the custom KornShell scripting with a standardized, Python-based framework offering advanced scheduling, monitoring, error handling, and dependency management capabilities. This aligns with GCP best practices for data workflows.
*   **BigQuery for Data Processing and Storage**: Migrating from Oracle to BigQuery for both data storage and processing (`d_ausd_v_ta_acc_ref.sql` translation) leverages BigQuery's serverless, highly scalable, and cost-effective data warehousing capabilities. This eliminates the operational overhead of managing an Oracle database and provides superior performance for analytical workloads.
*   **Re-implementation of Utility Logic in Python**: Instead of attempting to port KornShell utilities directly, their functionalities were re-implemented in Python. This ensures seamless integration with the Airflow DAG, promotes code reusability within the Cloud Composer environment, and aligns with the Python-centric nature of Airflow.
*   **Elimination of Temporary Files for Metrics**: The original script's reliance on temporary files for capturing record counts was replaced by direct integration with BigQuery's capabilities. `BigQueryOperator` can directly return job statistics, including affected row counts, which can then be logged or passed via XComs, simplifying the data flow and reducing I/O overhead.
*   **Leveraging Airflow's Native Features**: Airflow's built-in parameter handling, task dependencies, retries, and failure callbacks were adopted to replace the custom shell script's parameter parsing, sequential execution, and `DWMSG_MeldeFehler` calls. This standardizes error handling and operational patterns.
*   **Focus on BigQuery Standard SQL**: The translation of `d_ausd_v_ta_acc_ref.sql` prioritized BigQuery Standard SQL, ensuring compatibility, performance optimization (e.g., through partitioning and clustering), and adherence to modern SQL practices.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Ensure the BigQuery dataset `isbert_ds` exists in the target GCP project. If not, create it.
2.  **BigQuery Table Creation & Data Ingestion**:
    *   **DDL Execution**: Execute the BigQuery DDLs to create the `ta_acc_ref` table and all other source tables identified as dependencies for `d_ausd_v_ta_acc_ref.bqsql` within the `isbert_ds` dataset.
    *   **Data Migration**: Ingest historical and/or initial data from the legacy Oracle source tables into their respective BigQuery counterparts. This may involve using services like BigQuery Data Transfer Service, Dataflow, or custom scripts.
3.  **IAM Permissions Configuration**:
    *   Ensure the Cloud Composer Service Account (associated with the Airflow environment) has the necessary IAM roles and permissions:
        *   `BigQuery Data Editor` (or more granular permissions) for the `isbert_ds` dataset to allow the DAG to write/update `ta_acc_ref`.
        *   `BigQuery Job User` to execute BigQuery queries.
        *   `Storage Object Viewer` and `Storage Object Creator` for the Cloud Composer DAGs bucket.
        *   Any other permissions required for specific GCP services if the DAG expands in scope.
4.  **Airflow Utility Package Deployment**:
    *   Deploy the `airflow_utils.py` (or equivalent Python package) to the Cloud Composer environment's `dags/repo` or `plugins` folder, ensuring it's accessible by the DAG.
5.  **Airflow Connection Configuration (if applicable)**:
    *   While BigQuery is natively integrated, if any other external systems are introduced, configure Airflow Connections (e.g., for external databases, APIs).
6.  **Airflow Variables Configuration**:
    *   If `p_JobKennung` and `p_EintragsNr` are to be managed as Airflow Variables (e.g., for default values or sensitive parameters), create these variables in the Airflow UI.
7.  **DAG Deployment**:
    *   Upload the `k_ausd_v_ta_acc_ref_dag.py` file to the DAGs folder in the Cloud Composer environment's Cloud Storage bucket.
8.  **Scheduling Configuration**:
    *   Configure the desired schedule for the `k_ausd_v_ta_acc_ref` DAG within the `k_ausd_v_ta_acc_ref_dag.py` file (e.g., `schedule_interval`).

## 5. Known Gaps & Unresolved References

The following items were flagged during the design phase and require further investigation or follow-up:

*   **Content of `d_ausd_v_ta_acc_ref.sql`**: The exact Oracle SQL code for `d_ausd_v_ta_acc_ref.sql` was not available during the design phase. The complexity of its logic (e.g., use of proprietary Oracle functions, PL/SQL blocks, dynamic SQL, or external calls) will directly impact the effort and potential challenges during BigQuery SQL translation. This is a critical B4 item.
*   **Source Tables for `d_ausd_v_ta_acc_ref.sql`**: The specific input tables read by `d_ausd_v_ta_acc_ref.sql` are unknown. Their schemas, data types, and current migration status to BigQuery need to be confirmed to ensure all dependencies are met.
*   **Detailed Utility Script Functionality**: A thorough analysis of the full functionality of `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, and `h_alis_sqlplus.ksh` is required to ensure their complete and accurate re-implementation in Python.
*   **`r_ausd_vertrag.ksh` Reference**: The design document noted a reference to `r_ausd_vertrag.ksh` as a control script. Its exact relationship to `k_ausd_v_ta_acc_ref.ksh` (e.g., if it's a wrapper or orchestrator) and its migration strategy need to be clarified.
*   **Dynamic SQL/External Commands in `d_ausd_v_ta_acc_ref.sql`**: If the Oracle SQL script contains dynamic SQL or invokes external commands (e.g., via `DBMS_SCHEDULER` or `UTL_FILE`), this will significantly increase the complexity of BigQuery migration and may require alternative approaches (e.g., Dataflow, Cloud Functions, or more complex Airflow operators).
*   **UC4 Integration**: The `UC4_PROD` reference suggests a broader UC4-to-Airflow migration strategy might be in play. The integration of this specific Airflow DAG into the larger UC4 migration plan needs to be confirmed to ensure seamless transition and scheduling.

## 6. Validation

Validation ensures the migrated job functions correctly and produces accurate results.

**How to Run Tests:**

1.  **Manual Trigger**: Access the Airflow UI for the Cloud Composer environment. Locate the `k_ausd_v_ta_acc_ref` DAG and manually trigger a run.
2.  **Scheduled Trigger**: If a `schedule_interval` is defined, wait for the DAG to trigger automatically at its next scheduled time.
3.  **Parameter Passing**: When triggering manually, provide test values for `job_kennung` and `eintrags_nr` (corresponding to `p_JobKennung` and `p_EintragsNr`).

**What "Passing" Means:**

A successful validation run implies the following:

*   **DAG Execution Success**: The `k_ausd_v_ta_acc_ref` DAG completes all its tasks successfully in the Airflow UI (all tasks show green).
*   **BigQuery Job Success**: The `BigQueryOperator` task executes the `d_ausd_v_ta_acc_ref.bqsql` query without errors in BigQuery.
*   **Data Integrity**:
    *   The `isbert_ds.ta_acc_ref` table in BigQuery is updated or populated as expected.
    *   Row counts in the target `ta_acc_ref` table match the expected output based on the source data and transformation logic.
    *   Spot checks on key data values in `ta_acc_ref` confirm accuracy and consistency with the original Oracle output.
    *   If applicable, checksums or hash comparisons of the processed data between source and target yield identical results.
*   **Parameter Handling**: The DAG correctly receives and processes the `job_kennung` and `eintrags_nr` parameters. Validation logic (if implemented) correctly identifies invalid parameters.
*   **Logging**: Airflow logs for the DAG and its tasks show no errors or unexpected warnings. Custom logging for record counts or other metrics is present and accurate.
*   **Performance**: The BigQuery job completes within acceptable performance thresholds.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Deactivate Airflow DAG**:
    *   In the Airflow UI, toggle off the `k_ausd_v_ta_acc_ref` DAG to prevent further scheduled runs.
    *   Alternatively, remove the `k_ausd_v_ta_acc_ref_dag.py` file from the Cloud Composer DAGs folder.
2.  **Re-enable Legacy Job**:
    *   Re-enable the original `k_ausd_v_ta_acc_ref.ksh` script in its legacy scheduler (e.g., UC4, cron).
    *   Verify that the legacy Oracle database and all original utility scripts are operational and accessible by the KornShell script.
3.  **Data Reversion (if necessary)**:
    *   **Impact Assessment**: Determine the impact of the failed BigQuery run on the `isbert_ds.ta_acc_ref` table.
    *   **Revert Data**:
        *   If the BigQuery SQL performs `TRUNCATE` and `INSERT`, the table might need to be reloaded from a previous backup or a known good state.
        *   If it performs `UPDATE` or `MERGE`, a reverse operation might be required, or the table might need to be restored from a snapshot/backup.
        *   If the original Oracle script is idempotent and can safely re-process the data, running the legacy job might correct the data in the Oracle `ta_acc_ref` table, which would then need to be re-migrated to BigQuery if the BigQuery table was affected.
    *   **Note**: The specific data reversion strategy depends heavily on the exact DML operations performed by `d_ausd_v_ta_acc_ref.sql` and the availability of BigQuery table snapshots or backups. This step requires careful planning and execution.
4.  **Monitor Legacy System**:
    *   Monitor the re-enabled legacy job and the Oracle database to ensure normal operation.

This rollback procedure aims to quickly restore the system to its previous working state using the legacy infrastructure while the issues with the migrated job are investigated and resolved.