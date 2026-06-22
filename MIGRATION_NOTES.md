# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the legacy KornShell script `k_ausd_v_ta_vvl_dwh.ksh` and its dependent SQL script `d_ausd_v_ta_vvl_dwh.sql`. The original script served as an orchestration layer, handling environment setup, parameter parsing, validation, and execution of the SQL script which performs data processing.

The migration targets Google Cloud Platform (GCP), specifically:
*   **Cloud Composer (Airflow)** for orchestration, replacing the KornShell script and the UC4 scheduler.
*   **BigQuery** for data storage and SQL execution, replacing the legacy database and its SQL dialect.

The overall business purpose is to process data related to `ta_vvl_dwh` (Vertragsverlängerung Data Warehouse) for reporting and further data warehousing stages.

## 2. Generated Artifacts

The migration process has generated the following files:

*   **`sql/ddl/dwtk_meldungen.sql`**:
    *   **Role**: BigQuery DDL (Data Definition Language) script for creating the `dwtk_meldungen` table in BigQuery. This table serves as a source for the data processing pipeline.
    *   **Note**: This is a placeholder schema and requires review and update with actual column definitions and data types from the legacy source.

*   **`sql/ddl/dwh_ta_f_vvl_ereignisse.sql`**:
    *   **Role**: BigQuery DDL script for creating the `dwh_ta_f_vvl_ereignisse` table in BigQuery. This table is a primary source for the data processing logic.
    *   **Note**: This is a placeholder schema, inferred from the SQL logic, and requires review and update with actual column definitions and data types from the legacy source.

*   **`sql/ddl/sof_ta_vvl_dwh.sql`**:
    *   **Role**: BigQuery DDL script for creating the `sof_ta_vvl_dwh` table in BigQuery. This is one of the target tables where processed data is inserted.
    *   **Note**: This is a placeholder schema, inferred from the SQL logic, and requires review and update with actual column definitions and data types from the legacy source.

*   **`sql/ddl/via.sql`**:
    *   **Role**: BigQuery DDL script for creating the `via` table in BigQuery. This is another target table for processed data.
    *   **Note**: This is a generic placeholder schema as no specific definition was found. It requires full definition based on the legacy `VIA` table.

*   **`sql/d_ausd_v_ta_vvl_dwh_transformed.sql`**:
    *   **Role**: The core data processing logic, translated from the original `d_ausd_v_ta_vvl_dwh.sql` to BigQuery Standard SQL. This script performs the `DELETE` (equivalent to `TRUNCATE`) and `INSERT` operations into `sof_ta_vvl_dwh`.
    *   **Note**: This script contains placeholders for project and dataset IDs and highlights areas requiring further manual re-implementation, particularly regarding the `DWPA_UTIL_SKRIPT` PL/SQL package.

*   **`dags/bert_ausd_v_ta_vvl_dwh.py`**:
    *   **Role**: An Apache Airflow DAG (Directed Acyclic Graph) written in Python. This DAG orchestrates the entire pipeline, replacing the `k_ausd_v_ta_vvl_dwh.ksh` script and the UC4 scheduler. It handles parameter passing and executes the transformed BigQuery SQL using the `BigQueryExecuteQueryOperator`.

## 3. Key Design Decisions

*   **Orchestration with Cloud Composer (Airflow)**: The KornShell script's role as an orchestrator, along with the UC4 scheduler, is fully replaced by an Airflow DAG. This provides a cloud-native, scalable, and observable solution for pipeline management.
*   **Data Processing with BigQuery**: The legacy database and its SQL processing are migrated to BigQuery. This leverages BigQuery's serverless, highly scalable, and cost-effective data warehousing capabilities for analytical workloads.
*   **SQL Dialect Translation**: The original SQL (likely Oracle SQL) has been translated to BigQuery Standard SQL. This ensures compatibility with the target platform and allows for leveraging BigQuery's optimized query engine.
*   **Parameter Handling via Airflow DAG Parameters**: Command-line parameters (`p_JobKennung`, `p_EintragsNr`) previously handled by `getopts` in the KornShell script are now managed as Airflow DAG parameters. These parameters can be passed during manual runs or configured as part of the schedule, and are templated into the BigQuery SQL.
*   **Replacement of Utility Functions**: Legacy KornShell utility functions (e.g., `starteSQLSkript`, `pruefeParameterGesetzt`, `DWMSG_MeldeFehler`) are replaced by native Airflow operators, Python logic within the DAG, or BigQuery's capabilities. For instance, `starteSQLSkript` is replaced by `BigQueryExecuteQueryOperator`, and logging will use Airflow's native logging integrated with Cloud Logging.
*   **Handling of `DWPA_UTIL_SKRIPT`**: The functionality of the legacy PL/SQL package `DWPA_UTIL_SKRIPT` is identified as a critical component requiring re-implementation. The current design assumes it will be translated into BigQuery UDFs/Stored Procedures or Python tasks within the Airflow DAG, depending on its complexity. For the `TRUNCATE` equivalent, a `DELETE FROM WHERE TRUE` statement is used directly in the BigQuery SQL.
*   **`tmpFile` for Record Count**: The original script's use of a temporary file to capture record counts is addressed by suggesting a post-execution BigQuery query within a PythonOperator in Airflow, pushing the result to XComs if needed for downstream tasks. This avoids file system dependencies in a serverless environment.

## 4. Manual Steps Before Go-Live

Before the migrated pipeline can go live, the following manual steps are required:

1.  **GCP Project Setup**: Ensure a GCP project is provisioned and configured.
2.  **Cloud Composer Environment**:
    *   Provision a Cloud Composer environment (Airflow instance).
    *   Configure the `gcp_connection_id` in Airflow to connect to your GCP project.
    *   Upload the `dags/bert_ausd_v_ta_vvl_dwh.py` file to the DAGs folder of your Cloud Composer environment.
3.  **BigQuery Datasets Creation**:
    *   Create the necessary BigQuery datasets. Based on the generated code, these include:
        *   `project_id.isbert_source_dataset` (e.g., `your-gcp-project.isbert_source_dataset`)
        *   `project_id.target_dataset` (e.g., `your-gcp-project.target_dataset`)
    *   Replace `project_id` with your actual GCP Project ID.
4.  **BigQuery Table Creation (DDL Execution)**:
    *   Review and refine the generated DDL scripts (`sql/ddl/*.sql`) to accurately reflect the legacy schemas, including precise column names, data types, and any partitioning/clustering strategies.
    *   Execute these DDL scripts in BigQuery to create the tables:
        *   `dwtk_meldungen`
        *   `dwh_ta_f_vvl_ereignisse`
        *   `sof_ta_vvl_dwh`
        *   `via`
5.  **Initial Data Ingestion**:
    *   Load historical data from the legacy source tables (`DWTK_MELDUNGEN`, `DWH$TA_F_VVL_EREIGNISSE`) into their respective BigQuery tables. This can be done using `bq load` commands, Dataflow jobs, or other ETL tools.
6.  **IAM Permissions**:
    *   Grant the Cloud Composer Service Account (which runs the Airflow DAGs) appropriate IAM roles for BigQuery (e.g., `BigQuery Data Editor`, `BigQuery Job User`) to read from source tables and write to target tables.
7.  **Secrets Management**:
    *   If any sensitive information (e.g., API keys, external database credentials if still needed) is required, store them securely in Secret Manager and configure Airflow to access them. (Not explicitly identified in this migration, but good practice).
8.  **Airflow DAG Configuration**:
    *   Update the `params` dictionary in `dags/bert_ausd_v_ta_vvl_dwh.py` with your actual `gcp_project_id`, `isbert_source_dataset`, and `target_dataset` names.
    *   Set the desired `schedule` for the DAG (e.g., `@daily`, `0 0 * * *`).
9.  **`DWPA_UTIL_SKRIPT` Re-implementation**:
    *   Manually analyze the full functionality of the `DWPA_UTIL_SKRIPT` PL/SQL package.
    *   Re-implement its logic as BigQuery UDFs (SQL or JavaScript), BigQuery Stored Procedures, or Python functions within the Airflow DAG, as appropriate. This is a critical step flagged as a known gap.
10. **`v_datum` Parameter Handling**:
    *   If the `v_datum` logic (determining a date from `DWTK_MELDUNGEN`) is critical, ensure it's either passed as an Airflow parameter or re-implemented directly in BigQuery SQL or a Python task.

## 5. Known Gaps & Unresolved References

The following items have been identified as requiring further analysis, manual intervention, or are considered potential risks:

*   **Complexity of `DWPA_UTIL_SKRIPT` (B4 Item)**: The exact logic within the `DWPA_UTIL_SKRIPT` PL/SQL package is unknown. Its migration to BigQuery (UDFs, Stored Procedures) or Python tasks in Airflow could be complex and requires detailed analysis. This is the most significant unresolved item.
*   **Exact Behavior of `starteSQLSkript`**: While presumed to execute SQL, the precise mechanisms (e.g., connection parameters, error handling, output capture) within the original `starteSQLSkript` are abstracted. The `BigQueryExecuteQueryOperator` handles basic execution, but any nuanced behavior needs to be understood and replicated if critical.
*   **`tmpFile` Handling Criticality**: The specific use case and criticality of `v_records=\`cat $tmpFile\`` (capturing record counts) need clarification. If this record count is essential for downstream processes, a dedicated Airflow task (e.g., PythonOperator) to query the target BigQuery table for counts and push to XComs should be implemented.
*   **Implicit Dependencies from Sourced Scripts**: The original KornShell script sourced several utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`). These may contain implicit environment variables or functions not yet fully accounted for. A thorough review of these scripts is needed to ensure all necessary configurations or functionalities are replicated in Airflow.
*   **Data Volume and Performance**: The current performance characteristics of `d_ausd_v_ta_vvl_dwh.sql` are unknown. Post-migration, performance testing in BigQuery is crucial, and tuning (e.g., partitioning, clustering, query optimization) might be required for large datasets.
*   **Parameter `h` Handling**: The KornShell script has a parameter `h` that prints "Bitte ueber Rahmenscript aufrufen" and exits. This implies it's designed to be called by a "framework script." The Airflow DAG replaces this framework, so the `h` parameter's original intent is no longer directly applicable, but the DAG should be robust enough to handle its own invocation.
*   **Refinement of BigQuery DDLs**: The generated DDLs are placeholders. They must be updated with the exact column names, data types, nullability constraints, and any other schema details from the legacy source system.
*   **`v_datum` Parameter**: The original SQL script likely derived `v_datum` from `DWTK_MELDUNGEN`. This logic needs to be explicitly handled in the Airflow DAG (e.g., by passing it as a parameter or re-implementing the date derivation logic in a preceding task).
*   **Error Handling and Logging**: The original `DWMSG_MeldeFehler` and other error handling mechanisms need to be fully integrated with Airflow's native logging and alerting capabilities.

## 6. Validation

Validation of the migrated pipeline involves several stages:

1.  **Unit Testing of BigQuery SQL**:
    *   **How to run**: Execute `sql/d_ausd_v_ta_vvl_dwh_transformed.sql` directly in BigQuery (e.g., via the BigQuery UI or `bq query` command) against representative sample data in the BigQuery source tables.
    *   **Passing means**: The query executes successfully without syntax errors, and the `INSERT` operation produces the expected number of rows and correct data in `sof_ta_vvl_dwh`.

2.  **Airflow DAG Integration Testing**:
    *   **How to run**: Trigger the `bert_ausd_v_ta_vvl_dwh` DAG manually in the Cloud Composer UI.
    *   **Passing means**: The DAG runs to completion without task failures. All tasks (start, log_parameters, execute_bigquery_sql, end) show a "success" status. Airflow logs should indicate successful BigQuery job execution.

3.  **Data Validation**:
    *   **How to run**:
        *   Run the legacy `k_ausd_v_ta_vvl_dwh.ksh` script in the original environment with a specific set of input parameters and source data.
        *   Run the `bert_ausd_v_ta_vvl_dwh` Airflow DAG with equivalent parameters and the same source data (loaded into BigQuery).
        *   Compare the output data in the legacy target tables (`SOF$TA_VVL_DWH`, `VIA`) with the data in the BigQuery target tables (`sof_ta_vvl_dwh`, `via`). This can involve row counts, checksums, and detailed column-by-column comparisons.
    *   **Passing means**: The data in the BigQuery target tables is identical (or functionally equivalent, considering data type conversions) to the data produced by the legacy system for the same input. Row counts match, and key metrics derived from the output data are consistent.

4.  **Performance Testing**:
    *   **How to run**: Execute the Airflow DAG with production-like data volumes in BigQuery. Monitor BigQuery job execution times and resource consumption.
    *   **Passing means**: The pipeline completes within acceptable timeframes, and BigQuery costs are within expected limits. Performance should ideally be comparable to or better than the legacy system.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Disable New Airflow DAG**:
    *   In the Cloud Composer UI, disable the `bert_ausd_v_ta_vvl_dwh` DAG. This will prevent any further scheduled or manual runs of the migrated pipeline.
2.  **Re-enable Legacy UC4 Job**:
    *   Re-enable the original UC4 job (e.g., `DW.BERT_AUSD_V_TA_VVL_DWH.xml`) that invokes `k_ausd_v_ta_vvl_dwh.ksh`. Ensure the legacy environment and its dependencies are fully operational.
3.  **Data Reversion (if necessary)**:
    *   If the migrated pipeline has written incorrect or incomplete data to the BigQuery target tables (`sof_ta_vvl_dwh`, `via`) and this data needs to be reverted, perform one of the following:
        *   **Point-in-Time Restore**: If BigQuery tables are configured with time travel, restore the tables to a state before the problematic run.
        *   **Truncate/Reload**: If the impact is limited and a full reload is feasible, truncate the affected BigQuery target tables and reload them with data from a known good state (e.g., a backup or a previous successful run's output).
        *   **Corrective SQL**: Execute corrective SQL statements to fix the erroneous data in BigQuery.
    *   **Note**: Since the `d_ausd_v_ta_vvl_dwh_transformed.sql` starts with a `DELETE FROM WHERE TRUE` (equivalent to `TRUNCATE`), a rollback might involve simply letting the legacy system run and overwrite/re-insert into its own target tables, and then deciding whether to clear or correct the BigQuery target tables.
4.  **Monitor Legacy System**:
    *   Monitor the re-enabled legacy system to ensure it is functioning correctly and processing data as expected.
5.  **Root Cause Analysis**:
    *   Investigate the root cause of the migration failure using logs from Cloud Composer, BigQuery, and any other relevant GCP services. Address the identified issues before attempting another migration or re-deployment.