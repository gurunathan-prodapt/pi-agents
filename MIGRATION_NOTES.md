# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the legacy KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_msisdn.ksh` and its associated Oracle SQL script `d_ausd_bp_ta_bcp_msisdn.sql`. The original job was responsible for data preparation, including parameter parsing, date validation, error handling, and orchestrating the execution of an SQL script to process data from source tables `DWTK_MELDUNGEN` and `SOF$TA_BPR_BCP` into the target table `SOF$TA_BCP_MSISDN`.

The job has been migrated to Google Cloud Platform, leveraging **Google Cloud BigQuery** for data storage and processing, and **Apache Airflow (via Cloud Composer)** for workflow orchestration. The KornShell orchestration logic has been re-implemented as a Python Airflow DAG, and the Oracle SQL has been translated to BigQuery Standard SQL.

## 2. Generated Artifacts

The following files were generated as part of this migration:

*   **`ddl/DWTK_MELDUNGEN.sql`**: BigQuery DDL (Data Definition Language) script to create the `DWTK_MELDUNGEN` table in the target BigQuery dataset. This table serves as a source for the data transformation.
*   **`ddl/SOF_TA_BPR_BCP.sql`**: BigQuery DDL script to create the `SOF_TA_BPR_BCP` table in the target BigQuery dataset. This table serves as another source for the data transformation.
*   **`ddl/SOF_TA_RN_VERTRAG.sql`**: BigQuery DDL script to create the `SOF_TA_RN_VERTRAG` table. This table was identified as an implied source based on the SQL transformation logic.
*   **`ddl/SOF_TA_BCP_MSISDN.sql`**: BigQuery DDL script to create the `SOF_TA_BCP_MSISDN` table in the target BigQuery dataset. This is the final target table where the processed data will be stored.
*   **`sql/d_ausd_bp_ta_bcp_msisdn_bq.sql`**: Contains the core data transformation logic, translated from Oracle SQL to BigQuery Standard SQL. This file defines the `SELECT` statement that extracts and processes data.
*   **`utils/error_handling.py`**: A Python module that provides basic error logging and handling utilities, replacing the functionality of the legacy `f_alis_msgerr.ksh` script. It integrates with Airflow's logging mechanisms.
*   **`utils/date_helpers.py`**: A Python module containing functions for date validation and calculation (e.g., getting "today" and "yesterday" dates), replacing the functionality of `h_alis_date.ksh` and `gestern.ksh`.
*   **`utils/parameter_parser.py`**: A Python module for parsing and validating job parameters, replacing the `getopts` logic and `h_alis_parameter.ksh`. In an Airflow context, this primarily helps process `dag_run.conf` parameters.
*   **`dags/k_ausd_bp_ta_bcp_msisdn_dag.py`**: The main Apache Airflow DAG (Directed Acyclic Graph) file. This Python script orchestrates the entire data preparation process, replacing the control flow and execution logic of the original `k_ausd_bp_ta_bcp_msisdn.ksh` script. It handles parameter validation, date calculations, and triggers the BigQuery SQL transformation.

## 3. Key Design Decisions

*   **Cloud-Native Platform Adoption**: The decision was made to migrate to Google Cloud Platform (GCP) to leverage its scalable, managed services. BigQuery was chosen for its analytical capabilities and cost-effectiveness for large datasets, while Cloud Composer (Apache Airflow) provides robust, scalable workflow orchestration.
*   **Orchestration Re-platforming (KornShell to Airflow DAG)**: The complex orchestration logic, parameter handling, and error management embedded in the original KornShell script were re-implemented in Python as an Airflow DAG. This provides better maintainability, observability, and integration with other cloud services compared to shell scripting.
*   **SQL Dialect Translation (Oracle to BigQuery Standard SQL)**: The core data transformation logic, originally written in Oracle SQL, was directly translated to BigQuery Standard SQL. This involved converting Oracle-specific functions, data types, and syntax to their BigQuery equivalents.
*   **Utility Script Replacement (Shell to Python Modules)**: All auxiliary KornShell utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `gestern.ksh`) were re-implemented as modular Python functions within dedicated `utils` modules. This ensures seamless integration with the Python-based Airflow DAG and improves code reusability and testability.
*   **`DWPA_UTIL_SKRIPT` Handling**: The functionality of the Oracle `DWPA_UTIL_SKRIPT` package was analyzed. For this specific migration, its relevant logic was either in-lined directly into the BigQuery SQL transformation or determined not to be critical for the `SELECT` statement's core functionality. If more complex, reusable logic were required, it would have been migrated to BigQuery Stored Procedures or User-Defined Functions (UDFs).
*   **Data Loading Strategy**: While not explicitly generated in this set of artifacts, the design assumes that historical and incremental data for source tables (`DWTK_MELDUNGEN`, `SOF_TA_BPR_BCP`, `SOF_TA_RN_VERTRAG`) will be ingested into BigQuery using appropriate GCP tools (e.g., Dataflow, `gsutil`, BigQuery Load jobs) as a prerequisite.
*   **Idempotent BigQuery Transformation**: The BigQuery transformation within the DAG uses a `TRUNCATE TABLE` followed by an `INSERT INTO` pattern. This ensures that each successful run of the DAG produces a consistent state in the target table `SOF_TA_BCP_MSISDN`, making the operation idempotent.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Create the target BigQuery dataset, replacing `YOUR_BIGQUERY_DATASET` with the actual name (e.g., `isbert_dataset`).
    *   Ensure the BigQuery project ID (`YOUR_BIGQUERY_PROJECT`) is correctly configured.
2.  **BigQuery Table Schema Creation**:
    *   Execute the DDL scripts (`ddl/*.sql`) in the target BigQuery dataset to create the necessary source and target tables:
        *   `ddl/DWTK_MELDUNGEN.sql`
        *   `ddl/SOF_TA_BPR_BCP.sql`
        *   `ddl/SOF_TA_RN_VERTRAG.sql`
        *   `ddl/SOF_TA_BCP_MSISDN.sql`
3.  **Data Ingestion**:
    *   Ingest all required historical and incremental data from the legacy Oracle source tables (`DWTK_MELDUNGEN`, `SOF$TA_BPR_BCP`, `SOF$TA_RN_VERTRAG`) into their respective BigQuery counterparts. This is a critical prerequisite for the job to function correctly.
4.  **IAM Permissions Configuration**:
    *   Grant the Cloud Composer service account (or the service account used by your Airflow environment) the necessary BigQuery permissions. This typically includes `BigQuery Data Editor` or more granular roles for `bigquery.tables.create`, `bigquery.tables.updateData`, `bigquery.tables.getData`, and `bigquery.jobs.create` on the target BigQuery project and dataset.
    *   Ensure the service account has permissions to read/write to the Cloud Storage bucket where the Airflow DAGs and supporting files are stored.
5.  **Airflow Connection Configuration**:
    *   Verify that the `google_cloud_default` Airflow connection is properly configured in your Cloud Composer environment. This connection is used by the `BigQueryOperator` to authenticate with BigQuery.
6.  **Deployment of DAG and Supporting Files**:
    *   Upload the `dags/k_ausd_bp_ta_bcp_msisdn_dag.py` file to the DAGs folder of your Cloud Composer environment.
    *   Upload the `sql/d_ausd_bp_ta_bcp_msisdn_bq.sql` file and the `utils/` directory (containing `error_handling.py`, `date_helpers.py`, `parameter_parser.py`) to a location accessible by the Airflow worker, typically within the DAGs folder or a designated `dags/repo` structure.
7.  **Upstream Orchestration Update**:
    *   Modify the upstream job or scheduler (e.g., the Airflow DAG replacing `r_ausd_bp_ta_bcp_msisdn.ksh`) to trigger the new `k_ausd_bp_ta_bcp_msisdn_dag` in Cloud Composer, passing the required parameters (`job_kennung`, `eintrags_nr`, `stichtag`, `wiederanlauf_wert`) via `dag_run.conf`.
8.  **Placeholder Replacement**:
    *   Ensure all instances of `YOUR_BIGQUERY_PROJECT` and `YOUR_BIGQUERY_DATASET` within the generated code (especially in the DAG and DDLs) are replaced with the actual project and dataset IDs.

## 5. Known Gaps & Unresolved References

The following items were identified during the migration design and generation process and require further attention or are considered known limitations:

*   **`DWPA_UTIL_SKRIPT` Package Functionality**: The exact, comprehensive functionality of the Oracle `DWPA_UTIL_SKRIPT` package was not fully detailed. While the specific logic required for `d_ausd_bp_ta_bcp_msisdn.sql` was addressed (either in-lined or deemed not directly applicable to the simple `SELECT DISTINCT` query), any other dependencies or complex logic within this package that might be used by other jobs would require separate migration to BigQuery Stored Procedures, UDFs, or Python functions. This remains a potential risk for other dependent processes.
*   **Commented-out KornShell Code**: The original KornShell script contained commented-out sections involving file manipulations (`sed`, `sort`, `join`). It was assumed these functionalities are not currently active or required. If they become active in the future, they would need to be re-evaluated and potentially migrated to Dataflow, PySpark, or Python processing within the Airflow DAG, possibly utilizing Google Cloud Storage for intermediate files.
*   **`FOSJob*` Calls**: The commented-out `FOSJobDeaktivate` and `FOSJobErzeugeEintrag` calls suggest integration with an external job management system (FOS). This migration replaces that with Airflow's native scheduling and monitoring. If there are still active external dependencies on FOS for this job's status reporting, a custom Airflow operator or integration with Cloud Logging/Monitoring might be required to replicate that communication.
*   **Environment Variables (`BERT_DIR_ROOT`, `DW_DIR_UTL`)**: The specific values and usage of these environment variables were not fully detailed. In the migrated solution, file paths are explicitly defined or managed by Airflow's environment. If these variables pointed to external resources or configuration files, those resources would need to be migrated to Cloud Storage or other GCP services, and their paths updated in the DAG.
*   **Upstream Invoker (`r_ausd_bp_ta_bcp_msisdn.ksh`)**: This migration focuses on `k_ausd_bp_ta_bcp_msisdn.ksh`. The migration of its upstream invoker, `r_ausd_bp_ta_bcp_msisdn.ksh`, is crucial for end-to-end workflow continuity and needs to be planned and executed separately to ensure it correctly triggers the new Airflow DAG.
*   **Record Count Logging**: The original script used a temporary file (`tmpFile`) to capture record counts. The migrated DAG includes a placeholder `log_record_count` task. While Airflow logs provide execution details, a specific BigQuery query would be needed within this task to fetch and log the exact row count from `SOF_TA_BCP_MSISDN` after the `INSERT` operation, if that level of detail is required for auditing or monitoring.

## 6. Validation

To validate the successful migration and functionality of the `k_ausd_bp_ta_bcp_msisdn_dag` Airflow DAG, follow these steps:

1.  **Prerequisites**: Ensure all manual steps (Section 4) are completed, including BigQuery table creation and initial data ingestion.
2.  **Trigger the DAG**:
    *   Access the Cloud Composer UI.
    *   Locate the `k_ausd_bp_ta_bcp_msisdn_dag`.
    *   Manually trigger the DAG, providing the necessary configuration in the `dag_run.conf` JSON payload:
        ```json
        {
            "job_kennung": "YOUR_JOB_ID",
            "eintrags_nr": "YOUR_ENTRY_NUMBER",
            "stichtag": "DDMMYYYY",
            "wiederanlauf_wert": 0
        }
        ```
        Replace `YOUR_JOB_ID`, `YOUR_ENTRY_NUMBER`, and `DDMMYYYY` with appropriate test values.
3.  **Monitor DAG Execution**:
    *   Observe the DAG run in the Airflow UI. All tasks (`start_job`, `parse_and_validate_parameters`, `calculate_dates`, `read_sql_transform_content`, `execute_bigquery_transformation`, `log_record_count`, `end_job`) should complete successfully (green status).
    *   Review the logs for each task to ensure:
        *   Parameters are parsed and validated correctly.
        *   Dates are calculated as expected.
        *   The BigQuery job is initiated and completes without errors.
4.  **Verify BigQuery Output**:
    *   Navigate to the BigQuery UI.
    *   Query the target table `YOUR_BIGQUERY_PROJECT.YOUR_BIGQUERY_DATASET.SOF_TA_BCP_MSISDN`.
    *   **Passing Criteria**:
        *   The table `SOF_TA_BCP_MSISDN` should be populated with data.
        *   The number of rows in `SOF_TA_BCP_MSISDN` should match the expected record count from the legacy system for the same input parameters and source data.
        *   Perform a data integrity check by comparing a sample of the output data in BigQuery with the corresponding output from the legacy Oracle system. All columns (`CNTRCT_ID`, `BPR_ID`, `CNTRCT_ID_REF`, `TN_TEL_MSISDN`) should match precisely.
        *   The `created_at` timestamp should reflect the time of insertion in BigQuery.

## 7. Rollback Procedure

In the event of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Deactivate New DAG**:
    *   In the Cloud Composer UI, disable the `k_ausd_bp_ta_bcp_msisdn_dag` to prevent further executions.
2.  **Re-enable Legacy Job**:
    *   Re-enable the original KornShell script (`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_msisdn.ksh`) in the legacy environment.
    *   Ensure the legacy scheduler (e.g., the script replacing `r_ausd_bp_ta_bcp_msisdn.ksh`) is reverted to trigger the original KornShell script.
3.  **Data Rollback (if necessary)**:
    *   If the `SOF_TA_BCP_MSISDN` table in BigQuery was populated incorrectly, you have a few options:
        *   **Time Travel**: BigQuery supports time travel. You can query the table as of a timestamp before the erroneous run to retrieve the correct state, or use `CREATE TABLE ... AS SELECT * FROM your_table FOR SYSTEM_TIME AS OF 'YYYY-MM-DD HH:MM:SS'` to restore.
        *   **Truncate/Delete**: If a full reset is acceptable, `TRUNCATE TABLE YOUR_BIGQUERY_PROJECT.YOUR_BIGQUERY_DATASET.SOF_TA_BCP_MSISDN` can be executed to clear the table.
        *   **Restore from Snapshot**: If BigQuery snapshots were configured, restore the table from a known good snapshot.
    *   Ensure the legacy Oracle target table `SOF$TA_BCP_MSISDN` is in a consistent state, potentially requiring a restore from a backup if the issue propagated to the legacy system during a dual-run period.
4.  **Investigate and Remediate**:
    *   Analyze the logs from the failed Airflow DAG run and BigQuery jobs to identify the root cause of the issue.
    *   Apply necessary fixes to the DAG, SQL, or BigQuery schema.
    *   Re-test thoroughly in a staging environment before attempting another go-live.