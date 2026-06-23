# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the legacy KornShell script `k_ausd_v_ta_notice.ksh` and its associated Oracle SQL script `d_ausd_v_ta_notice.sql`. The original system orchestrated data processing for `SOF$TA_NOTICE` and `VIA` tables, including parameter parsing, job state management, and error handling.

The migration targets Google Cloud BigQuery, where the orchestration logic is encapsulated within a BigQuery Stored Procedure (`r_ausd_vertrag_control`), and the data transformation logic is converted to BigQuery Standard SQL. Job state management and error logging are handled by dedicated BigQuery tables (`job_table`, `error_log`). The overall job execution will be orchestrated by Cloud Composer (Apache Airflow).

## 2. Generated Artifacts

The following files were generated as part of this migration:

*   **`bigquery/ddl/job_table.sql`**: BigQuery DDL for the `job_table`. This table tracks the status, parameters, and execution details of each job run, replacing the legacy file-based or implicit job state management.
*   **`bigquery/ddl/error_log.sql`**: BigQuery DDL for the `error_log` table. This table centralizes error messages and details, replacing the functionality of the legacy `f_alis_msgerr.ksh` script.
*   **`bigquery/ddl/DWTK_MELDUNGEN.sql`**: BigQuery DDL for the `DWTK_MELDUNGEN` source table. This defines the schema for the migrated source data.
*   **`bigquery/ddl/CDS_TA_NOTICE.sql`**: BigQuery DDL for the `CDS_TA_NOTICE` source table. This defines the schema for the migrated source data.
*   **`bigquery/ddl/SOF_TA_NOTICE.sql`**: BigQuery DDL for the `SOF_TA_NOTICE` target table. This defines the schema for the primary target data.
*   **`bigquery/ddl/VIA.sql`**: BigQuery DDL for the `VIA` target table. This defines the schema for the secondary target data.
*   **`bigquery/udf/dwpa_util_skript_get_date_formatted.sql`**: A placeholder BigQuery UDF demonstrating how functions from the legacy `DWPA_UTIL_SKRIPT` package or `h_alis_date.ksh` would be migrated. Specific functions need to be implemented based on their original Oracle logic.
*   **`bigquery/stored_procedures/r_ausd_vertrag_control.sql`**: The core BigQuery Stored Procedure. It encapsulates the parameter validation, job state management, error logging, and the data transformation logic previously found in `k_ausd_v_ta_notice.ksh` and `d_ausd_v_ta_notice.sql`. It also includes a helper procedure `log_error`.
*   **`composer/dags/r_ausd_vertrag_dag.py`**: A Cloud Composer (Apache Airflow) DAG definition. This Python script orchestrates the execution of the `r_ausd_vertrag_control` BigQuery Stored Procedure, replacing the upstream shell script (`r_ausd_vertrag.ksh`) that previously invoked `k_ausd_v_ta_notice.ksh`.

## 3. Key Design Decisions

*   **BigQuery Stored Procedures for Orchestration**: The decision to use a BigQuery Stored Procedure (`r_ausd_vertrag_control`) for orchestration was made to leverage BigQuery's native capabilities for complex SQL logic, parameter handling, and transactional integrity. This centralizes the logic, reduces external dependencies (like shell scripting), and improves performance by keeping computation within BigQuery.
*   **Dedicated BigQuery Tables for Job Control and Error Logging**: Instead of relying on file-based mechanisms or implicit states, dedicated BigQuery tables (`job_table`, `error_log`) were introduced. This provides a structured, queryable, and scalable way to monitor job execution, track history, and manage errors, aligning with modern data warehousing practices.
*   **Cloud Composer for External Orchestration**: Cloud Composer (Airflow) was chosen to replace the upstream shell script (`r_ausd_vertrag.ksh`) for scheduling and triggering the BigQuery Stored Procedure. This provides robust scheduling, monitoring, alerting, and dependency management capabilities inherent to Airflow, offering a more scalable and manageable solution than legacy shell scripts.
*   **Direct SQL Translation**: The core data transformation logic from `d_ausd_v_ta_notice.sql` is directly translated into BigQuery Standard SQL and embedded within the Stored Procedure. This minimizes the need for intermediate processing layers and maximizes BigQuery's query optimization.
*   **Re-implementation of Utility Functions**: Legacy utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) and Oracle packages (`DWPA_UTIL_SKRIPT`) are replaced by BigQuery UDFs or integrated directly into the Stored Procedure. This ensures all logic is BigQuery-native and avoids external calls.

## 4. Manual Steps Before Go-Live

1.  **BigQuery Dataset Creation**:
    *   Ensure the BigQuery dataset `mydataset` (or your chosen dataset name) exists in your GCP project. If not, create it.
    *   `bq mk --dataset your_gcp_project_id:mydataset`

2.  **Schema/Dataset Creation (DDLs)**:
    *   Execute the DDL scripts for all tables in the `bigquery/ddl/` directory within your BigQuery `mydataset`.
        *   `bigquery/ddl/job_table.sql`
        *   `bigquery/ddl/error_log.sql`
        *   `bigquery/ddl/DWTK_MELDUNGEN.sql`
        *   `bigquery/ddl/CDS_TA_NOTICE.sql`
        *   `bigquery/ddl/SOF_TA_NOTICE.sql`
        *   `bigquery/ddl/VIA.sql`
    *   **Important**: Review the inferred schemas for `DWTK_MELDUNGEN`, `CDS_TA_NOTICE`, `SOF_TA_NOTICE`, and `VIA` and adjust them to precisely match the actual source system schemas and target requirements.

3.  **UDF and Stored Procedure Deployment**:
    *   Deploy the placeholder UDF `bigquery/udf/dwpa_util_skript_get_date_formatted.sql` (and any other necessary UDFs after full analysis of `DWPA_UTIL_SKRIPT`).
    *   Deploy the main stored procedure `bigquery/stored_procedures/r_ausd_vertrag_control.sql` to your BigQuery `mydataset`.

4.  **IAM/Permissions**:
    *   The service account used by Cloud Composer (or any other orchestrator) must have the following BigQuery roles:
        *   `BigQuery Data Editor` (to insert/update/delete data in `job_table`, `error_log`, `SOF_TA_NOTICE`, `VIA`).
        *   `BigQuery Data Viewer` (to read data from `DWTK_MELDUNGEN`, `CDS_TA_NOTICE`, `job_table`).
        *   `BigQuery Job User` (to run BigQuery jobs, including stored procedures).
    *   Ensure the service account has permissions to create/update UDFs and Stored Procedures if they are deployed dynamically.

5.  **Connection Strings/Secrets (Airflow)**:
    *   In Cloud Composer (Airflow), ensure the `google_cloud_default` connection is properly configured and has the necessary permissions to interact with BigQuery.
    *   If any sensitive parameters are passed to the Stored Procedure, consider using Airflow Secrets Backend (e.g., Google Secret Manager) for secure storage.

6.  **Data Ingestion**:
    *   Ingest historical data from the legacy `DWTK_MELDUNGEN` and `CDS_TA_NOTICE` into their respective BigQuery tables. This can be done via BigQuery Data Transfer Service, `bq load` commands, or other ETL tools.

7.  **Cloud Composer DAG Deployment**:
    *   Upload the `composer/dags/r_ausd_vertrag_dag.py` file to your Cloud Composer environment's DAGs folder.
    *   **Configuration**:
        *   Replace `'your_gcp_project_id'` with your actual Google Cloud Project ID in the DAG file.
        *   Adjust the `schedule` parameter in the DAG to match the required execution frequency (e.g., `schedule='0 0 * * *'` for daily).
        *   Review and adjust the `p_JobKennung` and `p_EintragsNr` parameters passed to the `BigQueryExecuteStoredProcedureOperator` to align with your job's requirements.

## 5. Known Gaps & Unresolved References

*   **Complexity of `d_ausd_v_ta_notice.sql`**: The actual content of `d_ausd_v_ta_notice.sql` was not fully analyzed by the migration tool. If it contains complex, highly procedural Oracle PL/SQL, or very specific Oracle functions, its migration might require significant manual effort and could be classified as `complex` or `redesign`. A detailed manual review and translation of this SQL file is crucial.
*   **Full Functionality of Utility Scripts**: The complete scope of `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, and `h_alis_sqlplus.ksh` needs to be thoroughly reviewed. While `h_alis_sqlplus.ksh` is entirely replaced, other utilities might have functions that require specific BigQuery UDFs or direct integration into the Stored Procedure.
*   **`PACKAGE:DWPA_UTIL_SKRIPT`**: The functions within this Oracle package need to be identified, analyzed, and re-implemented in BigQuery as SQL UDFs, JavaScript UDFs, or integrated directly into the Stored Procedure logic. The provided UDF is a placeholder.
*   **Upstream `r_ausd_vertrag.ksh`**: The migration of the `r_ausd_vertrag.ksh` script (or its upstream process) needs to be fully designed to call the new BigQuery Stored Procedure via the Cloud Composer DAG. This implies migrating the entire job workflow, not just this specific component.
*   **Job-specific Parameters and Configuration**: Any implicit configurations or environment variables sourced by `$HOME/.dw_init` in the legacy system need to be explicitly defined and managed in the BigQuery environment (e.g., as Cloud Composer variables, BigQuery project constants, or configuration tables).

## 6. Validation

To validate the migrated job, follow these steps:

1.  **Unit Test BigQuery Stored Procedure**:
    *   Manually execute the `mydataset.r_ausd_vertrag_control` stored procedure in the BigQuery console with various valid and invalid `p_JobKennung` and `p_EintragsNr` parameters.
    *   **Passing Criteria**:
        *   Successful execution for valid parameters.
        *   `RAISE` statements triggered for invalid parameters, with corresponding entries in `mydataset.error_log`.
        *   `mydataset.job_table` accurately reflects job `ACTIVE`, `DEACTIVATED`, `COMPLETED`, or `FAILED` statuses, start/end times, and record counts.
        *   Data in `mydataset.SOF_TA_NOTICE` and `mydataset.VIA` is transformed correctly according to the migrated SQL logic.

2.  **End-to-End Testing via Cloud Composer**:
    *   Trigger the `r_ausd_vertrag_control_dag` in Cloud Composer.
    *   **Passing Criteria**:
        *   The DAG runs successfully without Airflow task failures.
        *   The `execute_bq_sp` task completes successfully.
        *   Monitor BigQuery job history for the stored procedure execution.
        *   Verify `mydataset.job_table` and `mydataset.error_log` for correct entries.
        *   **Data Parity**: Compare the output data in `mydataset.SOF_TA_NOTICE` and `mydataset.VIA` with the output of the legacy `k_ausd_v_ta_notice.ksh` script for the same input parameters and source data. This is the most critical validation step. Ensure record counts, data values, and data types match.

## 7. Rollback Procedure

In case of issues or critical failures after go-live, the following rollback procedure can be initiated:

1.  **Deactivate New Orchestration**:
    *   Pause or delete the `r_ausd_vertrag_control_dag` in Cloud Composer to prevent further execution of the BigQuery Stored Procedure.

2.  **Revert Upstream Invocation**:
    *   Reconfigure the upstream process (e.g., `r_ausd_vertrag.ksh` or its equivalent) to resume invoking the legacy `k_ausd_v_ta_notice.ksh` script.

3.  **Data Rollback (if necessary)**:
    *   If the migrated job has written incorrect data to `mydataset.SOF_TA_NOTICE` or `mydataset.VIA`, use BigQuery's time travel feature to restore the tables to a state before the erroneous runs.
        *   `CREATE OR REPLACE TABLE mydataset.SOF_TA_NOTICE AS SELECT * FROM mydataset.SOF_TA_NOTICE FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL X MINUTE);` (Adjust `X` as needed).
    *   Alternatively, if a full backup was taken before go-live, restore from that backup.

4.  **Monitor Legacy System**:
    *   Verify that the legacy `k_ausd_v_ta_notice.ksh` script is running as expected and producing correct output.

5.  **Post-Rollback Analysis**:
    *   Analyze the root cause of the rollback. Review BigQuery job logs, `mydataset.error_log`, and Cloud Composer logs to identify and resolve the issues before attempting re-migration.