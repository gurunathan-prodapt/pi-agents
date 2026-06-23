# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `r_ausd_bp_ta_rn_vertrag.ksh`. This script served as an orchestration wrapper, responsible for parameter parsing, environment setup, logging, and invoking a core processing script (`k_ausd_bp_ta_rn_vertrag.ksh`) for the initial provisioning of selected base products for BERT.

The script has been re-platformed to Google Cloud Platform (GCP). The orchestration logic is now managed by a Python DAG in **Cloud Composer (Apache Airflow)**, which triggers a **BigQuery Stored Procedure**. The procedural logic of the original KornShell script, including parameter handling, defaulting, and job logging, has been translated into a BigQuery Stored Procedure. The invocation of the core processing script is also handled via a BigQuery Stored Procedure call. Logging is now persisted in dedicated BigQuery tables.

## 2. Generated artifacts

The migration process generated the following artifacts:

*   **`bigquery/ddl/job_log.sql`**
    *   **Role:** BigQuery Data Definition Language (DDL) script to create the `job_log` table. This table is used to record the execution history, status, and key parameters (like `stichtag` and `wiederanlaufwert`) for each run of the migrated job, replacing the file-based logging of the original script.
*   **`bigquery/ddl/job_error_log.sql`**
    *   **Role:** BigQuery DDL script to create the `job_error_log` table. This table captures detailed error messages, stack traces, and statement text when the BigQuery Stored Procedure encounters an error, replacing the `DWMSG_MeldeFehler` functionality.
*   **`bigquery/stored_procedures/ausd_bp_ta_rn_vertrag_wrapper.sql`**
    *   **Role:** A BigQuery Stored Procedure that directly replaces the `r_ausd_bp_ta_rn_vertrag.ksh` KornShell script. It handles input parameter validation, defaulting (`stichtag`, `wiederanlaufWert`), job logging (inserting into `job_log`), error handling (inserting into `job_error_log` and raising exceptions), and crucially, calls the `k_ausd_bp_ta_rn_vertrag` BigQuery Stored Procedure (the migrated core logic).
*   **`bigquery/stored_procedures/k_ausd_bp_ta_rn_vertrag.sql`**
    *   **Role:** A placeholder BigQuery Stored Procedure for the core processing logic originally contained in `k_ausd_bp_ta_rn_vertrag.ksh`. This procedure is invoked by `ausd_bp_ta_rn_vertrag_wrapper.sql`. Its actual implementation will be part of a separate migration effort for the core script.
*   **`airflow/dags/ausd_bp_ta_rn_vertrag_dag.py`**
    *   **Role:** A Python DAG definition for Apache Airflow (Cloud Composer). This DAG orchestrates the execution of the `ausd_bp_ta_rn_vertrag_wrapper` BigQuery Stored Procedure. It defines the workflow, handles parameter passing from Airflow to BigQuery, and incorporates Airflow's native logging, retries, and error handling mechanisms.

## 3. Key design decisions

The following key design decisions were made during this migration:

*   **Orchestration Re-platforming**: The KornShell script's role as an orchestrator has been fully transitioned to **Cloud Composer (Apache Airflow)**. This provides a robust, scalable, and cloud-native platform for workflow management, scheduling, monitoring, and error handling, replacing the custom shell-based orchestration.
*   **Logic Translation to BigQuery Stored Procedures**: The procedural logic of the original KornShell script (parameter parsing, defaulting, conditional execution, logging calls) has been directly translated into a **BigQuery Stored Procedure (`ausd_bp_ta_rn_vertrag_wrapper`)**. This keeps the core logic close to the data and leverages BigQuery's performance for SQL-based operations.
*   **Persistent Logging in BigQuery**: The file-based logging and status updates (`DWMSG_*` functions) of the original script are replaced by inserts and updates to dedicated **BigQuery tables (`job_log`, `job_error_log`)**. This centralizes logging, makes it queryable, and integrates with GCP's monitoring tools.
*   **Parameter Handling and Validation**: Command-line parameter parsing (`getopts`) is replaced by **Airflow DAG parameters** and **BigQuery Stored Procedure parameters**. Defaulting logic (e.g., for `stichtag` and `wiederanlaufWert`) is implemented using `IFNULL` checks within the BigQuery SP. Date format validation (`DDMMYYYY`) is also handled within the SP, with additional pre-checks in the Airflow DAG.
*   **Robust Error Handling**: The shell `trap` mechanism and custom error functions (`DWMSG_MeldeFehler`) are replaced by **BigQuery's `EXCEPTION WHEN ERROR` blocks and `RAISE` statements**, coupled with **Airflow's native retry mechanisms and `on_failure_callback`**. This provides structured error capture and reporting.
*   **Maintaining Wrapper Pattern**: The original script's role as a wrapper invoking a core script (`k_ausd_bp_ta_rn_vertrag.ksh`) is preserved. The `ausd_bp_ta_rn_vertrag_wrapper` BigQuery Stored Procedure explicitly `CALL`s the migrated (or placeholder) `k_ausd_bp_ta_rn_vertrag` BigQuery Stored Procedure, ensuring the architectural pattern remains consistent.
*   **Modular Utility Replacement**: Shared utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) are either absorbed directly into the wrapper SP (e.g., date formatting, parameter validation) or would be re-implemented as BigQuery UDFs/SPs if they were more complex and widely used.

## 4. Manual steps before go-live

Before the migrated job can be fully operational, the following manual steps must be performed:

1.  **GCP Project and Dataset Configuration**:
    *   Replace all occurrences of `gcp-project-id` with your actual GCP project ID.
    *   Replace all occurrences of `bq_dataset_name` with the name of your target BigQuery dataset.
    *   Ensure the specified BigQuery dataset exists in your GCP project.
2.  **BigQuery DDL Deployment**:
    *   Execute the `bigquery/ddl/job_log.sql` script in BigQuery to create the `job_log` table.
    *   Execute the `bigquery/ddl/job_error_log.sql` script in BigQuery to create the `job_error_log` table.
3.  **BigQuery Stored Procedure Deployment**:
    *   Execute the `bigquery/stored_procedures/k_ausd_bp_ta_rn_vertrag.sql` script in BigQuery to create the placeholder core stored procedure.
    *   Execute the `bigquery/stored_procedures/ausd_bp_ta_rn_vertrag_wrapper.sql` script in BigQuery to create the wrapper stored procedure.
4.  **Cloud Composer / Airflow Setup**:
    *   Ensure your Cloud Composer environment is running and accessible.
    *   Upload the `airflow/dags/ausd_bp_ta_rn_vertrag_dag.py` file to the DAGs folder of your Cloud Composer environment. Airflow will automatically parse and activate the DAG.
    *   Verify the `gcp_conn_id` (defaulting to `google_cloud_default`) in the DAG. If your environment uses a different connection ID for BigQuery, update it accordingly.
5.  **IAM Permissions**:
    *   Grant the service account associated with your Cloud Composer environment the necessary BigQuery permissions:
        *   `BigQuery Data Editor` or `BigQuery Data Owner` on the `bq_dataset_name` dataset (for inserting/updating `job_log` and `job_error_log`).
        *   `BigQuery Job User` (for running BigQuery queries and stored procedures).
6.  **Core Script Migration (Critical Dependency)**:
    *   The `bigquery/stored_procedures/k_ausd_bp_ta_rn_vertrag.sql` is currently a placeholder. The full migration of the original `k_ausd_bp_ta_rn_vertrag.ksh` script to its actual BigQuery Stored Procedure logic (or other appropriate GCP service) must be completed and deployed. The placeholder must be replaced with the final, functional code before the job can perform its intended data processing.

## 5. Known gaps & unresolved references

*   **Core Script (`k_ausd_bp_ta_rn_vertrag.ksh`) Logic**: The generated `k_ausd_bp_ta_rn_vertrag.sql` is a placeholder. The actual business logic and data transformations performed by the original `k_ausd_bp_ta_rn_vertrag.ksh` are not yet migrated. This is the most significant outstanding item and requires a separate, dedicated migration effort.
*   **Shared Utilities Scope**: The original script sourced several utility KornShell scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`). While their direct functionalities have been absorbed or replaced in the wrapper SP, a full understanding of their broader usage across other legacy jobs is needed. If these utilities are complex and widely shared, a more centralized BigQuery utility library or common Airflow helper functions might be warranted.
*   **`trap` Semantics**: The direct translation of shell `trap` (signal handling) to BigQuery SQL or Python in Airflow is not 1:1. While Airflow's retry mechanisms and error callbacks provide robust alternatives, any highly specific custom logic embedded within the original `trap` handlers (beyond basic error logging and exit) needs careful review and re-implementation if critical.
*   **`AL??` Comments**: The presence of `AL??` comments in the original KornShell script suggests potential alternative or deprecated logic. These should be clarified to ensure no critical business logic is missed or unnecessary code is ported during the full migration of the core script.
*   **`DWH$TA_C_VERTRAG` Table**: This table is identified as a dependency for the core processing script. Its migration to BigQuery (e.g., `project.dataset.DWH_TA_C_VERTRAG`) and availability to the `k_ausd_bp_ta_rn_vertrag` BigQuery Stored Procedure is a prerequisite for the full functionality of the migrated job.

## 6. Validation

To validate the successful migration and functionality of the `r_ausd_bp_ta_rn_vertrag.ksh` wrapper, follow these steps:

1.  **Deployment Verification**:
    *   Confirm that the `job_log` and `job_error_log` tables exist in the target BigQuery dataset.
    *   Confirm that the `ausd_bp_ta_rn_vertrag_wrapper` and `k_ausd_bp_ta_rn_vertrag` stored procedures exist in the target BigQuery dataset.
    *   Verify that the `ausd_bp_ta_rn_vertrag_dag` is visible and active in the Airflow UI.

2.  **Test Execution Scenarios**:
    *   **Successful Run (Default Parameters)**:
        *   Trigger the `ausd_bp_ta_rn_vertrag_dag` from the Airflow UI without providing any custom parameters. This should use the default `stichtag` (today's date) and `wiederanlaufwert` ('0').
    *   **Successful Run (Custom Parameters)**:
        *   Trigger the DAG with valid custom parameters, e.g., `stichtag = '01012023'` and `wiederanlaufwert = '100'`.
    *   **Invalid `stichtag` Format**:
        *   Trigger the DAG with an invalid `stichtag` format, e.g., `stichtag = '2023-01-01'` or `stichtag = '01/01/2023'`.
    *   **Invalid `stichtag` Value**:
        *   Trigger the DAG with an unparseable `stichtag` value, e.g., `stichtag = '32012024'` (invalid day).
    *   **Invalid `wiederanlaufwert`**:
        *   Trigger the DAG with a non-numeric `wiederanlaufwert`, e.g., `wiederanlaufwert = 'abc'`.

3.  **"Passing" Criteria**:

    *   **For Successful Runs (Scenarios 1 & 2)**:
        *   The Airflow DAG run should complete with a `success` status.
        *   The `execute_ausd_bp_ta_rn_vertrag_wrapper` task in Airflow should succeed.
        *   A new entry should be present in the `gcp-project-id.bq_dataset_name.job_log` table with:
            *   `jobkennung = 'ausd_bp_ta_rn_vertrag_wrapper'`
            *   `status = 'COMPLETED'`
            *   Correct `stichtag` and `wiederanlaufwert` values.
        *   The `gcp-project-id.bq_dataset_name.job_error_log` table should *not* contain any new entries related to this job run.
        *   BigQuery job logs for the `ausd_bp_ta_rn_vertrag_wrapper` SP should show the successful invocation of `k_ausd_bp_ta_rn_vertrag` and its placeholder message.
    *   **For Failed Runs (Scenarios 3, 4 & 5)**:
        *   The Airflow DAG run should complete with a `failed` status.
        *   The `execute_ausd_bp_ta_rn_vertrag_wrapper` task in Airflow should fail.
        *   A new entry should be present in the `gcp-project-id.bq_dataset_name.job_log` table with:
            *   `jobkennung = 'ausd_bp_ta_rn_vertrag_wrapper'`
            *   `status = 'FAILED'`
        *   A corresponding entry should be present in the `gcp-project-id.bq_dataset_name.job_error_log` table, containing the specific error message (e.g., "Invalid p_stichtag format", "unable to parse DDMMYYYY date", "Invalid p_wiederanlaufWert").
        *   Airflow task logs for the failed task should clearly indicate the error message from the BigQuery Stored Procedure.

## 7. Rollback procedure

In case of issues or a decision to revert the migration, follow these steps to roll back to the original legacy system:

1.  **Deactivate Airflow DAG**:
    *   In the Airflow UI, set the `ausd_bp_ta_rn_vertrag_dag` to "Off" or delete the DAG file from the Composer DAGs folder. This prevents any further execution of the migrated job.
2.  **Remove BigQuery Stored Procedures**:
    *   Execute the following SQL commands in BigQuery to drop the migrated stored procedures:
        ```sql
        DROP PROCEDURE IF EXISTS `gcp-project-id.bq_dataset_name.ausd_bp_ta_rn_vertrag_wrapper`;
        DROP PROCEDURE IF EXISTS `gcp-project-id.bq_dataset_name.k_ausd_bp_ta_rn_vertrag`;
        ```
3.  **Remove BigQuery Logging Tables (Optional but Recommended for Clean-up)**:
    *   If no other processes rely on these tables, drop the logging tables:
        ```sql
        DROP TABLE IF EXISTS `gcp-project-id.bq_dataset_name.job_log`;
        DROP TABLE IF EXISTS `gcp-project-id.bq_dataset_name.job_error_log`;
        ```
    *   **Note**: If these tables are intended to be shared or retain historical data, this step should be skipped.
4.  **Revert to Legacy Execution**:
    *   Ensure that the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_vertrag.ksh` script is re-enabled and scheduled in its original environment.
5.  **Monitor Legacy System**:
    *   Closely monitor the execution of the original KornShell script to ensure it functions as expected after the rollback.

**Impact of Rollback**:
The rollback procedure primarily affects the orchestration and logging mechanisms. Since the `k_ausd_bp_ta_rn_vertrag.sql` is a placeholder, its rollback has no data impact. If the core script had been fully migrated and performed data transformations, a more comprehensive data rollback strategy would be required. For this wrapper, the main concern is ensuring the legacy orchestration can resume without interruption.