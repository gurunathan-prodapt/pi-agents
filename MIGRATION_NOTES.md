# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `r_ausd_bp_ta_iccid_vertrag.ksh`. This script, originally an orchestrator for a core data preparation job, has been replatformed to Google Cloud Platform.

The original `r_ausd_bp_ta_iccid_vertrag.ksh` script was responsible for parsing command-line arguments (`Stichtag` and `Wiederanlaufwert`), setting up the environment, handling errors, logging job status, and invoking a core data preparation script (`k_ausd_bp_ta_iccid_vertrag.ksh`). Its primary business purpose was the initial provisioning of selected base products for the BERT system by extracting contract cache data from the Data Warehouse (DWH) for demand scoring.

The target platform for this migration is Google Cloud Platform, leveraging **BigQuery** for data processing and orchestration logic, and **Cloud Composer (Airflow)** for external scheduling. The orchestrator logic of `r_ausd_bp_ta_iccid_vertrag.ksh` has been translated into a **BigQuery Stored Procedure**, which will manage parameter handling, validation, audit logging, and the invocation of the migrated core data preparation logic (also expected to be a BigQuery Stored Procedure).

## 2. Generated artifacts

The migration process generated the following files:

*   **`sql/ddl/job_audit_ddl.sql`**
    *   **Role**: This SQL script defines the Data Definition Language (DDL) for the `job_audit` table in BigQuery. This table serves as the central logging mechanism for job executions, capturing start, success, and error statuses, along with relevant metadata like `job_nr`, `job_kennung`, `stichtag`, and `messages`. It replaces the filesystem-based logging and `DWMSG_*` functions of the original KornShell script.

*   **`sql/stored_procedures/ausd_bp_ta_iccid_vertrag_wrapper.sql`**
    *   **Role**: This BigQuery SQL script creates or replaces the `ausd_bp_ta_iccid_vertrag_wrapper` stored procedure. This procedure is the direct migration of the `r_ausd_bp_ta_iccid_vertrag.ksh` orchestrator. It handles input parameters (`p_stichtag`, `p_wiederanlaufWert`), applies default values, performs validation, records audit entries in the `job_audit` table, and orchestrates the call to the core data preparation logic (`k_ausd_bp_ta_iccid_vertrag` BigQuery Stored Procedure). It also includes robust error handling using BigQuery's `BEGIN...EXCEPTION` blocks.

*   **`sql/stored_procedures/k_ausd_bp_ta_iccid_vertrag.sql`**
    *   **Role**: This BigQuery SQL script provides a placeholder/stub for the `k_ausd_bp_ta_iccid_vertrag` stored procedure. This procedure is intended to encapsulate the core data transformation and provisioning logic that was originally present in `k_ausd_bp_ta_iccid_vertrag.ksh`. Its actual implementation is a prerequisite and must be developed separately based on the analysis of the original core script. The wrapper procedure calls this stub.

*   **`dags/ausd_bp_ta_iccid_vertrag_orchestrator.py`**
    *   **Role**: This Python script defines an Apache Airflow DAG (Directed Acyclic Graph). It is designed to orchestrate the execution of the `ausd_bp_ta_iccid_vertrag_wrapper` BigQuery Stored Procedure. The DAG handles scheduling, passes dynamic parameters (e.g., `Stichtag` derived from the Airflow execution date), and provides a robust external scheduling mechanism for the migrated job.

## 3. Key design decisions

*   **Replatforming Orchestrator to BigQuery Stored Procedure**: The core decision was to translate the KornShell orchestrator script directly into a BigQuery Stored Procedure. This leverages BigQuery's native scripting capabilities for parameter handling, control flow (`IF`, `ASSERT`, `BEGIN...EXCEPTION`), and direct invocation of other BigQuery Stored Procedures.
    *   **Trade-off**: This moves the orchestration logic closer to the data, reducing cross-platform communication overhead. However, it means losing direct access to the operating system environment and filesystem for logging, which is mitigated by structured logging to a BigQuery audit table.

*   **Centralized Audit Logging in BigQuery**: Instead of disparate log files and custom shell functions (`DWMSG_*`), a dedicated `job_audit` table in BigQuery is used for all job status and logging.
    *   **Trade-off**: This provides structured, queryable logs within BigQuery, simplifying monitoring and analysis. It requires a schema definition and `INSERT` statements instead of simple `echo` or file redirection.

*   **Airflow for External Orchestration**: While the wrapper logic is in BigQuery, an Airflow DAG is used for external scheduling and parameter management.
    *   **Trade-off**: This introduces an additional component (Airflow) but provides enterprise-grade scheduling, dependency management, and monitoring capabilities, which are superior to simple cron jobs or internal BigQuery scheduling for complex workflows.

*   **Assumption of Core Logic Migration**: The design explicitly assumes that the core business logic script (`k_ausd_bp_ta_iccid_vertrag.ksh`) will also be migrated to a BigQuery Stored Procedure with a compatible interface.
    *   **Trade-off**: This simplifies the wrapper's design but creates a critical dependency. The wrapper cannot function correctly until the core logic is also migrated and deployed.

*   **Parameter Handling and Validation**: Shell `getopts` and custom validation functions are replaced by BigQuery Stored Procedure input parameters, `IFNULL` for defaulting, and `ASSERT` statements for validation.
    *   **Trade-off**: This integrates parameter handling directly into the SQL context, making it more robust and less prone to shell-specific parsing issues.

*   **Error Handling**: Shell `trap` mechanisms are replaced by BigQuery's `BEGIN...EXCEPTION` blocks, which provide structured error trapping and allow for consistent error logging to the `job_audit` table.
    *   **Trade-off**: This provides a more modern and robust error handling paradigm within the SQL environment.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Ensure the target BigQuery dataset (`your_bigquery_dataset` in the generated code, e.g., `isbert_dwh_migrated`) exists within your GCP project (`your-gcp-project-id`). If not, create it:
        ```bash
        bq mk --dataset --default_table_expiration 365 your-gcp-project-id:your_bigquery_dataset
        ```

2.  **`job_audit` Table Deployment**:
    *   Execute the `sql/ddl/job_audit_ddl.sql` script in BigQuery to create the `job_audit` table.
        ```bash
        bq query --use_legacy_sql=false < sql/ddl/job_audit_ddl.sql
        ```

3.  **Core Logic Stored Procedure Deployment (`k_ausd_bp_ta_iccid_vertrag`)**:
    *   **Crucial Step**: The actual business logic for `k_ausd_bp_ta_iccid_vertrag.ksh` must be migrated to a BigQuery Stored Procedure. The generated `sql/stored_procedures/k_ausd_bp_ta_iccid_vertrag.sql` is a stub. Replace the placeholder logic with the actual transformations and deploy it to BigQuery.
        ```bash
        bq query --use_legacy_sql=false < sql/stored_procedures/k_ausd_bp_ta_iccid_vertrag.sql
        ```

4.  **Wrapper Stored Procedure Deployment (`ausd_bp_ta_iccid_vertrag_wrapper`)**:
    *   Execute the `sql/stored_procedures/ausd_bp_ta_iccid_vertrag_wrapper.sql` script in BigQuery to deploy the wrapper procedure.
        ```bash
        bq query --use_legacy_sql=false < sql/stored_procedures/ausd_bp_ta_iccid_vertrag_wrapper.sql
        ```

5.  **IAM/Permissions**:
    *   The service account used by Cloud Composer (Airflow) must have the necessary BigQuery permissions:
        *   `BigQuery Data Editor` on the target dataset (`your_bigquery_dataset`) to create/update tables and execute stored procedures.
        *   `BigQuery Job User` to run BigQuery jobs.
    *   Ensure the service account has permissions to read/write to any source/target tables involved in the `k_ausd_bp_ta_iccid_vertrag` procedure.

6.  **Airflow Connection**:
    *   Ensure the `google_cloud_default` connection is properly configured in your Airflow environment. This connection typically uses the service account associated with the Cloud Composer environment.

7.  **Airflow DAG Deployment**:
    *   Update the `PROJECT_ID` and `DATASET_ID` variables in `dags/ausd_bp_ta_iccid_vertrag_orchestrator.py` to match your GCP project and BigQuery dataset.
    *   Upload the `dags/ausd_bp_ta_iccid_vertrag_orchestrator.py` file to your Cloud Composer environment's DAGs folder. Airflow will automatically detect and schedule it.

## 5. Known gaps & unresolved references

*   **Core Script (`k_ausd_bp_ta_iccid_vertrag.ksh`) Migration**: The most significant gap is the actual implementation of the `k_ausd_bp_ta_iccid_vertrag` BigQuery Stored Procedure. The current generated code provides only a stub. A detailed analysis and migration of the original `k_ausd_bp_ta_iccid_vertrag.ksh` script's logic is essential and must be completed for the job to function correctly. This is a critical dependency.
*   **Utility Script Translation**: The specific functions within the original shell utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) have been absorbed or replaced by BigQuery native functions (e.g., `FORMAT_DATE`, `ASSERT`). While the core functionality is covered, a detailed review of all edge cases handled by the original utilities might be necessary if any subtle behaviors are critical.
*   **Job Number Generation Robustness**: The current `job_nr` generation (`MAX(job_nr) + 1`) in the `ausd_bp_ta_iccid_vertrag_wrapper` procedure is susceptible to race conditions if multiple instances of the job are started concurrently. For a production system with high concurrency, a more robust mechanism (e.g., a BigQuery sequence table, a dedicated job ID service, or relying on `GENERATE_UUID()` for a unique identifier) should be considered.
*   **Missing Complexity Tier Data**: The absence of a complexity tier for the original script means the migration effort was estimated without an automated complexity assessment. This could lead to underestimation if the script had hidden complexities not immediately apparent from its orchestrator role.
*   **Environment Variables**: The original script sourced `$HOME/.dw_init`. Any critical environment variables or configurations previously set by this file must now be managed either within the BigQuery Stored Procedure directly, as parameters passed from Airflow, or through BigQuery project/dataset configurations.

## 6. Validation

To validate the migrated job, follow these steps:

1.  **Manual BigQuery Stored Procedure Execution (Wrapper)**:
    *   Execute the `ausd_bp_ta_iccid_vertrag_wrapper` stored procedure directly in BigQuery.
    *   **Test Case 1: Default Parameters**:
        ```sql
        CALL `your-gcp-project-id.your_bigquery_dataset.ausd_bp_ta_iccid_vertrag_wrapper`(NULL, NULL);
        ```
        *   **Passing Criteria**:
            *   The procedure completes without error.
            *   An entry with `status = 'STARTED'` and `status = 'OK'` is found in `your-gcp-project-id.your_bigquery_dataset.job_audit`.
            *   The `stichtag` in the audit log should default to the current date in `DDMMYYYY` format.
            *   The `wiederanlaufWert` in the audit log should default to `0`.
            *   The `k_ausd_bp_ta_iccid_vertrag` stub procedure should show its debug message in the BigQuery job logs, indicating it was called with the correct parameters.
    *   **Test Case 2: Explicit Parameters**:
        ```sql
        CALL `your-gcp-project-id.your_bigquery_dataset.ausd_bp_ta_iccid_vertrag_wrapper`('01012023', 10);
        ```
        *   **Passing Criteria**:
            *   The procedure completes without error.
            *   Audit log entries show `stichtag = '01012023'` and `wiederanlaufWert = 10`.
            *   The `k_ausd_bp_ta_iccid_vertrag` stub procedure should show its debug message with the explicit parameters.
    *   **Test Case 3: Invalid `Stichtag` (Validation)**:
        ```sql
        CALL `your-gcp-project-id.your_bigquery_dataset.ausd_bp_ta_iccid_vertrag_wrapper`('20230101', NULL); -- Incorrect format
        ```
        *   **Passing Criteria**:
            *   The procedure fails with an `ASSERT` error message indicating invalid `Stichtag` format.
            *   An entry with `status = 'ERROR'` is found in `job_audit`, reflecting the validation failure.
    *   **Test Case 4: Simulate Core Logic Failure**:
        *   Temporarily uncomment the `RAISE USING MESSAGE = 'Simulating an error...'` line in `sql/stored_procedures/k_ausd_bp_ta_iccid_vertrag.sql` and redeploy the stub.
        *   Run Test Case 1 or 2 again.
        *   **Passing Criteria**:
            *   The `ausd_bp_ta_iccid_vertrag_wrapper` procedure fails.
            *   An entry with `status = 'ERROR'` is found in `job_audit`, and the `message` field contains the simulated error from the core procedure.

2.  **Airflow DAG Execution**:
    *   Trigger the `ausd_bp_ta_iccid_vertrag_orchestrator` DAG in your Cloud Composer environment (either manually or wait for its scheduled run).
    *   **Passing Criteria**:
        *   The Airflow task `call_ausd_bp_ta_iccid_vertrag_wrapper` completes successfully (green in Airflow UI).
        *   Check the Airflow task logs for successful BigQuery job execution.
        *   Verify that `job_audit` table contains `STARTED` and `OK` entries for the job, with `stichtag` derived from the Airflow execution date (e.g., `ds_nodash` formatted to `DDMMYYYY`).

## 7. Rollback procedure

In case of issues or critical failures with the migrated job, the following rollback procedure can be executed:

1.  **Disable Airflow DAG**:
    *   In the Cloud Composer (Airflow) UI, disable the `ausd_bp_ta_iccid_vertrag_orchestrator` DAG to prevent further executions of the migrated job.

2.  **Remove BigQuery Stored Procedures**:
    *   Drop the migrated BigQuery Stored Procedures:
        ```sql
        DROP PROCEDURE IF EXISTS `your-gcp-project-id.your_bigquery_dataset.ausd_bp_ta_iccid_vertrag_wrapper`;
        DROP PROCEDURE IF EXISTS `your-gcp-project-id.your_bigquery_dataset.k_ausd_bp_ta_iccid_vertrag`;
        ```
    *   (Optional) If the `job_audit` table was created solely for this migration and is not used by other processes, it can also be dropped:
        ```sql
        DROP TABLE IF EXISTS `your-gcp-project-id.your_bigquery_dataset.job_audit`;
        ```

3.  **Re-enable Original Job**:
    *   Re-enable or restart the original `r_ausd_bp_ta_iccid_vertrag.ksh` KornShell script in its legacy environment. Ensure its original scheduling mechanism (e.g., cron job) is reactivated.
    *   Verify that the original job runs successfully and produces expected outputs in the legacy environment.