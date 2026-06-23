# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `vobs/dw_source/isrpt/isbert/install_save/k_ausd_bp_ta_tarifoption.ksh`. The original script served as a control mechanism for a data processing job related to 'PoolBasisprodukt', handling parameter validation, environment setup, and orchestrating the execution of a core SQL script (`d_ausd_bp_ta_tarifoption.sql`).

The job has been migrated to Google Cloud Platform's BigQuery, leveraging a BigQuery Stored Procedure for its core logic and orchestration, and Cloud Composer (Apache Airflow) for scheduling and execution management. The migration aims to preserve the business logic and data processing intent while transitioning to a cloud-native, scalable, and managed environment.

## 2. Generated Artifacts

The migration process generated the following files:

*   **`job_audit_table.sql`**
    *   **Role:** This SQL script defines the Data Definition Language (DDL) for the `job_audit_table` in BigQuery. This table is designed to capture execution details, status, and metrics (like records processed) for the migrated job, replacing the temporary file-based record count and the commented-out `FOSJobErzeugeEintrag` calls from the original KornShell script.
*   **`target_table_ddl.sql`**
    *   **Role:** This SQL script provides the DDL for the BigQuery target data tables, specifically `sof_ta_tarifoption`, which will store the final processed output. It also includes DDL for assumed source/intermediate tables like `sof_ta_bpr_opt_filter`, `dwtk_meldungen`, `sof_ta_l_bpr_optionen_filter`, and `sof_ta_bpr_opt_text`, which are necessary for the core logic to function. These DDLs serve as a blueprint for the required BigQuery schema.
*   **`k_ausd_bp_ta_tarifoption.sql`**
    *   **Role:** This is the core BigQuery Stored Procedure. It encapsulates the entire logic of the original KornShell script, including parameter parsing and validation, date derivation, and the translated business logic from `d_ausd_bp_ta_tarifoption.sql`. It performs data transformations and loads the results into `sof_ta_tarifoption`, and logs its execution status to `job_audit_table`.
*   **`k_ausd_bp_ta_tarifoption_dag.py`**
    *   **Role:** This Python script defines an Apache Airflow Directed Acyclic Graph (DAG) for Cloud Composer. Its purpose is to orchestrate the execution of the `k_ausd_bp_ta_tarifoption` BigQuery Stored Procedure. It handles the scheduling and parameter passing to the stored procedure, replacing the manual execution or legacy scheduler of the original KornShell script.

## 3. Key Design Decisions

The following key design decisions were made during the migration:

*   **KornShell Orchestration to BigQuery Stored Procedure:** The entire control flow, parameter handling, and sequential execution logic of the KornShell script were translated into a single BigQuery Stored Procedure. This centralizes the job's logic within BigQuery, leveraging its native capabilities for SQL execution, error handling (`RAISE`), and variable management, eliminating the need for external shell environments.
*   **Embedding Core SQL Logic:** The business logic originally residing in `d_ausd_bp_ta_tarifoption.sql` (assumed Oracle SQL) was directly translated and embedded within the BigQuery Stored Procedure. This avoids external SQL files and simplifies deployment and execution within BigQuery.
*   **Native BigQuery Functions for Utilities:** Shell script utilities for date manipulation (`h_alis_date.ksh`, `gestern.ksh`) and parameter validation (`pruefeParameterGesetzt`) were replaced with native BigQuery SQL functions (e.g., `CURRENT_DATE()`, `DATE_SUB()`, `SAFE.PARSE_DATE()`, `IF/RAISE` statements). This reduces external dependencies and improves performance.
*   **BigQuery Audit Table for Logging:** The temporary file-based record count and the commented-out `FOSJobErzeugeEintrag` calls were replaced by direct `INSERT` statements into a dedicated BigQuery `job_audit_table`. This provides a structured, queryable, and centralized logging mechanism for job executions.
*   **Cloud Composer (Airflow) for Scheduling:** Apache Airflow, via Cloud Composer, was chosen as the orchestration tool. This provides robust scheduling, monitoring, and dependency management capabilities, replacing any legacy scheduler that might have triggered the original KornShell script.
*   **Handling of Dynamic Table Naming:** The original script's reference to `sof$ta_bpr_opt_text_&v_datum` (implying a date-suffixed table) was addressed by assuming a unified `sof_ta_bpr_opt_text` table or view in BigQuery. If the original intent was to process data from a specific historical snapshot, this BigQuery table should be partitioned by date or a view should be created to filter a base table based on the `v_datum_suffix` derived from `dwtk_meldungen`. The current implementation derives `v_datum_suffix` but assumes `sof_ta_bpr_opt_text` is a single, non-dynamic table for the core logic.
*   **Idempotency:** The stored procedure includes a `TRUNCATE TABLE` statement before inserting data into `sof_ta_tarifoption`. This ensures that each run of the job starts with a clean slate for the target table, making the job idempotent and simplifying restarts or re-runs.
*   **Structured Error Handling:** BigQuery's `EXCEPTION WHEN ERROR THEN` block is used to catch and log errors to the `job_audit_table`, providing clear error messages and status for failed runs.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps are required:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (`your_project.your_dataset`) exists. If not, create it in your GCP project.
2.  **Schema and Table Creation:**
    *   Execute `job_audit_table.sql` to create the `job_audit_table`.
    *   Execute `target_table_ddl.sql` to create the `sof_ta_tarifoption` table and any other necessary source/intermediate tables (e.g., `sof_ta_bpr_opt_filter`, `dwtk_meldungen`, `sof_ta_l_bpr_optionen_filter`, `sof_ta_bpr_opt_text`).
    *   **Crucially, populate the source tables** (`dwtk_meldungen`, `sof_ta_l_bpr_optionen_filter`, `sof_ta_bpr_opt_text`) with the necessary data, either through a one-time migration or ongoing replication from the legacy system.
3.  **BigQuery Stored Procedure Deployment:**
    *   Execute `k_ausd_bp_ta_tarifoption.sql` to create the BigQuery Stored Procedure in your target dataset.
4.  **IAM and Permissions Configuration:**
    *   **Service Account for BigQuery:** Ensure the service account used by Cloud Composer (or any other orchestrator) has the necessary BigQuery roles (e.g., `BigQuery Data Editor`, `BigQuery Job User`) to create/truncate/insert into tables and execute stored procedures in `your_project.your_dataset`.
    *   **Service Account for Cloud Composer:** If using Cloud Composer, ensure the Composer environment's service account has permissions to deploy DAGs and interact with BigQuery.
5.  **Cloud Composer/Airflow Setup:**
    *   Deploy `k_ausd_bp_ta_tarifoption_dag.py` to your Cloud Composer environment's DAGs folder.
    *   **Update Placeholders:** In `k_ausd_bp_ta_tarifoption_dag.py`, replace `"your_project"` and `"your_dataset"` with your actual GCP Project ID and BigQuery Dataset ID.
    *   **Airflow Connection:** Verify that the `google_cloud_default` Airflow connection is correctly configured and has the necessary permissions for BigQuery access.
    *   **Scheduling:** Configure the `schedule` parameter in the DAG definition (`schedule=None`) to match the desired execution frequency of the original job.
    *   **Parameterization:** Review the `parameters` section in the DAG. For production, consider using Airflow Variables, XComs, or dynamic values based on the DAG run context (e.g., `ds_nodash` for `as_of_date_str`) instead of hardcoded examples.
6.  **Secrets Management (if applicable):**
    *   If any sensitive information (e.g., API keys, external system credentials) were involved in the original `d_ausd_bp_ta_tarifoption.sql` or related processes, ensure they are securely managed in GCP Secret Manager and accessed appropriately by the BigQuery Stored Procedure or Airflow.

## 5. Known Gaps & Unresolved References

The following items were identified as known gaps or unresolved references during the migration:

*   **Content of `d_ausd_bp_ta_tarifoption.sql`:** The migration design and generated code assume a specific structure and logic for `d_ausd_bp_ta_tarifoption.sql` (Oracle SQL for aggregating tariff options). The actual complexity, use of proprietary functions, or advanced features within this original SQL file were not fully analyzed in this document. A thorough review of the original SQL is critical to ensure the BigQuery translation is accurate and complete.
*   **Purpose of Commented-out File Processing:** The original KornShell script contained commented-out sections for `sed`, `sort`, and `join` operations on files like `cibasis_data24.dat`. The current migration does not include these operations. If these operations become active or are required for future processing, their precise logic, input/output formats, and business purpose must be fully understood and translated into BigQuery SQL transformations.
*   **FOS Job Management Integration:** The original script had commented-out calls to `FOSJobErzeugeEintrag` and `FOSJobDeaktivate`, indicating integration with a legacy job management system. While `job_audit_table` replaces the logging aspect, any "deactivation" or external control mechanisms of the FOS system are not replicated. If the FOS system actively managed job states or dependencies, this integration needs to be re-evaluated and potentially integrated with Cloud Composer's monitoring or external systems.
*   **Error Reporting (`DWMSG_MeldeFehler`):** The exact functionality and integration points of the `DWMSG_MeldeFehler` utility were not fully detailed. The BigQuery Stored Procedure uses `RAISE` statements and logs errors to `job_audit_table`. If `DWMSG_MeldeFehler` triggered broader alerting or incident management, this integration needs to be re-established using GCP's alerting services (e.g., Cloud Monitoring, Cloud Logging) or Airflow's notification mechanisms.
*   **Security Context and Credentials:** The original script's reliance on `sqlplus` implies a specific security context and credential management. The migration assumes BigQuery access via service accounts and IAM roles. A comprehensive security review is needed to ensure all access patterns are secure and compliant.

## 6. Validation

To validate the successful migration and functionality of the `k_ausd_bp_ta_tarifoption` job, follow these steps:

1.  **Trigger the Job:**
    *   **Via Airflow:** Unpause the `k_ausd_bp_ta_tarifoption_dag` in your Cloud Composer UI and trigger a manual run.
    *   **Manually (for testing):** You can also manually execute the BigQuery Stored Procedure directly from the BigQuery console for isolated testing:
        ```sql
        CALL `your_project.your_dataset.k_ausd_bp_ta_tarifoption`(
            'BERT_TA_TARIFOPTION', -- job_kennung
            '1',                   -- entry_nr
            '01012023',            -- as_of_date_str (example: DDMMYYYY)
            0                      -- restart_val
        );
        ```
        Adjust parameters as needed for your test case.

2.  **Monitor Execution:**
    *   **Airflow UI:** Observe the DAG run in the Airflow UI. Ensure all tasks complete successfully (green status). Check task logs for any errors or warnings.
    *   **BigQuery Jobs History:** In the BigQuery console, navigate to "SQL workspace" -> "Query history" to see the execution status of the stored procedure.

3.  **Verify Audit Log:**
    *   Query the `your_project.your_dataset.job_audit_table`:
        ```sql
        SELECT *
        FROM `your_project.your_dataset.job_audit_table`
        WHERE job_identifier = 'BERT_TA_TARIFOPTION'
        ORDER BY start_timestamp DESC
        LIMIT 1;
        ```
    *   **Passing Criteria:**
        *   `status` column should be 'SUCCESS'.
        *   `records_processed` should reflect the expected number of records processed by the job.
        *   `start_timestamp` and `end_timestamp` should be populated.
        *   If `status` is 'FAILED', the `error_message` column should contain relevant details.

4.  **Validate Target Data:**
    *   Query the target table `your_project.your_dataset.sof_ta_tarifoption`:
        ```sql
        SELECT *
        FROM `your_project.your_dataset.sof_ta_tarifoption`
        LIMIT 100; -- Or more specific queries
        ```
    *   **Passing Criteria:**
        *   The table should contain data.
        *   The `COUNT(*)` of records in `sof_ta_tarifoption` should match the `records_processed` value in the `job_audit_table`.
        *   Perform data quality checks:
            *   Spot-check a sample of records to ensure `cntrct_id`, `business_option`, `sonstige_option`, and `gprs_option` are correctly populated and formatted as expected.
            *   Compare the output data with a sample from the legacy system (if available) for the same as-of date.
            *   Verify that `business_option`, `sonstige_option`, and `gprs_option` columns are truncated to 500 characters as per the `SAFE_SUBSTR` logic.

## 7. Rollback Procedure

In case of issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Deactivate New Job:**
    *   **Airflow:** Pause the `k_ausd_bp_ta_tarifoption_dag` in the Cloud Composer UI to prevent further executions of the BigQuery Stored Procedure.
    *   **BigQuery (Optional):** If necessary, you can drop or rename the `k_ausd_bp_ta_tarifoption` stored procedure to ensure it cannot be accidentally called.
        ```sql
        DROP PROCEDURE IF EXISTS `your_project.your_dataset.k_ausd_bp_ta_tarifoption`;
        ```

2.  **Revert to Legacy System:**
    *   Re-enable or re-configure the original scheduling mechanism for `vobs/dw_source/isrpt/isbert/install_save/k_ausd_bp_ta_tarifoption.ksh` on the legacy platform.
    *   Ensure all necessary environment variables, source files, and database connections for the legacy script are correctly configured and operational.

3.  **Data Rollback (Conditional):**
    *   The BigQuery Stored Procedure uses `TRUNCATE TABLE` before inserting data into `sof_ta_tarifoption`. If the legacy system also processes data into a similar target, and the new job has run successfully, the data in `sof_ta_tarifoption` might need to be reverted or restored from a backup if the legacy system needs to take over processing for the same period.
    *   If the `sof_ta_tarifoption` table is exclusively populated by the new BigQuery job, and the legacy system writes to a different target, data rollback might not be strictly necessary for the legacy system to resume. However, any downstream systems relying on `sof_ta_tarifoption` would need to be informed or redirected.
    *   Review the `job_audit_table` for the last successful run of the BigQuery job to understand the state of processed data.

4.  **Investigation and Remediation:**
    *   Analyze the `job_audit_table` and Airflow logs for the failed BigQuery job runs to identify the root cause of the issue.
    *   Address the identified problems in the BigQuery Stored Procedure, DDL, or Airflow DAG.
    *   Once the issues are resolved, re-deploy the corrected artifacts and re-initiate the validation process.