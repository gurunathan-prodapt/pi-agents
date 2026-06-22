# MIGRATION_NOTES.md for DW.BERT_AUSD_BP_TA_TARIFOPTION

## 1. Summary

The legacy ETL job `DW.BERT_AUSD_BP_TA_TARIFOPTION` has been migrated from its original UC4 orchestration, KornShell scripting, and Oracle SQL data processing environment to Google Cloud Platform. The new solution leverages Apache Airflow on Cloud Composer for orchestration, and BigQuery for all data storage and transformation logic. The job's core function, which involves the preparation and provisioning of selected base products and tariff options for the BERT system, has been preserved.

## 2. Generated Artifacts

The migration process has resulted in the following key artifacts:

*   **`dw_bert_ausd_bp_ta_tarifoption.py`** (Airflow DAG):
    *   **Role**: Orchestrates the entire job workflow on Cloud Composer. It serves as the entry point, replacing the UC4 job definition. It will trigger the main BigQuery stored procedure.
*   **`procedures/sp_bereitstellung_basisprodukte_bert.sql`** (BigQuery Stored Procedure):
    *   **Role**: Translates the logic of `r_ausd_bp_ta_tarifoption.ksh`. This is the top-level BigQuery procedure called by the Airflow DAG. It handles job-level parameters, logging, and orchestrates the call to the next processing step. (Note: This procedure was designed but not explicitly generated in the provided code snippet, though its existence is implied by the design).
*   **`procedures/sp_k_ausd_bp_ta_tarifoption.sql`** (BigQuery Stored Procedure):
    *   **Role**: Translates the logic of `k_ausd_bp_ta_tarifoption.ksh`. This procedure performs parameter validation, date derivations, and executes the core BigQuery SQL transformation logic, replacing the shell script's control flow.
*   **`sql/d_ausd_bp_ta_tarifoption_bq.sql`** (BigQuery SQL Script - embedded):
    *   **Role**: This is the BigQuery-adapted version of `d_ausd_bp_ta_tarifoption.sql`. It performs the main data transformation, reading from source tables and writing to target tables. Its logic is embedded within `sp_k_ausd_bp_ta_tarifoption.sql` for execution.
*   **`udfs/concat_functions.sql`** (BigQuery User-Defined Functions - UDFs):
    *   **Role**: Re-implementation of the custom Oracle `sof$ab_con.concatX` functions as BigQuery UDFs (e.g., `project.dataset.concat1`, `project.dataset.concat1r`, etc.) to handle specific string concatenation logic. (Note: These UDFs are referenced in the generated code but their definitions are not provided).
*   **BigQuery Tables**:
    *   **`project.dataset.dwtk_meldungen`**: Target for `isbert_schema.dwtk_meldungen`.
    *   **`project.dataset.sof_ta_l_bpr_optionen_filter`**: Target for `isbert_schema.sof$ta_l_bpr_optionen_filter`.
    *   **`project.dataset.sof_ta_bpr_opt_text_YYYYMMDD`**: Target for `sof$ta_bpr_opt_text_&v_datum` (dynamically named).
    *   **`project.dataset.sof_ta_bpr_opt_filter`**: Intermediate table, target for `sof$ta_bpr_opt_filter`.
    *   **`project.dataset.sof_ta_tarifoption`**: Final output table, target for `sof$ta_tarifoption`.
    *   **`project.dataset.job_audit_log`**: Custom logging table for job execution status and messages.

## 3. Key Design Decisions

*   **Orchestration with Airflow**: UC4's scheduling and job control have been replaced by an Airflow DAG. This provides a cloud-native, scalable, and observable orchestration layer.
*   **BigQuery for All Data Operations**: Oracle database and SQL*Plus scripts are entirely replaced by BigQuery for data storage, transformation, and execution. This centralizes data processing on a serverless, high-performance platform.
*   **KornShell to BigQuery Stored Procedures**: The complex control flow and parameter handling logic from `r_ausd_bp_ta_tarifoption.ksh` and `k_ausd_bp_ta_tarifoption.ksh` have been translated into BigQuery Stored Procedures. This keeps the logic within the data platform, reducing cross-platform dependencies and improving maintainability.
*   **Custom Oracle Functions to BigQuery UDFs**: Oracle-specific custom concatenation functions (`sof$ab_con.concatX`) are re-implemented as BigQuery User-Defined Functions (UDFs). This ensures functional equivalence and allows the core SQL logic to remain within BigQuery.
*   **Dynamic SQL for Dynamic Table Names**: The original Oracle script used a dynamically named table (`sof$ta_bpr_opt_text_&v_datum`). This has been replicated in BigQuery using `EXECUTE IMMEDIATE` with string formatting to construct the table name based on the derived date.
*   **Standardized Logging**: Custom shell script logging (`DWMSG_...`) has been replaced by `INSERT` statements into a dedicated BigQuery `job_audit_log` table, providing a centralized and queryable audit trail.
*   **Deterministic `LEAD` Function**: The `LEAD` analytic function, which lacked an `ORDER BY` clause in the original Oracle script, has been adapted to include a deterministic `ORDER BY cntrct_id, pds_description` to ensure consistent results in BigQuery.
*   **Error Handling**: BigQuery's `EXCEPTION WHEN ERROR` blocks are used within stored procedures to catch and log errors, providing robust error management.

## 4. Manual Steps Before Go-Live

The following manual steps are required to prepare the environment for the migrated job:

1.  **BigQuery Dataset Creation**:
    *   Ensure the target BigQuery dataset (`project.dataset`) exists in the GCP project.
2.  **Source Table Creation**:
    *   Create the necessary source tables in BigQuery with appropriate schemas and data types:
        *   `project.dataset.dwtk_meldungen`
        *   `project.dataset.sof_ta_l_bpr_optionen_filter`
        *   `project.dataset.sof_ta_bpr_opt_text_YYYYMMDD` (create a representative table, as the actual table name will be dynamic).
    *   Ensure these tables are populated with historical or initial data as required.
3.  **Audit Log Table Creation**:
    *   Create the `project.dataset.job_audit_log` table with columns like `log_time`, `job_kennung`, `entry_nr`, `message_type`, `message`, `stichtag`, `status`.
4.  **UDF Deployment**:
    *   Deploy the BigQuery UDFs for the `sof$ab_con.concatX` functions (e.g., `project.dataset.concat1`, `project.dataset.concat1r`, etc.) to the `project.dataset` dataset. The exact logic for these UDFs must be derived from the original Oracle functions.
5.  **Stored Procedure Deployment**:
    *   Deploy `procedures/sp_k_ausd_bp_ta_tarifoption.sql` and `procedures/sp_bereitstellung_basisprodukte_bert.sql` (once developed) to the `project.dataset` dataset.
6.  **IAM & Permissions**:
    *   Grant the Cloud Composer service account (or the service account running the Airflow DAG) appropriate BigQuery roles (e.g., `BigQuery Data Editor`, `BigQuery Job User`) to read from source tables, write to target tables, and execute stored procedures.
    *   Ensure necessary permissions for Cloud Logging if additional logging is configured.
7.  **Airflow DAG Deployment**:
    *   Upload the `dw_bert_ausd_bp_ta_tarifoption.py` DAG file to the DAGs folder of the Cloud Composer environment.
8.  **Scheduling**:
    *   Define the `schedule_interval` for the `dw_bert_ausd_bp_ta_tarifoption` Airflow DAG based on business requirements, as the original UC4 schedule was not provided.
9.  **Connection Strings/Secrets**:
    *   If the Airflow DAG requires any external connections or secrets (e.g., for notifications), ensure they are configured in Airflow Connections or Google Secret Manager.

## 5. Known Gaps & Unresolved References

*   **Custom Oracle `concatX` UDF Logic**: While the generated code uses placeholder UDFs, the precise implementation logic for `project.dataset.concat1`, `project.dataset.concat1r`, `project.dataset.concat2`, `project.dataset.concat2r`, `project.dataset.concat3`, `project.dataset.concat3r` needs to be fully derived from the original Oracle `sof$ab_con.concatX` functions and deployed. This is a critical item for data integrity.
*   **`sp_bereitstellung_basisprodukte_bert` Procedure**: The design document outlines a top-level BigQuery stored procedure (`project.dataset.sp_bereitstellung_basisprodukte_bert`) to encapsulate `r_ausd_bp_ta_tarifoption.ksh`. This procedure was not included in the generated code and needs to be developed and deployed.
*   **Airflow Schedule**: The exact `schedule_interval` for the Airflow DAG is still to be determined based on business requirements.
*   **Oracle `trace.sql.cfg`**: The original `d_ausd_bp_ta_tarifoption.sql` included `start ../trace.sql.cfg`. If this file contained critical logging or tracing functionality, its equivalent needs to be identified and replicated in BigQuery (e.g., via additional `job_audit_log` entries or Cloud Logging).
*   **`FOSJobDeaktivate` and `FOSJobErzeugeEintrag`**: The commented-out calls in the original ksh scripts (`AL??`) suggest potential job management functionality. It needs to be confirmed if these are truly deprecated or if their equivalent (e.g., job status updates in a central metadata store) needs to be implemented.
*   **BigQuery Cost Management & Performance**: While BigQuery is performant, large data volumes or complex queries might require performance tuning (e.g., partitioning, clustering, or optimizing SQL) to manage costs and execution times effectively.
*   **Data Volume Considerations**: The design assumes data volumes are manageable. For very large tables, partitioning and clustering strategies should be considered for the new BigQuery tables to optimize query performance and cost.

## 6. Validation

Validation of the migrated job involves several stages:

1.  **Unit Testing (BigQuery Stored Procedures & UDFs)**:
    *   Execute `sp_k_ausd_bp_ta_tarifoption` directly in BigQuery with various input parameters (valid, invalid dates, missing parameters) to ensure correct logic, error handling, and date derivations.
    *   Test each custom `concatX` UDF with sample data to verify their output matches the original Oracle function behavior.
    *   Verify that the `job_audit_log` table is populated correctly with `INFO` and `ERROR` messages.
    *   **Passing Criteria**: Stored procedures execute without unhandled errors, UDFs produce expected output, and audit logs are accurate.

2.  **Integration Testing (BigQuery SQL Flow)**:
    *   Execute the full BigQuery SQL logic (by calling `sp_k_ausd_bp_ta_tarifoption`) with representative source data.
    *   Verify that the intermediate table (`project.dataset.sof_ta_bpr_opt_filter`) and the final target table (`project.dataset.sof_ta_tarifoption`) are populated correctly.
    *   **Passing Criteria**: Data in target tables matches expected results based on the original Oracle job's output for the same input, considering the `LEAD` function's deterministic ordering.

3.  **End-to-End Testing (Airflow DAG)**:
    *   Trigger the `dw_bert_ausd_bp_ta_tarifoption` Airflow DAG manually in Cloud Composer.
    *   Monitor the DAG run for successful completion, checking Airflow logs for any errors.
    *   Verify that the BigQuery stored procedures are invoked correctly and complete successfully.
    *   **Passing Criteria**:
        *   The Airflow DAG run completes successfully (green status).
        *   The `job_audit_log` table shows a `SUCCESS` entry for the job run.
        *   The `project.dataset.sof_ta_tarifoption` table contains the expected data, matching the output of the legacy job for the same input parameters.
        *   Data comparison tools or queries should confirm row counts and key data points are identical or functionally equivalent to the legacy system's output.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Deactivate New Job**:
    *   Pause or delete the `dw_bert_ausd_bp_ta_tarifoption` Airflow DAG in Cloud Composer to prevent further execution of the migrated job.
2.  **Re-enable Legacy Job**:
    *   Re-enable the original `DW.BERT_AUSD_BP_TA_TARIFOPTION` job in UC4.
3.  **Data Restoration (if necessary)**:
    *   If the migrated job has overwritten or corrupted any critical data in BigQuery that cannot be easily corrected, restore the affected BigQuery tables (`project.dataset.sof_ta_bpr_opt_filter`, `project.dataset.sof_ta_tarifoption`) from a previous backup or snapshot.
    *   Alternatively, if the target tables are purely generated and can be safely truncated, clear them to avoid stale data.
4.  **Verify Legacy Job Functionality**:
    *   Run the legacy UC4 job and verify that it executes successfully and produces the expected output.
5.  **Cleanup (Optional)**:
    *   Once the legacy system is confirmed to be fully operational, the deployed BigQuery stored procedures, UDFs, and potentially the Airflow DAG can be removed or disabled until the issues are resolved.
6.  **Root Cause Analysis**:
    *   Investigate the reason for the rollback, analyze logs from Airflow and BigQuery, and address the identified issues before attempting re-deployment.