# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the legacy KornShell script `k_ausd_bp_ta_p_basisprod.ksh` to Google BigQuery. The original script served as an orchestration wrapper, handling parameter parsing, date validation, and executing a core Oracle SQL script (`d_ausd_bp_ta_p_basisprod.sql`) to process the `PoolBasisprodukt` dataset. It also included logic for capturing record counts and preparing for job logging.

The migration targets Google BigQuery, where the orchestration logic is now encapsulated within a BigQuery Stored Procedure. The core data transformation logic, originally in Oracle SQL, has been translated to BigQuery SQL and is executed by this stored procedure. Parameter handling, date validation, error management, and record count capture are all implemented using BigQuery's native scripting capabilities. Job logging is directed to a dedicated BigQuery audit table, and orchestration is managed via an Apache Airflow DAG (Cloud Composer).

## 2. Generated artifacts

The migration process generated the following artifacts:

*   **`bigquery/sp_k_ausd_bp_ta_p_basisprod.sql`**
    *   **Role:** This is the primary BigQuery Stored Procedure that replaces the original KornShell script. It handles input parameter validation, date derivation, error handling, and orchestrates the execution of the core data transformation logic. It also logs job status and record counts to the `job_audit_table`.
*   **`bigquery/d_ausd_bp_ta_p_basisprod_bq.sql`**
    *   **Role:** This file contains the *original* Oracle SQL content of `d_ausd_bp_ta_p_basisprod.sql`. It is provided as a reference for the manual translation effort required to convert the core business logic into BigQuery SQL. The `sp_k_ausd_bp_ta_p_basisprod.sql` contains a placeholder `EXECUTE IMMEDIATE` block where the *translated* BigQuery SQL version of this script should reside.
*   **`bigquery/ddl_target_tables.sql`**
    *   **Role:** Contains the Data Definition Language (DDL) for the primary target table, `sof_ta_p_basisprod`, in BigQuery SQL format. This table is where the transformed data is ultimately stored. The schema was inferred from the `INSERT` statement in the original SQL.
*   **`bigquery/ddl_audit_table.sql`**
    *   **Role:** Contains the DDL for the `job_audit_table`. This table is used by the BigQuery Stored Procedure to log the start time, end time, status, parameters, and record counts for each execution of the job, replacing the legacy temporary file and job management calls.
*   **`airflow/dags/k_ausd_bp_ta_p_basisprod_dag.py`**
    *   **Role:** An Apache Airflow DAG (for Google Cloud Composer) that orchestrates the execution of the `sp_k_ausd_bp_ta_p_basisprod` BigQuery Stored Procedure. It defines the task to call the stored procedure and can be configured for scheduling and dynamic parameter passing.

## 3. Key design decisions

*   **BigQuery Stored Procedure for Orchestration:** The KornShell script's role as an orchestration layer (parameter parsing, validation, date handling, SQL execution) is fully replicated by a BigQuery Stored Procedure. This keeps the entire logic within the BigQuery environment, leveraging its native scripting capabilities for control flow, error handling (`RAISE`, `EXCEPTION WHEN ERROR`), and variable management (`DECLARE`, `SET`). This approach minimizes external dependencies and simplifies deployment within GCP.
*   **Direct SQL Execution:** Instead of using an external SQL client like SQL*Plus, the core data transformation logic (translated from `d_ausd_bp_ta_p_basisprod.sql`) is executed directly within the BigQuery Stored Procedure, potentially using `EXECUTE IMMEDIATE` for flexibility or direct DML statements. This eliminates the need for an external wrapper and streamlines the execution.
*   **Native BigQuery Functions for Utilities:** Legacy shell utility scripts (e.g., `h_alis_date.ksh`, `gestern.ksh`) are replaced by BigQuery's rich set of built-in functions for date manipulation (`CURRENT_DATE()`, `DATE_SUB`, `PARSE_DATE`) and string operations (`REGEXP_CONTAINS`).
*   **Dedicated Audit Table for Logging:** The temporary file (`tmpFile`) used for record counts and the commented-out legacy job management calls are replaced by a structured `job_audit_table` in BigQuery. This centralizes logging, provides better traceability, and integrates seamlessly with BigQuery's analytics capabilities.
*   **Cloud Composer (Airflow) for Scheduling:** The scheduling and monitoring aspects of the job are delegated to Apache Airflow (via Google Cloud Composer). This provides a robust, scalable, and observable orchestration platform, replacing any legacy cron jobs or job schedulers.
*   **Handling of Commented-Out File Operations:** The commented-out `sed`, `sort`, and `join` operations from the original script are noted as potential future requirements. The design decision is to re-engineer these using BigQuery SQL if activated, likely involving data staging in Cloud Storage and subsequent BigQuery DML/DDL operations, rather than attempting to replicate file-based shell commands.
*   **`EXECUTE IMMEDIATE` for Core SQL:** The core SQL logic from `d_ausd_bp_ta_p_basisprod.sql` is intended to be embedded within an `EXECUTE IMMEDIATE` block in the stored procedure. This allows for dynamic SQL construction if needed (though not explicitly required by the current analysis) and cleanly separates the core DML from the orchestration logic within the stored procedure.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (`your_bigquery_dataset` in the generated code, e.g., `isrpt_prod`) exists within your GCP project (`your-gcp-project-id`).
    *   `bq mk --dataset your-gcp-project-id:your_bigquery_dataset`
2.  **Deploy DDL for Target and Audit Tables:**
    *   Execute the DDL scripts:
        *   `bigquery/ddl_target_tables.sql` (for `sof_ta_p_basisprod`)
        *   `bigquery/ddl_audit_table.sql` (for `job_audit_table`)
    *   These can be run via the BigQuery UI, `bq query`, or a deployment pipeline.
3.  **Translate and Integrate Core SQL Logic:**
    *   **Crucial Step:** Manually translate the Oracle SQL content from `bigquery/d_ausd_bp_ta_p_basisprod_bq.sql` into BigQuery-compliant SQL. This involves:
        *   Replacing Oracle-specific functions (e.g., `DECODE`, `NVL`, `TO_CHAR` with specific formats) with their BigQuery equivalents (`CASE WHEN`, `COALESCE`, `FORMAT_DATE`, `PARSE_DATE`).
        *   Adjusting table and column names if necessary (e.g., schema prefixes).
        *   Removing Oracle hints (`/*+ APPEND */`, `/*+ ORDERED ... */`).
        *   Ensuring `LEFT JOIN` syntax is correctly applied for `(+)` outer joins.
    *   Once translated, replace the placeholder `EXECUTE IMMEDIATE` block within `bigquery/sp_k_ausd_bp_ta_p_basisprod.sql` with the BigQuery-compliant DML.
4.  **Deploy BigQuery Stored Procedure:**
    *   Deploy the finalized `bigquery/sp_k_ausd_bp_ta_p_basisprod.sql` to your BigQuery dataset.
    *   `bq query --use_legacy_sql=false < bigquery/sp_k_ausd_bp_ta_p_basisprod.sql`
5.  **Ensure Source Table Existence and Schema:**
    *   Verify that all source tables referenced in the core SQL (e.g., `sof_ta_cntrct_dist`, `sof_ta_bcp_iccid`, `sof_ta_bcp_msisdn`, etc.) exist in your BigQuery environment and have compatible schemas. If these are also migrated, their DDLs should be deployed.
6.  **IAM Permissions:**
    *   The service account used by Cloud Composer (or any other orchestrator/user executing the SP) must have:
        *   `BigQuery Data Editor` role on the target dataset (`your_bigquery_dataset`) to create/write to tables.
        *   `BigQuery Job User` role to run BigQuery jobs.
        *   `BigQuery Data Viewer` role on any source datasets.
7.  **Cloud Composer (Airflow) Configuration:**
    *   Deploy the `airflow/dags/k_ausd_bp_ta_p_basisprod_dag.py` to your Cloud Composer environment's DAGs folder.
    *   **Update Placeholders:** Replace `your-gcp-project-id` and `your_bigquery_dataset` with actual values in the DAG file.
    *   **Parameter Configuration:** Configure the `p_JobKennung`, `p_EintragsNr`, and `p_wiederanlaufWert` parameters in the Airflow DAG. These should either be hardcoded, pulled from Airflow Variables, or derived dynamically based on your scheduling requirements.
    *   **Scheduling:** Set the `schedule_interval` in the DAG to match the desired execution frequency (e.g., `@daily`).

## 5. Known gaps & unresolved references

*   **Core SQL Translation (B4 Item):** The most significant gap is the manual translation of `d_ausd_bp_ta_p_basisprod.sql` from Oracle SQL to BigQuery SQL. The generated `d_ausd_bp_ta_p_basisprod_bq.sql` is merely the *original* content, and the `EXECUTE IMMEDIATE` block in the stored procedure is a placeholder. This requires careful review by someone familiar with the original logic and BigQuery SQL.
*   **Source Table DDLs:** The DDLs for the *source* tables (e.g., `sof_ta_cntrct_dist`, `sof_ta_bcp_iccid`, etc.) are not generated. It is assumed these tables already exist in BigQuery or will be migrated separately. Their schemas must be compatible with the translated SQL.
*   **Commented-Out File Processing:** The original script contained commented-out `sed`, `sort`, and `join` operations on `cibasis_data*.dat` files. If these operations are ever activated, they represent a significant redesign effort (B4 item) to translate file-based processing into BigQuery-native operations (e.g., Cloud Storage ingestion, BigQuery DML/DDL).
*   **Dynamic Parameter Values in Airflow:** The `p_JobKennung`, `p_EintragsNr`, and `p_wiederanlaufWert` parameters in the Airflow DAG are currently placeholders (`'DEFAULT_JOB_KENNUNG'`, etc.). These need to be configured to pass appropriate dynamic or static values at runtime.
*   **Legacy Environment Initialization (`. $HOME/.dw_init`):** The original script sourced a global initialization script. While BigQuery procedures don't have a direct equivalent, any global constants or configurations previously set by this script should be either hardcoded in the BigQuery SP, passed as parameters, or managed via a BigQuery configuration table.
*   **Oracle-Specific Features:** Any subtle Oracle-specific features or behaviors in the original `d_ausd_bp_ta_p_basisprod.sql` that are not immediately apparent from the syntax (e.g., specific optimizer hints, PL/SQL blocks, custom functions) might require additional attention during translation.

## 6. Validation

Validation ensures the migrated job performs identically to or better than the legacy system.

**How to Run Tests:**

1.  **Manual Stored Procedure Execution:**
    *   Open the BigQuery UI or use the `bq query` command-line tool.
    *   Call the stored procedure with test parameters:
        ```sql
        CALL `your-gcp-project-id.your_bigquery_dataset.sp_k_ausd_bp_ta_p_basisprod`(
          p_JobKennung => 'TEST_JOB',
          p_EintragsNr => 'TEST_ENTRY_001',
          p_Stichtag => '01012023', -- Use a specific test date
          p_wiederanlaufWert => 'N'
        );
        ```
    *   Monitor the job execution in the BigQuery UI.
2.  **Airflow DAG Trigger:**
    *   In the Cloud Composer UI (Airflow UI), navigate to the `k_ausd_bp_ta_p_basisprod_job` DAG.
    *   Manually trigger the DAG.
    *   Monitor the DAG run and task logs in the Airflow UI.
3.  **Data Comparison:**
    *   Run the legacy `k_ausd_bp_ta_p_basisprod.ksh` script for a specific `p_Stichtag` on a test environment.
    *   Run the migrated BigQuery job for the *same* `p_Stichtag` and source data.
    *   Extract the output from both the legacy system (e.g., `sof$ta_p_basisprod` in Oracle) and the BigQuery target table (`sof_ta_p_basisprod`).
    *   Perform a row-by-row and column-by-column comparison of the resulting datasets. Tools like `diff` or data comparison scripts can be used.

**What "Passing" Means:**

*   **Successful Execution:**
    *   The BigQuery Stored Procedure completes without raising any errors.
    *   The Airflow DAG run completes successfully, and all tasks are marked green.
    *   The `job_audit_table` shows an entry for the execution with `status = 'COMPLETED'` and a non-zero `records_processed` count (if expected).
*   **Data Integrity:**
    *   The `sof_ta_p_basisprod` table in BigQuery contains the expected number of records.
    *   The data in `sof_ta_p_basisprod` exactly matches the output of the legacy Oracle job for the same input parameters and source data. This includes:
        *   All columns are present and have the correct data types.
        *   All values match, accounting for any minor differences in floating-point precision or date/timestamp representation between Oracle and BigQuery.
*   **Performance:**
    *   The BigQuery job completes within acceptable timeframes, ideally matching or exceeding the performance of the legacy script.
*   **Error Handling:**
    *   Test cases with invalid parameters (e.g., missing `p_Stichtag`, incorrect date format) should correctly trigger the `RAISE` statements in the stored procedure, and the `job_audit_table` should log `status = 'FAILED'` with an informative `message`.

## 7. Rollback procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Stop New Job Execution:**
    *   **Airflow:** Pause the `k_ausd_bp_ta_p_basisprod_job` DAG in the Cloud Composer (Airflow) UI to prevent any further executions of the migrated job.
    *   **Manual:** Ensure no manual calls to the BigQuery Stored Procedure are made.
2.  **Revert to Legacy Script:**
    *   Re-enable the original `k_ausd_bp_ta_p_basisprod.ksh` script in the legacy scheduling system (e.g., cron, Control-M).
    *   Verify that the legacy script can execute successfully and produce the expected output.
3.  **Data Rollback (if necessary):**
    *   **Impact:** The migrated BigQuery job performs a `TRUNCATE TABLE` before `INSERT`. If the rollback is due to data corruption or incorrect data, the `sof_ta_p_basisprod` table in BigQuery will contain the problematic data.
    *   **Option A (Point-in-Time Restore):** If BigQuery table snapshots or point-in-time recovery are enabled, restore `sof_ta_p_basisprod` to a state before the problematic run.
    *   **Option B (Reload from Source):** If the source data is immutable or can be re-processed, truncate `sof_ta_p_basisprod` and reload it with data processed by the *legacy* script, or a known good state.
    *   **Option C (No Data Rollback):** If the issue is purely operational (e.g., performance, logging) and the data itself is correct or can be overwritten by the next successful legacy run, no specific data rollback might be needed for the target table.
4.  **Monitor Legacy System:**
    *   Closely monitor the legacy job's execution and output to ensure it is functioning correctly after the rollback.
5.  **Analyze and Rectify:**
    *   Investigate the root cause of the issue that necessitated the rollback. This may involve reviewing BigQuery job logs, Airflow task logs, and comparing data outputs.
    *   Address the identified issues in the BigQuery Stored Procedure, the translated SQL, or the Airflow DAG.
    *   Once rectified, re-validate the fix in a test environment before attempting another go-live.