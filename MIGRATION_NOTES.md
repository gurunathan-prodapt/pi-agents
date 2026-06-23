# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh` and its dependent SQL logic (`d_ausd_bp_ta_bpr_apn.sql`). The original script served as an orchestrator for a data processing step, handling parameter validation, date calculations, and the execution of a core SQL script.

The migration targets Google Cloud Platform (GCP), specifically:
*   **BigQuery:** For hosting the core data processing logic (migrated from `d_ausd_bp_ta_bpr_apn.sql`) and the orchestration logic (migrated from `k_ausd_bp_ta_bpr_apn.ksh`) within a BigQuery Stored Procedure. BigQuery tables will also store all source, intermediate, and target data, including `PoolBasisprodukt`, `error_log`, and `job_log`.
*   **Cloud Composer (Airflow):** For scheduling and orchestrating the execution of the BigQuery Stored Procedure.

The migration aims to replicate the original script's functionality, including parameter handling, validation, SQL execution orchestration, record counting, and logging, using cloud-native GCP services.

## 2. Generated Artifacts

The following artifacts have been generated as part of this migration:

*   **`your_gcp_project/your_bigquery_dataset/ddl_error_log.sql`**
    *   **Role:** Defines the Data Definition Language (DDL) for the `error_log` table in BigQuery. This table is used to capture and store detailed error messages and context when the BigQuery Stored Procedure encounters validation or execution failures.
*   **`your_gcp_project/your_bigquery_dataset/ddl_job_log.sql`**
    *   **Role:** Defines the DDL for the `job_log` table in BigQuery. This table records the execution status, parameters, and key metrics (like record count) for each run of the BigQuery Stored Procedure, replacing the original script's intended (but commented out) logging mechanisms.
*   **`your_gcp_project/your_bigquery_dataset/ddl_poolbasisprodukt.sql`**
    *   **Role:** Provides a placeholder DDL for the `PoolBasisprodukt` table in BigQuery. This table is the primary target or source for the core business logic. **Note:** This DDL is an example and must be replaced with the actual schema derived from the source system's `PoolBasisprodukt` table.
*   **`your_gcp_project/your_bigquery_dataset/r_ausd_bp_ta_bpr_apn.sql`**
    *   **Role:** This is the core BigQuery Stored Procedure. It encapsulates the migrated orchestration logic from `k_ausd_bp_ta_bpr_apn.ksh`, including parameter parsing, validation, date calculations, and error handling. It also contains a placeholder for the migrated business logic from `d_ausd_bp_ta_bpr_apn.sql`, which it executes dynamically.
*   **`airflow/dags/k_ausd_bp_ta_bpr_apn_dag.py`**
    *   **Role:** An Apache Airflow DAG definition for Google Cloud Composer. This Python script is responsible for scheduling and triggering the `r_ausd_bp_ta_bpr_apn` BigQuery Stored Procedure, passing the necessary parameters. It replaces the original script's cron-based or manual scheduling.

## 3. Key Design Decisions

The migration approach was guided by the following key design decisions:

*   **Consolidation into BigQuery Stored Procedure:** The orchestration logic (from `k_ausd_bp_ta_bpr_apn.ksh`) and the core business logic (from `d_ausd_bp_ta_bpr_apn.sql`) are combined into a single BigQuery Stored Procedure.
    *   **Why:** This approach leverages BigQuery's native capabilities for data processing and procedural logic, eliminating the need for external shell scripts to manage SQL execution. It centralizes the logic, simplifies deployment, and benefits from BigQuery's performance and scalability.
    *   **Trade-offs:** Requires a complete rewrite of the shell script's procedural logic into BigQuery SQL scripting and a full conversion of the `d_ausd_bp_ta_bpr_apn.sql` dialect. Debugging shifts from shell-level to BigQuery execution logs.
*   **Cloud Composer for Scheduling:** Google Cloud Composer (Airflow) was chosen for job orchestration.
    *   **Why:** Airflow provides robust scheduling, dependency management, monitoring, and retry mechanisms, which are superior to traditional cron jobs or custom shell-based schedulers. It integrates seamlessly with BigQuery.
    *   **Trade-offs:** Introduces a new technology stack (Airflow/Python) and requires managing an Airflow environment.
*   **Dedicated Logging Tables:** `error_log` and `job_log` tables were created in BigQuery.
    *   **Why:** Provides a structured, queryable, and centralized mechanism for tracking job execution status, errors, and key metrics, replacing disparate log files and commented-out logging logic in the original script.
    *   **Trade-offs:** Requires DDL creation and `INSERT` statements within the stored procedure, adding minor overhead.
*   **Dynamic SQL Execution (`EXECUTE IMMEDIATE`):** The core business logic from `d_ausd_bp_ta_bpr_apn.sql` is executed via `EXECUTE IMMEDIATE` within the BigQuery Stored Procedure.
    *   **Why:** Allows for flexible parameter substitution into the SQL logic, mirroring the original script's dynamic parameter passing to `sqlplus`. It also enables the core SQL to be developed and potentially maintained somewhat independently.
    *   **Trade-offs:** Increases the complexity of the stored procedure and requires careful handling of SQL injection risks (though parameters are controlled internally).

## 4. Manual Steps Before Go-Live

Before the migrated solution can go live, the following manual steps must be completed:

1.  **GCP Project and BigQuery Dataset Setup:**
    *   Ensure `your_gcp_project` is created and configured.
    *   Create the BigQuery dataset `your_bigquery_dataset` within your project.
2.  **BigQuery Table Creation:**
    *   Execute the DDL scripts:
        *   `your_gcp_project/your_bigquery_dataset/ddl_error_log.sql`
        *   `your_gcp_project/your_bigquery_dataset/ddl_job_log.sql`
    *   **Crucially, define and execute the correct DDL for `PoolBasisprodukt`** based on its source system schema. The provided `ddl_poolbasisprodukt.sql` is a placeholder and must be updated.
    *   Ensure all source tables referenced by the migrated `d_ausd_bp_ta_bpr_apn.sql` logic also exist in BigQuery with their correct schemas and data.
3.  **Data Migration:**
    *   Migrate all historical and current data from the source `PoolBasisprodukt` table and any other input tables to their respective BigQuery counterparts.
4.  **Complete `d_ausd_bp_ta_bpr_apn.sql` Migration:**
    *   **Replace the placeholder SQL within `your_gcp_project/your_bigquery_dataset/r_ausd_bp_ta_bpr_apn.sql`** with the fully converted and tested BigQuery Standard SQL logic from the original `d_ausd_bp_ta_bpr_apn.sql`. This is the most significant manual step. Ensure all Oracle-specific functions are converted and dynamic parameter handling is correct.
5.  **Deploy BigQuery Stored Procedure:**
    *   Execute the final `your_gcp_project/your_bigquery_dataset/r_ausd_bp_ta_bpr_apn.sql` script to create or replace the stored procedure in BigQuery.
6.  **IAM & Permissions:**
    *   **BigQuery Service Account:** Create or identify a GCP service account that has the necessary BigQuery permissions (e.g., `BigQuery Data Editor`, `BigQuery Job User`) to read from source tables, write to `PoolBasisprodukt`, `error_log`, and `job_log`, and execute stored procedures.
    *   **Cloud Composer Service Account:** Ensure the Cloud Composer environment's service account has the `BigQuery Data Editor` and `BigQuery Job User` roles (or equivalent custom roles) to invoke the BigQuery Stored Procedure.
7.  **Cloud Composer Environment Setup:**
    *   Ensure a Cloud Composer environment is provisioned and running.
    *   Configure an Airflow GCP connection named `google_cloud_default` in your Airflow environment, ensuring it uses the appropriate service account for BigQuery access.
8.  **Deploy Airflow DAG:**
    *   Upload the `airflow/dags/k_ausd_bp_ta_bpr_apn_dag.py` file to the DAGs folder of your Cloud Composer environment.
    *   **Review and adjust the `schedule_interval`** in the DAG to match the desired execution frequency.
    *   **Replace `your_gcp_project` and `your_bigquery_dataset`** placeholders in the DAG file with actual values.
    *   **Review and adjust DAG parameters** (`job_kennung`, `eintrags_nr`, `stichtag`, `wiederanlauf_wert`) to ensure they align with operational requirements. The `stichtag` macro `{{ yesterday_ds_nodash }}` provides the date in `YYYYMMDD` format, which needs to be converted to `DDMMYYYY` if the stored procedure expects it. The current stored procedure expects `DDMMYYYY`, so the DAG should be updated to `{{ ds_nodash[4:6] }}{{ ds_nodash[6:8] }}{{ ds_nodash[0:4] }}` for yesterday's date in `DDMMYYYY` format.
9.  **Secrets Management (if applicable):**
    *   If the original `d_ausd_bp_ta_bpr_apn.sql` or any other part of the process relied on sensitive credentials, these must be securely managed using GCP Secret Manager and accessed appropriately within the BigQuery Stored Procedure or Airflow DAG.

## 5. Known Gaps & Unresolved References

The following items are known gaps or unresolved references that require further attention:

*   **`d_ausd_bp_ta_bpr_apn.sql` Full Migration:** The most critical gap is the complete and verified migration of the `d_ausd_bp_ta_bpr_apn.sql` content to BigQuery Standard SQL. The provided `r_ausd_bp_ta_bpr_apn.sql` contains a placeholder. The exact SQL dialect (assumed Oracle) and any proprietary functions or complex PL/SQL constructs need careful conversion and testing.
*   **`PoolBasisprodukt` Schema Definition:** The DDL for `PoolBasisprodukt` is a generic placeholder. Its actual schema must be accurately defined in BigQuery based on the source system's structure.
*   **Dynamic SQL Complexity:** The original script's dynamic parameter substitution into `d_ausd_bp_ta_bpr_apn.sql` needs to be fully understood and accurately replicated within the `EXECUTE IMMEDIATE` statement in the BigQuery Stored Procedure.
*   **Commented-Out Logic:** The `sed`, `sort`, `join` operations that were commented out in the original KornShell script are not migrated. If these functionalities become active requirements in the future, they will need to be translated into BigQuery SQL transformations.
*   **FOS Job Management Replacement:** The original script referenced `FOSJobDeaktivate` and `FOSJobErzeugeEintrag`. While `job_log` table provides basic logging, a full replacement for the "FOS Job Management" system's capabilities (e.g., job deactivation, complex status updates) might require further integration with GCP services or custom solutions.
*   **Custom Error Code Mapping:** The original script used custom error codes (e.g., `ErrNr=193`, `ErrNr=192`). While the BigQuery Stored Procedure includes logic to map some errors, a comprehensive mapping of all potential original error codes to BigQuery error handling or custom `error_log` entries should be verified.
*   **`your_gcp_project` and `your_bigquery_dataset` Placeholders:** These generic names must be replaced with the actual GCP Project ID and BigQuery Dataset ID during deployment.
*   **Airflow `stichtag` Macro:** The `stichtag` parameter in the Airflow DAG uses `yesterday_ds_nodash` which produces `YYYYMMDD`. The BigQuery Stored Procedure expects `DDMMYYYY`. The DAG needs to be updated to format the date correctly (e.g., `{{ ds_nodash[4:6] }}{{ ds_nodash[6:8] }}{{ ds_nodash[0:4] }}`).

## 6. Validation

Validation ensures the migrated solution functions as expected and produces correct results.

**How to Run Tests:**

1.  **BigQuery Stored Procedure Unit Testing:**
    *   Manually execute the `r_ausd_bp_ta_bpr_apn` stored procedure directly in the BigQuery console or via the `bq` command-line tool.
    *   Test with various valid input parameters (e.g., `p_jobkennung`, `p_eintragsnr`, `p_stichtag` for different dates, `p_wiederanlaufwert`).
    *   Test with invalid inputs:
        *   Missing required parameters.
        *   Invalid `p_stichtag` format (e.g., `YYYY-MM-DD`, `12345678`).
        *   Semantically invalid `p_stichtag` (e.g., `31022023`).
    *   Verify the behavior of the core SQL logic by inspecting the `PoolBasisprodukt` table after execution.
2.  **Cloud Composer (Airflow) DAG Testing:**
    *   Trigger the `k_ausd_bp_ta_bpr_apn_workflow` DAG manually from the Airflow UI.
    *   Observe the DAG run in the Airflow UI for successful task completion.
    *   Review Airflow task logs for any errors or unexpected output.
3.  **Data Validation:**
    *   After successful runs, perform data integrity checks on the `PoolBasisprodukt` table. Compare a sample of processed data with expected results from the original system (if possible) or a known good state.
    *   Verify record counts match expectations.

**What "Passing" Means:**

*   **BigQuery Stored Procedure:**
    *   For valid inputs, the procedure completes without raising an error.
    *   The `job_log` table contains a new entry with `status = 'SUCCESS'`, the correct `record_count`, and accurate parameter values.
    *   The `PoolBasisprodukt` table is updated correctly according to the business logic.
    *   For invalid inputs, the procedure raises an error, and the `error_log` table contains a new entry with relevant error details (e.g., `error_message`, `error_nr`, `error_arg`).
*   **Cloud Composer DAG:**
    *   The Airflow DAG run completes successfully (all tasks turn green).
    *   No errors are reported in the Airflow task logs.
    *   The BigQuery job initiated by the DAG completes successfully, as verified by the `job_log` table.
*   **Data Integrity:**
    *   The data in `PoolBasisprodukt` after migration and execution matches the expected output based on the original script's logic.
    *   Record counts and data transformations are accurate.

## 7. Rollback Procedure

In case of issues during or after go-live, the following rollback procedure can be followed to revert to the original KornShell script execution:

1.  **Disable Cloud Composer DAG:**
    *   In the Airflow UI, toggle off the `k_ausd_bp_ta_bpr_apn_workflow` DAG to prevent further executions.
2.  **Delete/Revert BigQuery Stored Procedure:**
    *   If the `r_ausd_bp_ta_bpr_apn` stored procedure is causing issues, it can be deleted using `DROP PROCEDURE IF EXISTS your_gcp_project.your_bigquery_dataset.r_ausd_bp_ta_bpr_apn;`.
    *   Alternatively, if a previous stable version of the stored procedure exists, it can be redeployed.
3.  **Revert BigQuery Data (if necessary):**
    *   If the BigQuery Stored Procedure made incorrect data modifications to `PoolBasisprodukt` or other tables, revert the data:
        *   **BigQuery Time Travel:** Utilize BigQuery's time travel feature to restore tables to a state before the erroneous execution (e.g., `CREATE TABLE your_table_backup AS SELECT * FROM your_table FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);`).
        *   **Data Reload:** If time travel is not sufficient or data corruption is extensive, re-ingest data from a known good backup or the source system.
4.  **Re-enable Original KornShell Script:**
    *   Re-activate the original `k_ausd_bp_ta_bpr_apn.ksh` script in its legacy environment. This may involve re-enabling cron jobs or other scheduling mechanisms.
5.  **Monitor Legacy System:**
    *   Closely monitor the re-activated legacy script to ensure it functions correctly and processes data as expected.

This rollback procedure aims to quickly restore the system to a functional state using the original implementation while the issues with the migrated solution are investigated and resolved.