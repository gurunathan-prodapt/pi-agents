# MIGRATION_NOTES.md

## 1. Summary

The KornShell script `r_ausd_v_ta_p_vertrag.ksh`, an orchestration wrapper for contract data reconciliation, has been migrated from a Unix/Linux shell environment to Google Cloud BigQuery. The target platform leverages BigQuery Stored Procedures for execution and dedicated BigQuery tables for logging and job control. The core processing logic, originally in `k_ausd_v_ta_p_vertrag.ksh`, is represented by a placeholder BigQuery Stored Procedure, `project.dataset.sp_k_ausd_v_ta_p_vertrag`, which requires further implementation.

## 2. Generated artifacts

The migration process generated the following BigQuery SQL artifacts:

*   **`project.dataset.tables.sql`**
    *   **Role:** Defines the Data Definition Language (DDL) for the BigQuery tables used for job control and logging. These tables (`job_control`, `job_runtime_log`, `job_error_log`) replace the original script's file-based logging and `DWMSG_*` functions for status and error reporting.
*   **`project.dataset.sp_k_ausd_v_ta_p_vertrag.sql`**
    *   **Role:** A placeholder BigQuery Stored Procedure for the core contract data reconciliation logic. This procedure is intended to encapsulate the functionality of the original `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_p_vertrag.ksh` script. Its full implementation is a follow-up task.
*   **`project.dataset.sp_vertragsdatenabgleich_wrapper.sql`**
    *   **Role:** The main wrapper BigQuery Stored Procedure, which is the direct migration of `r_ausd_v_ta_p_vertrag.ksh`. It handles parameter parsing, job initialization, logging, error handling, and orchestrates the call to `sp_k_ausd_v_ta_p_vertrag`.
*   **`project.dataset.sp_log_runtime_message.sql`**
    *   **Role:** A helper BigQuery Stored Procedure designed to simplify the insertion of runtime messages into the `job_runtime_log` table. This centralizes logging logic and improves code readability within the main wrapper procedure.

## 3. Key design decisions

*   **Wrapper to BigQuery Stored Procedure:** The original KornShell wrapper script (`r_ausd_v_ta_p_vertrag.ksh`) was directly translated into a BigQuery Stored Procedure (`sp_vertragsdatenabgleich_wrapper`). This decision leverages BigQuery's native procedural capabilities for orchestration, eliminating the need for external compute resources for the wrapper logic itself.
*   **Centralized Logging and Control Tables:** All logging (`print` statements, `tee -a`) and job control (`DWMSG_*` functions) from the original script are replaced by `INSERT` and `UPDATE` operations on dedicated BigQuery tables (`job_control`, `job_runtime_log`, `job_error_log`). This provides a structured, queryable, and centralized repository for job metadata, runtime messages, and errors, improving observability and auditing.
*   **Modular Core Logic:** The core processing script (`k_ausd_v_ta_p_vertrag.ksh`) is designed to be migrated into its own BigQuery Stored Procedure (`sp_k_ausd_v_ta_p_vertrag`). This promotes modularity, reusability, and clear separation of concerns, allowing the core business logic to be developed and tested independently.
*   **BigQuery Native Error Handling:** Shell `trap` commands and custom error functions (`DWMSG_Fehlerbehandlung`) are replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks. This provides robust, structured error handling within the BigQuery environment, automatically capturing error messages and allowing for consistent logging to the `job_error_log` table.
*   **Parameter Mapping:** Command-line parameters from the original KornShell script (e.g., `-j`, `-s`, `-l`) are directly mapped to input parameters of the BigQuery Stored Procedures. This maintains the functional interface while adapting to the BigQuery execution model.

**Notable Trade-offs:**

*   **Loss of Direct File-Based Logging:** The shift from file-based logs to table-based logs means traditional `tail -f` monitoring is no longer directly applicable. Monitoring now relies on querying BigQuery tables or using BigQuery's built-in logging and monitoring tools.
*   **Increased Verbosity for Simple Operations:** Simple `print` statements in shell scripts are replaced by `INSERT` statements into logging tables, which can be more verbose. This is mitigated by using a helper stored procedure (`sp_log_runtime_message`).
*   **Dependency on BigQuery SQL Procedural Language:** The migration requires expertise in BigQuery SQL procedural language, which may differ from traditional shell scripting paradigms.
*   **Core Logic Implementation Unknown:** The most significant trade-off is that the core business logic of `k_ausd_v_ta_p_vertrag.ksh` is currently a placeholder. Its complexity and potential non-SQL operations could introduce further design challenges and require additional GCP services (e.g., Dataflow, Cloud Functions).

## 4. Manual steps before go-live

Before the migrated solution can be fully operational, the following manual steps are required:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (`project.dataset` in the generated code) exists. If not, create it:
        ```sql
        CREATE SCHEMA IF NOT EXISTS your_gcp_project_id.your_dataset_name;
        ```
    *   **Action:** Replace `project.dataset` placeholders in all generated SQL files with your actual GCP project ID and dataset name.
2.  **Deploy DDL for Logging and Control Tables:**
    *   Execute the SQL statements from `project.dataset.tables.sql` to create the `job_control`, `job_runtime_log`, and `job_error_log` tables in your target dataset.
3.  **Deploy Helper Stored Procedure:**
    *   Execute the SQL from `project.dataset.sp_log_runtime_message.sql` to create the logging helper procedure.
4.  **Deploy Wrapper Stored Procedure:**
    *   Execute the SQL from `project.dataset.sp_vertragsdatenabgleich_wrapper.sql` to create the main wrapper procedure.
5.  **Deploy Core Logic Placeholder (and eventual implementation):**
    *   Execute the SQL from `project.dataset.sp_k_ausd_v_ta_p_vertrag.sql` to create the placeholder procedure.
    *   **Crucially, the actual business logic from `k_ausd_v_ta_p_vertrag.ksh` must be translated and implemented within `sp_k_ausd_v_ta_p_vertrag` before go-live.** This is a significant development effort.
6.  **IAM/Permissions:**
    *   The service account or user identity that will execute the BigQuery stored procedures must have the following IAM roles:
        *   `BigQuery Data Editor` on the target dataset (`your_gcp_project_id.your_dataset_name`) to create/update tables and insert/update data.
        *   `BigQuery Job User` on the GCP project (`your_gcp_project_id`) to run BigQuery jobs (including stored procedures).
7.  **Scheduling:**
    *   Determine how the `sp_vertragsdatenabgleich_wrapper` procedure will be triggered. Options include:
        *   **Cloud Scheduler:** For simple time-based scheduling.
        *   **Cloud Composer (Airflow):** For complex workflows, dependencies, and external system orchestration.
        *   **Cloud Functions/Workflows:** For event-driven or more custom orchestration.
        *   **Custom Application:** Invoking the procedure via BigQuery API.
8.  **Configuration Management:**
    *   Review any environment variables or configuration parameters from the original `r_ausd_v_ta_p_vertrag.ksh` (e.g., `BERT_DIR_ROOT`, `HOME`, specific paths). These should be either:
        *   Passed as parameters to the BigQuery stored procedures.
        *   Stored in a BigQuery configuration table.
        *   Hardcoded if they are truly static and environment-independent.

## 5. Known gaps & unresolved references

The following items have been flagged for follow-up or represent areas of incomplete migration:

*   **Core Script Logic (`sp_k_ausd_v_ta_p_vertrag`) Implementation (B4 Item):** The most critical gap is the actual translation and implementation of the business logic from `k_ausd_v_ta_p_vertrag.ksh` into the `project.dataset.sp_k_ausd_v_ta_p_vertrag` stored procedure. This requires a detailed analysis of the original script's SQL queries, data transformations, and any non-SQL operations.
*   **`DWMSG_*` Functions Fidelity:** While the `DWMSG_*` functions have been mapped to BigQuery logging tables, a detailed comparison of the exact logging formats, unique ID generation logic, and error reporting behavior of the original `f_alis_msgerr.ksh` and other utilities is needed to ensure full functional parity.
*   **Environment Variable Resolution:** The precise values and usage of `HOME` and `BERT_DIR_ROOT` from the original environment need to be fully resolved. If they represent paths or configurations, these must be translated into BigQuery-compatible parameters or configuration table entries.
*   **Date Format Sensitivity:** The original script uses `date +%d%m%Y`. While BigQuery's `FORMAT_DATE` function can replicate this, ensuring consistent date interpretations across the entire data pipeline (especially if `k_ausd_v_ta_p_vertrag.ksh` has specific date parsing/formatting) is crucial.
*   **Missing Lineage Edges:** The migration tool could not infer direct data flow relationships for the original script. A broader understanding of what `k_ausd_v_ta_p_vertrag.ksh` reads from and writes to is still missing. This information is vital for a complete data flow picture and for ensuring all source/target tables are correctly identified and migrated.
*   **Hardcoded Project/Dataset Names:** The generated SQL uses `project.dataset` as placeholders. These must be replaced with the actual GCP project ID and BigQuery dataset name before deployment.

## 6. Validation

To validate the migrated solution, follow these steps:

1.  **Deployment Verification:**
    *   Confirm that all DDL (`project.dataset.tables.sql`) and stored procedures (`sp_k_ausd_v_ta_p_vertrag`, `sp_vertragsdatenabgleich_wrapper`, `sp_log_runtime_message`) have been successfully deployed to the target BigQuery dataset.
    *   Check BigQuery's `INFORMATION_SCHEMA.ROUTINES` and `INFORMATION_SCHEMA.TABLES` views to confirm their existence.
2.  **Execute the Wrapper Procedure:**
    *   Call `sp_vertragsdatenabgleich_wrapper` with various parameter combinations:
        *   **Successful run:** `CALL your_gcp_project_id.your_dataset_name.sp_vertragsdatenabgleich_wrapper('JOB_ID_TEST', CURRENT_DATE(), 'test_log');`
        *   **Run without Stichtag:** `CALL your_gcp_project_id.your_dataset_name.sp_vertragsdatenabgleich_wrapper('JOB_ID_NO_STICHTAG', NULL, 'test_log_no_stichtag');`
        *   **Simulate failure (if `sp_k_ausd_v_ta_p_vertrag` has error simulation):** Modify `sp_k_ausd_v_ta_p_vertrag` temporarily to `SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Simulated error';` and then call the wrapper.
3.  **Logging and Control Table Verification:**
    *   **`job_control` table:**
        *   Query `SELECT * FROM your_gcp_project_id.your_dataset_name.job_control ORDER BY created_ts DESC;`
        *   Verify that new entries are created for each run.
        *   Check that `status` is 'OK' for successful runs and 'ERROR' for failed runs.
        *   Confirm `created_ts` and `finished_ts` are populated correctly.
        *   Validate `stichtag_info` and `log_name` match expectations.
    *   **`job_runtime_log` table:**
        *   Query `SELECT * FROM your_gcp_project_id.your_dataset_name.job_runtime_log WHERE job_kennung = 'JOB_ID_TEST' ORDER BY log_ts;`
        *   Verify that all expected informational messages (e.g., "Started...", "Stichtag parameter set...", "Successfully completed...") are present.
    *   **`job_error_log` table:**
        *   Query `SELECT * FROM your_gcp_project_id.your_dataset_name.job_error_log ORDER BY error_ts DESC;`
        *   For simulated error runs, verify that an entry is created with the correct `job_kennung`, `eintragsnr`, `error_message`, and `source_proc`.
4.  **Core Logic Validation (once implemented):**
    *   After `sp_k_ausd_v_ta_p_vertrag` is fully implemented, perform data validation:
        *   Compare output data (if any) or state changes in target tables with expected results from the original `k_ausd_v_ta_p_vertrag.ksh` script.
        *   Run the procedure with various test datasets to ensure correctness and performance.

**What "passing" means:**

*   The `sp_vertragsdatenabgleich_wrapper` procedure executes without unhandled BigQuery errors.
*   For successful runs, the corresponding entry in `job_control` has `status = 'OK'` and `finished_ts` populated.
*   For expected error scenarios, the `job_control` entry has `status = 'ERROR'`, and a corresponding entry exists in `job_error_log` with relevant error details.
*   The `job_runtime_log` contains all expected informational and warning messages for each run.
*   (Once `sp_k_ausd_v_ta_p_vertrag` is implemented) The data transformations and business logic performed by the core procedure yield correct and expected results, matching the behavior of the original `k_ausd_v_ta_p_vertrag.ksh`.

## 7. Rollback procedure

In case of critical issues or if the migrated solution does not meet requirements, the following steps outline the rollback procedure:

1.  **Stop New Executions:**
    *   Immediately disable or remove any scheduling mechanisms (e.g., Cloud Scheduler jobs, Cloud Composer DAGs) that trigger the BigQuery stored procedures.
2.  **Revert to Original Script:**
    *   Ensure the original `r_ausd_v_ta_p_vertrag.ksh` script and its dependencies are available and functional in the legacy environment.
    *   Re-enable any scheduling or triggering mechanisms for the original KornShell script.
3.  **Clean Up BigQuery Artifacts (Optional but Recommended):**
    *   **Delete Stored Procedures:**
        ```sql
        DROP PROCEDURE IF EXISTS your_gcp_project_id.your_dataset_name.sp_vertragsdatenabgleich_wrapper;
        DROP PROCEDURE IF EXISTS your_gcp_project_id.your_dataset_name.sp_k_ausd_v_ta_p_vertrag;
        DROP PROCEDURE IF EXISTS your_gcp_project_id.your_dataset_name.sp_log_runtime_message;
        ```
    *   **Delete Logging and Control Tables:**
        ```sql
        DROP TABLE IF EXISTS your_gcp_project_id.your_dataset_name.job_control;
        DROP TABLE IF EXISTS your_gcp_project_id.your_dataset_name.job_runtime_log;
        DROP TABLE IF EXISTS your_gcp_project_id.your_dataset_name.job_error_log;
        ```
    *   **Note:** If the `job_control`, `job_runtime_log`, or `job_error_log` tables contain valuable historical data that should be preserved, consider renaming them or moving them to an archive dataset instead of dropping them.
4.  **Verify Original System Functionality:**
    *   Confirm that the original `r_ausd_v_ta_p_vertrag.ksh` script is running as expected and performing its reconciliation tasks correctly in the legacy environment.