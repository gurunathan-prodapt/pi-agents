# MIGRATION_NOTES.md

## 1. Summary

The KornShell wrapper script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount.ksh`, responsible for orchestrating the reconciliation of `ta_discount` contract data, has been migrated. Its functionality, including environment setup, parameter handling, logging, error management, and invocation of a core processing script, has been translated to Google Cloud's BigQuery platform.

The target platform consists of:
*   **BigQuery Stored Procedures:** To replicate the orchestration logic of the wrapper and provide a placeholder for the core data reconciliation logic.
*   **BigQuery Tables:** For centralized logging and job status tracking.
*   **External Orchestration (Optional):** Cloud Composer (Apache Airflow) for scheduling and advanced workflow management.

## 2. Generated Artifacts

The migration generated the following BigQuery SQL artifacts:

*   **`your_bq_dataset_id.job_log.sql`**:
    *   **Role:** Creates the `job_log` table in BigQuery. This table serves as the central repository for all job execution messages, replacing the legacy file-based log system. It captures details like `job_entry_nr`, `job_kennung`, `script_name`, `log_timestamp`, `log_level`, and `message`.
*   **`your_bq_dataset_id.job_status.sql`**:
    *   **Role:** Creates the `job_status` table in BigQuery. This table tracks the current and historical status of job runs, including `job_entry_nr`, `job_kennung`, `stichtag`, `status_code`, `status_text`, and `last_updated`.
*   **`your_bq_dataset_id.dwmsg_ermittlenr_proc.sql`**:
    *   **Role:** Creates a BigQuery stored procedure `dwmsg_ermittlenr_proc`. This procedure generates a unique job entry number and a job identifier (`job_kennung`), mimicking the `DWMSG_ErmittleNr` functionality from the legacy system.
*   **`your_bq_dataset_id.dwmsg_logdateiname_proc.sql`**:
    *   **Role:** Creates a BigQuery stored procedure `dwmsg_logdateiname_proc`. This procedure provides a conceptual log filename based on the `job_kennung`, aligning with the legacy `DWMSG_Logdateiname` function, though actual logging is to the `job_log` table.
*   **`your_bq_dataset_id.dwmsg_erzeugeeintrag_proc.sql`**:
    *   **Role:** Creates a BigQuery stored procedure `dwmsg_erzeugeeintrag_proc`. This procedure is responsible for inserting log entries into the `job_log` table, replacing direct `print` statements and `tee` commands from the KornShell script.
*   **`your_bq_dataset_id.dwmsg_setzestichtaginfo_proc.sql`**:
    *   **Role:** Creates a BigQuery stored procedure `dwmsg_setzestichtaginfo_proc`. This procedure initializes or updates the job status in the `job_status` table, including the `stichtag` (reference date) and initial status, similar to `DWMSG_SetzeStichtagInfo`.
*   **`your_bq_dataset_id.dwmsg_fehlerbehandlung_proc.sql`**:
    *   **Role:** Creates a BigQuery stored procedure `dwmsg_fehlerbehandlung_proc`. This procedure handles error conditions by logging the error to `job_log`, updating the job status to 'ERROR' in `job_status`, and optionally raising the error further. It replaces the `DWMSG_Fehlerbehandlung` trap handler.
*   **`your_bq_dataset_id.dwmsg_setzestatusok_proc.sql`**:
    *   **Role:** Creates a BigQuery stored procedure `dwmsg_setzestatusok_proc`. This procedure logs a success message and updates the job status to 'OK' in the `job_status` table upon successful completion, replacing `DWMSG_SetzeStatusOK`.
*   **`your_bq_dataset_id.k_ausd_v_ta_discount_proc.sql`**:
    *   **Role:** Creates a placeholder BigQuery stored procedure `k_ausd_v_ta_discount_proc`. This procedure is intended to house the core data reconciliation logic that was originally in `k_ausd_v_ta_discount.ksh`. It currently contains only logging statements and requires further development.
*   **`your_bq_dataset_id.vertragsdatenabgleich_wrapper_proc.sql`**:
    *   **Role:** Creates the main BigQuery stored procedure `vertragsdatenabgleich_wrapper_proc`. This procedure is the direct translation of `r_ausd_v_ta_discount.ksh`, handling parameter parsing, environment setup (via `DECLARE` variables), logging, status management, and invoking the core `k_ausd_v_ta_discount_proc`. It includes `BEGIN...EXCEPTION` blocks for robust error handling.

## 3. Key Design Decisions

*   **BigQuery Stored Procedures for Orchestration:** The KornShell wrapper's control flow, parameter handling, and utility calls were directly translated into BigQuery SQL stored procedures. This leverages BigQuery's native procedural capabilities for job orchestration.
*   **Centralized Logging and Status Tables:** Instead of file-based logging and custom status files, dedicated BigQuery tables (`job_log`, `job_status`) were created. This provides a structured, queryable, and scalable solution for monitoring job execution and status.
*   **Modular Utility Procedures:** Common functions from the legacy `DWMSG_` framework and sourced utility scripts were migrated into separate, reusable BigQuery stored procedures (e.g., `dwmsg_ermittlenr_proc`, `dwmsg_erzeugeeintrag_proc`). This promotes modularity and maintainability.
*   **BigQuery `EXCEPTION` Blocks for Error Handling:** The shell's `trap` mechanism for signal handling was replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks, providing robust error capture and custom handling within the SQL context.
*   **Parameter Handling via Procedure Arguments:** Command-line arguments (`getopts`) were translated into BigQuery stored procedure input parameters (e.g., `p_help`, `p_stichtag`), allowing for flexible invocation.
*   **Placeholder for Core Logic:** Recognizing that the core business logic (`k_ausd_v_ta_discount.ksh`) was outside the immediate scope, a placeholder stored procedure (`k_ausd_v_ta_discount_proc`) was created. This allows the wrapper migration to proceed independently while deferring the complex core logic migration.
*   **External Orchestration Consideration:** While the wrapper itself is a BigQuery procedure, the design acknowledges that an external orchestrator like Cloud Composer (Airflow) might be necessary for scheduling, advanced retry logic, and integration with other GCP services, especially if the core script has complex external dependencies.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps are required:

1.  **GCP Project and BigQuery Dataset Setup:**
    *   Ensure a Google Cloud Project (`your_gcp_project_id`) is active.
    *   Create the target BigQuery Dataset (`your_bq_dataset_id`) within the project. This dataset will host all the generated tables and stored procedures.
        ```bash
        bq mk --location=US your_gcp_project_id:your_bq_dataset_id
        ```
        (Adjust location as needed)

2.  **IAM Permissions:**
    *   The service account or user executing these BigQuery operations (creation and execution) must have appropriate IAM roles:
        *   `BigQuery Data Editor` or `BigQuery Admin` on `your_gcp_project_id.your_bq_dataset_id` to create tables and stored procedures.
        *   `BigQuery Job User` on `your_gcp_project_id` to run BigQuery jobs.
        *   If using Cloud Composer, the Composer service account will need these permissions.

3.  **Deploy Generated Artifacts:**
    *   Execute each generated `.sql` file in the specified order (tables first, then utility procedures, then core placeholder, then wrapper) against your BigQuery dataset.
    *   This can be done via the BigQuery UI, `bq` command-line tool, or a deployment script.
        ```bash
        # Example for tables
        bq query --use_legacy_sql=false < your_bq_dataset_id.job_log.sql
        bq query --use_legacy_sql=false < your_bq_dataset_id.job_status.sql
        # Example for procedures
        bq query --use_legacy_sql=false < your_bq_dataset_id.dwmsg_ermittlenr_proc.sql
        # ... and so on for all .sql files
        ```
    *   **Important:** Replace `your_gcp_project_id` and `your_bq_dataset_id` placeholders in the generated SQL files with your actual project and dataset IDs before deployment.

4.  **Migrate Core Logic (`k_ausd_v_ta_discount_proc`):**
    *   The `k_ausd_v_ta_discount_proc` is currently a placeholder. A separate, detailed analysis and migration effort is required to translate the actual business logic from `k_ausd_v_ta_discount.ksh` into BigQuery SQL.
    *   This involves identifying source tables, target tables, transformation rules, and any external dependencies of the original script.
    *   Once migrated, update the `k_ausd_v_ta_discount_proc.sql` file and redeploy it.

5.  **Scheduling and Orchestration:**
    *   If using Cloud Composer/Airflow, create a new DAG that calls `your_gcp_project_id.your_bq_dataset_id.vertragsdatenabgleich_wrapper_proc` with the necessary parameters (e.g., `p_stichtag`).
    *   Configure the DAG's schedule, error handling, and retry policies.
    *   If not using Cloud Composer, consider BigQuery Scheduled Queries for simpler, time-based execution.

## 5. Known Gaps & Unresolved References

*   **Core Logic (`k_ausd_v_ta_discount.ksh`) Migration:** The most significant gap is the actual business logic within `k_ausd_v_ta_discount.ksh`. The generated `k_ausd_v_ta_discount_proc` is a placeholder and requires a dedicated migration effort to translate its data reconciliation logic into BigQuery SQL. This impacts the `semi_auto` migration bucket and is a critical pre-requisite for full functionality.
*   **Shell-Specific Features:** Direct emulation of OS-level traps (signals like `INT`, `ERR`) and advanced shell process management within BigQuery SQL is not possible. BigQuery's `EXCEPTION` blocks handle SQL errors, but external orchestration (e.g., Cloud Composer) would be needed for broader process monitoring, retry mechanisms, and failure handling beyond SQL exceptions.
*   **File System Interactions:** If `k_ausd_v_ta_discount.ksh` performs complex file I/O or interacts with specific legacy file systems, this will require further investigation during the core logic migration. These operations might need to be re-engineered using Cloud Storage, Cloud Functions, or Cloud Run in conjunction with BigQuery.
*   **Parameter Complexity of Core Script:** The full set of parameters and their types expected by the original `k_ausd_v_ta_discount.ksh` (beyond `-j` and `-f`) is currently unknown. This will need to be determined during the core script's migration and reflected in the `k_ausd_v_ta_discount_proc` signature.
*   **Configuration Externalization:** While `DECLARE` variables are used, a more robust solution for environment variables and constants might involve dedicated BigQuery configuration tables or Secret Manager for sensitive values, if the core script requires them.

## 6. Validation

To validate the migrated wrapper, follow these steps:

1.  **Prerequisites:** Ensure all generated BigQuery tables and stored procedures (including the placeholder `k_ausd_v_ta_discount_proc`) have been deployed to `your_gcp_project_id.your_bq_dataset_id`.

2.  **Test Help Message:**
    *   Execute the wrapper procedure with the help flag:
        ```sql
        CALL your_gcp_project_id.your_bq_dataset_id.vertragsdatenabgleich_wrapper_proc(p_help => TRUE);
        ```
    *   **Passing Criteria:** The query should return a result set containing the usage instructions, similar to the original shell script's output. No entries should be made in `job_log` or `job_status` for a help invocation.

3.  **Test Successful Execution (Wrapper only):**
    *   Execute the wrapper procedure with a specific `stichtag` (or let it default to `CURRENT_DATE()`):
        ```sql
        CALL your_gcp_project_id.your_bq_dataset_id.vertragsdatenabgleich_wrapper_proc(p_stichtag => '2023-01-15');
        ```
    *   **Passing Criteria:**
        *   The procedure should complete without raising an unhandled error.
        *   Query the `job_log` table:
            ```sql
            SELECT * FROM your_gcp_project_id.your_bq_dataset_id.job_log ORDER BY log_timestamp DESC LIMIT 10;
            ```
            You should see entries for job start, metadata, core processing start/end (from the placeholder), and job completion, all with `log_level = 'INFO'`.
        *   Query the `job_status` table:
            ```sql
            SELECT * FROM your_gcp_project_id.your_bq_dataset_id.job_status ORDER BY last_updated DESC LIMIT 1;
            ```
            The latest entry for the executed `job_kennung` should show `status_code = 'OK'` and `status_text = 'Completed successfully'`.

4.  **Test Error Handling:**
    *   **Simulate an error in the core procedure:** Temporarily modify `k_ausd_v_ta_discount_proc` to `RAISE` an error. For example:
        ```sql
        CREATE OR REPLACE PROCEDURE your_gcp_project_id.your_bq_dataset_id.k_ausd_v_ta_discount_proc(
            IN p_job_kennung STRING,
            IN p_dw_eintrags_nr INT64,
            IN p_stichtag DATE
        )
        BEGIN
            RAISE USING MESSAGE 'Simulated error during core processing!';
        END;
        ```
    *   Redeploy the modified `k_ausd_v_ta_discount_proc`.
    *   Execute the wrapper procedure again:
        ```sql
        CALL your_gcp_project_id.your_bq_dataset_id.vertragsdatenabgleich_wrapper_proc(p_stichtag => '2023-01-16');
        ```
    *   **Passing Criteria:**
        *   The wrapper procedure should catch the error and call `dwmsg_fehlerbehandlung_proc`.
        *   Query `job_log`: You should see an entry with `log_level = 'ERROR'` and a message indicating the failure.
        *   Query `job_status`: The latest entry for the executed `job_kennung` should show `status_code = 'ERROR'` and `status_text` containing the error message.
        *   The wrapper procedure itself might still complete (as the error is handled internally), but the `job_status` should reflect the failure.

## 7. Rollback Procedure

In case of issues or if the migration needs to be reverted, follow these steps:

1.  **Stop New Executions:**
    *   If using Cloud Composer/Airflow, disable or delete the DAG that schedules `vertragsdatenabgleich_wrapper_proc`.
    *   If using BigQuery Scheduled Queries, disable or delete the scheduled query.
    *   Ensure no new manual executions of the BigQuery wrapper procedure are initiated.

2.  **Delete BigQuery Objects:**
    *   Delete the generated BigQuery stored procedures and tables from `your_gcp_project_id.your_bq_dataset_id`.
    *   **Order of Deletion:** Procedures that call other procedures should be deleted first, or all procedures can be deleted before tables.
        ```bash
        # Delete wrapper and core procedures
        bq rm -f -r your_gcp_project_id:your_bq_dataset_id.vertragsdatenabgleich_wrapper_proc
        bq rm -f -r your_gcp_project_id:your_bq_dataset_id.k_ausd_v_ta_discount_proc
        # Delete utility procedures
        bq rm -f -r your_gcp_project_id:your_bq_dataset_id.dwmsg_setzestatusok_proc
        bq rm -f -r your_gcp_project_id:your_bq_dataset_id.dwmsg_fehlerbehandlung_proc
        bq rm -f -r your_gcp_project_id:your_bq_dataset_id.dwmsg_setzestichtaginfo_proc
        bq rm -f -r your_gcp_project_id:your_bq_dataset_id.dwmsg_erzeugeeintrag_proc
        bq rm -f -r your_gcp_project_id:your_bq_dataset_id.dwmsg_logdateiname_proc
        bq rm -f -r your_gcp_project_id:your_bq_dataset_id.dwmsg_ermittlenr_proc
        # Delete tables
        bq rm -f your_gcp_project_id:your_bq_dataset_id.job_log
        bq rm -f your_gcp_project_id:your_bq_dataset_id.job_status
        ```
    *   Alternatively, if the dataset was created solely for this migration, you can delete the entire dataset:
        ```bash
        bq rm -r -f your_gcp_project_id:your_bq_dataset_id
        ```

3.  **Revert to Original Execution:**
    *   Ensure the original KornShell script `r_ausd_v_ta_discount.ksh` and its dependencies are in place and functional in the legacy environment.
    *   Resume the original scheduling mechanism for `r_ausd_v_ta_discount.ksh`.

This rollback procedure ensures that all BigQuery artifacts are removed and the system can revert to using the original KornShell script.