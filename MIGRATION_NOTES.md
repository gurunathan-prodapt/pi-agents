# MIGRATION_NOTES.md

## 1. Summary

The KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_apn_ve.ksh`, which serves as an orchestration wrapper for a contract data reconciliation job (`ta_apn_ve`), has been migrated.

The migration transforms the script's functionality from a shell-based execution and logging model to a BigQuery-native solution. The target platform is Google Cloud Platform, leveraging BigQuery for data processing, job control, and logging, and Cloud Composer (Apache Airflow) for external orchestration and scheduling.

## 2. Generated artifacts

The migration process generated the following files:

*   **`bigquery/tables/job_control.sql`**
    *   **Role:** Defines the BigQuery table `my-gcp-project.my_dataset.job_control`. This table replaces the shell script's internal job entry number management and status tracking, providing a centralized, queryable record of job executions, their start/end times, status, and key parameters.
*   **`bigquery/tables/job_log.sql`**
    *   **Role:** Defines the BigQuery table `my-gcp-project.my_dataset.job_log`. This table replaces the file-based logging (`>> $LogDatei`) of the original KornShell script, storing detailed operational messages for each job run.
*   **`bigquery/tables/job_error_log.sql`**
    *   **Role:** Defines the BigQuery table `my-gcp-project.my_dataset.job_error_log`. This table captures specific error details, including error codes, messages, SQLSTATE, and stack traces, providing a structured repository for troubleshooting job failures. It replaces the error handling and logging functions (`DWMSG_Fehlerbehandlung`) of the legacy script.
*   **`bigquery/stored_procedures/k_ausd_v_ta_apn_ve.sql`**
    *   **Role:** Defines the BigQuery Stored Procedure `my-gcp-project.my_dataset.k_ausd_v_ta_apn_ve`. This procedure is a placeholder for the core business logic of contract data reconciliation for `ta_apn_ve`, which was originally contained in the `k_ausd_v_ta_apn_ve.ksh` script. It is designed to be invoked by the wrapper procedure.
*   **`bigquery/stored_procedures/vertragsdatenabgleich_wrapper.sql`**
    *   **Role:** Defines the BigQuery Stored Procedure `my-gcp-project.my_dataset.vertragsdatenabgleich_wrapper`. This procedure is the direct replacement for the `r_ausd_v_ta_apn_ve.ksh` wrapper script. It handles parameter validation, job initialization, logging to the `job_control` and `job_log` tables, invocation of the core logic (`k_ausd_v_ta_apn_ve`), and comprehensive error handling, including updates to `job_control` and `job_error_log`.
*   **`composer/dags/vertragsdatenabgleich_dag.py`**
    *   **Role:** An Apache Airflow DAG (Directed Acyclic Graph) script for Cloud Composer. This DAG is responsible for scheduling and orchestrating the execution of the `vertragsdatenabgleich_wrapper` BigQuery Stored Procedure, replacing the legacy scheduling mechanism of the KornShell script.

## 3. Key design decisions

*   **Migration to BigQuery Stored Procedures for Modularity:** The monolithic KornShell script and its invoked core script were broken down into two distinct BigQuery Stored Procedures (`vertragsdatenabgleich_wrapper` and `k_ausd_v_ta_apn_ve`). This promotes modularity, reusability, and aligns with BigQuery's native capabilities for data processing and procedural logic.
*   **Centralized BigQuery Tables for Job Control and Logging:** Instead of file-based logging and ad-hoc shell variable management, dedicated BigQuery tables (`job_control`, `job_log`, `job_error_log`) were introduced. This provides structured, queryable, and centralized observability for job executions, statuses, and errors, significantly improving monitoring and debugging capabilities.
*   **Leveraging BigQuery's Native Error Handling:** The `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks in BigQuery Stored Procedures replace the shell script's `trap` commands and manual error checks. This provides a robust, declarative, and integrated error handling mechanism that automatically captures SQL-specific error details and logs them to the `job_error_log` table.
*   **Explicit Parameter Passing:** Command-line arguments (`getopts`) from the KornShell script are replaced by explicit `IN` parameters in the BigQuery Stored Procedures. This enforces type safety and provides a clear interface for invoking the procedures.
*   **Cloud Composer for External Orchestration:** For scheduling and managing dependencies, Cloud Composer (Apache Airflow) was chosen. This provides a robust, scalable, and feature-rich orchestration platform, replacing potentially simpler, less observable legacy scheduling methods.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be performed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (`my-gcp-project.my_dataset`) exists. If not, create it:
        ```sql
        CREATE SCHEMA `my-gcp-project.my_dataset` OPTIONS(location='<YOUR_GCP_REGION>');
        ```
2.  **BigQuery Table Deployment:**
    *   Execute the DDL scripts to create the job control and logging tables:
        *   `bigquery/tables/job_control.sql`
        *   `bigquery/tables/job_log.sql`
        *   `bigquery/tables/job_error_log.sql`
    *   These can be run via the BigQuery UI, `bq` command-line tool, or a deployment pipeline.
3.  **BigQuery Stored Procedure Deployment:**
    *   Execute the DDL scripts to create the stored procedures:
        *   `bigquery/stored_procedures/k_ausd_v_ta_apn_ve.sql` (Note: This is a placeholder and needs to be fully implemented as a B4 item).
        *   `bigquery/stored_procedures/vertragsdatenabgleich_wrapper.sql`
    *   These can be run via the BigQuery UI, `bq` command-line tool, or a deployment pipeline.
4.  **IAM Permissions Configuration:**
    *   Ensure the service account used by Cloud Composer (or any other orchestrator) has the necessary BigQuery permissions:
        *   `BigQuery Data Editor` (for inserting/updating records in `job_control`, `job_log`, `job_error_log`, and for the core logic to write data).
        *   `BigQuery Job User` (to run BigQuery jobs, including stored procedures).
5.  **Cloud Composer Environment Setup (if not already present):**
    *   Provision a Cloud Composer environment if one does not exist.
6.  **Airflow Connection Configuration:**
    *   Verify or create a `google_cloud_default` connection in your Airflow environment, ensuring it uses the correct GCP project and authentication method.
7.  **Airflow DAG Deployment:**
    *   Upload the `composer/dags/vertragsdatenabgleich_dag.py` file to the DAGs folder of your Cloud Composer environment.
8.  **Scheduling Configuration:**
    *   Adjust the `schedule` parameter within `vertragsdatenabgleich_dag.py` to match the desired execution frequency (e.g., `'@daily'`, `'0 5 * * *'`).
9.  **Configuration Management:**
    *   Review any environment variables or configuration values previously sourced from `.dw_init` or other utility scripts. Decide whether to hardcode them within the BigQuery procedures, manage them via BigQuery constants, or use a dedicated configuration table.

## 5. Known gaps & unresolved references

The following items have been flagged for follow-up or represent known limitations in the current migration:

*   **Core Logic Implementation (B4 Item):** The `bigquery/stored_procedures/k_ausd_v_ta_apn_ve.sql` procedure is currently a placeholder. The actual contract data reconciliation logic from the original `k_ausd_v_ta_apn_ve.ksh` script needs to be fully migrated and implemented within this BigQuery Stored Procedure. This is the most significant remaining task.
*   **Detailed Logic of Sourced Utility Scripts:** The exact logic within `$HOME/.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, and `h_alis_date.ksh` needs to be thoroughly reviewed. While basic parameter handling and date functions are covered, any complex or custom logic from these scripts might require specific BigQuery SQL procedural language implementations or dedicated UDFs/procedures.
*   **Error Code Mapping Completeness:** While some legacy error codes (e.g., 192 for missing parameters) have been explicitly mapped, a comprehensive review is needed to ensure all potential legacy error codes are correctly translated and logged in the `job_error_log` table.
*   **Original Script Complexity (Tier & Migration Flags):** The `file_complexity` table did not provide information on the tier or specific migration flags for `r_ausd_v_ta_apn_ve.ksh`. This means the `semi_auto` classification might require more manual analysis if unforeseen complexities arise during the core logic migration or testing.
*   **Configuration Management Strategy:** The design document notes that environment variables from `.dw_init` need to be re-evaluated. The current implementation hardcodes some values or expects them as parameters. A more robust configuration management strategy (e.g., using BigQuery configuration tables or environment variables managed by Composer) might be beneficial for production environments.

## 6. Validation

To ensure the successful migration and functionality of the new BigQuery job, follow these validation steps:

1.  **Deployment Verification:**
    *   Confirm that all BigQuery tables (`job_control`, `job_log`, `job_error_log`) and stored procedures (`k_ausd_v_ta_apn_ve`, `vertragsdatenabgleich_wrapper`) are successfully deployed in the `my-gcp-project.my_dataset` dataset.
    *   Verify that the `vertragsdatenabgleich_ta_apn_ve` DAG is visible and unpaused in the Airflow UI.

2.  **Manual BigQuery Stored Procedure Execution:**
    *   **Successful Run:**
        ```sql
        CALL `my-gcp-project.my_dataset.vertragsdatenabgleich_wrapper`(
            p_job_kennung_param => 'TEST_APN_VE_DAILY',
            p_stichtag_param => CURRENT_DATE(),
            p_show_help => FALSE
        );
        ```
    *   **Parameter Validation Error (Missing Parameters):**
        ```sql
        -- This should raise an error due to missing p_stichtag_param
        CALL `my-gcp-project.my_dataset.vertragsdatenabgleich_wrapper`(
            p_job_kennung_param => 'TEST_APN_VE_DAILY',
            p_stichtag_param => NULL,
            p_show_help => FALSE
        );
        ```
    *   **Help Option:**
        ```sql
        CALL `my-gcp-project.my_dataset.vertragsdatenabgleich_wrapper`(
            p_job_kennung_param => NULL,
            p_stichtag_param => NULL,
            p_show_help => TRUE
        );
        ```
        (This should print usage info and return without error).
    *   **Simulated Core Logic Error (after `k_ausd_v_ta_apn_ve` is implemented):** Modify `k_ausd_v_ta_apn_ve` temporarily to `RAISE` an error to test the wrapper's error handling.

3.  **Airflow DAG Execution:**
    *   Trigger the `vertragsdatenabgleich_ta_apn_ve` DAG manually from the Airflow UI.
    *   Monitor the DAG run in the Airflow UI for successful completion (green status) or failures.

4.  **"Passing" Criteria:**
    *   **BigQuery Stored Procedures:**
        *   Successful executions complete without unhandled errors.
        *   Error scenarios (e.g., missing parameters, simulated core logic failure) correctly raise errors and are caught by the wrapper's `EXCEPTION` block.
    *   **`job_control` Table:**
        *   For successful runs, a new entry exists with `status = 'OK'`, `end_time` populated, and no `error_code` or `error_message`.
        *   For failed runs, an entry exists with `status = 'ERROR'`, `end_time` populated, and relevant `error_code` and `error_message` fields filled.
    *   **`job_log` Table:**
        *   Contains a sequence of `INFO` messages corresponding to the job's lifecycle (start, core logic start/end, success/failure).
        *   For failed runs, an `ERROR` level message is present.
    *   **`job_error_log` Table:**
        *   For failed runs, a detailed error entry is present, including `error_timestamp`, `script_name`, `error_code`, `error_message`, `sql_state`, and `stack_trace`.
    *   **Airflow DAG:**
        *   The DAG run completes successfully (green status) for valid inputs.
        *   The DAG fails (red status) for invalid inputs or if the underlying BigQuery Stored Procedure raises an unhandled error, indicating proper error propagation.
    *   **Data Output (once `k_ausd_v_ta_apn_ve` is implemented):**
        *   The core logic, when executed, should produce the expected data transformations and populate target tables correctly, matching the output of the legacy `k_ausd_v_ta_apn_ve.ksh` script.

## 7. Rollback procedure

In case of issues or unexpected behavior after migration, follow these steps to roll back to the original KornShell script:

1.  **Disable New Job:**
    *   In the Cloud Composer Airflow UI, pause or delete the `vertragsdatenabgleich_ta_apn_ve` DAG to prevent further executions of the migrated job.
2.  **Revert Scheduling:**
    *   Re-enable the original scheduling mechanism (e.g., cron job, scheduler entry) that was responsible for triggering `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_apn_ve.ksh`.
3.  **Verify Legacy Job Execution:**
    *   Manually trigger the original `r_ausd_v_ta_apn_ve.ksh` script to confirm it runs successfully and produces the expected output.
    *   Monitor its logs and any downstream systems to ensure data consistency.
4.  **Data Consistency Check (if applicable):**
    *   If the migrated `k_ausd_v_ta_apn_ve` procedure (even as a placeholder) has written any data to target tables, assess the impact. Depending on the nature of the data and the issue, a data rollback or cleanup might be necessary. This step is highly dependent on the actual implementation of the core logic.
5.  **Cleanup (Optional):**
    *   Once the rollback is confirmed successful and the legacy job is stable, you may choose to delete the BigQuery tables (`job_control`, `job_log`, `job_error_log`) and stored procedures (`k_ausd_v_ta_apn_ve`, `vertragsdatenabgleich_wrapper`) if they are no longer needed or if a re-migration attempt is planned.