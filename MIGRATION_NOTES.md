# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell orchestration script `r_ausd_v_ta_action_assoc.ksh`. This script, originally responsible for environment setup, parameter handling, logging, and invoking a core data reconciliation script (`k_ausd_v_ta_action_assoc.ksh`), has been migrated from a legacy Unix/KornShell environment.

The target platform for this migration is Google Cloud. Specifically:
*   **Orchestration:** Google Cloud Composer (Apache Airflow) for workflow management and scheduling.
*   **Data Processing:** Google BigQuery for the underlying data reconciliation logic (which will replace the functionality of `k_ausd_v_ta_action_assoc.ksh`).
*   **Logging & Monitoring:** Google Cloud Logging and Monitoring.

The migration focused on translating the orchestration patterns of the KornShell wrapper into an Airflow DAG, while the core data reconciliation logic is identified as a separate, subsequent migration effort to BigQuery SQL or Python.

## 2. Generated artifacts

The migration process generated the following files:

*   **`dags/ta_action_assoc_reconciliation_wrapper.py`**
    *   **Role:** This is the main Apache Airflow DAG file. It orchestrates the entire reconciliation process for `ta_action_assoc`. It replaces the original `r_ausd_v_ta_action_assoc.ksh` script by handling environment setup, dynamic parameter generation (like `DW_EintragsNr`), logging job start/end, and invoking the core data reconciliation logic as a separate task. It also incorporates Airflow's native error handling and logging capabilities.
*   **`dags/k_ausd_v_ta_action_assoc_core_logic.sql`**
    *   **Role:** This file serves as a placeholder for the core data reconciliation logic. It is intended to contain the BigQuery SQL statements that will replace the functionality originally found in `k_ausd_v_ta_action_assoc.ksh`. As per the migration design, the detailed analysis and implementation of this core logic is a subsequent step. The Airflow DAG `ta_action_assoc_reconciliation_wrapper.py` is designed to execute this SQL file using a `BigQueryOperator`.

## 3. Key design decisions

The following key design decisions were made during the migration:

*   **Airflow for Orchestration:** Apache Airflow on Google Cloud Composer was chosen to replace the KornShell wrapper due to its robust capabilities for workflow management, scheduling, dependency handling, parameterization, and integration with Google Cloud services. This provides a modern, scalable, and observable orchestration layer.
*   **Separation of Concerns:** The orchestration logic (from `r_ausd_v_ta_action_assoc.ksh`) was cleanly separated from the core business logic (from `k_ausd_v_ta_action_assoc.ksh`). The Airflow DAG handles the wrapper's responsibilities, while the core data reconciliation is delegated to a dedicated BigQuery SQL file (or potentially Python/PySpark). This improves modularity and maintainability.
*   **Native Airflow Features for Utilities:** Legacy shell utilities for environment initialization (`$HOME/.dw_init`), error messaging (`f_alis_msgerr.ksh`), parameter handling (`h_alis_parameter.ksh`), and date handling (`h_alis_date.ksh`) were replaced by native Airflow and Python features:
    *   **Environment:** Airflow Variables, Connections, and Python logic for configuration.
    *   **Logging & Error Handling:** Airflow's built-in logging, `on_failure_callback` for error notifications, and Python's `logging` module, all integrating with Google Cloud Logging.
    *   **Parameters:** Airflow DAG parameters, `op_kwargs` for Python operators, and XComs for inter-task communication.
    *   **Date Handling:** Python's `datetime` module and Airflow's Jinja templating.
*   **BigQueryOperator for Core Logic:** The `BigQueryOperator` was selected to execute the core reconciliation logic, assuming it will be migrated to BigQuery SQL. This operator provides a direct and efficient way to run SQL queries in BigQuery from an Airflow DAG.
*   **Dynamic Parameter Generation:** Parameters like `DW_EintragsNr` (Entry Number) are dynamically generated within the DAG using a PythonOperator and passed between tasks via XComs, mimicking the dynamic nature of the original script.
*   **Standard SQL:** The `BigQueryOperator` is configured to use Standard SQL (`use_legacy_sql=False`), aligning with modern BigQuery best practices.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps are required:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (e.g., `your_project.your_dataset`) exists where the `ta_action_assoc` table and any other related tables for reconciliation will reside.
2.  **IAM Permissions:**
    *   Grant the Google Cloud Composer service account (typically `service-<PROJECT_NUMBER>@cloudcomposer.gserviceaccount.com`) the necessary IAM roles:
        *   `BigQuery Data Editor` (or `BigQuery User` if only running queries) on the target dataset(s) for the `execute_core_reconciliation_task`.
        *   `Storage Object Viewer` on the GCS bucket where the DAGs are stored.
        *   `Composer Worker` and `Composer User` roles for general Composer operations.
3.  **Airflow Connection Configuration:**
    *   Verify that the `google_cloud_default` connection is correctly configured in your Airflow environment. This connection is used by the `BigQueryOperator` to authenticate with BigQuery.
4.  **Core Logic Implementation:**
    *   **Crucially, the `dags/k_ausd_v_ta_action_assoc_core_logic.sql` file is currently a placeholder.** It must be fully implemented with the actual BigQuery SQL logic derived from the detailed analysis and migration of the original `k_ausd_v_ta_action_assoc.ksh` script. This involves identifying source tables, transformation rules, and target tables.
5.  **Airflow Variables (if applicable):**
    *   If any environment variables from the original `$HOME/.dw_init` or other configuration files are required by the DAG or the core logic, they should be created as Airflow Variables in the Composer UI or via the `gcloud` CLI.
6.  **DAG Deployment:**
    *   Upload `dags/ta_action_assoc_reconciliation_wrapper.py` and `dags/k_ausd_v_ta_action_assoc_core_logic.sql` to the DAGs folder of your Cloud Composer environment's GCS bucket.
7.  **Scheduling:**
    *   Update the `schedule_interval` in `ta_action_assoc_reconciliation_wrapper.py` from `None` to the desired schedule (e.g., `'@daily'`, `'0 5 * * *'`) if it's meant to run on a recurring basis.

## 5. Known gaps & unresolved references

The following items are known gaps or require further follow-up:

*   **Core Logic Migration (`k_ausd_v_ta_action_assoc.ksh`):** The most significant unresolved item. The `dags/k_ausd_v_ta_action_assoc_core_logic.sql` file is a placeholder. A separate, detailed analysis of `k_ausd_v_ta_action_assoc.ksh` is required to understand its data sources, transformations, and targets. This analysis will inform the complete implementation of the BigQuery SQL (or Python/PySpark) for the core reconciliation logic.
*   **Environment Variables from `$HOME/.dw_init`:** The specific environment variables set by `$HOME/.dw_init` need to be cataloged. Any critical variables required by the core logic or the wrapper DAG must be replicated as Airflow Variables, Airflow Connections, or directly managed within the Python code.
*   **Custom Error Code Mapping:** The original script used custom error codes (`ErrNr=193`, `ErrNr=192`) and a custom error reporting function (`DWMSG_MeldeFehler`). While Airflow's `on_failure_callback` and Cloud Logging handle basic error reporting, a detailed mapping of these custom error codes to specific Cloud Monitoring alerts or custom notification channels (e.g., Slack, PagerDuty) needs to be defined and implemented.
*   **Full Parameter Usage (`getopts s:l:`):** The original `getopts` command in `r_ausd_v_ta_action_assoc.ksh` included options for `s:` and `l:`, although the provided script only explicitly handled `-h`. If `s:` or `l:` were intended for future use or are implicitly used by `k_ausd_v_ta_action_assoc.ksh`, their meaning and required values must be determined and incorporated into the Airflow DAG's parameter handling (e.g., as DAG run configurations or Airflow Variables).

## 6. Validation

To validate the migrated job, follow these steps:

1.  **Local Testing (Optional but Recommended):**
    *   Set up a local Airflow environment.
    *   Use `airflow dags test ta_action_assoc_reconciliation_wrapper <execution_date>` to run the DAG locally and verify task execution flow and basic Python logic.
2.  **Deployment to Cloud Composer:**
    *   Deploy the DAG files (`.py` and `.sql`) to your Cloud Composer environment.
3.  **Manual Trigger:**
    *   From the Airflow UI, manually trigger the `ta_action_assoc_reconciliation_wrapper` DAG.
4.  **Monitor DAG Run:**
    *   Observe the DAG run in the Airflow UI.
    *   Check the logs for each task (e.g., `setup_environment_and_log_start`, `execute_core_reconciliation`, `log_success`).
    *   Verify that the `Job started` and `Job completed successfully` messages appear in the logs with the correct `JobKennung` and dynamically generated `Entry Number`.
    *   Check Cloud Logging for the Composer environment to ensure all logs are captured and no unexpected errors occur.
5.  **Data Validation (Post-Core Logic Implementation):**
    *   Once `dags/k_ausd_v_ta_action_assoc_core_logic.sql` is fully implemented, perform comprehensive data validation.
    *   **"Passing" means:**
        *   The Airflow DAG completes successfully with all tasks marked green.
        *   No errors are reported in Airflow logs or Cloud Logging.
        *   The data in the target BigQuery tables (e.g., `ta_action_assoc_target_table`) is reconciled correctly, matching the expected output based on the original `k_ausd_v_ta_action_assoc.ksh` logic. This requires comparing the output of the migrated job with the output of the legacy job for a given dataset/period.
        *   Any specific business rules or data quality checks defined for the reconciliation process are met.

## 7. Rollback procedure

In case of issues or unexpected behavior after go-live, the following rollback procedure can be executed:

1.  **Deactivate/Delete Airflow DAG:**
    *   In the Airflow UI, set the `ta_action_assoc_reconciliation_wrapper` DAG to "Off" (pause it) or delete it from the DAGs folder in the Composer GCS bucket. This immediately stops any further scheduled or manual runs of the new job.
2.  **Re-enable Legacy Job:**
    *   Re-enable or restart the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_action_assoc.ksh` script in the legacy environment.
3.  **Data State Assessment:**
    *   Assess the state of the data in BigQuery. If the `execute_core_reconciliation_task` made partial or incorrect changes, a data rollback might be necessary. This could involve:
        *   Restoring the target BigQuery table(s) from a snapshot or backup taken before the new job ran.
        *   Running a compensating transaction if the changes are reversible.
        *   Manually correcting the data if the impact is small and well-understood.
    *   **Note:** The `k_ausd_v_ta_action_assoc_core_logic.sql` should be designed with idempotency or transactional integrity in mind to minimize rollback complexity.
4.  **Monitor Legacy Job:**
    *   Verify that the legacy job runs successfully and produces the expected output.
5.  **Root Cause Analysis:**
    *   Investigate the reason for the rollback, fix the issues in the Airflow DAG or the core BigQuery SQL, and re-test thoroughly before attempting another deployment.