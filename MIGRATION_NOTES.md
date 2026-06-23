# MIGRATION_NOTES.md

## 1. Summary

The KornShell wrapper script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_bp_ref.ksh` has been migrated. This script, originally responsible for environment setup, parameter parsing, logging, and orchestrating the `k_ausd_v_ta_bp_ref.ksh` core logic for contract data reconciliation (`Vertragsdatenabgleich` for `ta_bp_ref`), has been re-platformed.

The target platform for this migration is **Google Cloud Composer (Apache Airflow)** for orchestration and **Google BigQuery** for the underlying data processing logic (once `k_ausd_v_ta_bp_ref.ksh` is migrated). The legacy KornShell wrapper's functionality is now encapsulated within an Airflow Directed Acyclic Graph (DAG).

## 2. Generated Artifacts

The migration process has generated the following file:

*   **`r_ausd_v_ta_bp_ref_dag.py`**
    *   **Role:** This Python file defines an Apache Airflow DAG. It replaces the original KornShell wrapper script by:
        *   Initializing job parameters (e.g., `JobKennung`, `DW_EintragsNr`) using Python functions and Airflow's XComs.
        *   Orchestrating the execution of the core data reconciliation logic, which is expected to be migrated to BigQuery SQL (represented by a `BigQueryOperator` placeholder).
        *   Handling logging and success status reporting via Airflow's native mechanisms.
        *   Providing a structured, scheduled, and observable workflow within Google Cloud Composer.

## 3. Key Design Decisions

The migration strategy focused on leveraging Google Cloud's native services and Airflow's capabilities to replace the legacy KornShell wrapper's functionality.

*   **Orchestration Layer Transition:** The KornShell wrapper's role as an orchestrator has been fully transitioned to an Apache Airflow DAG running on Google Cloud Composer.
    *   **Why:** Composer provides robust scheduling, monitoring, logging, error handling, and retry mechanisms out-of-the-box, significantly improving operational efficiency and reliability compared to custom shell scripting. It also aligns with the broader GCP data ecosystem.
*   **Replacement of Legacy Utility Scripts:** The various sourced KornShell utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) have been replaced by native Python functions within the DAG or by leveraging standard Python libraries and Airflow's built-in features.
    *   **Why:** This eliminates dependencies on legacy shell scripts, simplifies the environment, and integrates seamlessly with the Python-based Airflow environment.
*   **Centralized Logging:** The custom `DWMSG_*` logging framework has been replaced by Airflow's native logging, which automatically integrates with Google Cloud Logging.
    *   **Why:** Provides centralized, searchable, and auditable logs, enhancing observability and debugging capabilities.
*   **Core Logic Invocation:** The direct invocation of `k_ausd_v_ta_bp_ref.ksh` has been replaced by a `BigQueryOperator` task in the DAG.
    *   **Why:** This design anticipates the migration of `k_ausd_v_ta_bp_ref.ksh`'s data processing logic to BigQuery SQL, allowing the DAG to directly execute the transformed SQL queries or stored procedures within the target data warehouse.
*   **Parameter Handling:** Command-line parameter parsing (`getopts`) has been replaced by Airflow's XComs for passing dynamic values (like `JobKennung`, `DW_EintragsNr`) between tasks.
    *   **Why:** XComs are the standard Airflow mechanism for inter-task communication, ensuring data flow within the DAG.
*   **Trade-offs:**
    *   **Dependency on `k_ausd_v_ta_bp_ref.ksh` Migration:** The full functionality of this DAG is contingent on the successful migration and implementation of the core reconciliation logic from `k_ausd_v_ta_bp_ref.ksh` into BigQuery. Until then, the `BigQueryOperator` contains placeholder SQL.
    *   **Initial Setup Complexity:** Setting up and configuring a Cloud Composer environment can be more complex than running a simple shell script, but the long-term benefits in manageability and scalability outweigh this.
    *   **Loss of Direct Shell Control:** While Airflow offers `BashOperator`, the design prioritizes Python and BigQuery native operators for better integration and maintainability, moving away from direct shell execution.

## 4. Manual Steps Before Go-Live

The following manual steps are required to ensure the successful deployment and operation of the migrated job:

1.  **BigQuery Dataset Creation:**
    *   Ensure the BigQuery dataset(s) required for the `ta_bp_ref` reconciliation process exist in the target GCP project. This includes datasets for source tables, staging tables, and the final `ta_bp_ref` table itself.
2.  **IAM Permissions:**
    *   The Google Cloud Composer service account (or the service account associated with the Airflow environment) must have the necessary IAM roles and permissions. At a minimum, this includes:
        *   `BigQuery Data Editor` or `BigQuery User` (to run queries and potentially write to tables).
        *   `Composer Worker` and `Composer Administrator` (for Composer operations).
        *   `Cloud Logging Writer` (for logging).
        *   `Storage Object Viewer` and `Storage Object Creator` (for DAG deployment and XComs).
3.  **Airflow Connection Configuration:**
    *   Verify that the `google_cloud_default` Airflow connection is correctly configured in your Composer environment. This connection is used by the `BigQueryOperator` to authenticate with BigQuery.
4.  **Secrets Management (if applicable):**
    *   While the wrapper script itself didn't expose explicit secrets, if the core `k_ausd_v_ta_bp_ref.ksh` logic relies on any database credentials, API keys, or other sensitive information, these must be securely managed in GCP Secret Manager and accessed by the Airflow DAG (e.g., via `SecretManagerHook` or environment variables).
5.  **Scheduling Configuration:**
    *   The generated DAG has `schedule_interval=None`. If the original `r_ausd_v_ta_bp_ref.ksh` script was scheduled (e.g., daily, hourly), this schedule must be explicitly configured in the `r_ausd_v_ta_bp_ref_dag.py` file before deployment.
6.  **Core Logic Migration & Integration:**
    *   **Crucially, the actual data reconciliation logic from `k_ausd_v_ta_bp_ref.ksh` must be migrated to BigQuery SQL (e.g., as a stored procedure or a series of SQL statements) or a Python script.**
    *   Once migrated, the placeholder SQL within the `execute_bigquery_reconciliation_logic` task in `r_ausd_v_ta_bp_ref_dag.py` must be replaced with the actual BigQuery SQL or the Python code invocation. This is the most significant manual step.

## 5. Known Gaps & Unresolved References

*   **Core Logic of `k_ausd_v_ta_bp_ref.ksh` (B4 Item):** The content and complexity of the core data reconciliation logic within `k_ausd_v_ta_bp_ref.ksh` are currently unknown. The `BigQueryOperator` in the generated DAG contains placeholder SQL. The full functionality of this migrated job depends entirely on the successful analysis, redesign, and implementation of this core logic in BigQuery. This is a critical follow-up item.
*   **Full Fidelity of Legacy Utility Scripts:** While basic replacements for environment setup and logging are in place, a detailed analysis of all functionalities within `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, and `h_alis_date.ksh` is recommended to ensure no subtle behaviors are missed in their Python re-implementation.
*   **Unused Parameters (`-s`, `-l`):** The original script defined `-s` and `-l` parameters but did not appear to use them. It needs to be confirmed if these were truly vestigial or if they were implicitly consumed by the invoked `k_ausd_v_ta_bp_ref.ksh` script. If the latter, their functionality needs to be incorporated into the migrated core logic.
*   **Original Scheduling:** The exact scheduling frequency and dependencies of the original `r_ausd_v_ta_bp_ref.ksh` script need to be determined and configured in the Airflow DAG's `schedule_interval`.

## 6. Validation

To validate the successful migration and operation of the `r_ausd_v_ta_bp_ref_dag.py` DAG:

1.  **Deployment:**
    *   Deploy the `r_ausd_v_ta_bp_ref_dag.py` file to your Google Cloud Composer environment's DAGs folder.
2.  **Execution:**
    *   **Manual Trigger:** Navigate to the Airflow UI, find `r_ausd_v_ta_bp_ref_dag`, and trigger a manual run.
    *   **Scheduled Run:** If a `schedule_interval` is configured, observe the DAG run at its next scheduled time.
3.  **"Passing" Criteria:**
    *   **Airflow UI:** The DAG run should complete successfully, with all tasks (`start_job_and_generate_params`, `execute_bigquery_reconciliation_logic`, `log_job_success`) showing a "success" (green) status.
    *   **Cloud Logging:**
        *   Verify that logs for the DAG run appear in Google Cloud Logging.
        *   Confirm that the `start_job_and_generate_params` task logs the correct `JobKennung` and `DW_EintragsNr`.
        *   Confirm that the `log_job_success` task logs the final success message.
        *   Check for any errors or warnings in the logs.
    *   **BigQuery (Post-Core Logic Migration):** Once the `k_ausd_v_ta_bp_ref.ksh` logic is migrated and integrated:
        *   Verify that the `execute_bigquery_reconciliation_logic` task successfully executes the BigQuery SQL.
        *   Inspect the target `ta_bp_ref` table (or any other affected tables) in BigQuery to ensure the data reconciliation has occurred correctly and as expected. This includes checking row counts, data values, and any audit trails.

## 7. Rollback Procedure

In case of issues or unexpected behavior after deployment, the following rollback procedure can be followed:

1.  **Immediate Action (Airflow):**
    *   **Pause the DAG:** In the Airflow UI, immediately pause the `r_ausd_v_ta_bp_ref_dag` to prevent further scheduled or manual executions.
2.  **Revert Deployment (Airflow):**
    *   **Delete the DAG:** Remove the `r_ausd_v_ta_bp_ref_dag.py` file from the Composer environment's DAGs folder. This will remove the DAG from the Airflow UI.
3.  **Reactivate Legacy Job:**
    *   **Re-enable Original Script:** Re-enable the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_bp_ref.ksh` script in its legacy scheduler (e.g., cron, Autosys) to resume operations on the old platform.
4.  **Data Rollback (if necessary):**
    *   If the `execute_bigquery_reconciliation_logic` task (after `k_ausd_v_ta_bp_ref.ksh` migration) made any irreversible data changes to `ta_bp_ref` or related tables, a data rollback strategy might be required. This could involve:
        *   Restoring tables from a BigQuery snapshot or backup.
        *   Running compensating transactions.
        *   This step is highly dependent on the specific logic of `k_ausd_v_ta_bp_ref.ksh` and should be planned during its migration.