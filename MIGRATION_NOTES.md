# MIGRATION_NOTES.md

## 1. Summary

The KornShell orchestration script `r_ausd_v_ta_p_vertrag.ksh`, responsible for synchronizing contract data within the `ta_p_vertrag` table, has been migrated. The migration targets Google Cloud Platform (GCP), utilizing **Cloud Composer (Airflow)** for workflow orchestration and **BigQuery** for data storage and transformation. The original script's functions, including environment setup, parameter parsing, error trapping, logging, and the orchestration of a core synchronization script (`k_ausd_v_ta_p_vertrag.ksh`), have been re-implemented within the GCP ecosystem.

## 2. Generated Artifacts

The migration has produced the following primary artifact:

*   **`vertragsdatenabgleich_ta_p_vertrag.py`** (Airflow DAG)
    *   **Role:** This Python file defines an Airflow Directed Acyclic Graph (DAG) that orchestrates the contract data synchronization process. It replaces the original KornShell script by managing environment initialization, parsing parameters, executing the core synchronization logic (via a placeholder BigQuery SQL task), and handling logging and error reporting using Airflow's native capabilities integrated with GCP services.

## 3. Key Design Decisions

Several key design decisions were made to transition from the KornShell environment to GCP:

*   **Orchestration Platform:** **Cloud Composer (Airflow)** was chosen to replace the KornShell script's orchestration capabilities. Airflow provides robust scheduling, dependency management, monitoring, and a Python-based environment for extensibility, significantly improving upon the custom shell scripting framework.
*   **Data Storage and Transformation:** **BigQuery** is the target for the `ta_p_vertrag` table and the core synchronization logic (originally in `k_ausd_v_ta_p_vertrag.ksh`). This leverages BigQuery's scalability, performance for analytical workloads, and native SQL capabilities for data manipulation.
*   **Logging and Monitoring:** Airflow's native logging, which integrates seamlessly with **Cloud Logging**, and its ability to trigger alerts via **Cloud Monitoring**, replaces the custom `DWMSG_` functions and `trap` commands used in the original KornShell script. This provides a standardized, centralized, and more powerful logging and alerting solution.
*   **Parameter Handling:** The `getopts` logic from the KornShell script is replaced by **Airflow DAG run configuration** and Python code within the DAG. Parameters are passed as DAG run configurations and can be shared between tasks using Airflow's XComs.
*   **Utility Script Reimplementation:** Common KornShell utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) are either reimplemented as **Python functions** within the DAG or separate Python modules, or their functionality is replaced by native Airflow/GCP features (e.g., environment variables, Airflow hooks).
*   **Core Logic Execution:** The call to `k_ausd_v_ta_p_vertrag.ksh` is replaced by an Airflow task, specifically a `BigQueryExecuteQueryOperator`, which will execute the migrated BigQuery SQL logic. This ensures the core data synchronization happens directly within BigQuery.

**Notable Trade-offs:**

*   **Dependency on `k_ausd_v_ta_p_vertrag.ksh`:** The full effectiveness of this migration is contingent on the complete and accurate migration of the core logic within `k_ausd_v_ta_p_vertrag.ksh`. Until this is done, the `execute_core_sync` task remains a placeholder.
*   **Increased Complexity for Simple Tasks:** While Airflow offers powerful features, migrating a relatively simple shell orchestrator to a full-fledged DAG might introduce an initial learning curve and overhead compared to maintaining a shell script. However, this is justified by the long-term benefits of scalability, observability, and maintainability on GCP.
*   **Shift in Skillset:** Maintenance and development will shift from KornShell scripting to Python and BigQuery SQL, requiring a different skillset from the original maintainers.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps are required:

1.  **BigQuery Dataset and Table Creation:**
    *   Create the target BigQuery dataset (e.g., `your_dataset`) if it doesn't already exist.
    *   Create the `ta_p_vertrag` table in BigQuery with the correct schema, including appropriate partitioning and clustering keys for performance optimization.
    *   Ensure any `source_vertrag` table or other upstream dependencies are also present and accessible in BigQuery.

2.  **IAM Permissions:**
    *   Ensure the Cloud Composer service account has the necessary IAM roles:
        *   `BigQuery Data Editor` (or more granular roles like `BigQuery Data Editor` for specific datasets/tables) to read from and write to `ta_p_vertrag` and other related tables.
        *   `BigQuery Job User` to run BigQuery queries.
        *   `Cloud Logging Writer` to write logs to Cloud Logging.
        *   `Cloud Monitoring Metric Writer` to send metrics to Cloud Monitoring.

3.  **Airflow Connection Configuration:**
    *   Verify that the `google_cloud_default` Airflow connection is correctly configured in your Cloud Composer environment, pointing to the target GCP project.

4.  **Secrets Management (if applicable):**
    *   If any sensitive parameters or credentials are required beyond what's passed via DAG run configuration, configure them using Airflow's Secrets Backend (e.g., GCP Secret Manager integration).

5.  **Scheduling Configuration:**
    *   The `schedule` parameter in the DAG is currently set to `None`. If the job requires scheduled execution (e.g., daily, hourly), update the `schedule` parameter in `vertragsdatenabgleich_ta_p_vertrag.py` accordingly (e.g., `schedule="@daily"`).

6.  **Complete `k_ausd_v_ta_p_vertrag.ksh` Migration:**
    *   **Crucially**, the core synchronization logic from `k_ausd_v_ta_p_vertrag.ksh` must be fully analyzed, translated into optimized BigQuery SQL (e.g., using `MERGE` statements for UPSERTs or `INSERT OVERWRITE` for full loads), and integrated into the `execute_core_sync` task within the `vertragsdatenabgleich_ta_p_vertrag.py` DAG. The current SQL in the DAG is a placeholder.

## 5. Known Gaps & Unresolved References

The following items are flagged for follow-up and represent potential risks or areas requiring further development:

*   **`k_ausd_v_ta_p_vertrag.ksh` Content (B4 Item):** The most significant unresolved item. The actual source code and logic of `k_ausd_v_ta_p_vertrag.ksh` are unknown. This is a **Blocker (B4)** for the complete migration of the core synchronization logic. The `execute_core_sync` task currently contains only a generic placeholder SQL. A detailed analysis and translation of this script are mandatory.
*   **Custom `DWMSG_` Functions:** While a general approach for logging and error handling has been defined, the exact functionality and criticality of all `DWMSG_` functions (e.g., `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_SetzeStatusOK`) need to be fully understood to ensure their proper and complete replacement with Cloud Logging/Monitoring capabilities.
*   **Missing Complexity/Automation Rate:** The absence of `file_complexity` and `automation_rate` for the source script means the initial effort estimation was based on manual analysis, potentially overlooking hidden complexities.
*   **Performance Implications:** Once the `k_ausd_v_ta_p_vertrag.ksh` logic is migrated to BigQuery SQL, its performance needs to be thoroughly evaluated. Optimization for BigQuery's columnar storage and distributed query execution might be required.
*   **`schedule` Parameter:** The DAG's `schedule` is currently `None`. If the job is intended to run on a recurring basis, this needs to be explicitly defined.
*   **`email_on_failure` Configuration:** The `email_on_failure` default argument is set to `False`. For production readiness, this should be configured to `True` with appropriate email addresses for alerts.

## 6. Validation

To validate the successful migration and functionality of the `vertragsdatenabgleich_ta_p_vertrag` DAG:

1.  **Deployment:**
    *   Deploy the `vertragsdatenabgleich_ta_p_vertrag.py` file to the DAGs folder of your Cloud Composer environment.

2.  **Triggering Tests:**
    *   **Manual Trigger:** In the Airflow UI, manually trigger the `vertragsdatenabgleich_ta_p_vertrag` DAG.
    *   **Configuration:** When triggering, provide the necessary parameters in the "Config" JSON field, for example:
        ```json
        {
            "JobKennung": "TEST_SYNC_CONTRACTS",
            "DW_EintragsNr": "12345"
        }
        ```
    *   **Scheduled Trigger (if configured):** If a schedule is defined, observe the DAG run at its scheduled time.

3.  **Monitoring and Verification:**
    *   **Airflow UI:** Monitor the DAG run status in the Airflow UI. All tasks (`initialize_environment`, `parse_parameters`, `execute_core_sync`, `handle_success`) should complete successfully (turn green).
    *   **Cloud Logging:** Check the Cloud Composer logs in Cloud Logging for any errors, warnings, or unexpected behavior. Verify that the custom print statements from the Python tasks appear as expected.
    *   **BigQuery Data Validation:**
        *   Query the `ta_p_vertrag` table in BigQuery to confirm that the data has been synchronized correctly according to the expected logic (once `k_ausd_v_ta_p_vertrag.ksh` is fully migrated).
        *   Compare the state of `ta_p_vertrag` after the Airflow run with the expected outcome based on the source system's data.
    *   **Error Handling Test:**
        *   To test the `handle_failure_task`, intentionally introduce an error in the `execute_core_sync` task (e.g., an invalid SQL query) and trigger the DAG. Verify that `handle_failure_task` is invoked and logs the failure as expected.
    *   **Cloud Monitoring:** If Cloud Monitoring alerts are configured, verify that they trigger correctly on simulated failures and that expected metrics (if any) are being reported.

**"Passing" Criteria:**

*   The `vertragsdatenabgleich_ta_p_vertrag` DAG completes successfully (green status) in the Airflow UI.
*   No critical errors or unexpected warnings are observed in Cloud Logging.
*   The `ta_p_vertrag` table in BigQuery reflects the correct and expected synchronized data, demonstrating data integrity.
*   Parameter parsing and environment initialization tasks execute as intended.
*   Error handling mechanisms (e.g., `on_failure_callback`) function correctly when failures occur.

## 7. Rollback Procedure

In case of issues or unexpected behavior after go-live, the following rollback procedure can be followed:

1.  **Disable/Delete Airflow DAG:**
    *   In the Airflow UI, disable the `vertragsdatenabgleich_ta_p_vertrag` DAG to prevent further executions.
    *   Alternatively, delete the DAG file from the Cloud Composer DAGs folder.

2.  **Re-enable Original System:**
    *   Ensure the original `r_ausd_v_ta_p_vertrag.ksh` script and its dependencies are fully operational and can be re-enabled in the legacy environment.
    *   Verify that the original job can resume its normal operation.

3.  **Data Reversion (if necessary):**
    *   If the migrated job made incorrect or undesirable changes to the `ta_p_vertrag` table in BigQuery, revert the table to a previous state. This can be done by:
        *   Utilizing BigQuery's [Time Travel](https://cloud.google.com/bigquery/docs/data-manipulation-language#time_travel) feature to restore the table to a point before the problematic run.
        *   Restoring the table from a backup if available.
        *   Running a reverse ETL process or a corrective SQL script to undo the changes.

4.  **Investigate and Rectify:**
    *   Analyze the logs from Cloud Logging and Airflow to identify the root cause of the issue.
    *   Address the identified problems in the `vertragsdatenabgleich_ta_p_vertrag.py` DAG or the underlying BigQuery SQL.
    *   Once rectified, re-deploy the updated DAG and re-attempt the migration.