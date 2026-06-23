# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the legacy KornShell orchestration script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_valid.ksh`. This script was responsible for setting up the environment, handling parameters, managing custom logging, and invoking the core business logic script `k_ausd_v_ta_cntrct_valid.ksh` for contract data validation.

The job has been migrated from a KornShell-based execution environment to a cloud-native architecture on Google Cloud Platform. The orchestration layer has been re-platformed to **Apache Airflow**, with the core logic (derived from `k_ausd_v_ta_cntrct_valid.ksh` after its separate analysis) implemented using **BigQuery SQL** and/or **Python scripts leveraging the BigQuery client library**.

## 2. Generated Artifacts

The migration process resulted in the following key artifacts:

*   **`r_ausd_v_ta_cntrct_valid_dag.py`**: An Airflow Directed Acyclic Graph (DAG) written in Python. This DAG replaces the original KornShell wrapper script, handling environment setup, parameter parsing, task orchestration, logging, and error handling using Airflow's native capabilities.
*   **`ta_cntrct_valid_core_logic.sql` (or similar)**: One or more BigQuery SQL scripts that encapsulate the data transformation and validation logic originally found within `k_ausd_v_ta_cntrct_valid.ksh`. These scripts are executed by the Airflow DAG.
*   **`ta_cntrct_valid_core_logic.py` (optional)**: A Python script (or module) utilizing the BigQuery client library, if the core logic from `k_ausd_v_ta_cntrct_valid.ksh` required complex procedural logic or API interactions not suitable for pure SQL. This script would be invoked by a `PythonOperator` within the Airflow DAG.
*   **`dw_utils.py`**: A Python module containing re-implemented functionalities from the legacy utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`). This module provides common functions for logging, parameter handling, and date operations, integrated with Airflow's ecosystem.

## 3. Key Design Decisions

The following key design decisions guided the migration:

*   **Orchestration Re-platforming to Airflow:** The KornShell wrapper's orchestration role was migrated to an Airflow DAG. This decision was driven by Airflow's robust scheduling, monitoring, dependency management, and native integration with GCP services, providing a modern, scalable, and observable orchestration layer.
*   **Core Logic to BigQuery SQL/Python:** The business logic from `k_ausd_v_ta_cntrct_valid.ksh` was translated into BigQuery SQL or Python scripts interacting with BigQuery. This leverages BigQuery's serverless, highly scalable, and cost-effective data warehousing capabilities for efficient data processing.
*   **Native Airflow Logging and Error Handling:** The custom `DWMSG_` functions and `trap` statements for logging and error handling were replaced by Airflow's built-in mechanisms. This standardizes logging, provides centralized access via the Airflow UI, and integrates with cloud monitoring and alerting systems.
*   **Cloud-Native Environment Management:** The environment initialization (`. $HOME/.dw_init`) was replaced by Airflow Variables, Connections, and Python-based configuration management. This aligns with cloud best practices for secure and manageable environment configurations.
*   **Re-implementation of Utility Functions:** Custom KornShell utility scripts were re-implemented as Python modules. This ensures that all components of the migrated job are within a unified Python ecosystem, simplifying maintenance and development.
*   **Trade-offs:** The primary trade-off involved the re-implementation effort for custom utility functions and the learning curve for Airflow and Python for teams accustomed to KornShell. However, the long-term benefits of improved maintainability, scalability, and integration with modern cloud platforms outweigh these initial costs.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (e.g., `dw_prod_dataset`) exists.
    *   Create any necessary tables (e.g., `ta_cntrct_valid`) and their schemas within this dataset, if not already present.
    *   Create any required staging or intermediate tables as identified during the `k_ausd_v_ta_cntrct_valid.ksh` analysis.
2.  **IAM & Permissions:**
    *   Create a dedicated Google Cloud Service Account for the Airflow environment.
    *   Grant this Service Account the necessary IAM roles to:
        *   Read/Write data in the target BigQuery dataset(s) (`BigQuery Data Editor`).
        *   Execute BigQuery jobs (`BigQuery Job User`).
        *   Access any Cloud Storage buckets used for staging or logging (`Storage Object Viewer/Creator`).
        *   Access Secret Manager if secrets are stored there.
3.  **Airflow Connections:**
    *   Create an Airflow Connection (e.g., `google_cloud_default`) configured to use the dedicated Service Account for BigQuery interactions.
4.  **Airflow Variables/Secrets:**
    *   Identify and configure any environment variables or sensitive parameters previously set by `$HOME/.dw_init` or used by `k_ausd_v_ta_cntrct_valid.ksh` as Airflow Variables or securely store them in Google Secret Manager and integrate with Airflow.
    *   Examples: `BERT_DIR_ROOT`, `JobKennung` (if dynamic), any database credentials (though BigQuery uses IAM).
5.  **DAG Deployment:**
    *   Upload the `r_ausd_v_ta_cntrct_valid_dag.py` file and any dependent Python modules (`dw_utils.py`, `ta_cntrct_valid_core_logic.py`) to the Airflow DAGs folder.
6.  **Scheduling Configuration:**
    *   Configure the desired schedule for the `r_ausd_v_ta_cntrct_valid_dag.py` within the Airflow UI, matching the legacy job's frequency.
7.  **Monitoring & Alerting:**
    *   Set up appropriate monitoring and alerting in Google Cloud Operations Suite (formerly Stackdriver) for the Airflow DAG's execution status, BigQuery job failures, and resource utilization.

## 5. Known Gaps & Unresolved References

The following items were identified as gaps or risks during the design phase and require continued attention:

*   **Core Logic Analysis (`k_ausd_v_ta_cntrct_valid.ksh`)**: The most critical unresolved item is the detailed analysis and migration of the `k_ausd_v_ta_cntrct_valid.ksh` script. Its specific content, data sources, transformation logic, and external dependencies (e.g., other databases, files) were not part of this design document and are crucial for the complete migration. This is a **B4 item** requiring a separate, detailed design and implementation.
*   **Custom `DWMSG_` Functions**: While replaced by Airflow's native logging, the exact output format and integration points for the legacy `DWMSG_` functions (e.g., for downstream monitoring or reporting systems) need to be fully validated to ensure no loss of functionality or compatibility.
*   **Environment Initialization (`.dw_init`)**: The full contents and implications of `$HOME/.dw_init` were not exhaustively analyzed. While common environment variables are expected to be handled, any highly specific or complex configurations within this script need to be verified and accurately replicated in the Airflow environment.

## 6. Validation

Validation of the migrated job involves several stages to ensure functional equivalence and performance:

1.  **Unit Testing:**
    *   Run unit tests for all custom Python modules (`dw_utils.py`, `ta_cntrct_valid_core_logic.py`) to verify individual function correctness.
2.  **Airflow DAG Integration Testing:**
    *   Manually trigger the `r_ausd_v_ta_cntrct_valid_dag.py` in a development Airflow environment.
    *   Verify that all tasks execute in the correct order and complete successfully.
    *   Check Airflow logs for any errors, warnings, or unexpected behavior.
    *   Test with various parameters (if applicable) to ensure correct parsing and execution flow.
3.  **Data Validation:**
    *   Execute the migrated Airflow DAG against a representative dataset.
    *   Run the legacy `r_ausd_v_ta_cntrct_valid.ksh` job against the *same* dataset in the legacy environment.
    *   Compare the output data in the target BigQuery table (`ta_cntrct_valid`) with the output from the legacy system.
    *   "Passing" means that the data produced by the migrated job is **identical** or **functionally equivalent** (as per agreed-upon business rules) to the data produced by the legacy job. This includes row counts, column values, and data types.
4.  **Performance Testing:**
    *   Measure the execution time of the migrated Airflow DAG and its individual tasks.
    *   Compare against the performance of the legacy job to ensure it meets or exceeds performance SLAs.
    *   Monitor BigQuery slot usage and costs.

## 7. Rollback Procedure

In the event of critical issues or failures after go-live, the following rollback procedure should be followed:

1.  **Disable New Job:**
    *   Immediately pause or un-schedule the `r_ausd_v_ta_cntrct_valid_dag.py` in the Airflow UI to prevent further execution.
2.  **Re-enable Legacy Job:**
    *   Re-enable and re-schedule the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_valid.ksh` job in the legacy environment.
3.  **Data Reversion (if necessary):**
    *   If the migrated job has written or modified data in BigQuery, assess the impact.
    *   If data corruption or incorrect data was written, revert the affected BigQuery tables to a previous state using BigQuery's time travel capabilities or by restoring from a backup/snapshot, if available. This step requires careful planning and should be executed only if data integrity is compromised.
4.  **Investigation:**
    *   Analyze Airflow logs, BigQuery job logs, and Cloud Monitoring metrics to identify the root cause of the failure.
    *   Address the identified issues in the migrated code or configuration.
5.  **Re-deployment & Re-validation:**
    *   Once issues are resolved, re-deploy the updated Airflow DAG and repeat the validation steps before attempting another go-live.