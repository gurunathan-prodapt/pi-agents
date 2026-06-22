# MIGRATION_NOTES.md — DW.BERT_AUSD_BP_TA_ICCID_VERTRAG

## 1. Summary

The `DW.BERT_AUSD_BP_TA_ICCID_VERTRAG` job, responsible for aggregating SIM card (ICCID) data at the contract level, has been migrated from its legacy Oracle, KornShell, and Automic (UC4) environment to Google Cloud Platform (GCP).

The migration involved:
*   **Source Platform:** Oracle Database for data storage, KornShell scripts for logic and orchestration, and Automic (UC4) for scheduling.
*   **Target Platform:** Google BigQuery for data storage and transformation, and Google Cloud Composer (Apache Airflow) for orchestration and scheduling.

The core functionality of processing individual ICCID records from `SOF$TA_ICCID_EINZELN`, grouping them by contract ID, and pivoting various ICCID types into distinct columns within `SOF$TA_ICCID_VERTRAG` has been preserved.

## 2. Generated Artifacts

The migration produced the following files:

*   **`bigquery_sql/d_ausd_bp_ta_iccid_vertrag.sql`**
    *   **Role:** This file contains the core data transformation logic, translated from the original Oracle SQL (`d_ausd_bp_ta_iccid_vertrag.sql`) to BigQuery SQL. It is responsible for truncating the target `SOF_TA_ICCID_VERTRAG` table and populating it by aggregating ICCID data from `SOF_TA_ICCID_EINZELN`, grouping by `CNTRCT_ID`, and applying `MAX()` aggregates to pivot various ICCID types into separate columns.

*   **`dags/dag_dw_bert_ausd_bp_ta_iccid_vertrag.py`**
    *   **Role:** This is the Apache Airflow DAG definition file. It replaces the UC4 job definition and the primary KornShell orchestrator scripts (`r_ausd_bp_ta_iccid_vertrag.ksh`, `k_ausd_bp_ta_iccid_vertrag.ksh`). It defines the workflow, including tasks for parameter handling (e.g., `stichtag`, `wiederanlaufwert`) and the execution of the BigQuery SQL transformation.

*   **`utils/bert_utilities.py`**
    *   **Role:** This Python module contains helper functions that re-implement the logic found in various common KornShell utility scripts (e.g., `.dw_init`, `h_alis_date.ksh`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`). It provides functionalities such as date validation, parameter parsing, and centralized logging, allowing the Airflow DAG to operate independently of the legacy UNIX environment.

## 3. Key Design Decisions

*   **Target Platform Selection (BigQuery & Cloud Composer):**
    *   **Why:** BigQuery was chosen for its scalability, performance, and cost-effectiveness as a managed data warehouse solution. Cloud Composer (Apache Airflow) provides a robust, cloud-native orchestration platform, offering flexibility, extensibility, and integration with other GCP services, making it a suitable replacement for UC4 and KornShell-based scheduling.
    *   **Trade-offs:** Requires re-platforming existing Oracle tables and re-implementing shell script logic in Python.

*   **Orchestration Logic Re-implementation in Python:**
    *   **Why:** The complex parameter parsing, date validation, and SQL execution wrapper logic from `r_ausd_bp_ta_iccid_vertrag.ksh` and `k_ausd_bp_ta_iccid_vertrag.ksh` were re-implemented in Python within the Airflow DAG. This leverages Airflow's native capabilities, improves maintainability, testability, and allows for better integration with GCP services compared to attempting to run shell scripts directly in a cloud environment.
    *   **Trade-offs:** Requires a complete rewrite of the shell script logic, necessitating careful analysis to ensure functional parity and avoid introducing regressions.

*   **Direct SQL Translation for Core Transformation:**
    *   **Why:** The core data transformation logic in `d_ausd_bp_ta_iccid_vertrag.sql` (TRUNCATE/INSERT, GROUP BY, `MAX()` aggregates) is largely standard SQL and translates directly to BigQuery SQL with minimal syntax adjustments. This approach minimizes the risk of introducing logical errors in the core data processing and speeds up the migration of this component.
    *   **Trade-offs:** Assumes the existing SQL logic is still valid and desired, despite the original file being marked for "retirement."

*   **Consolidation of Utility Functions:**
    *   **Why:** Common KornShell utilities (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) were consolidated and re-implemented as Python functions in `utils/bert_utilities.py`. This eliminates dependencies on the legacy UNIX environment, promotes code reusability, and centralizes common functionalities within the new Python-based ecosystem.
    *   **Trade-offs:** Requires thorough understanding and accurate re-implementation of potentially undocumented or complex shell utility behaviors.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the BigQuery dataset identified as `DATASET` (e.g., `your_project_id.your_dataset_id`) exists in the target GCP project.

2.  **IAM Permissions Configuration:**
    *   The Google Cloud Service Account used by the Cloud Composer environment (Airflow) must have the necessary IAM roles:
        *   `BigQuery Data Editor` (or equivalent) on the `PROJECT_ID.DATASET` to read from `SOF_TA_ICCID_EINZELN` and write/truncate `SOF_TA_ICCID_VERTRAG`.
        *   `BigQuery Job User` to run BigQuery jobs.
        *   `Storage Object Viewer` and `Storage Object Creator` on the Cloud Composer bucket for DAG deployment and log storage.
        *   Permissions to access `PROJECT_ID.DATASET.DWTK_MELDUNGEN` if logging/metadata is written there.

3.  **Airflow Connection Configuration:**
    *   Verify that the `google_cloud_default` connection is configured correctly in Airflow, pointing to the target GCP project.

4.  **Data Migration:**
    *   The source Oracle tables must be fully migrated to BigQuery:
        *   `SOF$TA_ICCID_EINZELN` -> `PROJECT_ID.DATASET.SOF_TA_ICCID_EINZELN`
        *   `SOF$TA_ICCID_VERTRAG` -> `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG`
        *   `isbert_schema.dwtk_meldungen` -> `PROJECT_ID.DATASET.DWTK_MELDUNGEN` (if still relevant for logging/metadata)
    *   If the Oracle DB link `@pcrs1` (implied by `DEFINE v_carmen = "@pcrs1"`) represents an active data source, the data it points to must also be migrated to BigQuery or made accessible via BigQuery federated queries/external tables.

5.  **Airflow DAG Scheduling:**
    *   Update the `schedule` parameter in `dags/dag_dw_bert_ausd_bp_ta_iccid_vertrag.py` to match the desired production schedule (e.g., `@daily`, `0 5 * * *`).
    *   Determine the exact mechanism for passing `stichtag` and `wiederanlaufwert` to the Airflow DAG (e.g., Airflow Variables, DAG run configuration, or dynamic calculation within the `_r_ausd_bp_ta_iccid_vertrag_logic` task).

## 5. Known Gaps & Unresolved References

*   **SQL Script Retirement Status (B0 - Retire):** The original `d_ausd_bp_ta_iccid_vertrag.sql` was marked for retirement. The current migration proceeds assuming the logic is still required. **Critical follow-up:** Business stakeholders must confirm if this job's functionality is still needed or if it should be decommissioned entirely. If decommissioned, this migration effort is unnecessary.
*   **Completeness of Utility Script Re-implementation:** The Python re-implementation of common KornShell utilities (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) is based on inferred functionality. There might be subtle behaviors or edge cases not fully captured. Thorough testing is required.
*   **`starteSQLSkript` Function Details:** The exact error handling, logging, and connection management behavior of the original `starteSQLSkript` function within `k_ausd_bp_ta_iccid_vertrag.ksh` needs to be fully understood and verified against the BigQueryOperator's capabilities and custom Python logic.
*   **`DEFINE v_carmen = "@pcrs1"`:** The purpose and data source behind the Oracle DB link `@pcrs1` are not fully clear from the provided documentation. If this link represents an active external data dependency, its migration or integration with BigQuery must be addressed.
*   **Airflow Parameter Handling Finalization:** The `_r_ausd_bp_ta_iccid_vertrag_logic` task in the DAG currently uses placeholders for `stichtag` and `wiederanlaufwert`. The definitive method for providing these parameters (e.g., Airflow configuration, dynamic date calculation, or trigger arguments) needs to be finalized and implemented.
*   **Performance Tuning:** The Oracle SQL used `parallel(rp,4)` hint, indicating performance considerations. BigQuery table design (partitioning, clustering) and Airflow task resource allocation should be reviewed and optimized to ensure equivalent or improved performance in GCP.

## 6. Validation

To validate the successful migration and functionality of the `DW.BERT_AUSD_BP_TA_ICCID_VERTRAG` job:

1.  **Unit Tests:**
    *   Run the provided example usage in `utils/bert_utilities.py` to verify the correct functioning of date validation, parameter parsing, and logging utilities.
    *   Develop additional unit tests for any complex Python logic within the Airflow DAG.

2.  **Integration Tests (Manual or Automated):**
    *   **Trigger the Airflow DAG:** Manually trigger `dag_dw_bert_ausd_bp_ta_iccid_vertrag` in Cloud Composer.
    *   **Monitor Task Execution:** Observe the Airflow UI for successful completion of all tasks (`r_ausd_bp_ta_iccid_vertrag_task`, `execute_bq_sql_task`). Check task logs for any errors or warnings.
    *   **Verify Target Table State:**
        *   Query `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG` in BigQuery to confirm it has been truncated and repopulated.
        *   Check the table schema to ensure it matches the expected structure.
    *   **Data Volume Comparison:**
        *   Compare the row count of `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG` with the corresponding `SOF$TA_ICCID_VERTRAG` table in the source Oracle environment for the same processing date (`stichtag`). The counts should match.
    *   **Data Integrity Check:**
        *   Select a representative sample of `CNTRCT_ID`s from both the source Oracle `SOF$TA_ICCID_VERTRAG` and the target BigQuery `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG`.
        *   Compare all pivoted ICCID fields (e.g., `TN_ICCID`, `MS1_IMSI_MCC`, `MS10_VALID_TO`) for these `CNTRCT_ID`s. The data should match exactly.
        *   Consider using a data validation tool or writing a specific BigQuery SQL query to compare checksums or row-by-row differences between source and target for a larger dataset.

3.  **"Passing" Criteria:**
    *   The `dag_dw_bert_ausd_bp_ta_iccid_vertrag` Airflow DAG completes successfully without any failed tasks.
    *   The target BigQuery table `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG` is populated with data.
    *   The row count in the target BigQuery table matches the source Oracle table for the processed data.
    *   Data integrity checks confirm that the transformed data in BigQuery is identical to the data produced by the legacy Oracle job for the same input.
    *   The job completes within the defined Service Level Agreement (SLA) for execution time.

## 7. Rollback Procedure

In case of critical failure or incorrect data generation after go-live:

1.  **Immediate Action:**
    *   **Disable Airflow DAG:** Immediately disable the `dag_dw_bert_ausd_bp_ta_iccid_vertrag` DAG in the Cloud Composer UI to prevent further execution.
    *   **Re-enable Legacy Job:** Re-enable the original `DW.BERT_AUSD_BP_TA_ICCID_VERTRAG` job in Automic (UC4) to ensure business continuity and data generation in the legacy environment.

2.  **Data Rollback (if necessary):**
    *   If the `PROJECT_ID.DATASET.SOF_TA_ICCID_VERTRAG` table was overwritten with incorrect data, attempt to restore it from a previous BigQuery snapshot or backup if available.
    *   Alternatively, if the legacy Oracle job is still running and populating a BigQuery replica, its next successful run will overwrite the BigQuery table with correct data.

3.  **Code Rollback:**
    *   Revert the Airflow DAG file (`dags/dag_dw_bert_ausd_bp_ta_iccid_vertrag.py`), BigQuery SQL (`bigquery_sql/d_ausd_bp_ta_iccid_vertrag.sql`), and any associated Python utility code (`utils/bert_utilities.py`) to the last known stable version in the version control system.
    *   Redeploy the reverted code to Cloud Composer.

4.  **Root Cause Analysis:**
    *   Investigate the failure using Airflow task logs, BigQuery job history, and Cloud Logging to identify the root cause of the issue in the GCP environment.
    *   Address the identified issues in a development or staging environment before attempting another go-live.