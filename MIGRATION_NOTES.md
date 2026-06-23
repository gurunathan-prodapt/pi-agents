# MIGRATION_NOTES for DW.BERT_AUSD_BP_TA_APN_VERTRAG

## 1. Summary

The legacy job `DW.BERT_AUSD_BP_TA_APN_VERTRAG`, responsible for preparing instantiated base products by processing and aggregating Access Point Name (APN) and contract reference data, has been migrated.

**Original Platform:**
*   **Orchestration:** UC4 scheduler
*   **Control Logic:** KornShell script (`k_ausd_bp_ta_apn_vertrag.ksh`)
*   **Data Storage & Transformation:** Oracle PL/SQL script (`d_ausd_bp_ta_apn_vertrag.sql`) interacting with Oracle database tables (`isbert_schema.dwtk_meldungen`, `sof$ta_bpr_apn`, `sof$ta_apn_vertrag`).

**Target Platform:**
*   **Orchestration:** Apache Airflow on Google Cloud Composer
*   **Control Logic:** Python script (`k_ausd_bp_ta_apn_vertrag_wrapper.py`) executed by an Airflow `PythonOperator`
*   **Data Storage & Transformation:** Google BigQuery Standard SQL script (`d_ausd_bp_ta_apn_vertrag_bq.sql`) executed by an Airflow `BigQueryOperator`, interacting with BigQuery tables (`isbert_schema.dwtk_meldungen`, `sof_ta_bpr_apn`, `sof_ta_apn_vertrag`).

## 2. Generated artifacts

The migration produced the following files:

*   **`dags/dw_bert_ausd_bp_ta_apn_vertrag.py`**
    *   **Role:** This is the main Airflow Directed Acyclic Graph (DAG) definition file. It orchestrates the entire workflow. It defines two primary tasks: `execute_control_script` (a `PythonOperator` to handle the control logic) and `execute_bq_sql` (a `BigQueryOperator` to run the data transformation). It sets up the DAG's metadata, schedule (currently `None`), and dependencies.
*   **`dags/k_ausd_bp_ta_apn_vertrag_wrapper.py`**
    *   **Role:** This Python script replaces the functionality of the original KornShell script (`k_ausd_bp_ta_apn_vertrag.ksh`). It handles parameter parsing, date validation, and dynamically loads the BigQuery SQL transformation from `d_ausd_bp_ta_apn_vertrag_bq.sql`. It then pushes the rendered SQL content to Airflow's XComs, making it available for the subsequent `BigQueryOperator` task.
*   **`dags/d_ausd_bp_ta_apn_vertrag_bq.sql`**
    *   **Role:** This file contains the core data transformation logic, translated from the Oracle PL/SQL script (`d_ausd_bp_ta_apn_vertrag.sql`) into BigQuery Standard SQL. It declares a `v_datum` variable, truncates the target table, and then inserts aggregated APN and contract reference data using set-based operations (`STRING_AGG` and `GROUP BY`).

## 3. Key design decisions

*   **Orchestration Re-platforming (UC4 to Airflow):** Airflow on Cloud Composer was chosen as the target orchestration platform due to its managed nature, Python-based DAGs, and native integration with Google Cloud services. This replaces the legacy UC4 scheduler.
*   **Data Platform Migration (Oracle to BigQuery):** BigQuery was selected for data storage and transformation due to its scalability, performance for analytical workloads, and cost-effectiveness. This necessitated the conversion of Oracle SQL/PL/SQL to BigQuery Standard SQL.
*   **Control Logic Translation (KornShell to PythonOperator):** The KornShell script's control flow, parameter handling, and environment setup were re-implemented in Python. This logic is encapsulated within a `PythonOperator` in the Airflow DAG, allowing for seamless integration with Airflow's ecosystem and leveraging Python's robust libraries.
*   **Procedural to Set-Based SQL Transformation:** The original Oracle PL/SQL script used a cursor-based, row-by-row processing approach. This was fundamentally redesigned into a more efficient, set-based BigQuery Standard SQL query utilizing `STRING_AGG` and `GROUP BY` clauses. This aligns with BigQuery's strengths and best practices for large-scale data processing.
*   **Dynamic SQL Loading via XComs:** The BigQuery SQL script (`d_ausd_bp_ta_apn_vertrag_bq.sql`) is loaded as a template by the `k_ausd_bp_ta_apn_vertrag_wrapper.py` (PythonOperator) and then pushed to Airflow XComs. The subsequent `BigQueryOperator` pulls this SQL from XComs for execution. This design allows for dynamic SQL generation or parameterization within the Python layer before execution in BigQuery.
*   **Direct `v_datum` Declaration in BQ SQL:** The logic to derive `v_datum` from `dwtk_meldungen` was directly translated into a `DECLARE` statement within the BigQuery SQL script. This keeps the date derivation logic close to where it's used and leverages BigQuery's SQL capabilities.
*   **BigQuery Naming Conventions:** Oracle table names like `sof$ta_apn_vertrag` were adjusted to `sof_ta_apn_vertrag` to conform to BigQuery's recommended naming conventions (e.g., avoiding special characters like `$`).
*   **Trade-offs:**
    *   **Increased Python Wrapper Complexity:** Re-implementing KornShell utilities and parameter parsing in Python adds complexity to the `k_ausd_bp_ta_apn_vertrag_wrapper.py` script compared to a direct `BigQueryOperator` if the SQL were static. However, it provides flexibility for future enhancements and maintains the original job's control logic separation.
    *   **XCom Usage:** While effective for passing the SQL string, XComs have size limitations. For very large SQL scripts or complex data structures, alternative methods (e.g., GCS for temporary storage) might be considered. For this use case, it's appropriate.

## 4. Manual steps before go-live

Before the migrated job can be run in production, the following manual steps are required:

1.  **BigQuery Dataset Creation:**
    *   Ensure the BigQuery datasets `isbert_schema` and `sof` exist in the target GCP project. If not, create them.
2.  **BigQuery Table Creation:**
    *   Create the following BigQuery tables with appropriate schemas, mirroring their Oracle counterparts:
        *   `isbert_schema.dwtk_meldungen`
        *   `sof.sof_ta_bpr_apn`
        *   `sof.sof_ta_apn_vertrag` (target table)
    *   Ensure the `sof.sof_ta_apn_vertrag` table has columns `cntrct_id`, `apn_list`, and `cntrct_ref_list` with appropriate data types (e.g., `STRING`).
3.  **Data Ingestion:**
    *   Ingest historical and ongoing data from the Oracle source tables (`isbert_schema.dwtk_meldungen`, `sof$ta_bpr_apn`) into their respective BigQuery tables. This is crucial for the `v_datum` calculation and the main aggregation logic.
4.  **IAM/Permissions:**
    *   Ensure the Google Cloud service account associated with the Cloud Composer environment has the necessary BigQuery permissions:
        *   `BigQuery Data Editor` on the `sof` dataset (for `TRUNCATE` and `INSERT` into `sof.sof_ta_apn_vertrag`).
        *   `BigQuery Data Viewer` on the `isbert_schema` dataset (for `SELECT` from `isbert_schema.dwtk_meldungen`).
        *   `BigQuery Data Viewer` on the `sof` dataset (for `SELECT` from `sof.sof_ta_bpr_apn`).
5.  **Airflow Connection Strings:**
    *   Verify that the `google_cloud_default` BigQuery connection is correctly configured in Airflow, pointing to the target GCP project.
6.  **DAG Deployment:**
    *   Copy the three generated files (`dw_bert_ausd_bp_ta_apn_vertrag.py`, `k_ausd_bp_ta_apn_vertrag_wrapper.py`, `d_ausd_bp_ta_apn_vertrag_bq.sql`) to the DAGs folder of the Cloud Composer environment.
7.  **Scheduling:**
    *   The DAG is currently configured with `schedule=None`. Determine the appropriate schedule (e.g., daily, hourly) based on business requirements and update the `schedule` parameter in `dw_bert_ausd_bp_ta_apn_vertrag.py` accordingly. Alternatively, if it's an externally triggered job, keep `schedule=None`.

## 5. Known gaps & unresolved references

The following items were identified during the migration and require further attention or confirmation:

*   **UC4 Includes (`DW.HOLE_PFAD`, `DW.BERT_LESE_LOG`):** The exact functionality of these UC4 include files was not fully determined. They likely contain common path definitions and logging configurations.
    *   **Follow-up:** Analyze these files in the legacy system. Path definitions should be translated to Airflow Variables or environment variables. Logging is now handled by Airflow's native logging to Cloud Logging.
*   **KornShell Utility Scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`):** The helper scripts sourced by `k_ausd_bp_ta_apn_vertrag.ksh` contain shared logic. While core parameter parsing and date validation were reimplemented, a comprehensive review of all utility functions is needed.
    *   **Follow-up:** Review each utility script for critical logic that might need to be translated into Python functions or replaced by Airflow/Python equivalents.
*   **KornShell Commented Code:** The `sed`, `sort`, `join` commands in the original ksh script were commented out.
    *   **Follow-up:** Confirm with business owners if this functionality is ever used or intended for future use. If so, it would require additional design and implementation in BigQuery SQL or Python.
*   **Oracle Utilities (`isbert_schema.DWPA_UTIL_SKRIPT.runstatement`, `SPR_SCHEMA.SPR$PA_ANALYZE.ANALYZE_OBJECTS`):** These Oracle-specific calls were removed during migration. `TRUNCATE` was replaced with BigQuery DDL. `ANALYZE_OBJECTS` is generally not required in BigQuery due to its columnar storage and automatic statistics management.
    *   **Follow-up:** Confirm that the removal of `ANALYZE_OBJECTS` does not impact any downstream processes or performance expectations.
*   **`dwtk_meldungen` Migration and Population:** The `v_datum` variable relies on `isbert_schema.dwtk_meldungen`.
    *   **Follow-up:** Ensure this table is correctly migrated to BigQuery and is being populated with up-to-date `timecreated` and `job_kennung` data for `BERT_DROP_TEMP_TABLE` entries, as its accuracy directly impacts the `v_datum` calculation.
*   **Airflow DAG Schedule:** The DAG is currently set to `schedule=None`.
    *   **Follow-up:** Define the appropriate schedule for the DAG based on the original UC4 schedule or new business requirements.

## 6. Validation

To validate the successful migration and functionality of the `DW.BERT_AUSD_BP_TA_APN_VERTRAG` job:

1.  **Trigger the DAG:**
    *   In the Airflow UI, navigate to the `dw_bert_ausd_bp_ta_apn_vertrag` DAG.
    *   Manually trigger a run.
2.  **Monitor Execution:**
    *   Observe the DAG run in the Airflow UI. Ensure both `execute_control_script` and `execute_bq_sql` tasks complete successfully (green status).
    *   Check the task logs for any errors or warnings. The `execute_control_script` task logs should show successful parameter parsing and SQL loading.
3.  **Verify Data in BigQuery:**
    *   After a successful DAG run, query the target table `sof.sof_ta_apn_vertrag` in BigQuery.
    *   **"Passing" Criteria:**
        *   **Row Count:** Compare the number of rows in `sof.sof_ta_apn_vertrag` with the expected row count from the original Oracle `sof$ta_apn_vertrag` table for the same processing date.
        *   **Data Integrity:** Sample data from `sof.sof_ta_apn_vertrag` and compare `cntrct_id`, `apn_list`, and `cntrct_ref_list` values against the corresponding data in the Oracle source. Pay close attention to the aggregated `apn_list` and `cntrct_ref_list` to ensure correct concatenation and truncation.
        *   **`v_datum` Accuracy:** Verify that the `v_datum` used in the BigQuery SQL (which can be inspected in the `execute_bq_sql` task logs if the SQL is printed) correctly reflects the `MAX(m.timecreated)` from `isbert_schema.dwtk_meldungen` for `BERT_DROP_TEMP_TABLE`.
        *   **No Errors:** No errors or unexpected behavior observed in Airflow logs or BigQuery job history.

## 7. Rollback procedure

In case of issues or critical failures after go-live, the following rollback procedure can be initiated:

1.  **Disable Airflow DAG:**
    *   In the Airflow UI, toggle off the `dw_bert_ausd_bp_ta_apn_vertrag` DAG to prevent further runs.
2.  **Re-enable Legacy UC4 Job:**
    *   Re-activate the original `DW.BERT_AUSD_BP_TA_APN_VERTRAG` job in the UC4 scheduler.
3.  **Data State Assessment:**
    *   Assess the state of the BigQuery target table `sof.sof_ta_apn_vertrag`. If the BigQuery job modified data incorrectly, it might be necessary to:
        *   Restore `sof.sof_ta_apn_vertrag` from a previous snapshot or backup if available.
        *   Alternatively, if the Oracle source tables were not modified by the BigQuery job (which is the case here), the Oracle system should be able to resume processing without data integrity issues.
4.  **Revert BigQuery Schema/Table Changes (if applicable):**
    *   If any BigQuery schema or table structures were modified specifically for this migration and are incompatible with the rollback, revert them to their pre-migration state.
5.  **Monitor Legacy Job:**
    *   Monitor the re-enabled UC4 job to ensure it runs successfully and processes data as expected.
6.  **Remove Migrated Artifacts (Optional):**
    *   Once the rollback is confirmed stable, the migrated DAG files can be removed from the Cloud Composer DAGs folder.