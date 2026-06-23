This document outlines the migration of the `DW.BERT_AUSD_BP_TA_RN_DA_VDA_TK` job from its legacy UC4/KornShell/Oracle environment to Google Cloud Platform (GCP).

---

# MIGRATION_NOTES.md: DW.BERT_AUSD_BP_TA_RN_DA_VDA_TK

## 1. Summary

The `DW.BERT_AUSD_BP_TA_RN_DA_VDA_TK` job, originally an ETL workflow responsible for preparing "instantiated basic products" for the BERT process, has been migrated. This job selects and transforms data related to contract IDs and MSISDNs from a source Oracle table and inserts them into a target Oracle table based on specific date and filtering criteria.

The migration re-platforms this workflow from its original UC4/KornShell/Oracle stack to a cloud-native GCP environment, utilizing:
*   **Airflow** for workflow orchestration.
*   **Horizon Python** for wrapper and control logic.
*   **BigQuery SQL** for data transformation.

## 2. Generated artifacts

The migration process generated the following artifacts:

*   **`bq_ddl/your_dataset.sof_ta_rn_einzeln.sql`**
    *   **Role:** BigQuery Data Definition Language (DDL) script to create the `sof_ta_rn_einzeln` table in the `your_dataset` dataset. This table serves as the BigQuery equivalent of the original Oracle source table.
*   **`bq_ddl/your_dataset.sof_ta_rn_da_vda_tk.sql`**
    *   **Role:** BigQuery DDL script to create the `sof_ta_rn_da_vda_tk` table in the `your_dataset` dataset. This table serves as the BigQuery equivalent of the original Oracle target table.
*   **`bq_ddl/your_metadata_dataset.dwtk_meldungen.sql`**
    *   **Role:** BigQuery DDL script to create the `dwtk_meldungen` table in the `your_metadata_dataset` dataset. This table stores metadata, specifically used to derive the `v_datum` for the transformation, replacing the Oracle `isbert_schema.dwtk_meldungen` table.
*   **`bq_sql/d_ausd_bp_ta_rn_da_vda_tk_bq.sql`**
    *   **Role:** BigQuery SQL script containing the core data transformation logic. This script directly translates the original Oracle SQL (`d_ausd_bp_ta_rn_da_vda_tk.sql`), including the `DECLARE` statement for `v_datum`, `TRUNCATE` of the target table, and `INSERT ... SELECT` operation.
*   **`airflow_dags/dw_bert_ausd_bp_ta_rn_da_vda_tk.py`** (Implicitly generated)
    *   **Role:** Airflow DAG definition file. This Python script orchestrates the entire workflow, replacing the UC4 job definition. It contains a `DataprocSubmitJobOperator` task to execute the Horizon Python script.
*   **`horizon_python/dw_bert_ausd_bp_ta_rn_da_vda_tk.py`** (Implicitly generated)
    *   **Role:** Horizon Python script. This script consolidates the logic from the original KornShell wrapper (`r_ausd_bp_ta_rn_da_vda_tk.ksh`) and core control (`k_ausd_bp_ta_rn_da_vda_tk.ksh`) scripts. It handles parameter parsing, date validation, and executes the BigQuery SQL transformation using `script.func_execute_bq`.

## 3. Key design decisions

The following key design decisions were made during the migration:

*   **Cloud-Native Orchestration with Airflow:** Airflow was chosen to replace UC4 for workflow orchestration due to its cloud-native capabilities, scalability, and robust scheduling and monitoring features within the GCP ecosystem. As no explicit schedule was found in the source UC4 XML, the DAG is configured with `schedule=None`, implying manual or external triggering.
*   **Consolidated Control Logic with Horizon Python:** The logic from the two KornShell scripts (`r_ausd_bp_ta_rn_da_vda_tk.ksh` and `k_ausd_bp_ta_rn_da_vda_tk.ksh`) was consolidated into a single Horizon Python script. This centralizes the control flow, leverages Python's extensive ecosystem for parameter parsing, date handling, and error management, and integrates seamlessly with GCP services via the Horizon Framework.
*   **BigQuery for Data Transformation and Storage:** Oracle SQL transformations were directly translated to BigQuery SQL, and all Oracle tables (`isbert_schema.dwtk_meldungen`, `sof$ta_rn_einzeln`, `sof$ta_rn_da_vda_tk`) were migrated to BigQuery tables. BigQuery offers a serverless, highly scalable, and cost-effective data warehousing solution, aligning with the overall GCP strategy.
*   **Direct DML for Truncation:** The Oracle procedure `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` used for `TRUNCATE` was replaced by a direct `TRUNCATE TABLE` statement in BigQuery SQL, simplifying the transformation logic.
*   **GCP-Native Error Handling and Logging:** The custom KornShell `f_alis_msgerr.ksh` and `DWMSG_*` error handling framework is replaced by Python's `try-except` blocks and integrated with GCP Cloud Logging for centralized log management and monitoring.
*   **Elimination of Temporary Files:** The use of temporary files (e.g., `${DW_DIR_UTL}/bert_k_ausd_bp_ta_rn_da_vda_tk.tmp`) for record counting is replaced by in-memory variables within the Horizon Python script or by leveraging the result metadata directly from BigQuery query execution.

**Notable Trade-offs:**
*   The direct `SQL*Plus` features and specific Oracle hints are no longer applicable and are handled by the Python wrapper or ignored by BigQuery.
*   Custom KornShell utilities for environment setup, date handling, and parameter parsing required re-implementation in Python.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Create the BigQuery datasets: `your_dataset` and `your_metadata_dataset`.
2.  **BigQuery Table Creation:**
    *   Execute the DDL scripts to create the necessary tables:
        *   `bq_ddl/your_dataset.sof_ta_rn_einzeln.sql`
        *   `bq_ddl/your_dataset.sof_ta_rn_da_vda_tk.sql`
        *   `bq_ddl/your_metadata_dataset.dwtk_meldungen.sql`
3.  **Data Ingestion:**
    *   Ingest historical data from the original Oracle source tables (`sof$ta_rn_einzeln` and `isbert_schema.dwtk_meldungen`) into their respective BigQuery counterparts (`your_dataset.sof_ta_rn_einzeln` and `your_metadata_dataset.dwtk_meldungen`).
4.  **IAM Permissions Configuration:**
    *   Ensure the GCP service account used by Airflow/Dataproc has the following minimum permissions:
        *   `BigQuery Data Editor` on `your_dataset` (for `sof_ta_rn_da_vda_tk`).
        *   `BigQuery Data Viewer` on `your_dataset` (for `sof_ta_rn_einzeln`).
        *   `BigQuery Data Viewer` on `your_metadata_dataset` (for `dwtk_meldungen`).
        *   `Dataproc Worker` and `Dataproc Editor` roles (if using a managed Dataproc cluster).
        *   `Storage Object Viewer` and `Storage Object Creator` on the GCS bucket where the Horizon Python script and logs are stored.
5.  **GCP Environment Setup:**
    *   Verify that a Dataproc cluster is available and configured for use by the `DataprocSubmitJobOperator` in Airflow, or ensure on-demand cluster creation is properly set up.
    *   Ensure a GCS bucket is designated for deploying the Horizon Python script and storing any temporary files or logs.
6.  **Configuration Updates:**
    *   Replace all placeholder values (e.g., `your_dataset`, `your_metadata_dataset`, GCP project ID, region, Dataproc cluster name, GCS bucket names) within the Airflow DAG (`airflow_dags/dw_bert_ausd_bp_ta_rn_da_vda_tk.py`) and the Horizon Python script (`horizon_python/dw_bert_ausd_bp_ta_rn_da_vda_tk.py`) with the actual environment-specific values.
7.  **Scheduling:**
    *   The Airflow DAG is configured with `schedule=None`. This means it will not run automatically and must be triggered manually or by an external system. Define the external trigger mechanism if required.

## 5. Known gaps & unresolved references

The following items have been flagged for follow-up or represent known gaps:

*   **`AL??` Comments:** Several `AL??` comments in the original KornShell scripts indicate potentially incomplete or commented-out functionality (e.g., `FOSHoleLadedatum`, `FOSJobDeaktivate`, `FOSJobErzeugeEintrag`). Clarification is needed on whether these features are still required in the migrated solution or if they represent deprecated logic that can be ignored.
*   **`p_wiederanlaufWert` Parameter Usage:** The `p_wiederanlaufWert` parameter is passed to the core SQL execution, but its exact usage within the original `d_ausd_bp_ta_rn_da_vda_tk.sql` script is not explicitly visible. Its role in restartability or incremental processing needs to be fully understood to ensure correct implementation in BigQuery, if applicable.
*   **Commented-out Post-processing:** The `k_ausd_bp_ta_rn_da_vda_tk.ksh` script contains extensive commented-out `sed`, `sort`, and `join` commands for post-processing data files. It must be confirmed if this functionality is intended to be revived in the migration or if it can be ignored. If needed, these transformations would require implementation using BigQuery SQL or PySpark/Python in a subsequent task.
*   **Shared Environment (`.dw_init`)**: The contents of the `.dw_init` script, crucial for setting up the original environment, need to be fully analyzed. All environment variables and paths defined within it must be replicated or replaced in the GCP environment (e.g., via Airflow variables, Kubernetes secrets, or within the Horizon Python script).
*   **Error Handling Framework:** The custom `f_alis_msgerr.ksh` and `DWMSG_*` functions are part of a legacy error handling framework. While replaced by GCP-native logging, any specific custom notification or escalation logic from this framework needs to be identified and re-implemented using GCP services (e.g., Pub/Sub, Cloud Functions, Monitoring Alerts).

## 6. Validation

To ensure the migrated job functions correctly and produces accurate results, the following validation steps should be performed:

1.  **Prerequisites:**
    *   Ensure all manual steps before go-live (Section 4) are completed.
    *   Populate the BigQuery source and metadata tables with representative data from the Oracle source system.
2.  **Execution:**
    *   Trigger the Airflow DAG `dw_bert_ausd_bp_ta_rn_da_vda_tk` manually from the Airflow UI, providing any necessary parameters (e.g., `p_stichtag`, `p_wiederanlaufWert`) if they are exposed as DAG run configurations.
3.  **Verification Steps:**
    *   **Airflow UI:** Monitor the DAG run in the Airflow UI to ensure all tasks complete successfully without errors.
    *   **Cloud Logging:** Check Cloud Logging for the Dataproc job and Horizon Python script execution. Look for any errors, warnings, or unexpected behavior. Verify that parameters are parsed correctly and that the `v_datum` derived from `dwtk_meldungen` matches expectations.
    *   **BigQuery Data Validation:**
        *   **Row Count Comparison:** Compare the row count of the target BigQuery table (`your_dataset.sof_ta_rn_da_vda_tk`) with the row count of the original Oracle target table after running the original job for the same period and input parameters.
        *   **Data Sample Comparison:** Select a representative sample of rows from both the BigQuery target table and the Oracle target table and compare their content to ensure data integrity and transformation accuracy.
        *   **Schema Validation:** Confirm that the schema of the BigQuery target table matches the expected schema and that data types are correct.
    *   **Performance Monitoring:** Observe the execution time of the BigQuery queries and the overall Airflow DAG run. Compare this against the performance of the original UC4 job.
    *   **Cost Analysis:** Monitor the BigQuery query costs to ensure they are within acceptable limits.

**"Passing" Criteria:**

*   The Airflow DAG completes successfully without any failed tasks.
*   No critical errors or unexpected warnings are reported in Cloud Logging for the Dataproc job or Horizon Python script.
*   The row count in `your_dataset.sof_ta_rn_da_vda_tk` exactly matches the row count in the corresponding Oracle target table for the same execution period and parameters.
*   A sample data comparison confirms that the data content in the BigQuery target table is identical to the Oracle target table.
*   The job completes within acceptable performance thresholds and cost limits.

## 7. Rollback procedure

In the event of critical issues or data discrepancies identified after go-live, the following rollback procedure should be initiated:

1.  **Immediate Action:**
    *   Disable the Airflow DAG `dw_bert_ausd_bp_ta_rn_da_vda_tk` in the Airflow UI to prevent further execution of the migrated job.
2.  **Revert to Source System:**
    *   Resume execution of the original UC4 job `DW.BERT_AUSD_BP_TA_RN_DA_VDA_TK` to ensure business continuity and data processing.
3.  **Data Restoration (if necessary):**
    *   If the BigQuery target table (`your_dataset.sof_ta_rn_da_vda_tk`) was corrupted or incorrectly populated by the migrated job, restore it to a known good state. This can be achieved using BigQuery's time travel feature (up to 7 days by default) or by loading a backup if one was created prior to the problematic run.
4.  **Analysis and Remediation:**
    *   Thoroughly investigate the root cause of the failure in the migrated job using Airflow logs, Cloud Logging, and BigQuery query history.
    *   Implement necessary fixes in the Horizon Python script, BigQuery SQL, or Airflow DAG.
    *   Perform comprehensive re-testing in a non-production environment.
5.  **Re-attempt Go-Live:**
    *   Once the issues are resolved and thoroughly tested, follow the go-live procedure again.