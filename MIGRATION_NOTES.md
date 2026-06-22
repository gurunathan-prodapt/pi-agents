# MIGRATION_NOTES.md for DW.BERT_AUSD_V_TA_CNTRCT_CRS3

## 1. Summary

This migration involved re-platforming the `DW.BERT_AUSD_V_TA_CNTRCT_CRS3` job, which is responsible for updating contract data, including twin-bill information, into the `sof$ta_cntrct_crs3` table.

The original system used:
*   **Orchestration:** UC4 job executing KornShell scripts (`r_ausd_v_ta_cntrct_crs3.ksh`, `k_ausd_v_ta_cntrct_crs3.ksh`).
*   **Data Processing:** Oracle SQL script (`d_ausd_v_ta_cntrct_crs3.sql`) running against an Oracle database.

The job has been migrated to Google Cloud Platform, utilizing:
*   **Target Platform:** Google Cloud Platform (GCP).
*   **Orchestration:** Apache Airflow on Cloud Composer.
*   **Data Processing:** Google BigQuery for data storage and transformation.

The core logic of truncating and populating the `sof$ta_cntrct_crs3` table (now `sof_dataset_target.ta_cntrct_crs3`) from `sof$ta_cntrct_crs2` (now `sof_dataset_target.ta_cntrct_crs2`) and `isbert_schema.dwtk_meldungen` (now `isbert_schema_target.dwtk_meldungen`) has been preserved and translated to BigQuery SQL.

## 2. Generated Artifacts

The migration produced the following files:

*   **`d_ausd_v_ta_cntrct_crs3.sql`**
    *   **Role:** This file contains the BigQuery SQL script that performs the core data transformation logic. It replaces the original Oracle SQL script (`d_ausd_v_ta_cntrct_crs3.sql`). It declares and sets a `v_datum` variable, truncates the target table (`sof_dataset_target.ta_cntrct_crs3`), and then inserts processed contract data, including twin-bill information, by joining and unioning data from `sof_dataset_target.ta_cntrct_crs2`.
    *   **Location:** This SQL is embedded directly within the Airflow DAG for execution by a `BigQueryExecuteQueryOperator`.

*   **`dw_bert_ausd_v_ta_cntrct_crs3.py`**
    *   **Role:** This is the Apache Airflow DAG definition file. It orchestrates the execution of the BigQuery SQL. It replaces the UC4 job definition and the KornShell wrapper scripts (`r_ausd_v_ta_cntrct_crs3.ksh`, `k_ausd_v_ta_cntrct_crs3.ksh`). The DAG contains a single `BigQueryExecuteQueryOperator` task that executes the entire BigQuery SQL script.
    *   **Location:** This file should be deployed to the Airflow DAGs folder in Cloud Composer.

## 3. Key Design Decisions

*   **Migration to Airflow and BigQuery:** The decision was made to fully re-platform the job to GCP, leveraging Airflow for robust orchestration and BigQuery for scalable and cost-effective data processing. This aligns with the broader cloud migration strategy.
*   **Direct SQL Conversion:** The core transformation logic, originally in Oracle SQL, was directly translated to BigQuery SQL. This minimizes changes to the business logic and leverages BigQuery's native capabilities.
    *   **Oracle-specific syntax handling:** Oracle's `(+)` outer join syntax was converted to standard `LEFT JOIN`. `NVL` was replaced with `IFNULL`. `TO_CHAR(date, 'YYYYMMDD')` was replaced with `FORMAT_DATE('%Y%m%d', DATE(...))`.
    *   **SQL*Plus directives removal:** All SQL*Plus specific commands (e.g., `DEFINE`, `SPOOL`, `WHENEVER SQLERROR`) were removed as they are not applicable in BigQuery.
*   **Consolidation of Orchestration:** The complex chain of UC4 -> KornShell wrapper -> KornShell control -> Oracle SQL was simplified into a single Airflow DAG with a single `BigQueryExecuteQueryOperator` task. This reduces operational overhead and centralizes logging and monitoring within Airflow.
    *   **Trade-off:** This approach means that the granular functionalities of the original KornShell utility scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`) are now either handled implicitly by Airflow's features (logging, error handling) or are no longer directly replicated as separate components.
*   **In-SQL Variable Declaration:** The `v_datum` variable, previously handled by SQL*Plus `DEFINE` and `START` commands, is now declared and set directly within the BigQuery SQL script using `DECLARE` and `SET` statements. This keeps the logic self-contained within the SQL.
*   **Target Table Truncation:** The `TRUNCATE TABLE` operation is performed directly within the BigQuery SQL script, mirroring the original Oracle behavior.
*   **Dataset Naming Convention:** Target BigQuery datasets are suffixed with `_target` (e.g., `isbert_schema_target`, `sof_dataset_target`) to clearly distinguish them from potential source/landing datasets.

## 4. Manual Steps Before Go-Live

The following manual steps must be completed before the migrated job can go live:

1.  **BigQuery Dataset Creation:**
    *   Ensure the BigQuery datasets `isbert_schema_target` and `sof_dataset_target` exist in the target GCP project.

2.  **BigQuery Table Schema Definition & Initial Data Load:**
    *   Define the schemas for the following tables in BigQuery, ensuring data types accurately reflect the Oracle source:
        *   `isbert_schema_target.dwtk_meldungen`
        *   `sof_dataset_target.ta_cntrct_crs2`
        *   `sof_dataset_target.ta_cntrct_crs3` (target table, schema only)
    *   Perform an initial historical data load for `isbert_schema_target.dwtk_meldungen` and `sof_dataset_target.ta_cntrct_crs2` from their respective Oracle source tables. This can be done using tools like Data Transfer Service, Dataflow, or custom scripts.

3.  **IAM Permissions:**
    *   The Service Account associated with the Cloud Composer environment (or the specific Airflow connection used) must have the following BigQuery permissions:
        *   `bigquery.datasets.get`
        *   `bigquery.tables.get`
        *   `bigquery.tables.getData`
        *   `bigquery.tables.updateData` (for `TRUNCATE TABLE` and `INSERT INTO`)
        *   `bigquery.jobs.create`

4.  **Airflow GCP Connection:**
    *   Ensure the `google_cloud_default` connection is configured correctly in Airflow, pointing to the target GCP project.

5.  **Scheduling Configuration:**
    *   The `start_date` and `schedule_interval` in `dw_bert_ausd_v_ta_cntrct_crs3.py` are currently placeholders. These *must* be confirmed with business stakeholders to match the original UC4 schedule and updated in the DAG file before deployment.

6.  **KornShell Utility Script Equivalents (B4 Item):**
    *   The functionalities of the original KornShell utility scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`) were not explicitly re-implemented as separate Python modules. If specific custom logging, error handling, or parameter parsing beyond Airflow's native capabilities is required, these need to be developed and integrated into the DAG.

## 5. Known Gaps & Unresolved References

*   **UC4 Schedule Confirmation:** The exact schedule of the original UC4 job was not derivable. The `start_date` and `schedule_interval` in the Airflow DAG are placeholders and *must be confirmed* with business stakeholders and updated accordingly.
*   **`v_carmen` DB-Link Usage:** The `v_carmen` variable was defined in the original Oracle SQL but not explicitly used in the provided script. If this DB-link was used dynamically or in other parts of the original system not captured, its migration strategy (e.g., federated query, data replication to BigQuery) needs to be clarified and implemented. This is a potential **B4 item** if usage is confirmed.
*   **KornShell Utility Script Equivalents:** The specific logic and functionality of the sourced KornShell utility scripts (e.g., `f_alis_msgerr.ksh` for error messaging, `h_alis_parameter.ksh` for parameter handling) were not explicitly translated into Python functions or Airflow components. While Airflow provides robust logging and error handling, any custom logic from these scripts might be missing. This is a **B4 item** for review and potential re-implementation if critical custom logic is identified.
*   **Oracle Specifics (Hints):** Oracle-specific hints like `PARALLEL` and `/* +append */` were removed. BigQuery handles parallelism automatically, and `INSERT` behavior is different. While generally safe to remove, a review of their original intent and any potential BigQuery equivalents (if performance issues arise) is a **B4 item**.
*   **Data Type Mapping:** While general data type conversions were considered, a detailed column-by-column schema mapping between Oracle and BigQuery was not explicitly performed during this migration. A thorough review of data types for all columns in `dwtk_meldungen`, `ta_cntrct_crs2`, and `ta_cntrct_crs3` is recommended to prevent data truncation or type mismatch errors. This is a **B4 item**.

## 6. Validation

To validate the migrated job:

1.  **Deploy the DAG:** Upload `dw_bert_ausd_v_ta_cntrct_crs3.py` to the Cloud Composer DAGs folder.
2.  **Trigger the DAG:** Manually trigger the `dw_bert_ausd_v_ta_cntrct_crs3` DAG from the Airflow UI.
3.  **Monitor Execution:** Observe the DAG run in the Airflow UI. Ensure all tasks complete successfully without errors. Check Cloud Logging for any BigQuery job errors or warnings.
4.  **Data Verification:**
    *   **Row Counts:** Compare the row count of `sof_dataset_target.ta_cntrct_crs3` after the BigQuery job completes with the row count of the original `sof$ta_cntrct_crs3` table in Oracle (after a successful run of the legacy job).
    *   **Sample Data Comparison:** Select a representative sample of records from both the BigQuery target table and the Oracle target table. Compare column values to ensure data accuracy and transformation logic correctness. Pay special attention to `twinbill` and `twin_vertrag_id` columns.
    *   **`v_datum` check:** Verify that the `v_datum` logic correctly retrieves the expected date from `isbert_schema_target.dwtk_meldungen`.

**"Passing" Criteria:**
*   The Airflow DAG completes successfully without any task failures.
*   The BigQuery job executes successfully, inserting data into `sof_dataset_target.ta_cntrct_crs3`.
*   Row counts in `sof_dataset_target.ta_cntrct_crs3` match the expected counts from the original Oracle table.
*   Sample data comparison confirms data accuracy and integrity, including correct application of the twin-bill logic.
*   No unexpected errors or warnings are observed in Airflow logs or Cloud Logging.

## 7. Rollback Procedure

In case of issues with the migrated job, the following rollback procedure can be followed:

1.  **Disable Airflow DAG:** In the Airflow UI, disable the `dw_bert_ausd_v_ta_cntrct_crs3` DAG to prevent further executions.
2.  **Revert Target Table:** If necessary, revert `sof_dataset_target.ta_cntrct_crs3` to its state before the problematic run. This might involve restoring from a BigQuery snapshot or a backup if such mechanisms are in place.
3.  **Re-enable Legacy Job:** Re-enable the original UC4 job `DW.BERT_AUSD_V_TA_CNTRCT_CRS3.xml` in the legacy system.
4.  **Verify Legacy Job:** Monitor the legacy UC4 job to ensure it runs successfully and continues to update the Oracle `sof$ta_cntrct_crs3` table as expected.
5.  **Investigate and Remediate:** Analyze the cause of the failure in the migrated job, make necessary corrections to the Airflow DAG or BigQuery SQL, and re-attempt the migration and validation process.