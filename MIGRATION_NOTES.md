# MIGRATION_NOTES.md: DW.BERT_AUSD_BP_TA_TARIFOPTION

## 1. Summary

The `DW.BERT_AUSD_BP_TA_TARIFOPTION` job, responsible for preparing and processing "instantiated base products" related to tariff options for the BERT system, has been migrated. This workflow extracts, transforms, and loads data to populate `sof$ta_bpr_opt_filter` and `sof$ta_tarifoption` tables.

**Original Platform:** UC4 for orchestration, KornShell scripts, Oracle SQL for data processing.
**Target Platform:** Google Cloud Airflow for orchestration, Google BigQuery for data processing.

## 2. Generated Artifacts

The migration process generated the following files:

*   **`sql/dw_bert_ausd_bp_ta_tarifoption.sql`**
    *   **Role:** This file contains the BigQuery SQL script responsible for the data transformation and loading logic. It performs the following actions:
        *   Declares a variable `v_datum` to dynamically determine a date suffix for a source table.
        *   Drops and recreates the `sof$ta_bpr_opt_filter` table.
        *   Drops and recreates the `sof$ta_tarifoption` table, performing complex transformations including string manipulation (`RTRIM`, `SUBSTR`, `LTRIM`, `CONCAT`, `CAST`) and window functions (`LEAD`).
    *   **Migration Notes:** This SQL script is a direct translation of the original Oracle SQL, adapted for BigQuery syntax and features.

*   **`dags/dw_bert_ausd_bp_ta_tarifoption_dag.py`**
    *   **Role:** This file defines the Airflow Directed Acyclic Graph (DAG) that orchestrates the execution of the BigQuery SQL script.
        *   It sets up a daily schedule (`@daily`) for the job.
        *   It uses the `BigQueryInsertJobOperator` to execute the SQL script, embedding the SQL from `sql/dw_bert_ausd_bp_ta_tarifoption.sql` using Jinja templating (`{% include ... %}`).
    *   **Migration Notes:** This DAG replaces the legacy UC4 job and KornShell script, providing cloud-native orchestration capabilities.

## 3. Key Design Decisions

*   **Platform Transition:** The core decision was to move from an on-premise Oracle/KornShell/UC4 environment to a fully managed Google Cloud solution using BigQuery for data warehousing and Airflow for workflow orchestration. This leverages cloud scalability, cost-effectiveness, and managed services.
*   **SQL Dialect Conversion:**
    *   Oracle SQL was translated to BigQuery Standard SQL. This involved adapting functions like `TO_CHAR(SYSDATE, 'YYYYMMDD')` to `FORMAT_DATE('%Y%m%d', CURRENT_DATE())` (or similar date functions), `NVL` to `COALESCE`, and string concatenation `||` to `CONCAT()`.
    *   The `DECLARE` statement for `v_datum` was introduced in BigQuery SQL to mimic the variable usage in the original script.
    *   Oracle's `SUBSTR`, `RTRIM`, `LTRIM` functions were directly translated to their BigQuery equivalents.
    *   The `LEAD` window function was adapted. Notably, `ORDER BY NULL` was explicitly added to the `OVER` clause in BigQuery, as BigQuery requires an `ORDER BY` for window functions, and `NULL` indicates a non-deterministic order. This might differ from implicit ordering in Oracle if the original query relied on it.
*   **Orchestration with Airflow:** The complex shell script logic and UC4 scheduling were replaced by a single Airflow DAG. The `BigQueryInsertJobOperator` was chosen for its direct integration with BigQuery, allowing the entire SQL script to be executed as a single BigQuery job.
*   **Dynamic Table Naming (B4 Item):** The original script used a dynamic table name `sof$ta_bpr_opt_text_` concatenated with a date variable (`v_datum`). In the generated BigQuery SQL, this is represented as ``isbert_schema.sof$ta_bpr_opt_text_` || v_datum`. BigQuery's `DECLARE` variables cannot be directly used to form dynamic table names in the `FROM` clause at query compilation time. This requires a specific handling mechanism (see "Known Gaps").
*   **`nologging` Option:** The Oracle `NOLOGGING` table option was translated to a `description` option in BigQuery table creation. BigQuery manages logging and transaction semantics differently, so a direct equivalent is not necessary or available.
*   **Scheduling Assumption:** The `schedule_interval="@daily"` in the Airflow DAG was chosen based on the common ETL pattern and the presence of a `p_Stichtag` (reference date) in the original context, implying daily processing.

## 4. Manual Steps Before Go-Live

The following manual steps are required to prepare the Google Cloud environment for the migrated job:

1.  **BigQuery Dataset Creation:**
    *   Ensure the BigQuery dataset `isbert_schema` exists in your Google Cloud project. If not, create it:
        ```bash
        bq mk --dataset --default_table_expiration 36000 --default_partition_expiration 36000 your_gcp_project_id:isbert_schema
        ```
        (Adjust `default_table_expiration` and `default_partition_expiration` as per retention policies).
2.  **IAM Permissions:**
    *   Grant the service account used by your Airflow environment (e.g., the Composer service account) the necessary BigQuery roles:
        *   `BigQuery Data Editor` on the `isbert_schema` dataset to allow table creation, dropping, and data manipulation.
        *   `BigQuery Data Viewer` on any source datasets/tables (`isbert_schema.dwtk_meldungen`, `isbert_schema.sof$ta_l_bpr_optionen_filter`, `isbert_schema.sof$ta_bpr_opt_text_YYYYMMDD`) to read data.
3.  **Airflow Connection:**
    *   Verify that the `google_cloud_default` connection (or the specified `gcp_conn_id`) is correctly configured in your Airflow environment and points to the target Google Cloud project.
4.  **Source Table Existence and Population:**
    *   Ensure that the source tables (`isbert_schema.dwtk_meldungen`, `isbert_schema.sof$ta_l_bpr_optionen_filter`, and the dynamically named `isbert_schema.sof$ta_bpr_opt_text_YYYYMMDD` tables) exist and are populated with data in BigQuery before the DAG runs.
5.  **Airflow Deployment:**
    *   Deploy `dags/dw_bert_ausd_bp_ta_tarifoption_dag.py` and `sql/dw_bert_ausd_bp_ta_tarifoption.sql` to your Airflow DAGs folder. The SQL file should be placed in a subfolder accessible by the DAG (e.g., `dags/sql/`).

## 5. Known Gaps & Unresolved References

The following items have been identified as gaps or require further attention (B4 items):

*   **B4: Dynamic Table Naming in BigQuery SQL (`sof$ta_bpr_opt_text_` || `v_datum`)**
    *   **Issue:** The generated BigQuery SQL uses a `DECLARE` variable `v_datum` to construct a dynamic table name in the `FROM` clause (e.g., ``isbert_schema.sof$ta_bpr_opt_text_` || v_datum`). BigQuery Standard SQL does not support using script variables directly in table names within a `FROM` clause at query compilation time. The current SQL will fail.
    *   **Required Redesign:** This needs to be addressed by dynamically templating the table name in the Airflow DAG *before* the query is sent to BigQuery.
        *   **Proposed Solution:** Modify the Airflow DAG to fetch `v_datum` (or the date part) using a separate BigQuery operator or a Python function, and then pass this date as a Jinja template variable to the SQL query. The SQL would then look like `FROM `isbert_schema.sof$ta_bpr_opt_text_{{ params.dynamic_date }}` AS t`.
*   **B4: `LEAD` Window Function `ORDER BY NULL` Behavior**
    *   **Issue:** The `LEAD` function in BigQuery uses `OVER (ORDER BY NULL)`, which explicitly states a non-deterministic order for the window function. If the original Oracle query implicitly relied on a specific order (e.g., insertion order or a default sort order not explicitly stated in the `OVER` clause), the results in BigQuery might differ or be non-deterministic. The inner subquery's `ORDER BY cntrct_id, pds_description` only applies to that subquery, not the `LEAD` function's window.
    *   **Required Redesign:** Review the original Oracle query's behavior for the `LEAD` function. If a specific order was intended or implicitly used, the BigQuery `LEAD` function's `OVER` clause must be updated with the correct `ORDER BY` columns to ensure deterministic and accurate results.
*   **Scheduling Confirmation:** The `schedule_interval="@daily"` was an assumption based on common ETL patterns and the `p_Stichtag` context. This should be confirmed with business stakeholders to ensure it aligns with the required execution frequency.

## 6. Validation

To validate the successful migration and operation of the `DW.BERT_AUSD_BP_TA_TARIFOPTION` job:

1.  **Trigger the Airflow DAG:**
    *   In the Airflow UI, navigate to the `dw_bert_ausd_bp_ta_tarifoption_dag` DAG.
    *   Manually trigger the DAG.
2.  **Monitor DAG Execution:**
    *   Observe the DAG run in the Airflow UI. Ensure all tasks complete successfully (green status).
    *   Check Airflow task logs for any errors or warnings.
3.  **Verify BigQuery Tables:**
    *   After successful DAG execution, navigate to the BigQuery UI.
    *   Confirm that the tables `isbert_schema.sof$ta_bpr_opt_filter` and `isbert_schema.sof$ta_tarifoption` have been created/updated.
4.  **Data Validation (Passing Criteria):**
    *   **Row Count Comparison:** Compare the row counts of the newly created BigQuery tables (`isbert_schema.sof$ta_bpr_opt_filter`, `isbert_schema.sof$ta_tarifoption`) with their corresponding legacy Oracle tables for the same processing date.
        *   `SELECT COUNT(*) FROM `your_gcp_project_id.isbert_schema.sof$ta_bpr_opt_filter`;`
        *   `SELECT COUNT(*) FROM `your_gcp_project_id.isbert_schema.sof$ta_tarifoption`;`
    *   **Data Sample Comparison:** Run sample queries on both BigQuery and Oracle tables to compare a subset of data, focusing on key columns and edge cases.
        *   Example: `SELECT * FROM `your_gcp_project_id.isbert_schema.sof$ta_tarifoption` LIMIT 100;`
    *   **Data Integrity Checks:** Perform basic data integrity checks, such as:
        *   Verify `NULL` values where expected/not expected.
        *   Check for data type consistency.
        *   Ensure string manipulations (`RTRIM`, `SUBSTR`, `LTRIM`) have produced the expected output.
    *   **"Passing" means:**
        *   The Airflow DAG completes successfully without errors.
        *   Row counts in the BigQuery target tables match the legacy Oracle tables for the corresponding processing date.
        *   Sample data comparisons show identical or functionally equivalent results.
        *   No unexpected data integrity issues are found.

## 7. Rollback Procedure

In case of issues or failure during or after go-live, the following rollback procedure can be executed:

1.  **Disable Airflow DAG:**
    *   In the Airflow UI, toggle off the `dw_bert_ausd_bp_ta_tarifoption_dag` to prevent further executions.
2.  **Re-enable Legacy Job:**
    *   Re-enable the original `DW.BERT_AUSD_BP_TA_TARIFOPTION` job in the UC4 scheduler.
    *   Verify that the legacy job runs successfully and populates the Oracle tables as expected.
3.  **Optional: Clean Up BigQuery Tables:**
    *   If necessary, drop the BigQuery tables created by the migration to revert the environment to its pre-migration state.
        ```sql
        DROP TABLE IF EXISTS `your_gcp_project_id.isbert_schema.sof$ta_bpr_opt_filter`;
        DROP TABLE IF EXISTS `your_gcp_project_id.isbert_schema.sof$ta_tarifoption`;
        ```
    *   This step is optional and depends on whether the BigQuery tables are causing conflicts or are simply unwanted.