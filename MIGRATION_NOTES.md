# MIGRATION_NOTES.md

## 1. Summary

The ETL job `DW.BERT_AUSD_V_TA_VERTRAG_TMP`, originally orchestrated by UC4 and executing Oracle SQL via KornShell wrappers, has been migrated.

**What was migrated:**
*   The UC4 job scheduler definition (`DW.BERT_AUSD_V_TA_VERTRAG_TMP.xml`).
*   The core Oracle SQL transformation logic (`d_ausd_v_ta_vertrag_tmp.sql`) which populates a temporary contract table (`sof$ta_vertrag_tmp`).
*   The intermediary KornShell wrapper scripts (`r_ausd_v_ta_vertrag_tmp.ksh`, `k_ausd_v_ta_vertrag_tmp.ksh`) have been retired.

**To which target platform:**
*   **Orchestration:** Apache Airflow on Google Cloud.
*   **Data Storage & Transformation:** Google BigQuery.
*   **Processing:** Direct BigQuery SQL execution from Airflow.

The migrated job now prepares contract data from various BigQuery source tables and views, populating a BigQuery staging table `bert_dw_staging.bert_ausd_v_ta_vertrag_tmp`.

## 2. Generated artifacts

The migration process generated the following files:

*   **`dw_bert_ausd_v_ta_vertrag_tmp_transform.sql`**
    *   **Role:** This file contains the core data transformation logic, converted from Oracle SQL to BigQuery SQL. It is responsible for truncating the target table and then inserting processed contract data by joining various source tables. It includes a `DECLARE` statement for `v_datum` and uses BigQuery-specific functions and syntax.
*   **`dw_bert_ausd_v_ta_vertrag_tmp.py`**
    *   **Role:** This is the Apache Airflow DAG definition file. It orchestrates the execution of the BigQuery SQL transformation. It defines the DAG's schedule, default arguments, and a single `BigQueryOperator` task that executes the `dw_bert_ausd_v_ta_vertrag_tmp_transform.sql` script.

## 3. Key design decisions

*   **UC4 to Airflow Orchestration:** The legacy UC4 job scheduler was replaced by an Airflow DAG. This centralizes scheduling and monitoring within the Google Cloud ecosystem.
*   **Oracle SQL to BigQuery SQL Transformation:** The core data transformation logic was rewritten from Oracle PL/SQL to BigQuery Standard SQL. This leverages BigQuery's analytical capabilities and scalability.
*   **Retirement of KornShell Wrappers:** The original KornShell scripts (`r_ausd_v_ta_vertrag_tmp.ksh`, `k_ausd_v_ta_vertrag_tmp.ksh`) were identified as pure wrappers for SQL execution. They have been retired, and the BigQuery SQL is now executed directly from Airflow using a `BigQueryOperator`, simplifying the architecture.
*   **BigQuery as Unified Data Platform:** All source Oracle tables/views referenced by the job are assumed to be migrated or replicated to BigQuery. The target temporary table (`sof$ta_vertrag_tmp`) is now a native BigQuery table, ensuring all data processing occurs within BigQuery.
*   **Direct SQL Execution via `BigQueryOperator`:** Instead of using intermediate processing layers like Dataproc for the KornShell logic, the `BigQueryOperator` was chosen for its efficiency in directly executing BigQuery SQL scripts, aligning with the simplification goal.
*   **SQL Conversion Details:**
    *   Oracle's `TRUNCATE TABLE` was directly translated to BigQuery's `TRUNCATE TABLE`.
    *   Oracle's `NVL` was converted to BigQuery's `COALESCE`.
    *   Oracle's `DECODE` was converted to BigQuery's `CASE` statement.
    *   Oracle's proprietary `(+)` outer join syntax was converted to explicit `LEFT JOIN` clauses.
    *   Oracle date/time functions (`TO_CHAR`, `TO_DATE`, `MONTHS_BETWEEN`) were converted to BigQuery equivalents (`FORMAT_DATE`, `PARSE_DATE`, `DATE_DIFF`).
    *   The `v_datum` variable derivation logic was translated to a BigQuery `DECLARE` and `SET` statement.
    *   Oracle-specific directives (`PROMPT`, `SPOOL`, `WHENEVER SQLERROR`, etc.) were removed as they are not applicable in BigQuery SQL executed via Airflow.
*   **Target Table Naming:** The target table `sof$ta_vertrag_tmp` was renamed to `bert_ausd_v_ta_vertrag_tmp` within the `bert_dw_staging` dataset for BigQuery, following BigQuery naming conventions and best practices for staging tables.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **Source Data Migration:**
    *   Ensure all Oracle source tables and views referenced in the `dw_bert_ausd_v_ta_vertrag_tmp_transform.sql` script are fully migrated or replicated to BigQuery. This includes:
        *   `isbert_schema.dwtk_meldungen`
        *   `sof$ta_cntrct_crs3`
        *   `sof$ta_bp_ref`
        *   `sof$ta_inv_acc`
        *   `dwh$vi_s_rd_segment`
        *   `sof$ta_notice`
        *   `sof$ta_barrier_zusgf`
        *   `sof$ta_cntrct_templ`
        *   `sof$ta_cntrct_valid`
        *   `sof$ta_period`
        *   `sof$ta_vvl_upgrade`
        *   `sof$ta_apn_ve`
        *   `sof$ta_action_assoc`
        *   `sof$vi_c_bfc`
    *   Verify that the BigQuery table names and schemas for these sources match the references in the generated SQL (e.g., `isbert_schema.dwtk_meldungen` might become `project.dataset.isbert_schema_dwtk_meldungen`).

2.  **BigQuery Dataset Creation:**
    *   Create the target BigQuery dataset `bert_dw_staging` in your GCP project if it does not already exist.
        ```bash
        bq mk --location=US your-gcp-project-id:bert_dw_staging
        ```
        (Adjust location as necessary).

3.  **BigQuery Target Table Creation:**
    *   Create the target BigQuery table `bert_dw_staging.bert_ausd_v_ta_vertrag_tmp` with the correct schema. The schema should match the output columns of the `INSERT INTO` statement in `dw_bert_ausd_v_ta_vertrag_tmp_transform.sql`. It is recommended to run the `SELECT` part of the SQL against sample data to infer the schema or define it explicitly.

4.  **IAM/Permissions:**
    *   Ensure the Google Cloud Service Account used by your Airflow environment has the necessary BigQuery permissions:
        *   `BigQuery Data Editor` on the `bert_dw_staging` dataset (for `TRUNCATE` and `INSERT`).
        *   `BigQuery Data Viewer` on all source datasets/tables referenced in the SQL.
        *   `BigQuery Job User` to run BigQuery jobs.

5.  **Connection Strings / GCP Project ID:**
    *   Update the `GCP_PROJECT_ID` placeholder in `dw_bert_ausd_v_ta_vertrag_tmp.py` with your actual Google Cloud Project ID.
    *   Verify `BIGQUERY_DATASET` and `BIGQUERY_TARGET_TABLE` variables in the DAG match the created resources.

6.  **Scheduling:**
    *   The `schedule_interval` in `dw_bert_ausd_v_ta_vertrag_tmp.py` is currently `None`. Based on the original UC4 job's schedule, update this parameter (e.g., `timedelta(days=1)` for daily, a cron expression for specific times).
    *   Update the `start_date` in the DAG to an appropriate historical date.

7.  **Airflow DAG Deployment:**
    *   Upload `dw_bert_ausd_v_ta_vertrag_tmp.py` and `dw_bert_ausd_v_ta_vertrag_tmp_transform.sql` to your Airflow DAGs folder (e.g., `dags/`).

## 5. Known gaps & unresolved references

The following items were identified as risks or require further follow-up:

*   **`k_ausd_v_ta_vertrag_tmp.ksh` Retirement Confirmation:** The design document marks `k_ausd_v_ta_vertrag_tmp.ksh` for retirement (B0). This assumes its logic is purely a wrapper for SQL execution. It needs explicit confirmation that no critical business logic (e.g., specific error handling, logging, or parameter manipulation beyond SQL execution) is lost by retiring this script. If such logic exists, it must be incorporated into the Airflow DAG or the BigQuery SQL.
*   **`v_datum` Derivation Semantics:** The `v_datum` variable is derived from `isbert_schema.dwtk_meldungen` where `job_kennung = 'BERT_DROP_TEMP_TABLE'`. The exact business semantics and implications of this specific `job_kennung` for determining the job's execution date logic need to be thoroughly confirmed to ensure the BigQuery translation is functionally equivalent and produces the correct date.
*   **Database Links (`v_carmen`):** The Oracle source SQL contains `DEFINE v_carmen = "@pcrs1"`. While `v_carmen` is not explicitly used in the `FROM` clause of the provided SQL, the presence of `@pcrs1` suggests a potential database link to another Oracle instance. It must be verified if any data is implicitly sourced from `pcrs1` or if this definition is vestigial. If data is sourced, those sources must also be identified and migrated/replicated to BigQuery.
*   **Performance Tuning:** The original Oracle SQL used `/*+ parallel(c,4) ... */` hints. While BigQuery automatically handles parallelism, the migrated BigQuery SQL should be reviewed and tested for optimal performance and cost efficiency, especially with large datasets.
*   **Missing Scheduling Information:** The original UC4 XML did not provide specific scheduling details. The Airflow DAG is currently set with `schedule_interval=None`. The correct production schedule must be determined from the legacy system's operational documentation and configured in the Airflow DAG.

## 6. Validation

Validation ensures the migrated job functions correctly and produces accurate results.

**How to run the tests:**

1.  **BigQuery SQL Validation (Unit Test):**
    *   Ensure all source tables are populated with representative test data in BigQuery.
    *   Manually execute the `dw_bert_ausd_v_ta_vertrag_tmp_transform.sql` script directly in the BigQuery console or via the `bq query` CLI tool.
    *   Observe the job execution status and any errors.

2.  **Airflow DAG Validation (Integration Test):**
    *   Deploy the `dw_bert_ausd_v_ta_vertrag_tmp.py` and `dw_bert_ausd_v_ta_vertrag_tmp_transform.sql` files to your Airflow environment.
    *   Manually trigger the `dw_bert_ausd_v_ta_vertrag_tmp` DAG from the Airflow UI.
    *   Monitor the DAG run in the Airflow UI for successful completion of the `execute_bigquery_transformation` task.
    *   Check the BigQuery job history for the corresponding job initiated by Airflow.

**What "passing" means:**

*   **SQL Execution:**
    *   The BigQuery SQL script executes without syntax errors or runtime errors.
    *   The `TRUNCATE` operation completes successfully.
    *   The `INSERT INTO` operation completes successfully.
    *   The number of rows inserted into `bert_dw_staging.bert_ausd_v_ta_vertrag_tmp` is consistent with expectations (e.g., similar to the legacy job's output row count for comparable input data).
*   **Airflow DAG Execution:**
    *   The Airflow DAG runs successfully from start to finish, with all tasks (specifically `execute_bigquery_transformation`) marked as green.
    *   No Airflow task failures or retries occur.
*   **Data Validation:**
    *   **Row Count Comparison:** Compare the row count of the target BigQuery table (`bert_dw_staging.bert_ausd_v_ta_vertrag_tmp`) with the row count of the original Oracle `sof$ta_vertrag_tmp` table for the same processing period and source data.
    *   **Data Sample Comparison:** Select a statistically significant sample of records from both the legacy Oracle target table and the new BigQuery target table. Compare column by column to ensure data values match exactly or within acceptable tolerances (e.g., for floating-point numbers).
    *   **Key Metrics:** If the job contributes to any key business metrics or reports, verify that these metrics remain consistent after the migration.
    *   **Schema Conformance:** Ensure the schema of the BigQuery target table matches the expected output schema and is compatible with downstream consumers.

## 7. Rollback procedure

In case of issues during or after go-live, the following rollback procedure can be initiated:

1.  **Disable Airflow DAG:**
    *   In the Airflow UI, pause the `dw_bert_ausd_v_ta_vertrag_tmp` DAG to prevent further runs.

2.  **Re-enable Legacy UC4 Job:**
    *   Re-activate the original UC4 job `DW.BERT_AUSD_V_TA_VERTRAG_TMP` in the legacy environment. Ensure it has access to its original Oracle source and target tables.

3.  **Clean BigQuery Target Table (Optional but Recommended):**
    *   If the BigQuery target table `bert_dw_staging.bert_ausd_v_ta_vertrag_tmp` was populated incorrectly or partially, it might be necessary to truncate it or revert it to a known good state.
        ```sql
        TRUNCATE TABLE `your-gcp-project-id.bert_dw_staging.bert_ausd_v_ta_vertrag_tmp`;
        ```
    *   If BigQuery table snapshots or time travel were enabled, consider restoring the table to a point before the problematic run.

4.  **Verify Legacy System Functionality:**
    *   Confirm that the re-enabled UC4 job runs successfully and populates the Oracle `sof$ta_vertrag_tmp` table as expected.
    *   Verify that any downstream processes or reports relying on the Oracle table are functioning correctly.

5.  **Investigate and Rectify:**
    *   Analyze the root cause of the rollback (e.g., data discrepancies, performance issues, Airflow errors) and plan for remediation before attempting re-migration.