# MIGRATION_NOTES.md

## 1. Summary

The legacy job `DW.BERT_AUSD_V_TA_VERTRAG_TMP`, originally responsible for collecting and transforming contract-related information into a temporary staging table `sof$ta_vertrag_tmp` using UC4 (Automic) scheduled KornShell and Oracle PL/SQL, has been migrated.

The job has been re-platformed to Google Cloud Platform. The new implementation utilizes Apache Airflow for orchestration and Google BigQuery for data storage and transformation. The core transformation logic, previously in Oracle PL/SQL, has been converted to BigQuery SQL, and the shell scripting orchestration has been re-implemented using Python within an Airflow DAG. The target temporary table `sof$ta_vertrag_tmp` is now `bert_staging.ta_vertrag_tmp` in BigQuery.

## 2. Generated artifacts

The migration produced the following artifacts:

*   **`sql/dw_bert_ausd_v_ta_vertrag_tmp.sql`**
    *   **Role:** Contains the core BigQuery SQL transformation logic. This script is responsible for truncating the target table `bert_staging.ta_vertrag_tmp` and inserting transformed contract data into it. It is designed to be executed by an Airflow `BigQueryOperator` and expects the `v_datum` parameter to be templated from the Airflow context.
*   **`dags/dw_bert_ausd_v_ta_vertrag_tmp.py`**
    *   **Role:** This is the Apache Airflow DAG definition file. It orchestrates the execution of the migrated job.
        *   It includes a `PythonOperator` (`initialize_job_parameters`) to dynamically calculate the `v_datum` (cutoff date) by querying `bert_source.dwtk_meldungen` in BigQuery and pushing it to XCom.
        *   It uses a `BigQueryInsertJobOperator` (`execute_contract_transformation`) to run the BigQuery SQL from `sql/dw_bert_ausd_v_ta_vertrag_tmp.sql`, passing the `v_datum` from XCom.
        *   It includes a placeholder `PythonOperator` (`handle_job_completion`) for any final logging or status updates.

## 3. Key design decisions

*   **Orchestration Shift to Airflow:** The legacy UC4/Automic scheduler and KornShell scripts were replaced by an Apache Airflow DAG. This centralizes scheduling and monitoring within a cloud-native environment.
*   **Data Platform Shift to BigQuery:** The Oracle database for both source data and the temporary staging table was replaced by Google BigQuery. This leverages BigQuery's scalability, performance, and managed service benefits for data warehousing.
*   **Direct SQL Translation:** The complex Oracle PL/SQL transformation logic was directly translated into BigQuery SQL. This minimizes changes to the core business logic, reducing the risk of introducing new bugs. Specific translations included:
    *   Oracle `(+)` outer join syntax converted to explicit `LEFT JOIN`.
    *   Oracle `DECODE` functions converted to BigQuery `CASE` statements.
    *   Oracle date functions (`MONTHS_BETWEEN`, `TO_DATE`, `TO_CHAR`) replaced with BigQuery equivalents (`DATE_DIFF`, `PARSE_DATE`, `FORMAT_DATE`).
    *   Oracle `/*+ parallel(...) */` hints removed as BigQuery handles parallelism automatically.
*   **Python for Parameter Handling:** Instead of complex KornShell scripts for environment setup and parameter parsing, a PythonOperator within the Airflow DAG is used. This allows for dynamic parameter calculation (e.g., `v_datum` from `dwtk_meldungen`) and seamless integration with Airflow's XCom mechanism to pass values between tasks.
*   **Atomic BigQuery Operation:** The `TRUNCATE TABLE` and `INSERT INTO ... SELECT` pattern is used within a single BigQuery job. This ensures that the staging table is completely refreshed with the latest transformed data in one atomic operation, maintaining data consistency.
*   **Templating for Dynamic Values:** Airflow's Jinja templating is utilized to inject the dynamically calculated `v_datum` into the BigQuery SQL query, ensuring the transformation uses the correct cutoff date.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Create the `bert_source` BigQuery dataset (if it doesn't already exist) to house all source tables migrated from Oracle.
    *   Create the `bert_staging` BigQuery dataset (if it doesn't already exist) to house the target temporary table.
2.  **Source Data Ingestion:**
    *   Ensure all Oracle source tables and views listed in the design document (e.g., `ta_cntrct_crs3`, `ta_bp_ref`, `ta_inv_acc`, `ta_notice`, `ta_barrier_zusgf`, `ta_cntrct_templ`, `ta_cntrct_valid`, `ta_period`, `ta_vvl_upgrade`, `ta_apn_ve`, `vi_s_rd_segment`, `ta_action_assoc`, `vi_c_bfc`, `dwtk_meldungen`) are fully ingested into the `bert_source` BigQuery dataset. Verify schema compatibility and data integrity.
3.  **Target Table Schema Definition:**
    *   Create the `bert_staging.ta_vertrag_tmp` table in BigQuery with the correct schema, matching the output columns of the generated BigQuery SQL. This can be done using a `CREATE TABLE ... AS SELECT ... WITH NO DATA` statement or by manually defining the schema.
4.  **IAM Permissions:**
    *   Ensure the Airflow service account (or the service account used by the Airflow worker executing the DAG) has the necessary BigQuery permissions:
        *   `bigquery.dataViewer` on the `bert_source` dataset.
        *   `bigquery.dataEditor` on the `bert_staging` dataset (specifically for `bert_staging.ta_vertrag_tmp`).
        *   `bigquery.jobs.create` to run BigQuery jobs.
5.  **GCP Project ID and Region Configuration:**
    *   Update the `GCP_PROJECT_ID` and `BIGQUERY_REGION` placeholders in `dags/dw_bert_ausd_v_ta_vertrag_tmp.py` with the actual values for your GCP environment.
6.  **Airflow Deployment:**
    *   Upload the `dags/dw_bert_ausd_v_ta_vertrag_tmp.py` file to your Airflow DAGs folder.
    *   Ensure the `google-cloud-bigquery` Python client library is installed in your Airflow environment for the `initialize_job_parameters` task.
7.  **Scheduling:**
    *   The DAG is currently configured with `schedule=None` for manual triggering. If a specific schedule is required, update the `schedule` parameter in the DAG definition.

## 5. Known gaps & unresolved references

The following items were identified during the migration and require further attention or follow-up:

*   **Missing Complexity/Automation Data:** The original analysis lacked data on the complexity and automation bucket for the legacy components. This means the migration effort estimation and specific flags for potential challenges were not fully informed.
*   **`DW.HOLE_PFAD` and `DW.BERT_LESE_LOG` Replication:** The UC4 XML included references to `DW.HOLE_PFAD` (likely environment variables) and `DW.BERT_LESE_LOG` (a logging mechanism). While Airflow's native logging is used, the specific functionality of `DW.BERT_LESE_LOG` and any critical environment variables from `DW.HOLE_PFAD` need to be fully understood and replicated (e.g., using Airflow Variables, Connections, or custom Python logging).
*   **`$HOME/.dw_init` Contents:** The contents of this environment initialization file, sourced by the legacy KornShell scripts, were not fully analyzed. Any critical environment variables, paths, or configurations defined within it must be identified and replicated in the Airflow environment (e.g., as Airflow Variables, within Python code, or as part of the Airflow environment configuration).
*   **Source Table Availability and Schema Verification:** The migration assumes that all source tables/views from Oracle will be ingested into BigQuery with compatible schemas. A thorough verification of data types, nullability constraints, and potential data loss/truncation during ingestion is crucial.
*   **`v_carmen` DB-Link Handling:** The Oracle SQL used `DEFINE v_carmen = "@pcrs1"`, indicating a database link. The source of data from `pcrs1` needs to be identified. If `pcrs1` is another Oracle database, its data must also be migrated to BigQuery or made accessible to BigQuery (e.g., via federated queries, though direct migration is preferred for performance and consistency).
*   **`isbert_schema.dwtk_meldungen` Schema and Data:** This table is critical for the `v_datum` calculation. Its schema and data content must be accurately replicated in BigQuery to ensure the correct cutoff date is always determined.
*   **Legacy Error Handling and Logging:** The KornShell scripts used custom error handling (`f_alis_msgerr.ksh`, `DWMSG_` functions). While Airflow provides robust error handling and logging, a custom `on_failure_callback` or similar mechanism might be needed to replicate specific legacy error reporting or notification requirements.
*   **Performance Tuning:** Oracle `parallel` hints were removed. While BigQuery automatically handles parallelism, post-migration performance monitoring and potential optimization (e.g., partitioning, clustering, query optimization) are recommended to ensure the job meets SLA requirements.

## 6. Validation

To validate the successful migration and execution of the `DW.BERT_AUSD_V_TA_VERTRAG_TMP` job:

1.  **Trigger the Airflow DAG:**
    *   Navigate to the Airflow UI.
    *   Find the `dw_bert_ausd_v_ta_vertrag_tmp` DAG.
    *   Manually trigger a run.
2.  **Monitor DAG Execution:**
    *   Observe the DAG run in the Airflow UI. All tasks (`initialize_job_parameters`, `execute_contract_transformation`, `handle_job_completion`) should complete successfully without errors.
    *   Check the logs for each task for any warnings or unexpected output.
3.  **Verify Target Table Population:**
    *   After successful DAG execution, query the `bert_staging.ta_vertrag_tmp` table in BigQuery.
    *   Verify that the table is populated with data.
4.  **Data Quality and Completeness Checks:**
    *   **Row Count Comparison:** Compare the number of rows in `bert_staging.ta_vertrag_tmp` with the row count in the legacy `sof$ta_vertrag_tmp` table for the same execution period.
    *   **Data Sample Verification:** Select a sample of records from both the legacy and new tables and perform a column-by-column comparison to ensure data accuracy and correct transformation.
    *   **Checksum/Hash Comparison (if feasible):** If possible, generate a checksum or hash of key columns or the entire dataset in both the legacy and new environments to ensure data integrity.
    *   **Specific Business Logic Checks:** Verify the correctness of complex `CASE` statements (e.g., `upgradeberechtigt`, `VDA`) by spot-checking records that should trigger specific outcomes.
    *   **`v_datum` Verification:** Confirm that the `v_datum` used in the BigQuery transformation matches the expected value derived from `bert_source.dwtk_meldungen`.

**"Passing" means:**
*   The Airflow DAG completes successfully without any task failures.
*   The `bert_staging.ta_vertrag_tmp` table is truncated and re-populated with data.
*   The row count in `bert_staging.ta_vertrag_tmp` is consistent with the legacy `sof$ta_vertrag_tmp` table (allowing for minor discrepancies due to data ingestion timing or source system changes).
*   Spot checks and data quality validations confirm that the transformed data is accurate and matches the expected output from the legacy system.

## 7. Rollback procedure

In case of issues or critical failures with the migrated job, the following rollback procedure can be executed:

1.  **Disable New Airflow DAG:**
    *   In the Airflow UI, toggle off the `dw_bert_ausd_v_ta_vertrag_tmp` DAG to prevent any further runs.
2.  **Re-enable Legacy UC4 Job:**
    *   Re-activate the original `DW.BERT_AUSD_V_TA_VERTRAG_TMP` job in the UC4/Automic scheduler.
    *   Ensure all necessary legacy environment configurations and dependencies are in place and functional.
3.  **Verify Legacy Job Execution:**
    *   Trigger or wait for the next scheduled run of the legacy UC4 job.
    *   Monitor its execution to confirm it successfully populates the `sof$ta_vertrag_tmp` table in Oracle.
4.  **Data Restoration (if necessary):**
    *   If downstream systems have already consumed data from the BigQuery `bert_staging.ta_vertrag_tmp` table and are negatively impacted, a data restoration might be required. This could involve:
        *   Restoring `bert_staging.ta_vertrag_tmp` from a previous backup (if backups are configured).
        *   Manually loading data into `bert_staging.ta_vertrag_tmp` from a known good state or by re-running the legacy job and then ingesting its output to BigQuery.
    *   Communicate immediately with any downstream consumers of `bert_staging.ta_vertrag_tmp` about the rollback and potential data state.
5.  **Root Cause Analysis:**
    *   Investigate the reason for the rollback, addressing any identified issues in the Airflow DAG, BigQuery SQL, or data ingestion processes before attempting re-migration.