# MIGRATION_NOTES.md: DW.BERT_AUSD_V_TA_CNTRCT_TEMPL

## 1. Summary

The ETL job `DW.BERT_AUSD_V_TA_CNTRCT_TEMPL`, originally orchestrated by UC4 and executing KornShell scripts that in turn ran Oracle SQL*Plus to mirror Carmen contract templates, has been migrated.

The job has been migrated to Google Cloud Platform, utilizing **Apache Airflow on Cloud Composer** for orchestration and **Google BigQuery** for data transformation and storage.

## 2. Generated Artifacts

The migration process generated the following files:

*   **`dw_bert_ausd_v_ta_cntrct_temmpl.py`**
    *   **Role:** This is the main Airflow DAG (Directed Acyclic Graph) definition. It replaces the UC4 job and the KornShell wrapper scripts (`r_ausd_v_ta_cntrct_templ.ksh`, `k_ausd_v_ta_cntrct_templ.ksh`). It orchestrates the execution of the BigQuery SQL transformation.
*   **`d_ausd_v_ta_cntrct_templ_bq.sql`**
    *   **Role:** This file contains the core data transformation logic, translated from the original Oracle SQL*Plus script (`d_ausd_v_ta_cntrct_templ.sql`) into BigQuery Standard SQL. It includes the logic for determining the processing date, truncating the target table, and inserting transformed data.
*   **`ddl/sof_ta_cntrct_templ.sql`**
    *   **Role:** This DDL (Data Definition Language) script defines the schema for the target BigQuery table `sof_ta_cntrct_templ`. This table will store the mirrored Carmen contract template data.
*   **`ddl/cds_ta_cntrct_template.sql`**
    *   **Role:** This DDL script defines the schema for the source staging BigQuery table `cds_ta_cntrct_template`. This table is expected to be populated by an external data ingestion pipeline from the legacy Oracle system.
*   **`ddl/cds_ta_care_description.sql`**
    *   **Role:** This DDL script defines the schema for the source staging BigQuery table `cds_ta_care_description`. Similar to `cds_ta_cntrct_template`, this table is expected to be populated by an external data ingestion pipeline from the legacy Oracle system.
*   **`ddl/isbert_schema.dwtk_meldungen.sql`**
    *   **Role:** This DDL script defines the schema for the metadata BigQuery table `isbert_schema.dwtk_meldungen`. This table is used to determine the `v_datum` processing date and is assumed to be part of a broader metadata migration or recreation.

## 3. Key Design Decisions

*   **Orchestration Shift to Airflow:** The legacy UC4 job and KornShell scripts were replaced by an Airflow DAG. This leverages Airflow's native capabilities for scheduling, monitoring, logging, and error handling within the Google Cloud ecosystem, providing a modern, scalable, and observable orchestration layer.
*   **Transformation to BigQuery SQL:** The core Oracle SQL*Plus script was translated to BigQuery Standard SQL. This decision aligns with BigQuery being the target data warehouse, allowing for efficient, scalable, and cost-effective data processing directly within the platform without external database connections for transformation.
*   **Decoupling Source Data:** Instead of directly querying the source Oracle database via DB links, the design assumes that source data (`cds_ta_cntrct_template`, `cds_ta_care_description`) is pre-staged into BigQuery tables. This decouples the transformation job from the operational Oracle system, improving reliability, performance, and reducing direct dependencies on legacy infrastructure.
*   **BigQuery for Metadata:** The metadata table `isbert_schema.dwtk_meldungen`, previously in Oracle, is also assumed to be migrated to BigQuery. This centralizes all data and metadata within the BigQuery environment, simplifying queries and dependencies.
*   **Simplified SQL Execution:** The `BigQueryOperator` in Airflow directly executes the BigQuery SQL script. This eliminates the need for complex shell script logic to manage SQL*Plus sessions, environment variables, and error trapping, as Airflow handles these aspects natively.
*   **Trade-offs:**
    *   **Dependency on Data Ingestion:** The primary trade-off is the introduction of a critical dependency on a separate data ingestion pipeline to keep the BigQuery staging tables (`cds_ta_cntrct_template`, `cds_ta_care_description`, `isbert_schema.dwtk_meldungen`) synchronized with the source Oracle system. The reliability and latency of this pipeline are crucial.
    *   **Re-implementation of Utility Logic:** Generic KornShell utility functions (e.g., for environment setup, parameter parsing, advanced logging) were not directly translated but are expected to be replaced by Airflow's native features or custom Python modules, which requires careful re-implementation and testing.
    *   **`v_datum` Dependency:** The calculation of `v_datum` relies on the `isbert_schema.dwtk_meldungen` table being updated by another job (`BERT_DROP_TEMP_TABLE`). The migration of this upstream dependency is critical and must be coordinated.

## 4. Manual Steps Before Go-Live

Before deploying and running the migrated job, the following manual steps are required:

1.  **BigQuery Dataset Creation:**
    *   Ensure the BigQuery dataset for the target table exists (e.g., `your_bigquery_dataset`).
    *   Ensure the `isbert_schema` BigQuery dataset exists.
2.  **BigQuery Table Creation (DDL Execution):**
    *   Execute the DDL scripts for all generated tables in the target BigQuery project:
        *   `ddl/sof_ta_cntrct_templ.sql`
        *   `ddl/cds_ta_cntrct_template.sql`
        *   `ddl/cds_ta_care_description.sql`
        *   `ddl/isbert_schema.dwtk_meldungen.sql`
3.  **Data Ingestion Pipeline Setup:**
    *   **Crucial Step:** Establish and verify a robust data ingestion pipeline (e.g., Cloud DataStream, batch exports, or other ETL processes) to continuously or regularly populate the BigQuery staging tables:
        *   `cds_ta_cntrct_template`
        *   `cds_ta_care_description`
        *   `isbert_schema.dwtk_meldungen`
    *   Ensure these tables contain up-to-date and accurate data from the source Oracle system *before* the Airflow DAG runs.
4.  **IAM/Permissions:**
    *   The Airflow service account (associated with your Cloud Composer environment) must have the necessary BigQuery permissions:
        *   `BigQuery Data Editor` role (or equivalent) on the dataset containing `sof_ta_cntrct_templ` to allow `TRUNCATE` and `INSERT` operations.
        *   `BigQuery Data Viewer` role (or equivalent) on the datasets containing `cds_ta_cntrct_template`, `cds_ta_care_description`, and `isbert_schema.dwtk_meldungen` to allow `SELECT` operations.
5.  **Airflow Connection Configuration:**
    *   Verify that the `google_cloud_default` BigQuery connection in Airflow is correctly configured and points to the target GCP project. If a different connection ID is used, update the `bigquery_conn_id` parameter in the DAG.
6.  **Scheduling Configuration:**
    *   Update the `schedule` parameter in `dw_bert_ausd_v_ta_cntrct_temmpl.py` from `None` to the desired production schedule (e.g., `"@daily"`, `"0 0 * * *"`, or a specific cron expression).
7.  **Parameter Handling (if applicable):**
    *   If the original KornShell scripts used external parameters (`j`, `f`), ensure these are either hardcoded in the DAG, passed via Airflow Variables, or dynamically generated as needed. (For this specific job, no external parameters are directly used in the SQL, but the KornShell scripts did parse them).

## 5. Known Gaps & Unresolved References

The following items have been flagged for follow-up or represent known dependencies/risks:

*   **Source Table Availability & Data Ingestion Pipeline:** The most critical gap is the reliance on an external, robust data ingestion pipeline to ensure `cds_ta_cntrct_template`, `cds_ta_care_description`, and `isbert_schema.dwtk_meldungen` are continuously and accurately populated in BigQuery from the source Oracle system. The design assumes this pipeline is in place and functional.
*   **KornShell Utility Script Equivalents:** The full functionality of the legacy KornShell utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) has not been explicitly re-implemented in Python. While Airflow handles basic logging and error handling, any specific custom logic, environment setup, or advanced parameter parsing from these utilities needs to be reviewed and potentially integrated into the Airflow DAG's Python code or a shared utility module.
*   **"BERT_DROP_TEMP_TABLE" Job Dependency:** The `v_datum` calculation depends on the `isbert_schema.dwtk_meldungen` table being updated by another job with `job_kennung = 'BERT_DROP_TEMP_TABLE'`. The migration status and functionality of this upstream `BERT_DROP_TEMP_TABLE` job must be confirmed to ensure the correct processing date is derived.

## 6. Validation

To validate the successful migration and functionality of the `DW.BERT_AUSD_V_TA_CNTRCT_TEMPL` job:

1.  **Trigger the Airflow DAG:**
    *   Upload `dw_bert_ausd_v_ta_cntrct_temmpl.py` and `d_ausd_v_ta_cntrct_templ_bq.sql` to your Cloud Composer DAGs folder.
    *   Manually trigger the `dw_bert_ausd_v_ta_cntrct_temmpl` DAG from the Airflow UI.
2.  **Monitor DAG Execution:**
    *   Observe the DAG run in the Airflow UI. Ensure all tasks complete successfully without errors. Check task logs for any warnings or unexpected output.
3.  **Verify Data in BigQuery:**
    *   **Check `v_datum` calculation:** Query `isbert_schema.dwtk_meldungen` to confirm the `MAX(timecreated)` for `job_kennung = 'BERT_DROP_TEMP_TABLE'` is as expected, and that the `v_datum` declared in the SQL script correctly reflects this.
    *   **Target Table State:** After a successful DAG run, query the `sof_ta_cntrct_templ` table in BigQuery.
        *   Confirm the table was truncated (if it previously contained data).
        *   Verify that new data has been inserted.
    *   **Data Accuracy:**
        *   **Row Count Comparison:** Compare the row count of `sof_ta_cntrct_templ` in BigQuery with the row count of the corresponding `sof$ta_cntrct_templ` table in the legacy Oracle system (after a comparable run).
        *   **Data Sample Comparison:** Select a sample of records from both the BigQuery `sof_ta_cntrct_templ` and the legacy Oracle `sof$ta_cntrct_templ` tables and compare their contents to ensure data integrity and correctness of the transformation logic. Pay close attention to date fields and `NULL` handling.
        *   **Schema Validation:** Confirm that the schema of `sof_ta_cntrct_templ` in BigQuery matches the expected schema and data types.

**"Passing" Criteria:**
A successful validation means:
*   The Airflow DAG `dw_bert_ausd_v_ta_cntrct_temmpl` completes successfully without any task failures.
*   The `sof_ta_cntrct_templ` table in BigQuery is populated with data.
*   The data in `sof_ta_cntrct_templ` is accurate and consistent with the expected output based on the source data and the transformation logic, matching the legacy system's output within acceptable tolerances.
*   The `v_datum` derived in BigQuery matches the `s_datum` derived in the legacy Oracle environment for the same run.

## 7. Rollback Procedure

In case of issues during or after go-live, the following rollback procedure can be executed:

1.  **Disable New Airflow DAG:**
    *   In the Airflow UI, set the `dw_bert_ausd_v_ta_cntrct_temmpl` DAG to "Off" to prevent further runs.
2.  **Re-enable Legacy UC4 Job:**
    *   Re-activate the original `DW.BERT_AUSD_V_TA_CNTRCT_TEMPL` job in the UC4 system.
3.  **Data Restoration (if necessary):**
    *   If the new BigQuery job introduced incorrect data into `sof_ta_cntrct_templ` that affects downstream processes, consider restoring `sof_ta_cntrct_templ` from a previous backup or by re-running the legacy job to overwrite the incorrect data. Since the BigQuery job uses `TRUNCATE TABLE` followed by `INSERT`, the impact is typically limited to the target table itself.
4.  **Investigate and Rectify:**
    *   Analyze the logs and data discrepancies to identify the root cause of the failure in the migrated job. Rectify the issues in the Airflow DAG or BigQuery SQL script.
5.  **Re-deploy and Re-validate:**
    *   Once fixes are implemented, re-deploy the updated Airflow DAG and repeat the validation steps.