# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the ETL job originally defined by the KornShell script `k_ausd_v_ta_action_assoc.ksh` and its invoked Oracle SQL script `d_ausd_v_ta_action_assoc.sql`.

The job's primary function is to process `ta_action_assoc` data, determining a cutoff date from `isbert_schema.dwtk_meldungen`, truncating the `sof$ta_action_assoc` table, and then inserting filtered data from `cds$ta_action_assoc` into `sof$ta_action_assoc`.

The job has been migrated from a legacy Oracle/KornShell environment to Google Cloud Platform (GCP), specifically:
*   **Source Platform:** Oracle Database (SQL*Plus), KornShell, UC4 Scheduler.
*   **Target Platform:** Google BigQuery (for data processing and storage), Apache Airflow (for orchestration and scheduling).

## 2. Generated artifacts

The migration produced the following artifact:

*   **`d_ausd_v_ta_action_assoc.py`**
    *   **Role:** This is an Apache Airflow Directed Acyclic Graph (DAG) written in Python. It serves as the new orchestration layer, replacing the original KornShell script. It defines a single task that executes the entire transformed SQL logic within Google BigQuery. This DAG is responsible for scheduling, executing the BigQuery query, and handling task-level logging and error management.

## 3. Key design decisions

The following key design decisions were made during the migration:

*   **Orchestration Shift to Airflow:** The KornShell script's role in environment setup, parameter handling, and SQL execution has been fully replaced by an Airflow DAG. This leverages Airflow's robust scheduling, dependency management, and monitoring capabilities native to GCP Composer.
*   **BigQuery as Unified Data Platform:** All Oracle SQL logic, including data extraction, transformation, and loading, has been translated into BigQuery Standard SQL. Source and target tables (`isbert_schema.dwtk_meldungen`, `cds_ta_action_assoc`, `sof_ta_action_assoc`) are now hosted in BigQuery. This consolidates data processing and storage within a single, scalable, and performant cloud-native environment.
*   **Consolidated SQL Logic:** The entire ETL process (date determination, truncation, and insertion) is encapsulated within a single `BigQueryExecuteQueryOperator` task. This simplifies the DAG structure and leverages BigQuery's ability to execute multi-statement queries efficiently.
*   **`v_datum` Calculation:** The Oracle-specific `DEFINE` and `SELECT NVL(TO_CHAR(MAX(...)))` for `v_datum` have been translated into a BigQuery `DECLARE` statement using `COALESCE` and `FORMAT_DATE` to ensure equivalent date calculation and formatting.
*   **Direct Truncate/Insert:** The Oracle `TRUNCATE` command, previously invoked via a PL/SQL package (`DWPA_UTIL_SKRIPT`), is now a direct `TRUNCATE TABLE` statement in BigQuery Standard SQL. The `INSERT INTO ... SELECT ...` statement has been adapted to BigQuery syntax, including date function conversions (`DATE()`, `PARSE_DATE()`) and removal of Oracle-specific hints.
*   **Removal of Oracle-Specific Features:**
    *   SQL*Plus directives (`DEFINE`, `COLUMN`, `WHENEVER SQLERROR`, `SPOOL`) are removed as Airflow and BigQuery provide native logging and error handling.
    *   Oracle hints (`/*+ parallel(ac,4) full(ac) */`) are unnecessary in BigQuery, which handles query optimization and parallelism automatically.
    *   The Oracle DB-Link (`@pcrs1`) for `cds$ta_action_assoc` is replaced by direct access to the `cds_ta_action_assoc` table in BigQuery, assuming it has been migrated or is being ingested.
*   **Trade-offs:**
    *   **Granular SQL*Plus Logging:** The detailed spool file output from SQL*Plus is replaced by BigQuery's query history and Airflow's task logs, which may require a different approach for specific debugging or auditing needs.
    *   **`cds_ta_action_assoc` Data Ingestion:** The migration assumes `cds_ta_action_assoc` is available in BigQuery. If it remains an external Oracle source, a separate data ingestion mechanism (e.g., BigQuery Data Transfer Service, custom ETL) is required, adding complexity outside this specific job's scope.
    *   **`p_EintragsNr` Parameter:** The original `p_EintragsNr` parameter was identified as unused in the SQL logic and has been omitted from the generated DAG. If its purpose becomes relevant in the future, it would need to be re-introduced and handled via Airflow parameters or XComs.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the BigQuery datasets `isbert_schema` and `sof` exist in the target GCP project.
2.  **BigQuery Table Creation:**
    *   Create the target table `sof.sof_ta_action_assoc` in BigQuery with the appropriate schema (matching `cntrct_id`, `rv_action_id`, and any other columns from the original `sof$ta_action_assoc`).
    *   Ensure the source table `isbert_schema.dwtk_meldungen` exists in BigQuery with the `timecreated` and `job_kennung` columns.
    *   Ensure the source table `cds.cds_ta_action_assoc` exists in BigQuery with the `cntrct_id`, `rv_action_id`, `insert_at`, `valid_from`, `is_production`, `modified_at`, and `valid_to` columns.
3.  **Data Ingestion for `cds_ta_action_assoc`:**
    *   If `cds$ta_action_assoc` is still an active Oracle source, establish a robust data ingestion pipeline (e.g., BigQuery Data Transfer Service, custom ETL, or a separate Airflow DAG) to regularly load data from the Oracle `cds$ta_action_assoc` into the BigQuery `cds.cds_ta_action_assoc` table.
4.  **IAM Permissions:**
    *   Grant the Airflow service account (associated with the Composer environment) the necessary BigQuery permissions:
        *   `BigQuery Data Editor` on the `sof` dataset (for `TRUNCATE` and `INSERT`).
        *   `BigQuery Data Viewer` on the `isbert_schema` and `cds` datasets (for `SELECT`).
        *   `BigQuery Job User` for running queries.
5.  **Airflow GCP Connection:**
    *   Verify that the `google_cloud_default` connection is correctly configured in the Airflow environment, pointing to the target GCP project.
6.  **Airflow DAG Deployment & Scheduling:**
    *   Deploy the `d_ausd_v_ta_action_assoc.py` DAG to the Airflow environment.
    *   Configure the `schedule` parameter within the DAG to match the original UC4 job's schedule (e.g., `schedule="0 3 * * *"` for daily at 3 AM UTC, or `schedule=None` if triggered externally).
    *   Enable the DAG in the Airflow UI.

## 5. Known gaps & unresolved references

*   **`p_EintragsNr` Parameter Usage:** The original KornShell script parsed `p_EintragsNr`, but it was not used in the Oracle SQL. The migrated DAG omits this parameter. If `p_EintragsNr` has a latent or future purpose, it needs to be explicitly re-introduced and handled in the Airflow DAG.
*   **`VIA` Table Discrepancy:** The `lineage_edges` indicated a write to `TABLE:VIA`, which was not present in the provided Oracle SQL. This discrepancy was assumed to be a historical or irrelevant reference for this migration. If `TABLE:VIA` is a valid target, its migration and inclusion in the BigQuery logic must be addressed.
*   **Error Reporting/Alerting:** The original `f_alis_msgerr.ksh` and `DWMSG_MeldeFehler` likely provided specific error reporting. This migration relies on Airflow's native alerting mechanisms (e.g., email on failure). If custom or more sophisticated alerting is required, it needs to be implemented within the Airflow DAG or as a separate monitoring solution.
*   **Missing Complexity/Automation Data:** The absence of `file_complexity` and `automation_rate` in the source inventory means the migration effort was based solely on code analysis. This could imply unforeseen complexities that might require additional manual review.
*   **`h_alis_sqlplus.ksh` Hidden Functionalities:** While the core function of `starteSQLSkript` (executing SQL) was addressed, any other subtle functionalities or side effects within `h_alis_sqlplus.ksh` or other sourced utility scripts were not explicitly migrated. It's assumed they were primarily related to environment setup and basic execution, now handled by Airflow.

## 6. Validation

To validate the successful migration and operation of the `d_ausd_v_ta_action_assoc` job:

1.  **Trigger the Airflow DAG:** Manually trigger the `d_ausd_v_ta_action_assoc` DAG from the Airflow UI.
2.  **Monitor Task Execution:** Observe the `process_ta_action_assoc` task in the Airflow UI.
    *   **Passing:** The task should complete successfully (green status). Check the task logs for any BigQuery errors or warnings.
3.  **Verify BigQuery Job History:**
    *   Navigate to the BigQuery UI and check the "Query history" for the executed query. Ensure it completed without errors.
4.  **Data Verification in BigQuery:**
    *   **Row Count:** Execute `SELECT COUNT(*) FROM sof.sof_ta_action_assoc;` in BigQuery. Compare the number of rows with the expected count from a successful run of the legacy job.
    *   **Sample Data:** Query `SELECT * FROM sof.sof_ta_action_assoc LIMIT 100;` and visually inspect a sample of the inserted data to ensure correctness and consistency with the source data and expected transformations.
    *   **`v_datum` Calculation:** Manually verify the `v_datum` calculation by running the `SELECT COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101') FROM \`isbert_schema.dwtk_meldungen\` m WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE';` query in BigQuery and comparing it to the date used by the legacy job.
    *   **Filtering Logic:** Confirm that the `WHERE` clause conditions (especially date comparisons and `is_production`) are correctly applied by comparing a few records against the source `cds.cds_ta_action_assoc` table.

## 7. Rollback procedure

In case of issues or failure of the migrated job, the following rollback procedure can be executed:

1.  **Disable Airflow DAG:** In the Airflow UI, disable the `d_ausd_v_ta_action_assoc` DAG to prevent further executions.
2.  **Re-enable Legacy Job:** Re-enable the original UC4 job (`DW.BERT_AUSD_V_TA_ACTION_ASSOC.xml`) in the legacy environment.
3.  **Verify Legacy Job Execution:** Monitor the re-enabled UC4 job to ensure it runs successfully and processes data as expected in the Oracle environment.
4.  **Data State (Optional):** If the BigQuery target table `sof.sof_ta_action_assoc` was corrupted or incorrectly populated, it might be necessary to truncate it and re-load it with correct data, potentially from a backup or by re-running a corrected version of the Airflow DAG. This step depends on the severity of the issue and data retention policies.