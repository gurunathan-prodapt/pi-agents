# MIGRATION_NOTES.md

## 1. Summary

The ETL job `k_ausd_v_ta_action_assoc.ksh` has been migrated from its legacy KornShell and Oracle SQL*Plus environment to Google Cloud Platform (GCP).

**Original System:**
*   **Orchestration:** KornShell script (`k_ausd_v_ta_action_assoc.ksh`) executed via UC4 scheduler on a UNIX host (DWHDWH1P).
*   **Data Processing:** Oracle SQL script (`d_ausd_v_ta_action_assoc.sql`) connecting to an Oracle database (Carmen DB via `v_carmen` DB link for `cds$ta_action_assoc` and `isbert_schema.dwtk_meldungen`).
*   **Purpose:** Synchronize contract-to-action associations from a source system (`cds$ta_action_assoc`) into a data warehouse table (`sof$ta_action_assoc`), filtering records based on a dynamic date (`v_datum`) and production flags.

**Target Platform:**
*   **Orchestration:** Google Cloud Composer (Apache Airflow) using a Python DAG (`dw_bert_ausd_v_ta_action_assoc_dag.py`).
*   **Data Processing:** Google BigQuery, executing native BigQuery SQL (`d_ausd_v_ta_action_assoc_bq.sql`).
*   **Data Storage:** All source and target tables (`cds_ta_action_assoc`, `dwtk_meldungen`, `sof_ta_action_assoc`) are now resident in Google BigQuery.

## 2. Generated Artifacts

The migration process generated the following files:

*   **`d_ausd_v_ta_action_assoc_bq.sql`**
    *   **Role:** This file contains the core ETL logic, translated from the original Oracle SQL to BigQuery SQL. It is responsible for:
        *   Determining the `v_datum` by querying the `dwtk_meldungen` table.
        *   Truncating the target `sof_ta_action_assoc` table.
        *   Inserting filtered and transformed data from `cds_ta_action_assoc` into `sof_ta_action_assoc`.
    *   **Location:** Stored in a version control system (e.g., Git) and referenced by the Airflow DAG.

*   **`dw_bert_ausd_v_ta_action_assoc_dag.py`**
    *   **Role:** This is the Apache Airflow DAG definition written in Python. It replaces the legacy KornShell script and UC4 scheduling. Its responsibilities include:
        *   Defining the workflow (currently a single task).
        *   Orchestrating the execution of the BigQuery SQL script using the `BigQueryOperator`.
        *   Configuring default arguments, scheduling, and basic error handling for the job.
    *   **Location:** Deployed to the Airflow DAGs folder in Google Cloud Composer.

## 3. Key Design Decisions

*   **Cloud-Native Architecture:** The job was migrated to Google Cloud Platform to leverage its scalable, managed services (BigQuery for data warehousing, Cloud Composer for orchestration). This aligns with modern data platform strategies, offering improved performance, reliability, and reduced operational overhead compared to on-premise legacy systems.
*   **BigQuery as the Primary Data Platform:** All data sources (`cds$ta_action_assoc`, `isbert_schema.dwtk_meldungen`) and the target table (`sof$ta_action_assoc`) are assumed to be pre-migrated or continuously replicated into BigQuery. This eliminates the need for complex cross-database links or federated queries to external Oracle systems during runtime, simplifying the ETL process and maximizing BigQuery's performance benefits.
*   **Direct SQL Translation:** The core ETL logic from the Oracle SQL script was directly translated into BigQuery SQL. This approach minimizes changes to the business logic, ensuring functional parity with the original job. Oracle-specific functions (e.g., `TO_DATE`, `NVL`) were replaced with their BigQuery equivalents (e.g., `PARSE_DATE`, `COALESCE`).
*   **Airflow for Orchestration:** Apache Airflow was chosen to replace the KornShell script and UC4 scheduler. Airflow provides a robust, programmatic, and observable way to define, schedule, and monitor data pipelines. It offers superior capabilities for dependency management, error handling, logging, and extensibility compared to shell scripting.
*   **Truncate-and-Load Strategy Preservation:** The original job's `TRUNCATE TABLE` followed by `INSERT INTO ... SELECT` pattern was maintained. This implies a full refresh of the target table with each run, reflecting the source's state as of `v_datum`. This decision was made to preserve the existing data loading strategy.
*   **Hardcoded `job_kennung`:** The `job_kennung = 'BERT_DROP_TEMP_TABLE'` used in the `v_datum` calculation was hardcoded into the BigQuery SQL. This directly reflects the logic found in the original script and avoids introducing new dynamic parameters unless explicitly required by business.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (e.g., `my_dataset`) exists in `my_gcp_project`. If not, create it.

2.  **Target Table DDL Creation:**
    *   Create the `sof_ta_action_assoc` table in the designated BigQuery dataset. The schema can be inferred from the `INSERT` statement in `d_ausd_v_ta_action_assoc_bq.sql`.
        ```sql
        CREATE TABLE `my_gcp_project.my_dataset.sof_ta_action_assoc` (
            cntrct_id INT64,
            rv_action_id INT64
            -- Add other columns if they exist in the original target table
        );
        ```

3.  **Source Data Ingestion into BigQuery:**
    *   **Critical Step:** The source tables `cds_ta_action_assoc` and `dwtk_meldungen` must be present and populated in BigQuery (e.g., in `my_gcp_project.my_dataset`). This typically involves:
        *   Setting up **Google Cloud Data Transfer Service (DTS)** for continuous, incremental replication from the source Oracle database to BigQuery.
        *   Alternatively, for less frequent updates, establish a batch export process from Oracle to Cloud Storage, followed by BigQuery load jobs.
    *   Verify that the data in these BigQuery tables is up-to-date and accurately reflects the Oracle source.

4.  **IAM Permissions Configuration:**
    *   The Google Cloud Service Account used by the Airflow environment (Cloud Composer) must have the necessary BigQuery permissions:
        *   **BigQuery Data Editor** role (or equivalent custom role) on `my_gcp_project.my_dataset` to allow `TRUNCATE` and `INSERT` operations on `sof_ta_action_assoc`.
        *   **BigQuery Data Viewer** role (or equivalent custom role) on `my_gcp_project.my_dataset` to allow `SELECT` operations on `cds_ta_action_assoc` and `dwtk_meldungen`.

5.  **Airflow Connection Configuration:**
    *   Ensure the `google_cloud_default` connection is properly configured in your Airflow environment, pointing to the correct GCP project and credentials.

6.  **Scheduling Definition:**
    *   The `schedule_interval` in `dw_bert_ausd_v_ta_action_assoc_dag.py` is currently set to `None`. This must be updated to reflect the actual business scheduling requirements (e.g., `timedelta(days=1)` for daily, a cron expression, etc.).

7.  **Placeholder Replacement:**
    *   Replace all instances of `my_gcp_project` with the actual Google Cloud Project ID.
    *   Replace all instances of `my_dataset` with the actual BigQuery Dataset ID where the tables reside.

## 5. Known Gaps & Unresolved References

*   **Oracle Source Data Migration (`v_carmen` DB Link):** The migration design explicitly flags the `v_carmen` DB link (`@pcrs1`) as an unresolved dependency. While the generated BigQuery SQL assumes `cds_ta_action_assoc` and `dwtk_meldungen` are already in BigQuery, the actual process for migrating or continuously replicating these critical source tables from Oracle to BigQuery must be fully implemented and verified. This is a prerequisite for the migrated job to function correctly.
*   **Dynamic Parameter Handling:** The original KornShell script likely handled dynamic parameters (e.g., `-j` for JobKennung, `-f` for EintragsNr). The current BigQuery SQL hardcodes `job_kennung = 'BERT_DROP_TEMP_TABLE'`. If these parameters were intended to be dynamic and influence the `v_datum` calculation or other logic, the Airflow DAG needs to be enhanced to accept and pass these parameters (e.g., via Airflow variables, DAG run configuration, or a Python task) and the BigQuery SQL modified to utilize them.
*   **Custom Error Handling and Logging:** The legacy KornShell scripts used custom utilities (`f_alis_msgerr.ksh`, `DWMSG_MeldeFehler`, etc.) for error reporting and logging. The Airflow DAG currently relies on Airflow's default logging. If specific error notification mechanisms (e.g., custom email alerts, Slack integration, PagerDuty) are required, the `on_failure_callback` in the DAG's `default_args` needs to be implemented.
*   **DAG `start_date`:** The `start_date` in the Airflow DAG is set to `days_ago(1)`. For production deployments, it's best practice to set this to a fixed, non-dynamic historical date to ensure consistent DAG behavior.
*   **`v_datum` Default Value:** The `COALESCE` function in the `v_datum` calculation defaults to `'19000101'` if `MAX(m.timecreated)` is NULL. Confirm if this default value is still appropriate for the business logic in the BigQuery environment.

## 6. Validation

To ensure the migrated job functions correctly and produces accurate results, the following validation steps should be performed:

1.  **Unit Testing (BigQuery SQL):**
    *   Manually execute the `d_ausd_v_ta_action_assoc_bq.sql` script in the BigQuery console.
    *   Use representative sample data in `my_gcp_project.my_dataset.cds_ta_action_assoc` and `my_gcp_project.my_dataset.dwtk_meldungen`.
    *   Verify that the `v_datum` variable is correctly calculated.
    *   Confirm that the `TRUNCATE TABLE` and `INSERT INTO` operations execute without syntax errors and populate `my_gcp_project.my_dataset.sof_ta_action_assoc` as expected.

2.  **Airflow DAG Testing:**
    *   Deploy the `dw_bert_ausd_v_ta_action_assoc_dag.py` to a development or staging Airflow environment (Cloud Composer).
    *   Trigger the DAG manually.
    *   Monitor the Airflow UI and logs for successful task completion and any errors.
    *   Verify that the `BigQueryOperator` task successfully executes the SQL and interacts with BigQuery.

3.  **Data Validation and Comparison:**
    *   **Row Count Verification:** Compare the number of rows in the BigQuery `sof_ta_action_assoc` table after a successful DAG run with the row count from the corresponding target table in the legacy Oracle system (or the expected output of the original job).
    *   **Data Sample Comparison:** Select a representative sample of records from the BigQuery `sof_ta_action_assoc` table and compare them against the expected output based on the original Oracle job's logic and source data. Pay close attention to `cntrct_id` and `rv_action_id`.
    *   **Edge Case Testing:** Test with scenarios that might trigger the `v_datum` default or specific filtering conditions (e.g., `modified_at IS NULL`, `valid_to IS NULL`).

**"Passing" Criteria:**
*   The `dw_bert_ausd_v_ta_action_assoc_dag` completes successfully in Airflow without any task failures.
*   The `my_gcp_project.my_dataset.sof_ta_action_assoc` table in BigQuery is populated with data.
*   The data in `my_gcp_project.my_dataset.sof_ta_action_assoc` is functionally identical to the output produced by the legacy `k_ausd_v_ta_action_assoc.ksh` job, considering the same source data state.
*   All filtering conditions and date calculations (especially `v_datum`) are applied correctly, resulting in the expected subset of data.

## 7. Rollback Procedure

In the event of issues (e.g., DAG failure, incorrect data, performance degradation) after go-live, the following rollback procedure should be followed:

1.  **Immediate Action (Stop New Execution):**
    *   **Pause/Disable the Airflow DAG:** In the Airflow UI, locate `dw_bert_ausd_v_ta_action_assoc_dag` and set its status to "Paused" or "Off" to prevent further executions.
    *   **Re-enable Legacy Job:** Immediately re-enable the original UC4 job `k_ausd_v_ta_action_assoc.ksh` on the legacy UNIX host to ensure business continuity and data updates continue in the old system.

2.  **Data Rollback (if necessary):**
    *   Since the BigQuery job performs a `TRUNCATE` and `INSERT`, if the data in `my_gcp_project.my_dataset.sof_ta_action_assoc` is found to be incorrect or corrupted, the simplest approach is often to:
        *   Correct the underlying issue (e.g., fix the DAG code, address source data problems).
        *   Manually trigger a successful run of the corrected `dw_bert_ausd_v_ta_action_assoc_dag` in Airflow. This will truncate the incorrect data and reload it with the correct information.
    *   If a point-in-time restore is absolutely required and BigQuery table snapshots/backups are configured, restore `my_gcp_project.my_dataset.sof_ta_action_assoc` from a known good state.

3.  **Code Rollback:**
    *   If the issue is identified as a problem with the migrated Airflow DAG code:
        *   Revert the `dw_bert_ausd_v_ta_action_assoc_dag.py` file in your version control system to the last known stable version.
        *   Redeploy the reverted DAG file to the Airflow DAGs folder in Cloud Composer.
        *   Once the issue is resolved and the DAG is validated, re-enable it in Airflow.

This procedure ensures that data processing can quickly revert to the stable legacy system while the issues in the new environment are investigated and resolved.