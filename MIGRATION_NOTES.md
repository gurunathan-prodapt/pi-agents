# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the `r_ausd_v_ta_inv_assign.ksh` job, responsible for synchronizing `ta_inv_assign` contract data. The original workflow, comprising KornShell scripts (`r_ausd_v_ta_inv_assign.ksh`, `k_ausd_v_ta_inv_assign.ksh`) and an Oracle SQL script (`d_ausd_v_ta_inv_assign.sql`), has been replatformed.

**Original Platform:**
*   **Orchestration:** UC4 Scheduler
*   **Processing:** KornShell scripts, Oracle SQL*Plus
*   **Database:** Oracle (source `cds$ta_inv_assignment` via DB-Link, target `sof$ta_inv_assign`, metadata `isbert_schema.dwtk_meldungen`)
*   **Logging:** Custom shell functions (`DWMSG_*`) and `f_alis_msgerr.ksh`

**Target Platform:**
*   **Orchestration:** Google Cloud Scheduler (or Cloud Composer for complex scenarios)
*   **Processing:** Google BigQuery Stored Procedure
*   **Database:** Google BigQuery (tables `project.dataset.sof_ta_inv_assign`, `project.dataset.cds_ta_inv_assignment`, `project.dataset.dwtk_meldungen`, `project.dataset.job_log`)
*   **Logging:** Dedicated BigQuery `job_log` table and BigQuery's native error handling.

The core logic of extracting filtered contract assignment data based on date-effectiveness and production flags, truncating the target table, and inserting new data, has been fully translated into a BigQuery Stored Procedure.

## 2. Generated Artifacts

The migration process generated the following BigQuery-specific artifacts:

*   **`project/dataset/ddl/sof_ta_inv_assign.sql`**
    *   **Role:** BigQuery DDL (Data Definition Language) script to create the target table `project.dataset.sof_ta_inv_assign`. This table replaces the Oracle `sof$ta_inv_assign` and will store the synchronized contract assignment data. It includes partitioning and clustering for optimized performance.
*   **`project/dataset/ddl/cds_ta_inv_assignment.sql`**
    *   **Role:** BigQuery DDL script to create the `project.dataset.cds_ta_inv_assignment` table. This table will serve as the BigQuery representation of the original Oracle `cds$ta_inv_assignment` source table (accessed via the `CARMEN` DB-Link). Data for this table must be ingested separately into BigQuery.
*   **`project/dataset/ddl/dwtk_meldungen.sql`**
    *   **Role:** BigQuery DDL script to create the `project.dataset.dwtk_meldungen` table. This table replaces the Oracle `isbert_schema.dwtk_meldungen` and is used to store job metadata, specifically to determine the cutoff date for data synchronization. Data for this table must also be ingested separately into BigQuery.
*   **`project/dataset/ddl/job_log.sql`**
    *   **Role:** BigQuery DDL script to create the `project.dataset.job_log` table. This table is a new component designed to centralize logging and auditing for the migrated job, replacing the custom shell and SQL*Plus logging mechanisms. It captures job status, start/end times, record counts, and error messages.
*   **`project/dataset/stored_procedures/r_ausd_v_ta_inv_assign.sql`**
    *   **Role:** BigQuery Stored Procedure that encapsulates the entire business logic of the original `r_ausd_v_ta_inv_assign.ksh`, `k_ausd_v_ta_inv_assign.ksh`, and `d_ausd_v_ta_inv_assign.sql` scripts. It handles parameter validation, cutoff date determination, truncating the target table, inserting filtered data, and comprehensive logging into `project.dataset.job_log`.

## 3. Key Design Decisions

The following key design decisions were made during the migration:

*   **Consolidation into a Single BigQuery Stored Procedure (BQSP):**
    *   **Why:** To simplify the architecture, reduce operational overhead, and leverage BigQuery's native capabilities. Combining the wrapper script, core script, and SQL logic into one BQSP eliminates the need for shell scripting, SQL*Plus, and inter-process communication, leading to a more robust, performant, and maintainable solution.
    *   **Trade-offs:** Requires a complete rewrite of the shell logic into BigQuery SQL, including parameter handling, error management, and logging. This also means the BQSP becomes a single point of failure for the entire workflow.
*   **BigQuery as the Primary Data Platform:**
    *   **Why:** BigQuery offers a highly scalable, fully managed, and cost-effective data warehouse solution, ideal for analytical workloads. It provides superior performance for large datasets compared to traditional relational databases for this type of batch processing.
    *   **Trade-offs:** Requires a data ingestion strategy for source Oracle tables (`cds$ta_inv_assignment`, `isbert_schema.dwtk_meldungen`) into BigQuery, introducing potential data latency and additional ETL pipeline management.
*   **Replacement of Custom Logging with a Dedicated `job_log` Table:**
    *   **Why:** The original custom `DWMSG_*` functions and `f_alis_msgerr.ksh` were shell-specific. A dedicated `job_log` table in BigQuery provides a standardized, centralized, and queryable logging mechanism. This improves observability, simplifies monitoring, and enables easier integration with GCP's operational tools (e.g., Cloud Logging, Cloud Monitoring).
    *   **Trade-offs:** Requires defining a new schema for logging and implementing `INSERT` statements within the BQSP for each log event.
*   **Direct Translation of Truncate-and-Load Pattern:**
    *   **Why:** The original job used a `TRUNCATE TABLE` followed by `INSERT INTO ... SELECT ...` pattern. This was directly translated to BigQuery's `TRUNCATE TABLE` and `INSERT INTO ... SELECT` statements. This approach ensures idempotency and a fresh load of data, consistent with the original job's behavior.
    *   **Trade-offs:** This pattern can be resource-intensive for very large tables if the target table needs to be available continuously. However, for a staging table synchronization, it's often acceptable.
*   **Cloud-Native Orchestration (Cloud Scheduler/Composer):**
    *   **Why:** To replace the legacy UC4 scheduler with a managed, scalable, and integrated GCP service. Cloud Scheduler is suitable for simple, time-based triggers, while Cloud Composer (Apache Airflow) offers advanced workflow management, dependency handling, and monitoring for more complex scenarios.
    *   **Trade-offs:** Requires learning and configuring new scheduling tools.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the BigQuery dataset `project.dataset` exists in your GCP project. If not, create it:
        ```bash
        bq mk --location=US project:dataset
        ```
        (Adjust location as per your requirements).

2.  **Deploy BigQuery DDLs:**
    *   Execute the generated DDL scripts to create the necessary tables in BigQuery:
        ```bash
        bq query --use_legacy_sql=false < project/dataset/ddl/sof_ta_inv_assign.sql
        bq query --use_legacy_sql=false < project/dataset/ddl/cds_ta_inv_assignment.sql
        bq query --use_legacy_sql=false < project/dataset/ddl/dwtk_meldungen.sql
        bq query --use_legacy_sql=false < project/dataset/ddl/job_log.sql
        ```
    *   **Crucial:** Review and update the placeholder schemas in `cds_ta_inv_assignment.sql` and `dwtk_meldungen.sql` with the actual column names and data types from the source Oracle tables.

3.  **Deploy BigQuery Stored Procedure:**
    *   Execute the generated BQSP script to create the stored procedure:
        ```bash
        bq query --use_legacy_sql=false < project/dataset/stored_procedures/r_ausd_v_ta_inv_assign.sql
        ```

4.  **Data Ingestion Setup for Source Tables:**
    *   **`project.dataset.cds_ta_inv_assignment`:**
        *   Establish a robust and recurring data ingestion pipeline from the source Oracle `cds$ta_inv_assignment` table into `project.dataset.cds_ta_inv_assignment`. Options include:
            *   **BigQuery Data Transfer Service:** If Oracle is a supported source.
            *   **Cloud Dataflow/Dataproc:** For custom ETL using Java/Python/Go to extract from Oracle and load into BigQuery.
            *   **Third-party ETL tools:** That support Oracle to BigQuery replication.
        *   Ensure the ingestion frequency meets the business requirements for data freshness.
    *   **`project.dataset.dwtk_meldungen`:**
        *   Set up an initial load of historical data from `isbert_schema.dwtk_meldungen` into `project.dataset.dwtk_meldungen`.
        *   Establish an ongoing synchronization mechanism to keep this table up-to-date, as it's critical for determining the `v_datum` cutoff.

5.  **IAM Permissions:**
    *   Ensure the service account that will execute the BigQuery Stored Procedure (e.g., via Cloud Scheduler or Composer) has the following roles:
        *   `BigQuery Data Editor` on `project.dataset` (or more granular `bigquery.tables.updateData`, `bigquery.tables.truncate`, `bigquery.tables.getData`, `bigquery.routines.call` permissions).
        *   `BigQuery Job User` to run BigQuery jobs.
        *   If using Cloud Scheduler, the Cloud Scheduler service account needs `Cloud Functions Invoker` (if triggering a Cloud Function) or `BigQuery Data Editor` (if directly calling BQSP via a custom HTTP endpoint).
        *   If using Cloud Composer, the Composer service account needs the above BigQuery permissions.

6.  **Scheduling Configuration:**
    *   **Cloud Scheduler (for direct BQSP execution):**
        *   Create a Cloud Scheduler job that triggers the BigQuery Stored Procedure. This typically involves calling a Cloud Function that, in turn, executes the BQSP.
        *   Define the cron schedule (`0 0 * * *` for daily at midnight UTC, for example).
        *   Configure the payload to pass `p_JobKennung` and `p_EintragsNr` to the BQSP.
    *   **Cloud Composer (for Airflow DAG):**
        *   Develop and deploy an Apache Airflow DAG (Python) that uses the `BigQueryOperator` or `BigQueryExecuteQueryOperator` to call the `project.dataset.r_ausd_v_ta_inv_assign` stored procedure.
        *   Define the DAG's schedule, dependencies, and parameters.

7.  **Secrets Management:**
    *   If any connection details for the Oracle source (for ingestion pipelines) are required, ensure they are securely stored in Google Secret Manager and accessed appropriately by the ingestion jobs.

## 5. Known Gaps & Unresolved References

The following items were identified as gaps or require further attention:

*   **Full Schema for Source Tables:** The DDLs for `project.dataset.cds_ta_inv_assignment` and `project.dataset.dwtk_meldungen` contain placeholder columns (`some_value`, `message_text`). The actual schemas from the source Oracle tables must be obtained and used to create accurate BigQuery tables.
*   **`p_EintragsNr` Parameter Usage:** The original design document mentions `p_EintragsNr` as a parameter, and it's passed to the BQSP and logged. However, its functional use within the original `k_ausd_v_ta_inv_assign.ksh` or `d_ausd_v_ta_inv_assign.sql` was not explicitly detailed, and it's not used in the BQSP's core logic beyond logging. Confirm if this parameter had any other functional impact that needs to be replicated.
*   **`v_datum` Determination Logic:** The BQSP relies on `MAX(timecreated)` from `dwtk_meldungen` where `job_kennung = 'BERT_DROP_TEMP_TABLE'`. Ensure this logic accurately reflects the intended cutoff date determination from the original Oracle environment. Any nuances in how `BERT_DROP_TEMP_TABLE` entries are managed or if other `job_kennung` values are relevant should be investigated.
*   **Data Latency for `cds_ta_inv_assignment`:** The migration introduces an ingestion step for `cds_ta_inv_assignment`. The latency of this ingestion (e.g., hourly, daily) needs to be aligned with business requirements. If the original Oracle DB-Link provided near real-time access, a batch ingestion might introduce unacceptable delays.
*   **`DWPA_UTIL_SKRIPT.runstatement` Behavior:** The original Oracle script used `DWPA_UTIL_SKRIPT.runstatement('TRUNCATE TABLE sof$ta_inv_assign')`. While a direct `TRUNCATE TABLE` is used in BigQuery, it's important to confirm if the Oracle package performed any additional pre/post-truncation logic (e.g., logging, auditing, specific locking) that needs to be replicated.
*   **Original `.dw_init` Script:** The contents and dependencies of the `. $HOME/.dw_init` script from the original environment were not fully analyzed. While environment variables and basic setup are typically handled by the cloud environment, any critical functions or configurations within `.dw_init` might need to be explicitly addressed or re-implemented.

## 6. Validation

To validate the successful migration and operation of the `r_ausd_v_ta_inv_assign` job:

1.  **Manual Execution of BQSP:**
    *   Execute the BigQuery Stored Procedure manually with sample parameters:
        ```sql
        CALL project.dataset.r_ausd_v_ta_inv_assign('TEST_JOB', 123);
        ```
    *   Verify the execution completes without errors.

2.  **Scheduled Execution:**
    *   Trigger the job via the configured Cloud Scheduler job or Cloud Composer DAG.
    *   Monitor the job execution status in Cloud Logging and the `project.dataset.job_log` table.

3.  **Data Verification:**
    *   **Target Table:** Query `project.dataset.sof_ta_inv_assign` to ensure data is loaded correctly.
        *   Check the number of rows inserted (should match `record_count` in `job_log`).
        *   Spot-check data values against the source `cds_ta_inv_assignment` and the applied filtering logic.
        *   Verify that the `TRUNCATE` operation correctly cleared previous data before insertion.
    *   **Source Data:** Ensure `project.dataset.cds_ta_inv_assignment` and `project.dataset.dwtk_meldungen` contain up-to-date and accurate data from their respective Oracle sources.

4.  **Logging Verification:**
    *   Query `project.dataset.job_log` for the latest execution of `r_ausd_v_ta_inv_assign`.
    *   Verify `status` is 'SUCCESS', `start_time`, `end_time`, and `record_count` are populated correctly.
    *   In case of failure, `status` should be 'FAILED' and `error_message` should contain relevant details.
    *   Check Cloud Logging for any BigQuery job errors or warnings.

5.  **Performance Monitoring:**
    *   Monitor BigQuery job execution time to ensure it meets performance SLAs.
    *   Review BigQuery slot consumption and byte processing to optimize costs if necessary.

**"Passing" Criteria:**

*   The `project.dataset.r_ausd_v_ta_inv_assign` BigQuery Stored Procedure completes successfully with a `status` of 'SUCCESS' in the `project.dataset.job_log` table.
*   The `record_count` in `job_log` accurately reflects the number of rows inserted into `project.dataset.sof_ta_inv_assign`.
*   The `project.dataset.sof_ta_inv_assign` table contains the expected data, accurately filtered and transformed according to the business logic.
*   No unexpected errors or warnings are observed in Cloud Logging or BigQuery job history.
*   The job completes within the defined performance window.

## 7. Rollback Procedure

In the event of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Stop New Executions:**
    *   **Immediately disable** the Cloud Scheduler job or Cloud Composer DAG that triggers `project.dataset.r_ausd_v_ta_inv_assign`. This prevents further execution of the migrated job.

2.  **Re-enable Original Job:**
    *   Re-enable the original `r_ausd_v_ta_inv_assign.ksh` job in the UC4 scheduler. Ensure all necessary environment variables and dependencies for the original job are correctly configured and operational.

3.  **Data State (Critical Consideration):**
    *   The BigQuery Stored Procedure performs a `TRUNCATE TABLE` followed by an `INSERT`. This means `project.dataset.sof_ta_inv_assign` will contain data from the last successful BigQuery run.
    *   **If data consistency is paramount and the BigQuery data is deemed incorrect:**
        *   The `project.dataset.sof_ta_inv_assign` table may need to be truncated or restored from a backup if its state is critical for downstream processes.
        *   The original Oracle job, once re-enabled, will re-populate its `sof$ta_inv_assign` table. Depending on the timing and the nature of the error, a full re-run of the original job might be necessary to ensure data integrity.

4.  **Revert BigQuery Objects (Optional, for clean-up):**
    *   If a full rollback and removal of the migrated components are desired, the BigQuery Stored Procedure can be dropped:
        ```sql
        DROP PROCEDURE IF EXISTS project.dataset.r_ausd_v_ta_inv_assign;
        ```
    *   The BigQuery tables (`sof_ta_inv_assign`, `cds_ta_inv_assignment`, `dwtk_meldungen`, `job_log`) can be retained for analysis or dropped if no longer needed. Dropping them would require:
        ```sql
        DROP TABLE IF EXISTS project.dataset.sof_ta_inv_assign;
        -- ... and so on for other tables
        ```

5.  **Post-Rollback Analysis:**
    *   Thoroughly investigate the root cause of the issue that necessitated the rollback using the `project.dataset.job_log` table and Cloud Logging. Rectify the issue before attempting re-migration.