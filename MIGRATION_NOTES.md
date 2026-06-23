# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the `k_ausd_bp_ta_bpr_opt_text.ksh` job. This KornShell script, originally responsible for orchestrating data processing by executing an SQL script (`d_ausd_bp_ta_bpr_opt_text.sql`), validating parameters, and logging record counts, has been migrated.

The job's functionality has been re-platformed to **Google BigQuery** for data processing and **Cloud Composer (Apache Airflow)** for orchestration. The core logic of both the shell script and the embedded SQL has been consolidated into BigQuery Stored Procedures.

## 2. Generated Artifacts

The migration process has resulted in the following key artifacts:

*   **BigQuery Dataset:**
    *   `project.dataset` (e.g., `isrpt_bert_prod.aufbereitung`): The target BigQuery dataset where all related tables and stored procedures reside.

*   **BigQuery Tables:**
    *   `project.dataset.DWTK_MELDUNGEN`: Migrated version of the source Oracle `DWTK_MELDUNGEN` table.
    *   `project.dataset.SOF_TA_BPR_OPTIONEN`: Migrated version of the source Oracle `SOF$TA_BPR_OPTIONEN` table.
    *   `project.dataset.SOF_TA_BPR_OPT_TEXT`: Migrated version of the target Oracle `SOF$TA_BPR_OPT_TEXT` table.
    *   `project.dataset.job_log`: A new BigQuery table designed to capture job execution logs, status, parameters, and record counts, replacing the original file-based logging and job table entries.

*   **BigQuery Stored Procedures:**
    *   `project.dataset.p_bpr_opt_text_processing`: Encapsulates the core data processing logic originally found in `d_ausd_bp_ta_bpr_opt_text.sql`. This procedure performs the DML/DQL operations on the BigQuery tables.
    *   `project.dataset.r_ausd_bp_ta_bpr_opt_text`: The main orchestration BigQuery Stored Procedure. It replaces the `k_ausd_bp_ta_bpr_opt_text.ksh` script, handling parameter validation, date derivations, calling `p_bpr_opt_text_processing`, and logging results to `project.dataset.job_log`.

*   **Cloud Composer (Airflow) DAG:**
    *   `r_ausd_bp_ta_bpr_opt_text_dag.py`: A Python-based Airflow DAG responsible for scheduling and invoking the `project.dataset.r_ausd_bp_ta_bpr_opt_text` BigQuery Stored Procedure, passing necessary runtime parameters.

*   **Cloud Monitoring Configuration:**
    *   Alerting policies (YAML/JSON): Configured to monitor BigQuery job failures or specific log entries in `project.dataset.job_log`, providing cloud-native alerting capabilities.

## 3. Key Design Decisions

*   **Consolidation into BigQuery Stored Procedures:** The primary design decision was to merge the control flow logic of the KornShell script (`k_ausd_bp_ta_bpr_opt_text.ksh`) and the data processing logic of the SQL script (`d_ausd_bp_ta_bpr_opt_text.sql`) into BigQuery Stored Procedures.
    *   **Rationale:** This approach leverages BigQuery's native capabilities for complex SQL operations, parameter handling, and procedural logic, reducing the need for external scripting and simplifying the overall architecture. It eliminates the overhead of `sqlplus` calls and file-based inter-process communication.
    *   **Trade-offs:** Requires a complete rewrite of shell-specific logic (e.g., `getopts`, `if` conditions, external utility calls) into BigQuery SQL. Potential for increased complexity within a single BigQuery Stored Procedure if the original logic was highly distributed across many shell functions.

*   **Native BigQuery Features for Utilities:** All functionalities provided by external KornShell utilities (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`, `gestern.ksh`) have been replaced with native BigQuery SQL functions and constructs.
    *   **Rationale:** Improves performance by keeping all operations within the BigQuery engine, reduces external dependencies, and simplifies maintenance.
    *   **Trade-offs:** Requires careful translation of shell logic (e.g., date formatting, parameter validation regex) into BigQuery SQL equivalents.

*   **BigQuery Logging Table:** Replaced file-based temporary output and potential job table entries with a dedicated `project.dataset.job_log` table.
    *   **Rationale:** Centralizes logging within the data warehouse, making it easily queryable, auditable, and integrable with other BigQuery-based monitoring tools.
    *   **Trade-offs:** Requires explicit `INSERT` statements for logging within the Stored Procedure, which adds to the SQL code.

*   **Cloud Composer for Orchestration:** Cloud Composer (Airflow) was chosen to replace the original `r_ausd_bp_ta_bpr_opt_text.ksh` wrapper script for scheduling and triggering the BigQuery Stored Procedure.
    *   **Rationale:** Provides robust, scalable, and managed orchestration capabilities, including dependency management, retries, monitoring, and integration with other GCP services.
    *   **Trade-offs:** Introduces a new technology stack (Python, Airflow concepts) for orchestration, requiring new skill sets for development and maintenance.

## 4. Manual Steps Before Go-Live

The following manual steps must be completed before the migrated job can be put into production:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (`project.dataset`, e.g., `isrpt_bert_prod.aufbereitung`) exists. If not, create it.

2.  **Data Migration (Prerequisite):**
    *   Verify that all source Oracle tables (`DWTK_MELDUNGEN`, `SOF$TA_BPR_OPTIONEN`, `SOF$TA_BPR_OPT_TEXT`) have been successfully migrated to their respective BigQuery tables (`project.dataset.DWTK_MELDUNGEN`, `project.dataset.SOF_TA_BPR_OPTIONEN`, `project.dataset.SOF_TA_BPR_OPT_TEXT`) and contain up-to-date data. This is a critical dependency.

3.  **BigQuery Logging Table Creation:**
    *   Execute the DDL to create the `project.dataset.job_log` table.

4.  **IAM Permissions Configuration:**
    *   Ensure the service account used by the Cloud Composer environment has the necessary BigQuery permissions:
        *   `BigQuery Data Editor` on `project.dataset` to read from `DWTK_MELDUNGEN`, `SOF_TA_BPR_OPTIONEN`, and write to `SOF_TA_BPR_OPT_TEXT` and `job_log`.
        *   `BigQuery Job User` to run BigQuery jobs (including stored procedures).
    *   Ensure the BigQuery Stored Procedures themselves have the necessary permissions to access the tables they read from and write to (typically inherited from the invoker, but explicit grants might be needed for specific scenarios).

5.  **Secrets Management (if applicable):**
    *   If any parameters passed to the job (e.g., `p_wiederanlaufWert` if it contains sensitive info) are considered secrets, ensure they are securely stored in Google Secret Manager and retrieved by the Airflow DAG.

6.  **Cloud Composer Environment Setup:**
    *   Verify the Cloud Composer environment is operational and has access to the BigQuery project.

7.  **Airflow DAG Deployment:**
    *   Deploy the `r_ausd_bp_ta_bpr_opt_text_dag.py` to the Cloud Composer environment's DAGs folder.

8.  **Scheduling Configuration:**
    *   Configure the schedule for the `r_ausd_bp_ta_bpr_opt_text_dag.py` within Airflow to match the original job's execution frequency.

9.  **Alerting Configuration:**
    *   Deploy the Cloud Monitoring alerting policies to ensure notifications are sent for job failures or critical log events.

## 5. Known Gaps & Unresolved References

The following items have been identified as potential gaps or require further investigation/follow-up:

*   **Complexity of `d_ausd_bp_ta_bpr_opt_text.sql` and `DWPA_UTIL_SKRIPT`:** The migration assumed a straightforward translation of SQL. If `d_ausd_bp_ta_bpr_opt_text.sql` contains highly complex or proprietary Oracle-specific SQL features, or if `DWPA_UTIL_SKRIPT` contains extensive, non-standard logic, additional effort may be required to re-implement these as BigQuery UDFs or sub-procedures. This could impact the `semi_auto` complexity assessment.
*   **Legacy Job Control System (`FOSJobErzeugeEintrag`, `FOSJobDeaktivate`):** The original script contained commented-out calls to a legacy job control system. While the plan is to replace this with BigQuery logging and Cloud Composer, it must be confirmed that there are no implicit dependencies or downstream systems that rely on these original job control system entries.
*   **Alerting/Messaging (`SENDS_MAIL` and `FUNCTION:DWMSG_ERMITTLENR`):** The `SENDS_MAIL` functionality, likely part of the original error messaging system, has been replaced with Cloud Monitoring alerts. A concrete design and implementation for the new alerting mechanism, including recipient lists and notification channels, must be finalized and tested.
*   **Legacy Commented-Out Logic (`sed`, `sort`, `join`):** The original script contained commented-out sections for file processing. These were treated as inactive and not migrated. A final confirmation is needed that this logic is indeed obsolete and will not be reactivated in the future. If it were to become active, it would require a different migration strategy (e.g., Cloud Storage, Dataflow).
*   **`PROCEDURE:SETZEZUSATZINFOS`:** The functionality of this Oracle procedure, called by `f_alis_msgerr.ksh`, needs to be reviewed. If it performs critical context setting or logging that is not covered by the new `job_log` table, its functionality might need to be re-implemented in BigQuery.

## 6. Validation

Validation of the migrated job involves several steps to ensure functional equivalence and performance:

1.  **Functional Testing:**
    *   **Input Parameters:** Run the Airflow DAG with various valid and invalid combinations of `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, and `p_wiederanlaufWert`.
    *   **Date Validation:** Test with valid and invalid `Stichtag` formats and values to ensure BigQuery's date validation behaves as expected.
    *   **Data Processing:** Execute the job with a representative dataset.
    *   **Passing Criteria:**
        *   The Airflow DAG completes successfully without errors.
        *   The BigQuery Stored Procedure `project.dataset.r_ausd_bp_ta_bpr_opt_text` completes successfully.
        *   The `project.dataset.job_log` table contains a successful entry for the execution, including correct `record_count` and `status`.
        *   The output data in `project.dataset.SOF_TA_BPR_OPT_TEXT` is identical to the output produced by the original Oracle job for the same input data and parameters. This requires a data comparison tool or query.

2.  **Performance Testing:**
    *   Run the job with production-like data volumes.
    *   **Passing Criteria:** The execution time of the BigQuery job is within acceptable limits, ideally matching or improving upon the original job's performance. BigQuery slot consumption should be monitored.

3.  **Error Handling and Alerting:**
    *   Intentionally introduce errors (e.g., invalid parameters, missing source data) to trigger error paths.
    *   **Passing Criteria:**
        *   The job fails gracefully, and appropriate error messages are logged to `project.dataset.job_log`.
        *   The configured Cloud Monitoring alerts are triggered and notifications are sent to the correct channels/recipients.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Deactivate New Job:**
    *   Immediately pause or disable the `r_ausd_bp_ta_bpr_opt_text_dag.py` in Cloud Composer to prevent further executions of the migrated job.

2.  **Re-enable Original Job:**
    *   Re-enable and restart the original `k_ausd_bp_ta_bpr_opt_text.ksh` job in the legacy environment. Ensure all necessary environment variables and dependencies for the original job are correctly configured.

3.  **Data Rollback (Critical Step):**
    *   **If `SOF_TA_BPR_OPT_TEXT` was modified by the new job:**
        *   Identify the data written or modified by the migrated BigQuery job.
        *   Utilize BigQuery's time travel feature to restore `project.dataset.SOF_TA_BPR_OPT_TEXT` to its state *before* the migrated job's execution.
        *   Alternatively, if specific backups were taken, restore the table from the most recent valid backup.
        *   Ensure data consistency with other dependent systems that might have read from or written to this table.

4.  **Monitor Original Job:**
    *   Verify that the original job executes successfully and produces the expected output in the legacy environment.

5.  **Post-Rollback Analysis:**
    *   Thoroughly investigate the root cause of the issue that necessitated the rollback. This may involve reviewing BigQuery job logs, Airflow logs, and comparing data states.
    *   Address the identified issues before attempting another migration or re-enabling the new job.