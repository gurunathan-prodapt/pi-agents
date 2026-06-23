# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the `r_ausd_v_ta_apn_ve.ksh` job, a KornShell script responsible for orchestrating the reconciliation of contract data for the `ta_apn_ve` table. The original job acted as a wrapper, handling parameter parsing, environment setup, logging, and error trapping, before invoking a core script (`k_ausd_v_ta_apn_ve.ksh`) which, in turn, executed SQL logic (`D_AUSD_V_TA_APN_VE.SQL`).

The migration replatforms this orchestration and core logic to **Google Cloud Platform (GCP)**. Specifically:
*   The orchestration layer (`r_ausd_v_ta_apn_ve.ksh`) is migrated to a **BigQuery Stored Procedure**.
*   The core business logic (`k_ausd_v_ta_apn_ve.ksh` and `D_AUSD_V_TA_APN_VE.SQL`) is also migrated into a **BigQuery Stored Procedure**.
*   Data storage for source and target tables is moved to **BigQuery tables**.
*   Logging and auditing are handled via a new **BigQuery audit log table** and **Cloud Logging**.
*   Scheduling, if complex, is intended to be managed by **Cloud Composer (Apache Airflow)**.

## 2. Generated artifacts

The migration process generated the following BigQuery SQL artifacts:

*   **`sql/ddl/job_log.sql`**
    *   **Role:** This DDL script creates the `job_log` table in BigQuery. This table serves as the central audit log for job executions, capturing start/end times, status, error messages, and other relevant metadata for each run of the migrated BigQuery Stored Procedures. It replaces the custom `DWMSG_` logging functions from the legacy system.

*   **`sql/stored_procedures/sp_k_ausd_v_ta_apn_ve_combined.sql`**
    *   **Role:** This BigQuery Stored Procedure (`sp_k_ausd_v_ta_apn_ve_combined`) encapsulates the core business logic previously found in `k_ausd_v_ta_apn_ve.ksh` and `D_AUSD_V_TA_APN_VE.SQL`. It is designed to perform the actual data reconciliation by reading from source BigQuery tables (e.g., `DWTK_MELDUNGEN`, `PDS$TA_PDP_CONTEXT_ASSOC`) and writing to target BigQuery tables (e.g., `SOF$TA_APN_VE`, `VIA`). This procedure is called by the orchestrator stored procedure.

*   **`sql/stored_procedures/sp_r_ausd_v_ta_apn_ve_orchestrator.sql`**
    *   **Role:** This BigQuery Stored Procedure (`sp_r_ausd_v_ta_apn_ve_orchestrator`) is the primary entry point for the migrated job. It replaces the `r_ausd_v_ta_apn_ve.ksh` KornShell script. Its responsibilities include:
        *   Parsing input parameters (e.g., `p_process_date`).
        *   Initializing job execution by generating a unique `job_run_id`.
        *   Recording job start and status in the `job_log` table.
        *   Calling the `sp_k_ausd_v_ta_apn_ve_combined` procedure to execute the core logic.
        *   Handling errors and updating the `job_log` table with final status and error details.
        *   Providing correlation IDs for Cloud Logging.

## 3. Key design decisions

The following key design decisions were made during this migration:

*   **Consolidation into BigQuery Stored Procedures:** Both the orchestration logic (from `r_ausd_v_ta_apn_ve.ksh`) and the core transformation logic (from `k_ausd_v_ta_apn_ve.ksh` and `D_AUSD_V_TA_APN_VE.SQL`) are migrated into BigQuery Stored Procedures.
    *   **Why:** This approach leverages BigQuery's native capabilities for data processing and procedural SQL, keeping the logic close to the data. It eliminates the need for external shell scripts to manage BigQuery operations, reducing cross-platform dependencies and simplifying deployment. It also allows for BigQuery's built-in error handling and logging mechanisms to be utilized.
    *   **Trade-offs:** This requires re-implementing shell-specific functionalities (like `getopts`, `trap`, environment variable management) using BigQuery's procedural SQL constructs. Custom utility scripts and functions from the legacy environment (`DWMSG_`, `DWPA_UTIL_SKRIPT`, `PA_ANALYZE`) must be translated or re-implemented in BigQuery SQL.

*   **Dedicated BigQuery Audit Log Table:** A new `job_log` BigQuery table is introduced for centralized job auditing.
    *   **Why:** This replaces the custom `DWMSG_` logging functions, providing a structured, queryable, and scalable logging solution within BigQuery. It allows for easy monitoring of job statuses, historical analysis, and error tracking.
    *   **Trade-offs:** Requires a new DDL and modification of the original logging calls to `INSERT`/`UPDATE` statements against this table.

*   **Cloud Composer for Scheduling (Optional):** While not directly generated, the design anticipates Cloud Composer (Airflow) for scheduling.
    *   **Why:** Cloud Composer provides a robust, managed orchestration service capable of handling complex dependencies, retries, and monitoring, replacing the legacy UC4 scheduler.
    *   **Trade-offs:** Introduces an additional GCP service and requires developing Python DAGs to trigger the BigQuery Stored Procedures. For simpler schedules, Cloud Scheduler could be an alternative.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure a BigQuery dataset exists (e.g., `your_dataset_id`) where the `job_log` table and the stored procedures will reside, and where source/target tables are located.
    *   `gcloud bq mk --dataset --location=US your_project_id:your_dataset_id`

2.  **Source & Target Table Migration:**
    *   Migrate the legacy source tables (`DWTK_MELDUNGEN`, `PDS$TA_PDP_CONTEXT_ASSOC`) and target tables (`SOF$TA_APN_VE`, `VIA`) to BigQuery. Ensure their schemas are correctly defined and data is loaded.
    *   Replace `your_project_id.your_dataset_id` placeholders in the generated SQL with actual project and dataset IDs for all table references.

3.  **Deploy `job_log` DDL:**
    *   Execute the `sql/ddl/job_log.sql` script in BigQuery to create the audit log table.
    *   `bq query --use_legacy_sql=false < sql/ddl/job_log.sql`

4.  **Deploy BigQuery Stored Procedures:**
    *   Execute `sql/stored_procedures/sp_k_ausd_v_ta_apn_ve_combined.sql` to create the core logic procedure.
    *   Execute `sql/stored_procedures/sp_r_ausd_v_ta_apn_ve_orchestrator.sql` to create the orchestrator procedure.
    *   `bq query --use_legacy_sql=false < sql/stored_procedures/sp_k_ausd_v_ta_apn_ve_combined.sql`
    *   `bq query --use_legacy_sql=false < sql/stored_procedures/sp_r_ausd_v_ta_apn_ve_orchestrator.sql`

5.  **IAM & Permissions:**
    *   Create a dedicated GCP Service Account for executing these BigQuery jobs.
    *   Grant this Service Account the necessary IAM roles:
        *   `BigQuery Data Editor` (for `job_log` table and target tables like `SOF$TA_APN_VE`, `VIA`).
        *   `BigQuery Data Viewer` (for source tables like `DWTK_MELDUNGEN`, `PDS$TA_PDP_CONTEXT_ASSOC`).
        *   `BigQuery Job User` (to run queries and stored procedures).
        *   `BigQuery Metadata Viewer` (to view table/procedure metadata).
        *   If Cloud Composer is used, the Composer environment's service account will need these permissions.

6.  **Scheduling Configuration:**
    *   **If using Cloud Composer:** Create an Airflow DAG that calls the `sp_r_ausd_v_ta_apn_ve_orchestrator` procedure, passing the required `p_process_date` parameter. Configure the DAG's schedule and dependencies.
    *   **If using Cloud Scheduler (for simpler cron-like schedules):** Create a Cloud Scheduler job that triggers a Cloud Function or Pub/Sub topic, which then invokes the BigQuery Stored Procedure.

7.  **Secrets Management:**
    *   If any legacy `DWPA_UTIL_SKRIPT` or `PA_ANALYZE` logic involved external system credentials, these must be securely stored in Secret Manager and accessed by the BigQuery procedures or the orchestrating Cloud Composer DAG.

## 5. Known gaps & unresolved references

The following items are flagged for follow-up and represent known gaps or unresolved references:

*   **Core Business Logic Details (B4 Item):** The `sp_k_ausd_v_ta_apn_ve_combined.sql` procedure currently contains placeholder comments (`TODO: Migrate the actual SQL logic...`). A separate, detailed analysis and migration effort is required to accurately translate the SQL from `D_AUSD_V_TA_APN_VE.SQL` and any intermediate logic from `k_ausd_v_ta_apn_ve.ksh` into BigQuery SQL. This includes understanding and replicating the exact transformation rules, joins, and data manipulation.

*   **`DWMSG_` Function Implementation:** The precise behavior and side effects of all `DWMSG_` functions (e.g., `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_MeldeFehler`, `DWMSG_Fehlerbehandlung`, `DWMSG_SetzeStatusOK`) need to be fully understood. While the `job_log` table and Cloud Logging cover basic auditing, any specific business logic embedded within these functions (e.g., specific error codes, notification mechanisms) must be explicitly replicated.

*   **Legacy Database Packages (`DWPA_UTIL_SKRIPT`, `PA_ANALYZE`):** The logic encapsulated within these Oracle/legacy database packages, used by `D_AUSD_V_TA_APN_VE.SQL`, is not yet migrated. These will need to be analyzed, and their functionality either re-implemented as BigQuery Stored Procedures or User-Defined Functions (UDFs), or their logic embedded directly into the core reconciliation procedure if simple enough.

*   **External System Interactions:** The original design document mentions `LOGIN:DW.UNIX.ISBERT` and `HOST:DWHDWH1P/DWHDWH2P`. If these refer to external database connections or system interactions beyond the scope of the current BigQuery migration, their access methods, security, and integration points in GCP need to be re-evaluated and implemented (e.g., using Cloud SQL, external tables, or other GCP integration services).

*   **Placeholder Replacement:** All instances of `your_project_id.your_dataset_id` in the generated SQL scripts must be replaced with the actual GCP project ID and BigQuery dataset ID before deployment.

## 6. Validation

Validation of the migrated job involves ensuring functional equivalence and correct operation in the GCP environment.

*   **How to run tests:**
    1.  **Manual Execution:**
        *   Execute the orchestrator stored procedure directly in BigQuery:
            ```sql
            CALL `your_project_id.your_dataset_id.sp_r_ausd_v_ta_apn_ve_orchestrator`(
                DATE '2023-01-01' -- Replace with a suitable test date
            );
            ```
        *   Monitor the BigQuery UI for job completion and check Cloud Logging for detailed output.
    2.  **Orchestrator Trigger (if applicable):**
        *   If a Cloud Composer DAG is implemented, trigger the DAG manually or allow it to run on its schedule.
        *   Monitor Airflow logs and BigQuery job history.

*   **What "passing" means:**
    1.  **Successful Completion:** The `sp_r_ausd_v_ta_apn_ve_orchestrator` procedure completes without raising an unhandled error.
    2.  **Audit Log Entry:** A new entry is created in the `your_project_id.your_dataset_id.job_log` table with `status = 'SUCCESS'` for the corresponding `job_run_id`.
    3.  **Cloud Logging:** No `ERROR` or `CRITICAL` level logs are generated by the BigQuery job in Cloud Logging. Informational messages should reflect the expected execution flow.
    4.  **Data Integrity:**
        *   The target tables (`your_project_id.your_dataset_id.SOF_TA_APN_VE`, `your_project_id.your_dataset_id.VIA`) are populated with data.
        *   A data comparison between the output of the migrated job and the output of the legacy job (using identical input data and processing dates) shows no discrepancies. This is the most critical validation step for functional equivalence.
    5.  **Performance:** The job completes within acceptable performance thresholds, ideally matching or improving upon legacy execution times.

## 7. Rollback procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure should be followed:

1.  **Stop New Job Execution:**
    *   **If using Cloud Composer:** Pause or disable the Airflow DAG responsible for triggering `sp_r_ausd_v_ta_apn_ve_orchestrator`.
    *   **If using Cloud Scheduler:** Disable the Cloud Scheduler job.
    *   Ensure no further executions of the BigQuery Stored Procedures are initiated.

2.  **Revert to Legacy System:**
    *   Re-enable the original `r_ausd_v_ta_apn_ve.ksh` job in the legacy UC4 scheduler (or equivalent).
    *   Verify that the legacy job can run successfully and process data as expected.

3.  **Data State Management (if necessary):**
    *   If the migrated job made incorrect modifications to target tables (`SOF$TA_APN_VE`, `VIA`) that cannot be easily corrected, consider restoring these BigQuery tables from a previous backup or snapshot taken before the go-live. This step is highly dependent on the impact and nature of the error.
    *   **Note:** The design implies writing to new BigQuery tables, not modifying existing legacy ones. If the migration involved in-place updates to tables also used by other systems, a more complex data rollback strategy would be required.

4.  **Disable GCP Components:**
    *   Optionally, disable or delete the deployed BigQuery Stored Procedures (`sp_k_ausd_v_ta_apn_ve_combined`, `sp_r_ausd_v_ta_apn_ve_orchestrator`) and the `job_log` table in BigQuery to prevent accidental execution or resource consumption.

5.  **Root Cause Analysis:**
    *   Investigate the root cause of the failure using Cloud Logging, BigQuery job history, and the `job_log` table. Address the identified issues before attempting another deployment.