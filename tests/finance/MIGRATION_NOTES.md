# Migration Notes: `finance/finance_daily.json`

## 1. Summary
The legacy UC4 Job Plan `FINANCE_DAILY_WORKFLOW` has been migrated to Google Cloud Composer (Airflow 2) as a fully automated, native Python DAG. 

*   **Source Platform:** UC4 / Automic Engine (Job Plan `FINANCE_DAILY_WORKFLOW`)
*   **Target Platform:** Google Cloud Composer (Airflow 2) / Google Cloud Dataproc (PySpark)
*   **Business Domain:** Finance (General Ledger, Account Master metadata, and Currency Exchange Rates)
*   **Target Schedule:** `0 1 * * 1-5` (Monday through Friday at 01:00 Europe/London)

---

## 2. Generated Artifacts

The migration process generated the following files, which must be deployed to their respective environments:

| File Path | Target Environment | Role |
| :--- | :--- | :--- |
| `dags/finance_daily_workflow.py` | Cloud Composer DAGs Bucket (`gs://<composer-bucket>/dags/`) | Core orchestration pipeline defining tasks, dependencies, variables, and error-handling callbacks. |
| `pyspark_scripts/finance_daily_pre_check.py` | Cloud Storage (`gs://<gcs_bucket_name>/pyspark_scripts/`) | PySpark script validating connectivity to the source Oracle GL database. |
| `pyspark_scripts/finance_daily_acct_load.py` | Cloud Storage (`gs://<gcs_bucket_name>/pyspark_scripts/`) | PySpark script stub for refreshing the Account Master Dimension (requires manual implementation). |
| `pyspark_scripts/finance_daily_rate_extract.py` | Cloud Storage (`gs://<gcs_bucket_name>/pyspark_scripts/`) | PySpark script stub for daily exchange rate extraction (requires manual implementation). |
| `pyspark_scripts/finance_daily_gl_extract.py` | Cloud Storage (`gs://<gcs_bucket_name>/pyspark_scripts/`) | PySpark script stub for multi-entity GL journal extraction (requires manual implementation). |
| `pyspark_scripts/finance_daily_gl_close.py` | Cloud Storage (`gs://<gcs_bucket_name>/pyspark_scripts/`) | PySpark script performing audit logging and final validation before downstream triggers fire. |

---

## 3. Key Design Decisions

*   **Concurrency Guard:** UC4 strictly limits execution to a single active pipeline run. To prevent race conditions and database collisions in Airflow, we configured `max_active_runs=1` and implemented a custom `concurrency_guard` task using a `PythonOperator` that gracefully skips the run (`AirflowSkipException`) if another instance is already running.
*   **Dataproc Serverless / Shared Cluster Execution:** All heavy ETL operations are offloaded from the Composer environment to Google Cloud Dataproc using the `DataprocSubmitJobOperator`. This ensures the Airflow worker nodes do not experience memory exhaustion.
*   **Fire-and-Forget Downstream Triggers:** The downstream CRM and Retail pipelines are triggered using the `TriggerDagRunOperator` with `wait_for_completion=False`. This preserves the decoupled, event-driven nature of the original UC4 workflow.
*   **Unified Failure Callback:** An `on_failure_callback` (`on_failure_alarm`) is attached to all critical tasks to ensure immediate operational alerts are dispatched to the finance operations team upon any task failure.

---

## 4. Manual Steps Before Go-Live

Before enabling the DAG in production, the following infrastructure, security, and scheduling configurations must be completed:

### A. Schema & Dataset Creation
1. Ensure the target BigQuery datasets or Cloud Storage directories for the staging tables (`STG_PERIOD_RATES`, GL journals, and Account Master metadata) are created.
2. Create the audit directory in GCS: `gs://<YOUR_BUCKET_NAME>/audit/daily_audit_log/`.

### B. IAM & Permissions
Ensure the Cloud Composer environment's service account has the following IAM roles:
*   `roles/dataproc.editor` (to submit PySpark jobs to the Dataproc cluster)
*   `roles/storage.objectAdmin` (to read PySpark scripts and write staging/audit data)
*   `roles/composer.user` (to trigger downstream DAGs)
*   `roles/secretmanager.secretAccessor` (if retrieving database credentials from GCP Secret Manager)

### C. Airflow Variables & Connections
Create the following Airflow Variables in the Composer UI or via CLI:
*   `gcs_bucket_name`: Name of the GCS bucket hosting the PySpark scripts (e.g., `company-finance-etl-prod`).
*   `finance_notify_email`: Primary alert recipient (e.g., `finance-etl@company.com`).
*   `finance_retry_max`: Maximum retries for transient tasks (e.g., `3`).
*   `finance_allow_empty`: Flag to allow empty files in GL close (e.g., `N`).

Create the following Environment Variables on the Composer Environment:
*   `GCP_PROJECT`: Your Google Cloud Project ID.
*   `DATAPROC_REGION`: The region where your Dataproc cluster resides.
*   `DATAPROC_CLUSTER`: The name of your Dataproc cluster.

### D. Connection Strings & Secrets
*   Store the Oracle database credentials (`FIN_ORA_USER`, `FIN_ORA_PASS`) securely in GCP Secret Manager or configure them as environment variables on the Dataproc cluster.

### E. Scheduling & Holiday Calendar
*   The legacy UC4 schedule excluded UK Public Holidays (`PUBLIC_HOLIDAYS_UK`). Since Airflow's cron schedule does not natively support holiday calendars, you must either:
    1. Implement a custom Airflow Timetable.
    2. Maintain a holiday registry table in BigQuery and add a check inside the `concurrency_guard` task to skip execution if the current date is a registered holiday.

---

## 5. Known Gaps & Unresolved References

The following source components were missing from the scanned migration context and have been generated as **NotImplementedError** stubs. These must be resolved before go-live:

1.  **`finance_daily_acct_load.py` (Legacy: `run_account_load.ksh`)**
    *   *Gap:* The logic to refresh the Account Master Dimension is missing.
    *   *Action:* Locate the legacy shell script, extract the SQL/ETL logic, and implement it within the PySpark stub.
2.  **`finance_daily_rate_extract.py` (Legacy: `rate_extract.sql`)**
    *   *Gap:* The SQL query extracting daily exchange rates into `STG_PERIOD_RATES` is missing.
    *   *Action:* Locate `/opt/etl/sqlplus/rate_extract.sql` and implement the query execution inside the PySpark stub.
3.  **`finance_daily_gl_extract.py` (Legacy: `run_gl_close.ksh`)**
    *   *Gap:* The multi-entity loop logic extracting journals for `UK_ENTITY`, `DE_ENTITY`, and `FR_ENTITY` is missing.
    *   *Action:* Locate the legacy shell script and implement parallelized entity extraction tasks within the PySpark stub.
4.  **Downstream DAG Verification**
    *   *Gap:* The downstream DAGs `retail_daily_workflow` and `crm_weekly_workflow` must be deployed in the same Composer environment for the `TriggerDagRunOperator` tasks to succeed.

---

## 6. Validation

To validate the migration, execute the following steps in a non-production Composer environment:

### A. How to Run the Tests
1. Upload the DAG file to the Composer DAGs bucket.
2. Upload the PySpark scripts (including stubs updated with actual logic) to `gs://<YOUR_BUCKET_NAME>/pyspark_scripts/`.
3. Trigger the DAG manually from the Airflow UI.

### B. What "Passing" Means
The migration is considered successful and ready for production when:
*   The `concurrency_guard` task executes and successfully detects/allows a single run.
*   The `finance_daily_pre_check` task successfully connects to the Oracle database and returns `STATUS CHECK: SUCCESS`.
*   The `acct_load`, `rate_extract`, and `gl_extract` tasks execute without errors and populate their respective staging tables.
*   The `gl_close` task successfully writes a Parquet audit record to `gs://<YOUR_BUCKET_NAME>/audit/daily_audit_log/`.
*   The `trigger_retail_daily_workflow` and `trigger_crm_weekly_workflow` tasks successfully trigger their respective downstream DAGs without waiting for completion.

---

## 7. Rollback Procedure

If critical issues are detected in production after go-live, execute the following rollback steps:

1.  **Pause the Airflow DAG:** Navigate to the Airflow UI and toggle the switch for `finance_daily_workflow` to **Off** (Paused).
2.  **Re-enable the UC4 Job Plan:** In the UC4 Automic UI, locate the `FINANCE_DAILY_WORKFLOW` Job Plan and set its status to **Active** (ensure the schedule is re-activated).
3.  **Verify Downstream Triggers:** Ensure downstream consumers are informed of the rollback so they can monitor UC4 event signals instead of Airflow triggers.
4.  **Investigate Logs:** Analyze the task execution logs in Cloud Logging or the Airflow UI to diagnose the failure.