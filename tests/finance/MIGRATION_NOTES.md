# Migration Notes: `finance/finance_daily.json`

These migration notes detail the transition of the legacy UC4 workflow `FINANCE_DAILY_WORKFLOW` to Google Cloud Composer (Apache Airflow), targeting Google BigQuery, Cloud Dataproc, and Cloud Pub/Sub.

---

## 1. Summary

The legacy UC4 workflow `FINANCE_DAILY_WORKFLOW` (`finance/finance_daily.json`) has been migrated to **Google Cloud Composer (Apache Airflow)**. 

* **Source Platform:** UC4 (Automic) Engine orchestrating local Unix shell scripts (`.ksh`), SQL*Plus scripts (`.sql`), and inline database checks.
* **Target Platform:** Google Cloud Composer (Airflow 2.x) orchestrating serverless or managed **Cloud Dataproc** PySpark jobs, utilizing **Cloud Pub/Sub** for cross-domain event broadcasting, and writing to **Google BigQuery** / **Google Cloud Storage (GCS)**.
* **Target Schedule:** Monday through Friday at 01:00 Europe/London time, with dynamic exclusion of UK Public Holidays.

---

## 2. Generated Artifacts

The migration process generated the following target files:

| Target File Path | Role / Description |
| :--- | :--- |
| `dags/finance_daily_workflow.py` | **Master Airflow DAG**. Orchestrates the entire end-to-end pipeline, handles holiday filtering, manages parallel execution branches, configures retries, and triggers alerting callbacks. |
| `pyspark_scripts/run_account_load.py` | **Dataproc PySpark Stub**. Placeholder for the migrated account master dimension load logic (originally `run_account_load.ksh`). |
| `pyspark_scripts/rate_extract.py` | **Dataproc PySpark Stub**. Placeholder for the migrated currency exchange rate extraction logic (originally `/opt/etl/sqlplus/rate_extract.sql`). |
| `pyspark_scripts/run_gl_close.py` | **Dataproc PySpark Stub**. Placeholder for the migrated multi-entity concurrent GL extraction logic (originally `/opt/etl/scripts/run_gl_close.ksh`). |

---

## 3. Key Design Decisions

* **Dataproc Serverless / Managed Cluster Execution:** Legacy shell scripts and SQL*Plus scripts are transitioned to PySpark scripts executed via `DataprocSubmitJobOperator`. This ensures scalable, cloud-native execution while isolating database driver dependencies.
* **Short-Circuit Holiday Filtering:** The legacy UC4 calendar exclusion (`PUBLIC_HOLIDAYS_UK`) is implemented via an Airflow `ShortCircuitOperator` (`check_holiday_calendar`). If the execution date falls on a registered UK holiday, the DAG gracefully skips downstream tasks without failing.
* **Non-Blocking Failure Paths:** Legacy "CONTINUE" behaviors for `finance_daily_acct_load` and `finance_daily_rate_extract` are mapped to a custom `on_failure_alarm_continue` callback. This alerts the operations team of a partial failure but allows the pipeline to proceed, avoiding complex `TriggerRule` configurations that can lead to silent downstream skips.
* **Event-Driven Downstream Triggering:** The legacy `uc4api publish_event` step is replaced with a native `PubSubPublishMessageOperator` publishing to the `finance_gl_close_complete` topic. This decouples the finance pipeline from downstream consumers (`RETAIL_DAILY_WORKFLOW` and `CRM_WEEKLY_WORKFLOW`).
* **Strict Output Logging Preservation:** To comply with legacy audit requirements, the exact log format `"[FINANCE_DAILY_GL_CLOSE] Period=" + PERIOD_DATE + " complete"` is preserved verbatim inside the `audit_log_gl_close` Python task.

---

## 4. Manual Steps Before Go-Live

The following configuration steps must be completed in the target GCP environment before enabling the DAG:

### A. Schema & Dataset Creation
1. Ensure the target staging and production datasets (e.g., `finance_staging`, `finance_dw`) exist in **Google BigQuery** within the target region.
2. Ensure the GCS bucket specified in the environment variables exists and contains a `pyspark_scripts/` directory.

### B. IAM & Permissions
Ensure the Cloud Composer environment's service account has the following IAM roles:
* `roles/dataproc.editor` (To submit Spark jobs to the Dataproc cluster)
* `roles/pubsub.publisher` (To publish to the `finance_gl_close_complete` topic)
* `roles/storage.objectViewer` (To read PySpark scripts from GCS)

### C. Connection Strings & Secrets
1. Create an Airflow Connection or Secret Manager entry for the Oracle source database connection (`ORACLE_FIN_LOGIN`).
2. Ensure the Dataproc cluster has network access (via Cloud NAT or VPC Peering) to the Oracle source database.

### D. Airflow Variables
Configure the following Airflow Variables in the Composer UI or via CLI:
```json
{
  "GCP_PROJECT": "your-gcp-project-id",
  "GCP_REGION": "europe-west1",
  "DATAPROC_CLUSTER_NAME": "your-dataproc-cluster",
  "GCS_BUCKET_NAME": "your-environment-gcs-bucket",
  "finance_notify_email": "finance-etl@company.com"
}
```

### E. Pub/Sub Topic
Create the Cloud Pub/Sub topic:
* **Topic ID:** `finance_gl_close_complete`

---

## 5. Known Gaps & Unresolved References

The following components were referenced in the legacy UC4 metadata but did not have source code available in the migration package. They have been generated as **NotImplementedError** stubs and require manual development:

1. **`run_account_load.py` (Legacy: `run_account_load.ksh`)**
   * *Gap:* The shell script logic that refreshes account master dimensions must be rewritten in PySpark.
   * *Action:* Implement the Oracle connection, extraction query, and BigQuery load logic inside `pyspark_scripts/run_account_load.py`.
2. **`rate_extract.py` (Legacy: `/opt/etl/sqlplus/rate_extract.sql`)**
   * *Gap:* The Oracle-specific SQL*Plus extraction query must be ported.
   * *Action:* Embed the SQL query logic inside `pyspark_scripts/rate_extract.py` using Spark JDBC or BigQuery Federated Queries.
3. **`run_gl_close.py` (Legacy: `/opt/etl/scripts/run_gl_close.ksh`)**
   * *Gap:* The multi-threaded extraction loop for European entities (UK, DE, FR) must be implemented.
   * *Action:* Implement concurrent Spark read operations or sequential entity processing loops inside `pyspark_scripts/run_gl_close.py`.

---

## 6. Validation

To validate the migration, perform the following tests in a non-production environment:

### A. DAG Syntax & Compilation Test
Run the following command in your local development environment or Cloud Shell to ensure the DAG compiles without syntax errors:
```bash
python3 dags/finance_daily_workflow.py
```

### B. Unit Testing Holiday Logic
To verify the holiday calendar short-circuit logic:
1. Temporarily add today's date to the `uk_holidays` list inside `dags/finance_daily_workflow.py`.
2. Trigger the DAG. Verify that `check_holiday_calendar` completes successfully and all downstream tasks are marked as **Skipped**.

### C. End-to-End Integration Test
1. Upload mock PySpark scripts to `gs://{GCS_BUCKET_NAME}/pyspark_scripts/` that print success messages instead of raising `NotImplementedError`.
2. Trigger the DAG manually via the Airflow UI.
3. Verify that:
   * `finance_daily_pre_check` executes and succeeds.
   * `finance_daily_acct_load` and `finance_daily_rate_extract` execute in parallel.
   * `finance_daily_gl_extract` executes after both parallel tasks complete.
   * `finance_daily_gl_close_log` prints the exact audit string: `[FINANCE_DAILY_GL_CLOSE] Period=<YYYY-MM-DD> complete`.
   * `finance_daily_gl_close_publish` successfully publishes a message to the Pub/Sub topic.

---

## 7. Rollback Procedure

In the event of a critical failure during go-live, execute the following rollback steps:

1. **Pause the Airflow DAG:** Navigate to the Airflow UI and toggle the switch for `finance_daily_workflow` to **Off**.
2. **Re-enable the Legacy UC4 Job:** Reactivate the `FINANCE_DAILY_WORKFLOW` job plan in the UC4 console.
3. **Verify Legacy Execution:** Monitor the next scheduled run in UC4 to ensure it executes and logs to `/opt/etl/logs/finance/daily_audit.log` as expected.
4. **Post-Mortem:** Inspect the Airflow task logs and Dataproc driver logs in Google Cloud Logging to diagnose the root cause of the failure.