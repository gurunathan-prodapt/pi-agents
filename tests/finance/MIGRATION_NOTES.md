# Migration Notes: Finance Month-End Close Workflow

This document details the migration of the legacy UC4 month-end close job (`finance/finance_month_end.xml`) to Google Cloud Platform (GCP) using Cloud Composer (managed Apache Airflow) and Cloud Dataproc.

---

## 1. Summary

* **Source Component:** `finance/finance_month_end.xml` (UC4 XML Job Plan)
* **Target Platform:** Google Cloud Platform (GCP)
* **Orchestration Engine:** Cloud Composer (Apache Airflow 2.x)
* **Execution Engine:** Cloud Dataproc (Serverless or Managed Spark/PySpark Clusters)
* **Database Engine:** Oracle Database (via secure JDBC/SQLNet connection)

The legacy UC4 workflow has been refactored into a single, cohesive Airflow DAG that manages calendar-based execution guards, pre-flight database checks, parallel regional extractions, PySpark-based transformations, and downstream cross-domain triggers.

---

## 2. Generated Artifacts

The migration process has produced the following target file:

| File Path | Role | Description |
| :--- | :--- | :--- |
| `dags/finance_month_end_workflow.py` | Airflow DAG Definition | Orchestrates the entire month-end sequence, handles variables, sets up task dependencies, and manages error-handling callbacks. |

---

## 3. Key Design Decisions

### Calendar Guarding via ShortCircuitOperator
* **Decision:** Instead of relying on complex, custom cron schedules or external calendar files to calculate the "last business day of the month," the DAG runs on a broad cron schedule (`0 20 28-31 * *`) and immediately executes a `ShortCircuitOperator` (`is_last_business_day`).
* **Rationale:** This isolates the business-day calculation logic inside a standard Python task using `pandas.date_range`. If the execution date is not the last business day of the month, the DAG gracefully skips all downstream tasks, preventing unnecessary cluster resource allocation.

### Parallel Regional Extraction
* **Decision:** Regional extracts (`UK`, `DE`, `FR`) and the `account_master_load` are executed in parallel immediately following the successful completion of the `pre_flight` check.
* **Rationale:** This maximizes cluster utilization and significantly reduces the overall batch window compared to sequential execution.

### Non-Blocking Warning Branch
* **Decision:** The `abinitio_reconcile` task uses `on_failure_alarm_warning` and is joined downstream at `daily_gl_close_audit` using `trigger_rule="all_done"`.
* **Rationale:** This preserves the legacy behavior where reconciliation discrepancies trigger alerts to the finance team but do not block the final ledger closing and downstream reporting workflows.

### Externalized Configurations
* **Decision:** Environment-specific parameters (GCP Project, Dataproc Cluster, GCS Buckets, and Notification Emails) are retrieved dynamically using `Variable.get()`.
* **Rationale:** This ensures the DAG code remains environment-agnostic and can be promoted from Development to UAT and Production without code modifications.

---

## 4. Manual Steps Before Go-Live

Before enabling the DAG in production, the following infrastructure, security, and configuration steps must be completed:

### 1. Airflow Variables Setup
Configure the following Airflow Variables in the Cloud Composer UI (`Admin -> Variables`):

| Variable Key | Example Value | Description |
| :--- | :--- | :--- |
| `GCP_PROJECT` | `prod-finance-gcp-123` | Target GCP Project ID |
| `DATAPROC_REGION` | `europe-west2` | GCP Region where Dataproc runs |
| `DATAPROC_CLUSTER` | `finance-ephemeral-cluster` | Name of the Dataproc cluster |
| `GCS_BUCKET` | `gs://prod-finance-etl-bucket` | GCS Bucket containing scripts and logs |
| `finance_force_close` | `N` | Override flag for regional extracts (`Y`/`N`) |
| `finance_notify_email` | `finance-alerts@company.com` | Target email for success notifications |

### 2. Connection Strings & Secrets
* **Oracle Connection:** Create an Airflow Connection (`Admin -> Connections`) with the ID `oracle_finance_conn`.
  * **Conn Type:** `Oracle`
  * **Host / Port / Schema:** Your Oracle DB details.
  * **Credentials:** Store the username and password securely (ideally backed by Google Secret Manager integrated with Composer).

### 3. Executable Deployment (GCS)
Ensure all translated PySpark scripts are uploaded to the designated Cloud Storage bucket path (`gs://YOUR_BUCKET_NAME/pyspark_scripts/`):
* `run_account_load.py`
* `run_gl_close_uk.py`
* `run_gl_close_de.py`
* `run_gl_close_fr.py`
* `gl_transform.py`
* `gl_reconcile.py`
* `finance_etl_assembly.py`

### 4. IAM & Permissions
The Cloud Composer service account (typically `service-PROJECT_NUMBER@gcp-sa-composer.iam.gserviceaccount.com` or a custom user-managed service account) must have the following IAM roles:
* **Dataproc Editor** (`roles/dataproc.editor`) or **Dataproc Worker** (`roles/dataproc.worker`)
* **Storage Object Viewer** (`roles/storage.objectViewer`) on the script bucket.
* **Storage Object Admin** (`roles/storage.objectAdmin`) on target data and audit log buckets.

---

## 5. Known Gaps & Unresolved References

* **Downstream DAG Existence:** The DAG triggers `crm_weekly_workflow` and `retail_daily_workflow`. These target DAGs must exist in the same Cloud Composer environment, or the `TriggerDagRunOperator` tasks will fail.
* **Audit Log Destination:** The `write_close_audit_log` task currently prints the audit log to standard output. For production compliance, this should be updated to write directly to a secure GCS path (e.g., using the `GCSHook`).
* **Alerting Webhooks:** The `on_failure_alarm` and `on_terminal_failure` callback stubs print alerts to the logs. These must be integrated with your enterprise alerting system (e.g., PagerDuty, Slack, or Google Cloud Monitoring) prior to go-live.

---

## 6. Validation

### Local/Dev DAG Parsing Test
To verify that the DAG is syntactically correct and can be loaded by Airflow without import errors, run the following command within your CI/CD pipeline or local development environment:

```bash
python3 dags/finance_month_end_workflow.py
```
*(A successful test returns no output/errors).*

### Testing the Calendar Guard (Short Circuit)
To test the business-day logic without waiting for the end of the month, you can run an Airflow task test for a specific historical date:

```bash
# Test on a known last business day (e.g., Friday, June 30, 2023)
airflow tasks test finance_month_end_workflow is_last_business_day 2023-06-30

# Test on a non-last business day (e.g., Monday, June 15, 2023)
airflow tasks test finance_month_end_workflow is_last_business_day 2023-06-15
```
* **Passing Criteria (Last Business Day):** The task log shows `Today is verified as the last business day of the month. Proceeding.` and returns `True`.
* **Passing Criteria (Other Days):** The task raises `AirflowSkipException` and downstream tasks are marked as skipped.

---

## 7. Rollback Procedure

In the event of a critical failure or data corruption during the deployment window:

1. **Pause the DAG:** Immediately pause the `finance_month_end_workflow` in the Airflow UI to prevent further scheduled executions.
2. **Revert to Legacy UC4:**
   * Re-enable the legacy UC4 Job Plan (`finance/finance_month_end.xml`).
   * Ensure the legacy database connections and agents are active.
3. **Database Cleanup:** If regional extracts or master loads partially completed and corrupted target tables, execute your standard database rollback scripts to restore the ledger state to the pre-flight checkpoint.