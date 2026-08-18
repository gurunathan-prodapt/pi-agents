# Migration Notes: FINANCE.GL_AGGREGATE_AND_CLOSE

These migration notes document the transition of the legacy UC4 job `FINANCE.GL_AGGREGATE_AND_CLOSE` and its associated scripts to Google Cloud Platform (GCP) using Apache Airflow (Cloud Composer), BigQuery, and Cloud Dataproc.

---

## 1. Summary

The legacy UC4 job `FINANCE.GL_AGGREGATE_AND_CLOSE` has been migrated from an on-premises UNIX/Oracle environment to a cloud-native architecture on **Google Cloud Platform (GCP)**. 

### Scope of Migration
* **Orchestration:** Migrated from UC4 (`JOBS_UNIX`) to **Apache Airflow (Cloud Composer)**.
* **Processing Engine:** Migrated Spark-on-YARN execution to **Google Cloud Dataproc** (with local/YARN compatibility preserved as a fallback).
* **Database Operations:** Migrated Oracle SQL\*Plus transactional updates to **BigQuery Standard SQL Scripting**.
* **Notifications:** Migrated local UNIX `mailx` utility to native **SMTP** (with `mailx` fallback).

---

## 2. Generated Artifacts

The migration process generated three core files, maintaining the original directory structure:

| Target File Path | Language | Role / Description |
| :--- | :--- | :--- |
| `finance/finance_gl_aggregate_and_close.py` | Python (Airflow DAG) | The master orchestrator DAG. Resolves runtime variables (`PERIOD_NAME`, `FISCAL_YEAR`) using Airflow macros and triggers the execution wrapper. |
| `finance/r_gl_aggregate_and_close.py` | Python | Replacement for the legacy `.ksh` wrapper. Handles Dataproc Spark job submission, executes the BigQuery transaction script, and dispatches email alerts. |
| `finance/d_gl_close_audit.sql` | SQL (BigQuery) | Replacement for the legacy Oracle SQL script. Executes a transactional scripting block to insert audit logs and update period statuses atomically. |

---

## 3. Key Design Decisions

### Python-Based Wrapper Conversion (`.ksh` $\rightarrow$ `.py`)
* **Why:** Converting the shell script to Python allows native integration with GCP client libraries (such as `google-cloud-bigquery`), robust error handling, and cross-platform compatibility.
* **Trade-off:** Preserved a subprocess-based fallback execution path for environments where the Google Cloud SDK or Python client libraries are not fully installed.

### Transactional Safety in BigQuery
* **Why:** The legacy script relied on Oracle's implicit transaction boundaries and an explicit `COMMIT`. BigQuery is historically non-transactional for multi-statement scripts unless explicitly wrapped.
* **Approach:** Implemented a BigQuery scripting block using `BEGIN TRANSACTION`, `COMMIT TRANSACTION`, and `EXCEPTION WHEN ERROR ... ROLLBACK TRANSACTION`. This guarantees that the `GL_CLOSE_AUDIT` insert and the `GL_PERIOD_STATUS` update succeed or fail as a single atomic unit.

### Dynamic Dataset Parameterization
* **Why:** Hardcoding schema names (e.g., `ANALYTICS_SCHEMA`) prevents seamless deployment across Dev, Test, and Prod environments.
* **Approach:** Used BigQuery query parameters (`@bq_dataset`, `@gcp_project`) combined with `EXECUTE IMMEDIATE FORMAT` to dynamically construct and execute target DML statements at runtime.

### Timezone Standardization
* **Why:** Oracle's `SYSTIMESTAMP` records local database server time, whereas BigQuery's `CURRENT_TIMESTAMP()` always evaluates to UTC.
* **Trade-off:** We standardized on UTC (`CURRENT_TIMESTAMP()`) to align with cloud best practices. Downstream consumption views must handle timezone offsets if local-time reporting is strictly required.

---

## 4. Manual Steps Before Go-Live

To ensure a successful deployment, the following manual setup steps must be completed in the target environment:

### A. Schema & Dataset Creation
1. Ensure the target BigQuery dataset (e.g., `ANALYTICS_SCHEMA`) exists in your project.
2. Create or migrate the target tables with compatible schemas:
   * **`GL_CLOSE_AUDIT`**: Ensure the `CLOSED_BY` column is defined as `STRING` and is wide enough (at least 256 characters) to accommodate Google Service Account email formats (e.g., `sa-composer@prod-project.iam.gserviceaccount.com`), which are significantly longer than legacy Oracle database usernames (e.g., `FIN_ADMIN`).
   * **`GL_PERIOD_STATUS`**: Ensure columns `PERIOD_NAME` and `CLOSE_STATUS` are present.

### B. IAM & Permissions
The Cloud Composer / Airflow worker service account must be granted the following IAM roles:
* `roles/dataproc.editor` (to submit Spark jobs to the Dataproc cluster)
* `roles/bigquery.admin` (or specific `bigquery.dataEditor` and `bigquery.jobUser` roles on the target dataset)
* `roles/storage.objectViewer` (to read the Spark JAR file from GCS)

### C. Airflow Variables Configuration
Configure the following Airflow Variables in the Airflow UI (**Admin $\rightarrow$ Variables**):

| Variable Key | Example Value | Description |
| :--- | :--- | :--- |
| `GCP_PROJECT` | `my-gcp-project-id` | Target GCP Project ID |
| `GCP_REGION` | `us-central1` | Target GCP Region |
| `DATAPROC_REGION` | `us-central1` | Region where the Dataproc cluster resides |
| `DATAPROC_CLUSTER` | `finance-spark-cluster` | Name of the active Dataproc cluster |
| `GCS_BUCKET` | `my-finance-etl-bucket` | GCS bucket containing the Spark JAR |
| `ANALYTICS_SCHEMA` | `ANALYTICS_SCHEMA` | Target BigQuery dataset name |
| `CURRENT_FISCAL_YEAR` | `2024` | Active fiscal year for processing |
| `finance_gl_aggregate_and_close_notify_email` | `finance-alerts@company.com` | Target email for close notifications |

### D. Secrets & Connections
* **SMTP Configuration:** If using native SMTP email notifications, configure the following environment variables in your Cloud Composer environment:
  * `SMTP_HOST` (e.g., `smtp.sendgrid.net`)
  * `SMTP_PORT` (e.g., `587` or `25`)
  * `SMTP_FROM` (e.g., `noreply-composer@company.com`)
* **Legacy Fallback (Optional):** If running against the legacy Oracle database during a transition phase, configure the Airflow connection for Oracle and set `FIN_ORA_USER`, `FIN_ORA_PASS`, and `FIN_ORA_SID`.

### E. Scheduling
The migrated DAG is configured with `schedule=None` (externally triggered), matching its legacy UC4 behavior. You must configure your master orchestration workflow to trigger this DAG using the `TriggerDagRunOperator` or via the Airflow REST API.

---

## 5. Known Gaps & Unresolved References

1. **Downstream Dependency (`FINANCE.MONTH_END_SCHEDULE`):**
   * *Status:* Not yet migrated.
   * *Impact:* The downstream trigger cannot be finalized.
   * *Remediation:* Once `FINANCE.MONTH_END_SCHEDULE` is migrated to Airflow, append a `TriggerDagRunOperator` to the end of `finance_gl_aggregate_and_close` or configure an `ExternalTaskSensor` in the downstream DAG.
2. **Spark JAR Deployment:**
   * *Status:* The compiled Spark Scala/Java application (`finance-gl-aggregation-assembly.jar`) is outside the scope of this SQL/KSH migration pass.
   * *Remediation:* Ensure the build pipeline uploads the compiled JAR to `gs://<GCS_BUCKET>/jobs/finance-gl-aggregation-assembly.jar` prior to execution.
3. **Timezone Discrepancy:**
   * *Status:* Oracle `SYSTIMESTAMP` (local) vs. BigQuery `CURRENT_TIMESTAMP()` (UTC).
   * *Impact:* Audit timestamps will show in UTC. If downstream financial reports strictly require local timezone representation, wrap the audit queries in a timezone conversion function: `DATETIME(CLOSED_AT, "America/New_York")`.

---

## 6. Validation

To validate the migration, execute the following testing steps:

### Step 1: BigQuery Script Dry Run
Execute the SQL script manually in the BigQuery Console using declared variables to verify syntax and transactional rollback:
```sql
DECLARE period_name STRING DEFAULT 'DEC-2023';
DECLARE fiscal_year STRING DEFAULT '2023';
DECLARE gcp_project STRING DEFAULT 'your-project-id';
DECLARE bq_dataset STRING DEFAULT 'ANALYTICS_SCHEMA';

-- Run the block and verify no syntax errors occur.
```

### Step 2: Airflow DAG Manual Trigger
1. Navigate to the Airflow UI.
2. Unpause the `finance_gl_aggregate_and_close` DAG.
3. Trigger the DAG manually with a mock logical date.

### Step 3: Verification of "Passing" State
A test run is considered successful if and only if:
1. **Dataproc Job Success:** The Dataproc job completes with an exit code of `0` (verified via Dataproc job logs).
2. **Audit Record Inserted:** A query to `GL_CLOSE_AUDIT` returns exactly one new row for the target period:
   ```sql
   SELECT * FROM `ANALYTICS_SCHEMA.GL_CLOSE_AUDIT` WHERE PERIOD_NAME = 'DEC-2023';
   ```
3. **Status Updated:** The status in `GL_PERIOD_STATUS` is updated to `'CLOSED'`:
   ```sql
   SELECT CLOSE_STATUS FROM `ANALYTICS_SCHEMA.GL_PERIOD_STATUS` WHERE PERIOD_NAME = 'DEC-2023';
   -- Expected output: CLOSED
   ```
4. **Email Received:** An email notification is successfully delivered to the address configured in `finance_gl_aggregate_and_close_notify_email`.
5. **Atomic Rollback (Failure Test):** If you force the Spark job to fail, verify that *no* audit record is written and the period status remains unchanged.

---

## 7. Rollback Procedure

In the event of an issue during go-live, execute the following steps to revert to the legacy system:

### Step 1: Disable Cloud Orchestration
1. Pause the Airflow DAG `finance_gl_aggregate_and_close` in the Cloud Composer UI.
2. Cancel any active DAG runs.

### Step 2: Revert Database State
If the cloud migration partially executed and wrote invalid audit records, revert the database state in BigQuery:
```sql
-- 1. Delete the invalid close audit record
DELETE FROM `ANALYTICS_SCHEMA.GL_CLOSE_AUDIT` 
WHERE PERIOD_NAME = 'TARGET_PERIOD' AND FISCAL_YEAR = 'TARGET_YEAR';

-- 2. Revert the period status back to OPEN
UPDATE `ANALYTICS_SCHEMA.GL_PERIOD_STATUS`
SET    CLOSE_STATUS = 'OPEN',
       CLOSED_AT = NULL
WHERE  PERIOD_NAME = 'TARGET_PERIOD';
```

### Step 3: Reactivate Legacy Orchestration
1. Reactivate the `FINANCE.GL_AGGREGATE_AND_CLOSE` job in the UC4 environment.
2. Verify that the legacy environment variables (`$PREV_MONTH_MON_YYYY` and `$CURRENT_FISCAL_YEAR`) are correctly aligned.