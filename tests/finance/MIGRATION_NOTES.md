# Migration Notes: FINANCE.GL_AGGREGATE_AND_CLOSE

These migration notes document the transition of the legacy UC4 UNIX job `FINANCE.GL_AGGREGATE_AND_CLOSE` to Apache Airflow (Google Cloud Composer) and Google Cloud Platform (GCP).

---

## 1. Summary
The legacy UC4 job `FINANCE.GL_AGGREGATE_AND_CLOSE` has been migrated from an on-premises UNIX/YARN/Oracle environment to **Apache Airflow (Cloud Composer)**, **Google Cloud Dataproc**, and **Google Cloud BigQuery**.

This job orchestrates the monthly and yearly General Ledger (GL) close process by:
1. Executing a Spark aggregation job to generate analytical outputs.
2. Writing an immutable close-audit record and updating the period status in the database upon successful aggregation.
3. Sending a completion notification email to finance stakeholders.

### Target Platform Architecture
* **Orchestrator**: Apache Airflow (Cloud Composer)
* **Compute Engine (Spark)**: Google Cloud Dataproc (Serverless or Standard Cluster)
* **Database Engine**: Google Cloud BigQuery
* **Notification**: Native Python SMTP with a local `mailx` CLI fallback

---

## 2. Generated Artifacts
The migration process generated three core files, maintaining the original directory structure under the `finance/` namespace:

| File Path | Language / Type | Role |
| :--- | :--- | :--- |
| **`finance/gl_aggregate_and_close_dag.py`** | Python (Airflow DAG) | Orchestrates the pipeline. It dynamically calculates the target period and fiscal year using Airflow Jinja macros and triggers the execution script. |
| **`finance/r_gl_aggregate_and_close.py`** | Python 3 Script | Replaces the legacy KornShell wrapper (`r_gl_aggregate_and_close.ksh`). It manages the execution flow, submits the Dataproc Spark job, executes the BigQuery audit SQL, and dispatches emails. |
| **`finance/d_gl_close_audit.sql`** | BigQuery SQL | Replaces the legacy Oracle SQL*Plus script. It uses a BigQuery Scripting transaction block to ensure atomic updates to the audit and status tables. |

---

## 3. Key Design Decisions

### Single-Task DAG with Python Orchestrator
* **Decision**: Instead of splitting the Spark submission, BigQuery execution, and Email notification into separate Airflow tasks, we migrated the legacy shell script into a unified Python script (`r_gl_aggregate_and_close.py`) executed via a single `BashOperator`.
* **Reasoning**: This preserves the strict transactional coupling of the legacy process. The close-audit record must **never** be written if the Spark aggregation fails. Keeping this logic within a single script ensures that failures in the Spark phase immediately halt execution before any database modifications occur. It also allows the script to be run manually from a terminal or local environment outside of Airflow.

### BigQuery Scripting Transactions
* **Decision**: The BigQuery SQL script uses `BEGIN TRANSACTION ... COMMIT TRANSACTION` with an `EXCEPTION WHEN ERROR` rollback block.
* **Reasoning**: This guarantees database atomicity. If the status update fails after the audit log insertion, the entire transaction rolls back, preventing a mismatched state where an audit record exists but the period remains open.

### Dual Execution Paths (Client Libraries vs. CLI Fallback)
* **Decision**: The Python script is designed to detect if Google Cloud client libraries (`google-cloud-dataproc`, `google-cloud-bigquery`) are installed. If they are missing, it gracefully falls back to using system CLI commands (`gcloud`, `bq`, `mailx`).
* **Reasoning**: This provides maximum flexibility, allowing developers to test the script locally or on a jump box without needing to install complex Python dependencies, while still utilizing high-performance native APIs in the production Composer environment.

---

## 4. Manual Steps Before Go-Live

Before enabling the DAG in production, the following infrastructure, security, and configuration steps must be completed:

### A. BigQuery Schema and Dataset Creation
Ensure the target dataset and tables exist in BigQuery.
1. Create the dataset `analytics_schema` in your target GCP project.
2. Create the tables with the following schemas:

```sql
-- Target: analytics_schema.gl_close_audit
CREATE TABLE IF NOT EXISTS `analytics_schema.gl_close_audit` (
  PERIOD_NAME STRING NOT NULL,
  FISCAL_YEAR STRING NOT NULL,
  CLOSED_BY STRING NOT NULL,
  CLOSED_AT TIMESTAMP NOT NULL
);

-- Target: analytics_schema.gl_period_status
CREATE TABLE IF NOT EXISTS `analytics_schema.gl_period_status` (
  PERIOD_NAME STRING NOT NULL,
  CLOSE_STATUS STRING NOT NULL,
  CLOSED_AT TIMESTAMP
);
```
> **CRITICAL**: Ensure the `CLOSED_BY` column is typed as `STRING` (not a restricted-length character field). BigQuery's `SESSION_USER()` returns the full email address of the executing service account, which can be quite long.

### B. IAM & Permissions
The Cloud Composer worker service account (e.g., `composer-worker@<PROJECT>.iam.gserviceaccount.com`) must be granted the following IAM roles:
* **Dataproc**: `roles/dataproc.editor` (to submit jobs to the Dataproc cluster)
* **BigQuery**: `roles/bigquery.jobUser` and `roles/bigquery.dataEditor` on the `analytics_schema` dataset
* **Cloud Storage**: `roles/storage.objectViewer` on the GCS bucket hosting the Spark jar

### C. GCS Staging
Upload the compiled Spark assembly jar to your designated Google Cloud Storage bucket:
* **Source**: `/opt/spark/jobs/finance-gl-aggregation-assembly.jar`
* **Destination**: `gs://<GCS_BUCKET>/jobs/finance-gl-aggregation-assembly.jar`

### D. Airflow Variables
Configure the following Airflow Variables in the Airflow UI (**Admin -> Variables**):

| Variable Key | Example Value | Description |
| :--- | :--- | :--- |
| `GCP_PROJECT` | `prod-gcp-finance-project` | The target GCP Project ID. |
| `GCP_REGION` | `us-central1` | Default GCP region. |
| `DATAPROC_REGION` | `us-central1` | Region where the Dataproc cluster is running. |
| `DATAPROC_CLUSTER` | `finance-spark-cluster` | Name of the active Dataproc cluster. |
| `GCS_BUCKET` | `finance-etl-assets-prod` | GCS bucket where the Spark jar is staged. |
| `FIN_HOME` | `/home/airflow/gcs/dags` | Base path where the migrated SQL script resides. |

### E. Scheduling & Triggering
Because this job was an un-scheduled standalone job in UC4, the migrated DAG is configured with `schedule=None`. 
* If this job must be triggered by an upstream pipeline, add a `TriggerDagRunOperator` to the end of that upstream DAG, or configure an `ExternalTaskSensor` to monitor this DAG.

---

## 5. Known Gaps & Unresolved References

The following items have been identified as gaps or out-of-scope for this migration pass and require manual coordination:

1. **Downstream Dependency (`FINANCE.MONTH_END_SCHEDULE`)**:
   * *Status*: Not yet migrated.
   * *Impact*: The legacy system triggers `FINANCE.MONTH_END_SCHEDULE` after this job completes. Because that workflow does not yet exist in Airflow, cross-DAG triggering cannot be finalized.
   * *Action*: Once the month-end schedule is migrated, add a `TriggerDagRunOperator` to the end of `finance_gl_aggregate_and_close` or configure a sensor in the downstream DAG.
2. **Fiscal Calendar Alignment**:
   * *Status*: The DAG uses standard calendar year logic (`{{ execution_date.strftime('%Y') }}`) to resolve `FISCAL_YEAR`.
   * *Impact*: If the organization's fiscal calendar does not align with the standard calendar year (Jan-Dec), this macro will pass incorrect values.
   * *Action*: Adjust the Jinja template in the DAG parameters to match the corporate fiscal calendar logic if necessary.

---

## 6. Validation

To validate the migrated pipeline, perform the following tests in a non-production environment:

### A. Local Python Script Dry-Run
You can test the Python orchestration script directly from a terminal. Ensure you have authenticated with GCP (`gcloud auth application-default login`):

```bash
export PERIOD_NAME="Jan_2023"
export FISCAL_YEAR="2023"
export GCP_PROJECT="your-dev-project"
export GCS_BUCKET="your-dev-bucket"
export DATAPROC_CLUSTER="your-dev-cluster"
export DATAPROC_REGION="us-central1"
export FIN_HOME="/path/to/migrated/files"
export NOTIFY_EMAIL="your-email@example.com"

python3 finance/r_gl_aggregate_and_close.py
```

### B. Airflow Task Test
Run a test of the Airflow task for a specific historical execution date to verify that the Jinja templates render correctly:

```bash
airflow tasks test finance_gl_aggregate_and_close gl_aggregate_and_close 2023-02-15
```
* **Expected Output**: The task should render `PERIOD_NAME` as `Jan_2023` (previous month relative to February) and `FISCAL_YEAR` as `2023`.

### C. Definition of "Passing"
The validation is successful if and only if:
1. The Dataproc Spark job runs and returns exit code `0`.
2. The BigQuery transaction commits successfully:
   * A new row is appended to `analytics_schema.gl_close_audit` containing the correct period, year, and the service account email in `CLOSED_BY`.
   * The status of the period in `analytics_schema.gl_period_status` is updated to `CLOSED`.
3. A success email is received at the designated `NOTIFY_EMAIL` address.
4. The Airflow task completes with a `SUCCESS` state.

---

## 7. Rollback Procedure

If a deployment failure occurs or a production run must be reverted, follow these steps:

### A. Database State Rollback
To revert the database state for a specific period that was closed in error, execute the following SQL script in BigQuery:

```sql
-- Revert the status of the target period
UPDATE `analytics_schema.gl_period_status`
SET    CLOSE_STATUS = 'OPEN',
       CLOSED_AT    = NULL
WHERE  PERIOD_NAME  = 'Jan_2023'; -- Replace with target period

-- Remove the audit record
DELETE FROM `analytics_schema.gl_close_audit`
WHERE  PERIOD_NAME  = 'Jan_2023'; -- Replace with target period
```

### B. Orchestration Rollback
1. **Pause the Airflow DAG**: In the Airflow UI, toggle the switch for `finance_gl_aggregate_and_close` to **Off**.
2. **Re-enable Legacy UC4 Job**: If the legacy environment is still active, re-enable the `FINANCE.GL_AGGREGATE_AND_CLOSE` job in UC4 to resume legacy processing.