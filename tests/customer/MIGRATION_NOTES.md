# Migration Notes: CUSTOMER.HISTORIZATION_LOAD

This document provides the technical details, design decisions, manual setup requirements, validation steps, and rollback procedures for the migration of the `CUSTOMER.HISTORIZATION_LOAD` job from UC4 (Automic) to Apache Airflow (Google Cloud Composer) and Google Cloud BigQuery.

---

## 1. Summary

The `CUSTOMER.HISTORIZATION_LOAD` job has been migrated from a legacy UC4 UNIX-scheduled environment to **Apache Airflow (Google Cloud Composer)**, with the underlying data processing shifted from Oracle SQL*Plus to **Google Cloud BigQuery**.

### Scope of Migration
* **Legacy Platform:** UC4 (Automic) scheduler executing KornShell (`.ksh`) scripts on a remote UNIX host (`ETLHOST2`) interacting with an Oracle database (`CRMPRD`).
* **Target Platform:** Google Cloud Composer (Apache Airflow) orchestrating native Python scripts that execute GoogleSQL queries in Google Cloud BigQuery.
* **Business Logic:** Performs a weekly Slowly Changing Dimension Type 2 (SCD2) historization of customer segments and scores into a segment dimension table, followed by a statistical sanity check to flag abnormally high segment-shift percentages (indicative of join corruption).

---

## 2. Generated Artifacts

The migration process generated three core files, each mapping to a specific component of the legacy architecture:

| Generated File | Target Location | Role / Description |
| :--- | :--- | :--- |
| `customer_historization_load.py` | `dags/customer/` | **Airflow DAG:** Replaces the UC4 job definition. Defines execution parameters, default arguments, and orchestrates the execution tasks. Configured as an externally triggered workflow (`schedule=None`). |
| `k_historization_load.py` | `dags/customer/scripts/` | **Core Execution Script:** Replaces `k_historization_load.ksh`. Uses the `google-cloud-bigquery` client to run the SCD2 merge and quality check SQL files, parses the results, and performs threshold validation. |
| `r_historization_load.py` | `dags/customer/scripts/` | **Wrapper Script:** Replaces `r_historization_load.ksh`. Acts as the entry point for execution, handling logging initialization, environment setup, and subprocess execution monitoring. |

---

## 3. Key Design Decisions

### Decoupled Script Execution via Python BigQuery Client
* **Decision:** Instead of using Airflow's `BashOperator` to run legacy shell scripts or raw `sqlplus` commands, the core logic was rewritten into native Python (`k_historization_load.py`) using the `google-cloud-bigquery` client library.
* **Trade-off/Reasoning:** This removes the dependency on legacy database clients (SQL*Plus) and local UNIX environments. It also allows for structured, type-safe parsing of the quality check query results (extracting a scalar integer from a BigQuery row iterator) rather than relying on fragile shell-based stdout sanitization (`tr -d '[:space:]'`).

### Soft-Failure Threshold Validation
* **Decision:** Maintained the legacy behavior where exceeding the `MAX_EXPECTED_CHANGE_PCT` (default `25%`) logs a warning but does *not* fail the pipeline.
* **Trade-off/Reasoning:** This prevents statistical anomalies from hard-blocking downstream weekly processing while ensuring that data quality warnings are clearly visible in the Airflow task logs for operational review.

### Parameterized Execution Dates (Backfill Safety)
* **Decision:** Sourced the `RUN_DATE` parameter dynamically using Airflow's logical date macro `{{ ds }}` (or falling back to the current system date if run standalone).
* **Trade-off/Reasoning:** This ensures that if the DAG is run historically (backfilled), the queries execute against the correct historical business date rather than the real-world execution timestamp.

---

## 4. Manual Steps Before Go-Live

Before enabling this workflow in production, the following infrastructure, security, and configuration steps must be completed:

### A. Schema & Dataset Creation
1. Ensure the target BigQuery dataset (e.g., `customer_dimension`) exists in your GCP project.
2. Ensure the target SCD2 dimension table (e.g., `dim_customer_segment`) is created with fields supporting SCD2 tracking:
   * `customer_id` (BK)
   * `segment_score`
   * `segment_name`
   * `valid_from` (TIMESTAMP/DATE)
   * `valid_to` (TIMESTAMP/DATE)
   * `is_active` (BOOLEAN)

### B. IAM & Permissions
The Google Cloud Composer worker service account (typically `service-XXX@gcp-sa-composer.iam.gserviceaccount.com`) must be granted the following IAM roles:
* **`roles/bigquery.jobUser`** (on the project level to run query jobs).
* **`roles/bigquery.dataEditor`** (on the target dataset to perform the SCD2 merge).
* **`roles/storage.objectViewer`** (on the environment's GCS bucket to read SQL scripts).

### C. Airflow Variables & Environment Variables
Configure the following Airflow Variables in the Cloud Composer UI (**Admin -> Variables**):

| Variable Name | Example Value | Description |
| :--- | :--- | :--- |
| `GCP_PROJECT` | `my-gcp-prod-project` | Target Google Cloud Project ID. |
| `GCP_REGION` | `us-central1` | Deployment region. |
| `GCS_BUCKET` | `us-central1-composer-bucket-xyz` | GCS bucket associated with the Composer environment. |
| `BQ_DATASET` | `customer_dimension` | Target BigQuery dataset. |
| `CRM_HOME` | `/home/airflow/gcs/dags` | Base path where scripts and SQL files are deployed. |

### D. SQL File Deployment
The SQL files referenced by the execution script must be migrated to BigQuery-compliant SQL syntax and uploaded to the GCS bucket under the following paths:
* `gs://<composer-bucket>/dags/customer/d_historization_load.sql`
* `gs://<composer-bucket>/dags/customer/d_segment_quality_check.sql`

### E. Scheduling & Downstream Wiring
Because this job is designed to run as part of a larger weekly cycle, it is configured with `schedule_interval=None`. Once the downstream job `CUSTOMER.WEEKLY_SCHEDULE` is migrated, you must uncomment and configure the `TriggerDagRunOperator` at the end of `customer_historization_load.py` to trigger it.

---

## 5. Known Gaps & Unresolved References

The following items were flagged during migration as requiring manual follow-up or redesign (B4 items):

1. **Missing SQL Files:** The SQL files `d_historization_load.sql` and `d_segment_quality_check.sql` were not part of this migration bundle. They must be manually translated from Oracle SQL*Plus dialect to GoogleSQL (BigQuery Standard SQL).
   * *Oracle Specifics to Convert:* Replace Oracle-specific `MERGE` statements, `NVL`, and date functions (e.g., `SYSDATE`, `TO_DATE`) with BigQuery equivalents (`IFNULL`, `CURRENT_TIMESTAMP`, etc.).
2. **Unmigrated Downstream Dependency:** The downstream consumer `CUSTOMER.WEEKLY_SCHEDULE` is not yet migrated. Cross-DAG orchestration cannot be verified end-to-end until this asset is deployed.
3. **Empty Operator Placeholder:** The task `customer_historization_load` in the DAG is currently stubbed with an `EmptyOperator`. Once the deployment paths for `r_historization_load.py` are finalized on the Composer workers, this must be updated to a `BashOperator` or `PythonVirtualenvOperator` executing the script:
   ```python
   customer_historization_load = BashOperator(
       task_id='customer_historization_load',
       bash_command='python3 /home/airflow/gcs/dags/customer/scripts/r_historization_load.py',
       env={
           'CRM_HOME': '/home/airflow/gcs/dags',
           'RUN_DATE': '{{ ds }}',
           'GCP_PROJECT': Variable.get("GCP_PROJECT"),
           'BQ_DATASET': Variable.get("BQ_DATASET")
       }
   )
   ```

---

## 6. Validation

To validate the migration, execute the following testing steps in a non-production environment:

### Running the Test
1. **Manual Trigger:** In the Airflow UI, locate the `customer_historization_load` DAG and click **Trigger DAG w/ config**.
2. **Custom Parameters:** Pass a test execution date and threshold if desired:
   ```json
   {
     "run_date": "2023-10-15",
     "max_expected_change_pct": 25
   }
   ```
3. **CLI Execution (Alternative):** Run the Python script directly from a terminal with access to the target BigQuery environment:
   ```bash
   export CRM_HOME="/path/to/local/dags"
   export GCP_PROJECT="your-dev-project"
   export BQ_DATASET="your_dev_dataset"
   export RUN_DATE="2023-10-15"
   python3 dags/customer/scripts/r_historization_load.py
   ```

### What "Passing" Looks Like
Review the task execution logs in Airflow. The run is successful if:
* The log outputs: `Starting SCD2 historization for run date 2023-10-15`.
* The BigQuery merge job completes without errors.
* The quality check query executes and returns a valid numeric string.
* **Scenario A (Normal Run):** If the change percentage is $\le 25\%$, the log outputs:
  `[YYYY-MM-DD HH:MM:SS] Historization merge complete, 12% of customers re-versioned` and exits with code `0`.
* **Scenario B (Anomaly Warning):** If the change percentage is $> 25\%$, the log outputs:
  `[YYYY-MM-DD HH:MM:SS] WARN: 34% of customers changed segment this week (expected <= 25%) - flagging for review, not failing the job` and exits with code `0`.
* **Scenario C (Empty QC Output):** If the QC query returns no data, the log outputs:
  `[YYYY-MM-DD HH:MM:SS] WARN: could not compute changed-row percentage - skipping sanity check` and exits with code `0`.

---

## 7. Rollback Procedure

In the event of a critical failure or data corruption during deployment, execute the following rollback steps:

### Step 1: Disable the Airflow DAG
1. Navigate to the Airflow UI.
2. Locate `customer_historization_load` and toggle the DAG switch to **Off** (Paused) to prevent any automated or manual triggers.

### Step 2: Revert Code Changes
If code modifications caused the failure, revert the Git repository to the last known stable commit and redeploy the DAGs to the GCS bucket:
```bash
git revert <commit_hash>
git push origin main
# Sync GCS bucket with reverted code
gsutil -m rsync -d -r ./dags gs://<composer-bucket>/dags
```

### Step 3: Data Rollback (SCD2 State Restoration)
Because SCD2 is an additive process, a failed or corrupted run will have inserted new active records and closed out older records. To restore the database state to the previous week (before `RUN_DATE` executed):

1. Run the following SQL script in BigQuery to delete the newly inserted records and reactivate the previous versions:
   ```sql
   -- 1. Delete new records inserted by the failed run
   DELETE FROM `your_project.customer_dimension.dim_customer_segment`
   WHERE valid_from = DATE('2023-10-15');

   -- 2. Re-open the previous active records that were closed out by the failed run
   UPDATE `your_project.customer_dimension.dim_customer_segment`
   SET valid_to = NULL,
       is_active = TRUE
   WHERE valid_to = DATE('2023-10-15');
   ```
2. Verify the row counts and active flags in `dim_customer_segment` to ensure the state matches the pre-execution baseline.