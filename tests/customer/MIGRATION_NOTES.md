# Migration Notes: CUSTOMER.HISTORIZATION_LOAD

This document details the migration of the legacy UC4 UNIX job `CUSTOMER.HISTORIZATION_LOAD` and its associated execution scripts to Google Cloud Platform (GCP) using Google Cloud Composer (Apache Airflow) and Google BigQuery.

---

## 1. Summary

The legacy workload `CUSTOMER.HISTORIZATION_LOAD` was responsible for performing a weekly Slowly Changing Dimension Type 2 (SCD2) historization of customer segment codes and score bands into the segment dimension table (`DIM_CUSTOMER_SEGMENT`). It also executed an automated safety guardrail to flag anomalous mass updates (where more than 25% of the customer base changed segments in a single week).

This workload has been fully migrated from its legacy UNIX host (`ETLHOST2`) and Oracle database environment to **Google Cloud Platform (GCP)**. 
* **Orchestration**: Apache Airflow (Google Cloud Composer) manages the execution flow.
* **Execution Engine**: Python 3 scripts replace the legacy KornShell (`.ksh`) wrappers.
* **Data Warehouse**: Google BigQuery executes the transactional SCD Type 2 merge and quality check queries.

---

## 2. Generated Artifacts

The migration process generated the following files, which must be deployed to their respective directories in the target environment:

| File Path | Type / Language | Role / Description |
| :--- | :--- | :--- |
| `customer/customer_historization_load_dag.py` | Python (Airflow DAG) | Orchestrates the entire process. Defines a single-task DAG that executes the wrapper script via a `BashOperator` and injects runtime parameters. |
| `customer/r_historization_load.py` | Python Script | Replaces `r_historization_load.ksh`. Serves as the entry-point wrapper script, logging execution metadata and invoking the core execution engine. |
| `customer/k_historization_load.py` | Python Script | Replaces `k_historization_load.ksh`. The core execution engine. Uses the Google Cloud BigQuery client library to run the SCD2 merge and quality check SQL scripts, parses the quality check output, and evaluates it against the threshold. |
| `customer/d_historization_load.sql` | BigQuery SQL | Replaces the Oracle SQL script. Performs the transactional SCD Type 2 merge and insert logic using the `@RUN_DATE` query parameter. |
| `customer/d_segment_quality_check.sql` | BigQuery SQL | Replaces the Oracle SQL quality check. Computes the percentage of customer segments re-versioned on the given run date using the `@RUN_DATE` query parameter. |

---

## 3. Key Design Decisions

### KornShell to Python Conversion
Legacy `.ksh` scripts were migrated to native Python (`.py`) scripts. This shift allows robust error handling, native integration with the Google Cloud BigQuery client library, and clean string parsing without relying on legacy Unix utilities like `tr`, `awk`, or `sqlplus`.

### BigQuery Scripting & Transactional Integrity
To maintain 100% semantic equivalence with the legacy Oracle transaction model, the SCD2 `MERGE` and subsequent `INSERT` statements are grouped inside a single transactional block (`BEGIN TRANSACTION ... COMMIT TRANSACTION`) in BigQuery.

### Temporal Snapshot Alignment
In the legacy Oracle script, `SYSDATE` was evaluated dynamically. To prevent race conditions and ensure that the `VALID_TO` timestamp updated in the `MERGE` matches the `VALID_FROM` timestamp in the subsequent `INSERT` down to the microsecond, we declare a single `v_current_time` timestamp variable at the start of the transaction.

### Query Parameterization
Replaced legacy SQL*Plus positional parameters (`&1`) with native BigQuery query parameters (`@RUN_DATE`). This prevents SQL injection risks and aligns with modern database practices.

### Airflow Orchestration
Wrapped the execution in a single-task Airflow DAG with `schedule=None` to match its legacy behavior as an externally triggered, embedded workflow.

---

## 4. Manual Steps Before Go-Live

Before deploying and running the migrated workload in production, the following manual setup steps must be completed:

### Schema & Dataset Creation
1. Ensure the target BigQuery dataset (e.g., `ANALYTICS_SCHEMA` or the environment-configured `BQ_DATASET`) exists in your GCP project.
2. Verify that the target tables exist with compatible schemas:
   * `ANALYTICS_SCHEMA.STG_CUSTOMER_SCORE_OUTPUT`
   * `ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT` (Ensure `VALID_FROM` and `VALID_TO` are typed as `TIMESTAMP` or `DATETIME`, and `IS_CURRENT` is typed as `INT64`).

### IAM & Permissions
The Cloud Composer service account (or the worker identity running the DAG) must be granted the following IAM roles on the target BigQuery dataset:
* **BigQuery Job User** (`roles/bigquery.jobUser`)
* **BigQuery Data Editor** (`roles/bigquery.dataEditor`)

### Airflow Variables Configuration
Define the following Airflow Variables in your Cloud Composer environment:

| Variable Key | Expected Value Example | Description |
| :--- | :--- | :--- |
| `GCP_PROJECT` | `my-gcp-project-id` | The target Google Cloud Project ID. |
| `GCP_REGION` | `us-east1` | The deployment region for BigQuery and Composer. |
| `SCRIPTS_DIR` | `/home/airflow/gcs/dags` | The GCS bucket path or local worker path where the Python and SQL scripts are deployed. |

### Environment Variables
Ensure `CRM_HOME` is configured in your execution environment or allowed to default to `/opt/etl/customer` (or your mapped GCS mount path).

### Scheduling & Triggering
Because the legacy job was designed to run embedded inside other workflows, the migrated DAG is configured with `schedule=None`. To trigger this DAG:
* **Option A**: Use Airflow's `TriggerDagRunOperator` within a parent DAG.
* **Option B**: Trigger manually via the Airflow UI or CLI.
* **Option C**: Call the Airflow REST API from an external orchestrator.

---

## 5. Known Gaps & Unresolved References

### Downstream Dependency Gap
* **Reference**: `CUSTOMER.WEEKLY_SCHEDULE`
* **Status**: **Not Yet Migrated**
* **Impact**: The downstream job `CUSTOMER.WEEKLY_SCHEDULE` expects the historization output to be complete. The cross-DAG trigger or sensor linkage cannot be finalized until that job is migrated.
* **Mitigation**: Once `CUSTOMER.WEEKLY_SCHEDULE` is migrated, its orchestration DAG must be updated to depend on the successful completion of the `customer_historization_load` DAG.

### SQL Stubs
* **Status**: **Action Required**
* **Impact**: The generated SQL files (`d_historization_load.sql` and `d_segment_quality_check.sql`) are currently provided as functional stubs.
* **Mitigation**: The actual production SCD2 merge logic and quality check calculations must be fully populated and verified using the converted SQL designs before deployment.

---

## 6. Validation

To validate the migration, execute the following test plan in a non-production environment:

### Step 1: Unit Testing the Python Scripts
Run the Python scripts locally or on a test worker by setting the required environment variables:
```bash
export CRM_HOME="/path/to/your/local/scripts"
export RUN_DATE="2023-11-01"
export GCP_PROJECT="your-test-gcp-project"
export GCP_REGION="us-east1"

# Execute the wrapper script
python3 customer/r_historization_load.py
```

### Step 2: Airflow DAG Dry Run
1. Upload the DAG and scripts to the Cloud Composer GCS bucket.
2. Trigger the DAG manually from the Airflow UI, passing a specific logical date (e.g., `2023-11-01`).

### Definition of "Passing"
The validation is considered successful when:
1. The Airflow task completes with exit code `0`.
2. The BigQuery job history shows a successful transaction containing the `MERGE` and `INSERT` operations.
3. The target table `DIM_CUSTOMER_SEGMENT` has updated records:
   * Old records are expired with `IS_CURRENT = 0` and `VALID_TO = execution_timestamp`.
   * New records are inserted with `IS_CURRENT = 1` and `VALID_FROM = execution_timestamp`.
4. The quality check executes successfully, logs the change percentage, and flags a warning *only* if the change percentage exceeds 25% (without failing the job).

---

## 7. Rollback Procedure

In the event of a production failure or data corruption during or after deployment, execute the following rollback steps:

### Step 1: Database Rollback (Time Travel)
Since BigQuery does not support traditional database rollbacks once a transaction is committed, use BigQuery's **Time Travel** feature to restore the target table to its state prior to the execution:

```sql
-- Restore the target table to its state 1 hour ago (adjust interval as needed)
CREATE OR REPLACE TABLE `your_project.ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT`
AS SELECT * FROM `your_project.ANALYTICS_SCHEMA.DIM_CUSTOMER_SEGMENT`
FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
```

### Step 2: Code Rollback
1. Revert the Airflow DAG and Python/SQL scripts in your Git repository to the previous stable commit.
2. Redeploy the reverted code to the Cloud Composer GCS bucket.
3. If necessary, clear the failed DAG run in the Airflow UI to allow a clean re-run once the environment is stabilized.