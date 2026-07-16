# MIGRATION_NOTES.md — `finance/finance_month_end.xml`

This document outlines the migration details, design decisions, manual setup steps, known gaps, validation procedures, and rollback strategies for the **Month-End General Ledger (GL) Close Workflow** (`finance_month_end_workflow`).

---

## 1. Summary

The legacy Automic UC4 workflow `FINANCE_MONTH_END_WORKFLOW` (defined in `finance/finance_month_end.xml`) has been migrated to **Google Cloud Composer (Airflow)**. 

*   **Source Platform:** Automic UC4 (utilizing inline Oracle PL/SQL checks, legacy `.ksh` shell scripts, Ab Initio transformation/reconciliation graphs, and Spark-on-YARN assembly JARs).
*   **Target Platform:** Google Cloud Composer (Airflow) orchestrating serverless data processing workloads on **Google Cloud Dataproc** (PySpark), **Google Cloud Pub/Sub**, and **BigQuery**.

---

## 2. Generated Artifacts

The migration process generated the following files:

| File Path | Role | Description |
| :--- | :--- | :--- |
| `dags/finance_month_end_workflow.py` | **Orchestration DAG** | The primary Airflow DAG file containing task definitions, dependencies, dynamic calendar logic, and error-handling callbacks. |
| `pyspark_scripts/run_account_load.py` | **Dimension Loader** | Ported from `run_account_load.ksh`. Executes SCD Type 2 dimension loads for cost-centre hierarchies on Dataproc. |
| `pyspark_scripts/run_gl_close_uk.py`<br>`run_gl_close_de.py`<br>`run_gl_close_fr.py` | **Regional Extractors** | Ported from `run_gl_close.ksh`. Parametrized PySpark scripts to extract regional GL transactions. |
| `pyspark_scripts/gl_transform.py` | **Core Transformer** | Ported from Ab Initio `gl_transform.xfr`. Performs unified data transformations. |
| `pyspark_scripts/gl_reconcile.py` | **Reconciliation Engine** | Ported from Ab Initio `gl_reconcile.pdl`. Performs sub-ledger reconciliation checks. |
| `pyspark_scripts/finance_etl_assembly.py` | **Analytical Aggregator** | Ported from the legacy Scala Spark assembly JAR (`finance-etl-assembly.jar`) to native PySpark. |

---

## 3. Key Design Decisions

### A. Dynamic Calendar Gate (`ShortCircuitOperator`)
*   **Decision:** The legacy UC4 workflow relies on a calendar event (`LAST_BUSINESS_DAY_OF_MONTH`). Because standard cron expressions in Airflow cannot natively calculate the "last business day of the month" (excluding weekends), the DAG is scheduled to run daily at `20:00 Europe/London`.
*   **Mechanism:** A `ShortCircuitOperator` (`guard_last_business_day`) executes first. It dynamically calculates if the current execution date is the last weekday (Monday–Friday) of the month. If false, it skips all downstream tasks, preventing unnecessary resource consumption.
*   **Override:** A global Airflow variable `finance_force_close` can be set to `Y` to bypass this check for manual off-cycle runs.

### B. Non-Blocking Failure Path (Reconciliation)
*   **Decision:** The legacy task `FINANCE_ABINITIO_RECONCILE` was configured with `ON_FAILURE action="NOTIFY" then="CONTINUE"`. 
*   **Mechanism:** In Airflow, this is implemented by setting the downstream task `finance_daily_gl_close` to use `trigger_rule=TriggerRule.ALL_DONE`. This guarantees that even if the reconciliation task fails, the pipeline continues to log the audit trail, publish completion events, and trigger downstream dependencies.

### C. Transition from Ab Initio/Shell to PySpark on Dataproc
*   **Decision:** Legacy shell wrappers (`.ksh`) and Ab Initio graphs (`.xfr`, `.pdl`) were refactored into PySpark scripts.
*   **Trade-off:** This eliminates licensing costs and infrastructure overhead associated with Ab Initio and legacy on-premise Hadoop/YARN clusters, moving to a fully managed, pay-as-you-go Dataproc model.

---

## 4. Manual Steps Before Go-Live

The following configuration steps must be completed in the target GCP environment before enabling the DAG:

### A. Airflow Variables
Configure the following Airflow Variables in the Cloud Composer UI (**Admin -> Variables**):

| Variable Key | Example Value | Description |
| :--- | :--- | :--- |
| `GCP_PROJECT` | `gcp-finance-prod` | Target GCP Project ID where Dataproc and Pub/Sub reside. |
| `GCP_REGION` | `europe-west1` | GCP Region for Dataproc cluster execution. |
| `DATAPROC_CLUSTER` | `finance-spark-cluster` | Name of the active Dataproc cluster. |
| `GCS_BUCKET` | `finance-scripts-bucket` | GCS Bucket containing the migrated PySpark scripts. |
| `finance_notify_email` | `finance-etl@company.com` | Target email address for SLA and failure alerts. |
| `finance_force_close` | `N` | Set to `Y` to force-run the DAG on non-business days. |

### B. Airflow Connections
*   **Oracle Connection (`oracle_default`):** The `finance_pre_flight` task requires connectivity to the source Oracle GL database. Configure an Oracle connection in Airflow (**Admin -> Connections**) containing the host, port, schema, and credentials.

### C. IAM Permissions
Ensure the Cloud Composer environment's service account has the following IAM roles:
*   `roles/dataproc.editor` (To submit PySpark jobs to Dataproc)
*   `roles/pubsub.publisher` (To publish messages to the `finance-gl-close-complete` topic)
*   `roles/composer.worker` (Standard execution permissions)

### D. GCS Code Deployment
Upload all generated PySpark scripts to the designated GCS bucket:
```bash
gsutil cp pyspark_scripts/*.py gs://[YOUR_GCS_BUCKET]/pyspark_scripts/
```

### E. Pub/Sub Topic Creation
Ensure the target Pub/Sub topic exists in your GCP project:
```bash
gcloud pubsub topics create finance-gl-close-complete
```

---

## 5. Known Gaps & Unresolved References

1.  **Missing Upstream Dependency (`STG_CUSTOMER_SALES`):**
    *   *Gap:* The legacy workflow expects sales data to be staged before running. This dependency is not natively managed within this XML.
    *   *Remediation:* A BigQuery sensor or an external dataset sensor must be added upstream of `guard_last_business_day` once the Sales domain migration is finalized.
2.  **Cross-DAG Trigger Targets:**
    *   *Gap:* The DAG triggers `crm_weekly_workflow` and `retail_daily_workflow`. If these DAGs are not yet deployed in Airflow, the `TriggerDagRunOperator` tasks will fail.
    *   *Remediation:* Ensure placeholder/stub DAGs with those exact IDs exist in the Composer environment before running the month-end pipeline.
3.  **Legacy Mail Command:**
    *   *Gap:* The final task `finance_period_close_notify` uses a local `mailx` command. This requires the Composer worker containers to have a configured local mail transfer agent (MTA).
    *   *Remediation:* It is highly recommended to replace this `BashOperator` with Airflow's native `EmailOperator` utilizing SendGrid or your enterprise SMTP connection.

---

## 6. Validation

### A. Local/Staging DAG Parsing Test
Verify that the DAG is syntactically correct and loads without import errors:
```bash
python3 dags/finance_month_end_workflow.py
```

### B. Unit Testing the Calendar Logic
To test the `is_last_business_day` function, run the following Python snippet:
```python
from datetime import datetime
from dags.finance_month_end_workflow import is_last_business_day

# Test a known last business day (Friday, Jan 31, 2025)
assert is_last_business_day(datetime(2025, 1, 31)) == True

# Test a weekend (Sunday, Jan 26, 2025)
assert is_last_business_day(datetime(2025, 1, 26)) == False
```

### C. End-to-End Dry Run
1.  Set the Airflow Variable `finance_force_close` to `Y`.
2.  Trigger the DAG manually from the Airflow UI.
3.  Verify that:
    *   `guard_last_business_day` evaluates to `True`.
    *   Dataproc jobs are successfully submitted and tracked.
    *   The reconciliation task failure (if simulated) does not block the execution of `finance_daily_gl_close`.
    *   A Pub/Sub message is published to `finance-gl-close-complete`.

---

## 7. Rollback Procedure

If the migrated Airflow DAG fails in production and a rollback to the legacy Automic UC4 scheduler is required:

1.  **Pause the Airflow DAG:**
    ```bash
    gcloud composer environments run [COMPOSER_ENV] \
        --location [LOCATION] \
        dags pause -- finance_month_end_workflow
    ```
2.  **Re-enable UC4 Active Flag:**
    *   Log into the Automic UC4 client.
    *   Locate `FINANCE_MONTH_END_WORKFLOW` (`JOBP`).
    *   Set the active status flag back to `Active` (or resume the active schedule).
3.  **Verify Database State:**
    *   Ensure any partial extractions or dimension loads executed by Airflow are rolled back or safely overwritten by the UC4 execution (the PySpark scripts are designed to be idempotent based on the `PERIOD_DATE` parameter).