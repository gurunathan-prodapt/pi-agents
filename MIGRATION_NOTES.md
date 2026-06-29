# Migration Notes: `ausd_bp_ta_bpr_apn`

This document provides the comprehensive migration notes for transferring the legacy UC4 job **DW.BERT_AUSD_BP_TA_BPR_APN** to Google Cloud Platform (GCP).

---

## 1. Summary

The legacy batch job `DW.BERT_AUSD_BP_TA_BPR_APN` has been migrated from an on-premise environment (orchestrated by UC4/Automic, utilizing KornShell wrappers and Oracle SQL\*Plus) to a cloud-native architecture on **Google Cloud Platform (GCP)**.

*   **Legacy Platform:** UC4 Scheduler, KornShell (`.ksh`) scripts, Oracle Database.
*   **Target Platform:** Google Cloud Composer (Apache Airflow 2.x), Google BigQuery.
*   **Business Purpose:** This job processes refined, instantiated basic products (such as VPNs, Telemetry, and Blackberry solutions) and pairs them with their respective Access Point Names (APN) using contract references. The consolidated output is loaded into the target table `sof_ta_bpr_apn` (formerly `sof$ta_bpr_apn`), which is consumed downstream by rating, risk, and credit scoring systems (BERT/FOS - Forderungsscoring).

---

## 2. Generated Artifacts

The migration process generated the following files, organized by their respective roles in the target environment:

| Artifact Path | Type | Role |
| :--- | :--- | :--- |
| `src/sql/sof_ta_bpr_apn_ddl.sql` | BigQuery DDL | Initializes the target table `sof_ta_bpr_apn` inside the `isbert_schema` dataset with appropriate column descriptions. |
| `src/sql/sof_ta_bpr_apn_transform.sql` | BigQuery DML | Standalone SQL script containing the `TRUNCATE` and `INSERT` transformation logic for manual execution or testing. |
| `src/dags/ausd_bp_ta_bpr_apn.py` | Airflow DAG | Python orchestration script that defines the workflow, schedules execution, and runs the BigQuery transformation using the `BigQueryExecuteQueryOperator`. |

---

## 3. Key Design Decisions

### 3.1 Database Object Renaming
*   **Decision:** Renamed the target table from `sof$ta_bpr_apn` to `sof_ta_bpr_apn`.
*   **Reasoning:** BigQuery does not support special characters like `$` in table identifiers. Replacing it with an underscore (`_`) aligns with Google Cloud naming conventions and ensures compatibility.

### 3.2 Dead Code & Redundant Parameter Elimination
*   **Decision:** Removed the query targeting `isbert_schema.dwtk_meldungen` which historically set the `v_datum` variable.
*   **Reasoning:** A legacy refactoring (version `10.2.1`) removed the variable from all target table names, rendering this query dead code. Excluding it prevents unnecessary BigQuery execution overhead and costs.
*   **Decision:** Omitted the `Stichtag` parameter from the SQL logic.
*   **Reasoning:** The legacy shell script accepted and passed this parameter, but it was never referenced in the active SQL query.

### 3.3 Performance Optimization (Oracle Hint Removal)
*   **Decision:** Stripped out Oracle-specific optimizer hints (e.g., `/*+ full(bp) parallel(bp,4) ... */`).
*   **Reasoning:** BigQuery is a serverless, columnar data warehouse that automatically manages query execution plans and parallelization. Oracle hints are non-functional and syntactically redundant in BigQuery.

### 3.4 Simplification of Shell Logic & Logging
*   **Decision:** Retired the KornShell wrappers (`r_ausd_bp_ta_bpr_apn.ksh` and `k_ausd_bp_ta_bpr_apn.ksh`) along with the legacy `FOSJobErzeugeEintrag` logging mechanism.
*   **Reasoning:** Airflow natively handles environment variables, job scheduling, retries, and execution metadata. Detailed execution logs and row-count statistics are automatically captured and preserved in Google Cloud Logging. Commented-out legacy shell commands performing file operations (`sed`, `sort`, `join` on `.dat` files) were also fully retired.

---

## 4. Manual Steps Before Go-Live

Before deploying and enabling the pipeline in production, the following setup steps must be completed:

### 4.1 Schema and Dataset Creation
1. Ensure the BigQuery dataset `isbert_schema` exists in your target GCP project.
2. Execute the DDL script `src/sql/sof_ta_bpr_apn_ddl.sql` in the BigQuery console to initialize the target table:
   ```bash
   bq query --use_legacy_sql=false < src/sql/sof_ta_bpr_apn_ddl.sql
   ```

### 4.2 IAM & Permissions
Ensure that the service account running the Cloud Composer workers has the following IAM roles on the target BigQuery dataset and project:
*   `roles/bigquery.dataEditor` (to truncate and insert data into `sof_ta_bpr_apn`)
*   `roles/bigquery.jobUser` (to run BigQuery jobs)

### 4.3 Airflow Variables & Connections
1. **Airflow Variable:** Define a variable named `gcp_project` containing your target GCP Project ID.
   *   *Example:* `gcp_project` = `prod-bert-data-project`
2. **Airflow Connection:** Ensure the connection `google_cloud_default` is configured correctly with the appropriate GCP service account credentials.

### 4.4 Scheduling & Upstream Dependencies
The DAG is configured to run daily at **04:00 AM UTC** (`0 4 * * *`). Verify that the upstream tables `sof_ta_bpr_instance` and `sof_ta_apn_carmen` are fully populated and updated before this execution window.

---

## 5. Known Gaps & Unresolved References

*   **Upstream Dependency Coupling:** The pipeline currently relies on time-based scheduling. If the upstream ingestion jobs for `sof_ta_bpr_instance` and `sof_ta_apn_carmen` experience delays, this job could run against stale data.
    *   *Follow-up:* It is recommended to transition this DAG to use `ExternalTaskSensor` tasks or Airflow Dataset-based scheduling once the upstream jobs are migrated to Composer.
*   **Stichtag Date Filter:** If future business requirements demand historical snapshotting or date-partition filtering, the target table `sof_ta_bpr_apn` must be re-created as a partitioned table, and filtering logic must be introduced.
*   **Audit Trail Requirements:** If downstream legacy systems strictly require physical updates to the `dwtk_meldungen` table, a post-execution task must be added to the Airflow DAG to append status records.

---

## 6. Validation

To validate the migration, execute the following testing procedures:

### 6.1 Dry-Run Validation
Run a dry-run of the transformation query in the BigQuery console to verify syntax and estimate bytes processed:
```sql
-- Run this in the BigQuery console with Dry Run enabled
SELECT DISTINCT
  bp.cntrct_id,
  bp.bpr_id,
  bp.cntrct_id_ref,
  ap.access_point_name
FROM `your-gcp-project.isbert_schema.sof_ta_bpr_instance` bp
INNER JOIN `your-gcp-project.isbert_schema.sof_ta_apn_carmen` ap
   ON bp.cntrct_id_ref = ap.cntrct_id
WHERE bp.bpr_id IN (2828, 2829, 2830, 2831, 2925, 2926, 2998, 2999, 3000);
```

### 6.2 DAG Execution Test
1. Upload `src/dags/ausd_bp_ta_bpr_apn.py` to the Cloud Composer DAGs folder.
2. Navigate to the Airflow UI, locate `ausd_bp_ta_bpr_apn_dag`, and trigger it manually.
3. Verify that all tasks (`start_pipeline` -> `transform_basisprodukte_apn` -> `end_pipeline`) complete with a `success` status.

### 6.3 Data Reconciliation ("Passing" Criteria)
The migration is considered successful when:
1. The target table `isbert_schema.sof_ta_bpr_apn` is successfully populated.
2. A row-count comparison and checksum validation between the legacy Oracle table (`sof$ta_bpr_apn`) and the new BigQuery table (`sof_ta_bpr_apn`) show a **100% match** for the same source data snapshot.

---

## 7. Rollback Procedure

In the event of a critical failure or data corruption during deployment:

1.  **Pause the DAG:** Immediately pause the `ausd_bp_ta_bpr_apn_dag` in the Airflow UI to prevent further scheduled executions.
2.  **Restore Target Table:** If the target table was corrupted, restore it to its pre-execution state using BigQuery's Time Travel feature:
    ```sql
    CREATE OR REPLACE TABLE `isbert_schema.sof_ta_bpr_apn` AS
    SELECT * FROM `isbert_schema.sof_ta_bpr_apn`
    FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
    ```
3.  **Revert to Legacy:** If a fallback to the on-premise system is required, re-enable the UC4 schedule object `DW.BERT_AUSD_BP_TA_BPR_APN` and verify that the legacy database connections are active.