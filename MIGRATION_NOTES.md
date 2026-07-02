# MIGRATION NOTES: `ausd_bp_ta_bpr_basis_his`

This document provides the technical migration notes for transitioning the legacy UC4 job `BERT_AUSD_BP_TA_BPR_BASIS_HIS` and its associated KornShell and Oracle SQL scripts to Google Cloud Platform (GCP).

---

## 1. Summary

The legacy batch job `ausd_bp_ta_bpr_basis_his` has been migrated from an on-premises Oracle and UC4/Automic environment to a cloud-native architecture on **Google Cloud Platform (GCP)**. 

* **Source Platform:** UC4/Automic Scheduler, KornShell wrappers (`r_*.ksh`, `k_*.ksh`), and Oracle Database (accessing remote tables via DB Link `@pcrs1`).
* **Target Platform:** GCP Cloud Composer (Apache Airflow) and Google BigQuery.
* **Migration Strategy:** The multi-step legacy shell execution and SQL*Plus scripts have been consolidated into a single, idempotent BigQuery SQL script orchestrated by a lightweight Cloud Composer DAG.

---

## 2. Generated Artifacts

The migration process generated the following target files, which must be deployed to your Cloud Composer environment:

| Target File Path | Target Language | Role / Description |
| :--- | :--- | :--- |
| `dags/dw_bert_ausd_bp_ta_bpr_basis_his.py` | Python (Airflow DAG) | Orchestrates the workflow. It defines the DAG, sets the execution schedule, configures environment parameters, and triggers the BigQuery execution task. |
| `sql/d_ausd_bp_ta_bpr_basis_his.sql` | BigQuery Standard SQL | Contains the complete ETL logic: fetches the operational cutoff date, truncates the target table, performs the join/transformation, and inserts the results. |

---

## 3. Key Design Decisions

### 3.1. Consolidation of Shell and SQL Logic
The legacy architecture split execution across UC4 tasks, KornShell wrappers, and SQL*Plus scripts. To reduce orchestration overhead and simplify error handling, all transformation, filtering, and date-calculation logic have been consolidated into a single BigQuery SQL script (`sql/d_ausd_bp_ta_bpr_basis_his.sql`). This script is executed in a single call by the Airflow `BigQueryExecuteQueryOperator`.

### 3.2. Null-Safe String Concatenation
* **Legacy Behavior:** In Oracle SQL, concatenating null values (e.g., `NULL || '-' || NULL`) treats nulls as empty strings, resulting in `'-'`.
* **BigQuery Behavior:** In BigQuery, standard `CONCAT` propagates nulls; if any argument is `NULL`, the entire result is `NULL`.
* **Decision:** The migrated SQL uses `CONCAT` wrapped with `IFNULL(..., '')` for each component of the `ICCID` field to guarantee identical string formatting and prevent null propagation.

### 3.3. Idempotency and Restartability
To ensure the job can be safely retried or rerun without duplicating data, the SQL script executes a `TRUNCATE TABLE` on the target table `sof.ta_bpr_basis_his` immediately before performing the `INSERT INTO` operation. This replaces the complex, manual restart parameters (`p_wiederanlaufWert`) used in the legacy shell scripts.

### 3.4. Dynamic Cutoff Date (`v_datum`)
The legacy job dynamically determined its operational cutoff date by querying a metadata table. This design has been preserved in BigQuery by declaring a local variable `v_datum` and populating it from `isbert_schema.dwtk_meldungen` where `job_kennung = 'BERT_DROP_TEMP_TABLE'`.

---

## 4. Manual Steps Before Go-Live

Before enabling the DAG in production, the following setup steps must be completed:

### 4.1. Schema and Dataset Creation
Ensure that the following BigQuery datasets exist in your target GCP project (e.g., `gcp-dwh-prod`):
* `cds` (Source dataset containing `ta_cntrct`)
* `pds` (Source dataset containing `ta_bpri_com`)
* `isbert_schema` (Metadata dataset containing `dwtk_meldungen`)
* `sof` (Target dataset containing `ta_bpr_basis_his`)

If the target table `sof.ta_bpr_basis_his` does not exist, it will be created automatically by the DAG (`create_disposition="CREATE_IF_NEEDED"`), but it is recommended to pre-create it with the correct schema, clustering, and partitioning keys.

### 4.2. IAM & Permissions
The Cloud Composer environment's service account must be granted the following IAM roles:
* **`roles/bigquery.jobUser`** on the project level.
* **`roles/bigquery.dataEditor`** on the target dataset `sof`.
* **`roles/bigquery.dataViewer`** on the source datasets `cds`, `pds`, and `isbert_schema`.

### 4.3. Airflow Connections
Ensure that the Airflow connection specified in the DAG (`google_cloud_default`) is configured in your Cloud Composer environment and has the appropriate permissions to execute BigQuery jobs in the target project.

### 4.4. Upstream Data Replication
Because the legacy Oracle query accessed tables via DB Link (`@pcrs1`), you must ensure that your data replication pipeline (e.g., Qlik, Fivetran, or Google Datastream) is actively syncing the following tables from Oracle to BigQuery before scheduling this job:
* `cds.ta_cntrct`
* `pds.ta_bpri_com`

---

## 5. Known Gaps & Unresolved References

### 5.1. Hardcoded Project IDs in SQL
The generated SQL script (`sql/d_ausd_bp_ta_bpr_basis_his.sql`) contains hardcoded references to the production project `gcp-dwh-prod`. 
* **Follow-up Action:** For non-production environments (e.g., `gcp-dwh-dev`), these project prefixes must be updated. It is recommended to handle this dynamically via your CI/CD deployment pipeline (e.g., using token replacement) or by removing the project prefix if the DAG runs within the same project.

### 5.2. Upstream Metadata Dependency
The job relies on `isbert_schema.dwtk_meldungen` being populated with the latest run date for `BERT_DROP_TEMP_TABLE`. If the upstream job responsible for updating this table has not yet been migrated to GCP, this query will fall back to `'1900-01-01'`, resulting in an empty or incomplete target table.
* **Follow-up Action:** Verify the migration status of the upstream metadata writer. If necessary, temporarily replace the dynamic `v_datum` lookup with Airflow's execution date parameter: `DATE('{{ ds }}')`.

---

## 6. Validation

To validate the migration, perform the following tests in your development/staging environment:

### 6.1. Dry-Run Validation
Run a dry-run of the SQL script in the BigQuery console to verify syntax and estimate query costs:
```sql
-- Enable "Dry Run" in the BigQuery Console settings before executing
DECLARE v_datum DATE DEFAULT DATE('2026-04-21');
-- (Paste the rest of sql/d_ausd_bp_ta_bpr_basis_his.sql here)
```

### 6.2. DAG Execution Test
Trigger the DAG manually from the Airflow UI or via the gcloud CLI:
```bash
gcloud composer environments run <your-composer-env> \
    --location <your-region> \
    dags trigger -- dw_bert_ausd_bp_ta_bpr_basis_his
```
* **Definition of "Passing":** The DAG run status must be `success`, and the task `execute_ausd_bp_ta_bpr_basis_his` must complete without errors.

### 6.3. Data Reconciliation
Compare the output of the migrated BigQuery job against the legacy Oracle table:
1. **Row Count Check:**
   ```sql
   SELECT COUNT(*) FROM `gcp-dwh-prod.sof.ta_bpr_basis_his`;
   ```
   Compare this count with the row count of `sof.ta_bpr_basis_his` in the legacy Oracle database for the same operational date.
2. **Null Concatenation Check:**
   Verify that the `ICCID` column does not contain unexpected `NULL` values:
   ```sql
   SELECT COUNT(*) FROM `gcp-dwh-prod.sof.ta_bpr_basis_his` WHERE ICCID IS NULL;
   ```

---

## 7. Rollback Procedure

If critical issues are discovered in production post-go-live, follow these steps to roll back to the legacy system:

1. **Pause the Airflow DAG:**
   Go to the Airflow UI and toggle the switch for `dw_bert_ausd_bp_ta_bpr_basis_his` to **Off** (paused).
2. **Re-enable the UC4 Job:**
   In the Automic/UC4 UI, locate the job `BERT_AUSD_BP_TA_BPR_BASIS_HIS` and set its status back to **Active** / scheduled.
3. **Verify Legacy Target State:**
   Ensure that the legacy Oracle target table `sof.ta_bpr_basis_his` is intact. If the legacy table was truncated or modified during the cutover, trigger a manual run of the UC4 job to repopulate it.