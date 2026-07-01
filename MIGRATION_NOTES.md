# Migration Notes: `ausd_bp_ta_msisdn_his`

This document details the migration of the legacy MSISDN history preparation job from UC4/Oracle to Google Cloud Platform (GCP) using Cloud Composer (Airflow) and BigQuery.

---

## 1. Summary

The `ausd_bp_ta_msisdn_his` job has been migrated from a legacy on-premises environment to a modern, cloud-native architecture on GCP. 

* **Legacy Platform:** UC4/Automic Scheduler, KornShell (KSH) wrappers, and Oracle SQL*Plus executing queries over an Oracle Database Link (`@pcrs1`).
* **Target Platform:** Google Cloud Platform (GCP) using Cloud Composer (Apache Airflow) for orchestration and BigQuery for data warehousing and transformations.
* **Business Logic:** This job prepares the historical Mobile Station International Subscriber Directory Number (MSISDN) data. It truncates the target history table `ta_msisdn_his`, reads from the replicated Carmen source database, resolves a dynamic watermark date from a metadata table, and reconstructs valid subscriber numbers for active products.

---

## 2. Generated Artifacts

The legacy orchestration and execution scripts have been consolidated into two primary target files:

| Artifact Path | Language / Type | Role |
| :--- | :--- | :--- |
| `dags/bereitstellung_basisprodukte_bert.py` | Python (Apache Airflow) | Orchestrates the execution flow, handles runtime parameters (such as `wiederanlaufwert`), and triggers the BigQuery execution. |
| `queries/d_ausd_bp_ta_msisdn_his.sql` | BigQuery SQL | Performs the core data transformation, including the `TRUNCATE` of the target table and the `INSERT-SELECT` logic with null-safe concatenation and watermark filtering. |

---

## 3. Key Design Decisions

### Consolidation of Orchestration Layers
The legacy system utilized a multi-layered execution chain: a UC4 job calling an orchestrator wrapper (`r_ausd_bp_ta_msisdn_his.ksh`), which called a control script (`k_ausd_bp_ta_msisdn_his.ksh`), which finally executed the SQL*Plus script (`d_ausd_bp_ta_msisdn_his.sql`). 
* **Decision:** These layers have been collapsed into a single Airflow DAG (`bereitstellung_basisprodukte_bert`). Parameter validation, date calculation, and execution are handled directly by Airflow operators, reducing maintenance overhead and removing legacy shell dependencies.

### Decommissioning of Database Links
* **Decision:** The Oracle database link `@pcrs1` pointing to the Carmen database has been decommissioned. It is replaced by referencing a replicated BigQuery dataset (`gcp-project-id.pds_dataset.ta_callnumber`) which is kept up-to-date via an upstream data integration pipeline.

### Null-Safe Concatenation Semantics
* **Decision:** In Oracle SQL, concatenating strings with `NULL` values ignores the `NULL` (e.g., `'A' || NULL || 'B'` results in `'AB'`). In BigQuery, standard `CONCAT` returns `NULL` if any argument is `NULL`. To preserve legacy behavior and prevent data loss, the migrated SQL uses explicit `COALESCE` statements:
  ```sql
  CONCAT(
      COALESCE(cn1.cc, ''),
      COALESCE(cn1.ndc, ''),
      COALESCE(cn1.sn, '')
  )
  ```

### Parameterized Restartability
* **Decision:** The restart mechanism (`wiederanlaufwert` / Restart ID) is implemented using BigQuery named query parameters (`@wiederanlaufwert`). This prevents SQL injection risks and allows the Airflow DAG to pass dynamic values safely via the `BigQueryInsertJobOperator`'s `queryParameters` configuration.

---

## 4. Manual Steps Before Go-Live

Before deploying and running the migrated pipeline in production, the following setup steps must be completed:

### A. Schema & Target Table Creation
Ensure that the target dataset and table exist in BigQuery. If the target table `ta_msisdn_his` does not exist, create it using the following DDL:

```sql
CREATE TABLE IF NOT EXISTS `gcp-project-id.sof_dataset.ta_msisdn_his`
(
    BPRI_COM_ID INT64 OPTIONS(description="Business Partner Relationship Instance Component ID"),
    MSISDN STRING OPTIONS(description="Concatenated Country Code, Network Destination Code, and Subscriber Number"),
    CALLNUMBER_ROLE_ID INT64 OPTIONS(description="Role identifier for the call number"),
    VALID_TO DATE OPTIONS(description="Validity end date")
);
```

### B. IAM & Permissions
The Cloud Composer Service Account (e.g., `service-account@gcp-project-id.iam.gserviceaccount.com`) must be granted the following IAM roles:
* **BigQuery Data Editor** (`roles/bigquery.dataEditor`) on the target dataset `sof_dataset`.
* **BigQuery Data Viewer** (`roles/bigquery.dataViewer`) on the source datasets `pds_dataset` and `isbert_dataset`.
* **BigQuery Job User** (`roles/bigquery.jobUser`) on the project level to execute query jobs.

### C. Airflow Connections
Ensure that the Airflow connection `google_cloud_default` is configured correctly in the Cloud Composer environment with the appropriate GCP Project ID and region.

### D. Scheduling & Upstream Dependencies
This DAG is currently configured with `schedule_interval=None` (triggered). It must be scheduled to run only **after** the upstream replication of `pds.ta_callnumber` and the execution of the job that writes the `BERT_DROP_TEMP_TABLE` watermark to `isbert_dataset.dwtk_meldungen` have completed.

---

## 5. Known Gaps & Unresolved References

### Hardcoded Project and Dataset IDs
* **Description:** The SQL script `queries/d_ausd_bp_ta_msisdn_his.sql` contains placeholder project and dataset references (`gcp-project-id.sof_dataset`, etc.).
* **Mitigation:** These must be replaced with environment-specific variables during the CI/CD deployment pipeline, or parameterized within the Airflow DAG using Airflow variables (e.g., `{{ var.value.gcp_project_id }}`).

### Upstream Watermark Dependency
* **Description:** The query relies on the existence of a watermark record in `isbert_dataset.dwtk_meldungen` where `job_kennung = 'BERT_DROP_TEMP_TABLE'`. If this record is missing, the query defaults to `1900-01-01`, which will result in an empty or incomplete target table.
* **Mitigation:** Ensure that the upstream process populating this metadata table is fully migrated and operational before enabling this job.

### Redesign (B4) Recommendation: Table Partitioning
* **Description:** The current design uses a full `TRUNCATE` and `INSERT` pattern. For very large datasets, this can cause temporary downtime for downstream consumers querying `ta_msisdn_his` during the execution window.
* **Mitigation:** In a future sprint, consider redesigning this table to use BigQuery table partitioning (e.g., partitioned by `VALID_TO` or ingestion time) or utilizing a `CREATE OR REPLACE TABLE` pattern to ensure atomic updates.

---

## 6. Validation

To validate the migration, execute the following test plan in a non-production environment:

### A. How to Run the Test
1. Upload `bereitstellung_basisprodukte_bert.py` to the Airflow DAGs folder in Cloud GCS.
2. Upload `d_ausd_bp_ta_msisdn_his.sql` to the `queries/` directory in GCS.
3. Trigger the DAG manually from the Airflow UI with the following configuration JSON:
   ```json
   {
     "wiederanlaufwert": 0
   }
   ```

### B. Definition of "Passing"
The test run is considered successful if:
1. The Airflow DAG run completes with a status of `Success`.
2. The BigQuery job log shows that the query executed successfully without syntax or permission errors.
3. A row count comparison between the legacy Oracle table `sof$ta_msisdn_his` and the BigQuery table `ta_msisdn_his` (using the same watermark date) shows a **100% match**.
4. Spot checks confirm that no `MSISDN` values in the target table are `NULL` or contain unexpected empty strings due to concatenation issues.

---

## 7. Rollback Procedure

In the event of a critical failure during the go-live window, perform the following steps to roll back to the legacy system:

1. **Pause the Airflow DAG:** Navigate to the Airflow UI and toggle the switch for `bereitstellung_basisprodukte_bert` to **Off**.
2. **Re-enable Legacy Scheduler:** Reactivate the UC4 job definition `DW.BERT_AUSD_BP_TA_MSISDN_HIS` in the Automic scheduler.
3. **Verify Legacy Database State:** Ensure that the Oracle target table `sof$ta_msisdn_his` and the database link `@pcrs1` are intact and have not been modified.
4. **Execute Legacy Catch-up Run:** Trigger a manual execution of the legacy UC4 job to ensure data consistency for downstream consumers.