# MIGRATION NOTES: `ausd_bp_ta_apn_carmen`

This document provides comprehensive migration notes for transitioning the legacy batch job `ausd_bp_ta_apn_carmen` from an on-premise Oracle / UC4 / KornShell (ksh) environment to Google Cloud Platform (BigQuery and Cloud Composer / Apache Airflow).

---

## 1. Summary

The legacy batch job `ausd_bp_ta_apn_carmen` has been migrated to a modern, cloud-native architecture on **Google Cloud Platform (GCP)**. 

* **Legacy Platform**: Oracle Database, UC4/Automic Scheduler, and KornShell (`.ksh`) wrapper scripts.
* **Target Platform**: Google Cloud Composer (Apache Airflow) for orchestration and Google BigQuery for data storage and processing.
* **Business Purpose**: This job extracts active, production-relevant GPRS/UMTS PDP contexts and Access Point Name (APN) associations for contracts from a replicated Carmen source system. It filters these records using a dynamic, historical cutoff date and populates the target table `sof.ta_apn_carmen` (formerly `sof$ta_apn_carmen`).

---

## 2. Generated Artifacts

The migration process has produced three core artifacts to replace the legacy UC4 job and shell scripts:

| File Path | Language / Type | Role | Legacy Component Replaced |
| :--- | :--- | :--- | :--- |
| `dags/ausd_bp_ta_apn_carmen_dag.py` | Python / Airflow DAG | Orchestrates the daily execution of the pipeline, handles retries, and triggers the BigQuery stored procedure. | `DW.BERT_AUSD_BP_TA_APN_CARMEN.xml`, `r_ausd_bp_ta_apn_carmen.ksh`, `k_ausd_bp_ta_apn_carmen.ksh` |
| `sql/proc_d_ausd_bp_ta_apn_carmen.sql` | BigQuery SQL (Stored Procedure) | Encapsulates the core ETL logic: dynamic cutoff date calculation, target table truncation, temporal filtering, and audit logging. | `d_ausd_bp_ta_apn_carmen.sql` |
| `sql/ddl_sof_ta_apn_carmen.sql` | BigQuery SQL (DDL) | Defines the schema and creates the target BigQuery table `sof.ta_apn_carmen`. | Implicit Oracle Schema (`sof$ta_apn_carmen`) |

---

## 3. Key Design Decisions

### 3.1 Consolidation of Orchestration and Wrapper Scripts
The legacy architecture relied on a multi-layered wrapper structure: a UC4 XML job calling `r_ausd_bp_ta_apn_carmen.ksh`, which in turn called `k_ausd_bp_ta_apn_carmen.ksh` to execute the SQL script. 
* **Decision**: Consolidate these layers into a single **Cloud Composer (Airflow) DAG** that directly calls a **BigQuery Stored Procedure**.
* **Trade-off**: This removes shell-scripting overhead and file-based logging, but shifts operational logging entirely into BigQuery and Airflow's native logging mechanisms.

### 3.2 Stored Procedure Encapsulation
* **Decision**: Implement the core ETL logic inside a BigQuery Stored Procedure (`sof.proc_d_ausd_bp_ta_apn_carmen`) rather than writing raw SQL queries directly inside the Airflow DAG.
* **Reasoning**: This keeps the SQL logic version-controlled, modular, and easily testable directly within the BigQuery console. It also minimizes network round-trips and keeps the Airflow DAG lightweight.

### 3.3 Dynamic Cutoff Date Calculation
* **Decision**: Replicate the legacy logic of querying `isbert_schema.dwtk_meldungen` for the maximum `timecreated` timestamp associated with the job `BERT_DROP_TEMP_TABLE`.
* **Implementation**: The stored procedure uses `COALESCE(MAX(DATE(timecreated)), DATE '1900-01-01')` to handle cases where the upstream job has not yet run or has failed, ensuring the procedure remains idempotent and does not crash on missing metadata.

### 3.4 Temporal Logic Translation
* **Decision**: Convert Oracle's date comparisons to native BigQuery `DATE` comparisons.
* **Implementation**: Explicit `DATE()` casting is applied to timestamp fields (e.g., `insert_at`, `modified_at`, `valid_from`, `valid_to`) to ensure correct temporal filtering against the calculated `v_datum` cutoff date.

### 3.5 Transaction and Error Handling
* **Decision**: Wrap the stored procedure logic in a `BEGIN...EXCEPTION WHEN ERROR THEN...END` block.
* **Reasoning**: This allows the procedure to log failures to `isbert_schema.job_log` with a detailed error message before executing `RAISE USING MESSAGE` to ensure that Airflow registers the task as failed.

---

## 4. Manual Steps Before Go-Live

To ensure a successful deployment, the following manual setup steps must be completed in the target environment:

### 4.1 Schema and Dataset Creation
Ensure that the following BigQuery datasets exist in your target GCP project:
* `sof` (Target dataset)
* `isbert_schema` (Metadata and logging dataset)
* `carmen_replica` (Source replica dataset)

### 4.2 Target Table DDL Execution
Execute the DDL script to create the target table:
```bash
bq query --use_legacy_sql=false < sql/ddl_sof_ta_apn_carmen.sql
```

### 4.3 Stored Procedure Deployment
Deploy the stored procedure to the `sof` dataset:
```bash
bq query --use_legacy_sql=false < sql/proc_d_ausd_bp_ta_apn_carmen.sql
```

### 4.4 IAM & Permissions
Ensure that the service account running Cloud Composer / Airflow has the following IAM roles:
* `roles/bigquery.jobUser` on the GCP project.
* `roles/bigquery.dataEditor` on the `sof` and `isbert_schema` datasets.
* `roles/bigquery.dataViewer` on the `carmen_replica` dataset.

### 4.5 Airflow Connection Setup
In the Airflow UI, verify or create the connection specified by `CONN_ID` (default: `bigquery_default`). Ensure it is configured with the correct GCP Project ID.

### 4.6 Upstream Data Replication
Verify that the following tables are being actively replicated from the Carmen source system (`@pcrs1`) into the `carmen_replica` dataset:
* `pds_ta_pdp_context_assoc`
* `pds_ta_pdp_context`
* `pds_ta_access_point`

---

## 5. Known Gaps & Unresolved References

### 5.1 Upstream Dependency Synchronization
The dynamic cutoff date calculation relies on the table `isbert_schema.dwtk_meldungen` being updated by an upstream job named `BERT_DROP_TEMP_TABLE`. 
* **Gap**: If the upstream job is delayed or has not been migrated to GCP, this job will default to a cutoff date of `1900-01-01`, resulting in an empty target table.
* **Mitigation**: Ensure that the DAG scheduling or Airflow sensors are configured to wait for the completion of the upstream workflow.

### 5.2 Data Replication Lag
There is no built-in mechanism in the stored procedure to verify if the tables in `carmen_replica` are up-to-date. If replication lags, the output in `sof.ta_apn_carmen` will be stale.

### 5.3 Redesign (B4) Items
* **Hardcoded Project ID**: The Airflow DAG contains a hardcoded project ID placeholder (`gcp-project-id`). This must be parameterized using Airflow Variables or environment-specific configuration files prior to deployment in production.
* **Centralized Logging**: The stored procedure writes directly to `isbert_schema.job_log`. A centralized logging framework should be established across all migrated jobs to standardize audit trails.

---

## 6. Validation

To validate the migration, execute the following testing procedures:

### 6.1 Dry Run / Manual Execution
Run the stored procedure manually in the BigQuery console:
```sql
CALL `sof.proc_d_ausd_bp_ta_apn_carmen`();
```

### 6.2 Airflow DAG Test
1. Upload `dags/ausd_bp_ta_apn_carmen_dag.py` to your Cloud Composer DAGs folder.
2. Trigger the DAG manually from the Airflow UI.
3. Verify that the task `run_proc_d_ausd_bp_ta_apn_carmen` completes successfully.

### 6.3 "Passing" Criteria
The migration is considered successful if:
1. The Airflow DAG run status is `SUCCESS`.
2. The target table `sof.ta_apn_carmen` contains data (verify with `SELECT COUNT(*) FROM sof.ta_apn_carmen`).
3. An entry is written to `isbert_schema.job_log` with `event_type = 'SUCCESS'` and a message containing the row count and effective date.
4. No duplicate records exist for the same contract-to-APN mapping:
   ```sql
   SELECT CNTRCT_ID, ACCESS_POINT_NAME, COUNT(*) 
   FROM `sof.ta_apn_carmen` 
   GROUP BY 1, 2 
   HAVING COUNT(*) > 1;
   -- Expected result: 0 rows returned.
   ```

---

## 7. Rollback Procedure

If critical issues are discovered post-go-live, follow these steps to roll back to the legacy environment:

1. **Pause the Airflow DAG**: In the Airflow UI, toggle the switch to pause `ausd_bp_ta_apn_carmen_dag`.
2. **Truncate the BigQuery Target Table**: Clear any data loaded by the migrated pipeline to prevent downstream confusion:
   ```sql
   TRUNCATE TABLE `sof.ta_apn_carmen`;
   ```
3. **Re-enable Legacy UC4 Job**: Re-activate the UC4 job `DW.BERT_AUSD_BP_TA_APN_CARMEN` in the on-premise environment.
4. **Execute Legacy Run**: Manually trigger the legacy UC4 job and verify that the Oracle table `sof$ta_apn_carmen` is populated correctly.