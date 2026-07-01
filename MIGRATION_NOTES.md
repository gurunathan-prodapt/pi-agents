# MIGRATION NOTES

**System/Job Name:** `ausd_bp_ta_cntrct_dist`  
**Source Platform:** UC4 / KornShell (KSH) / Oracle PL/SQL  
**Target Platform:** Google Cloud Platform (GCP) - BigQuery & Cloud Composer (Apache Airflow)

---

## 1. Summary

The `ausd_bp_ta_cntrct_dist` job has been migrated from a legacy on-premises environment to a modern, cloud-native architecture on Google Cloud Platform (GCP). 

* **Legacy Environment:** Orchestrated by UC4, controlled via KornShell wrapper scripts (`r_ausd_bp_ta_cntrct_dist.ksh` and `k_ausd_bp_ta_cntrct_dist.ksh`), and executed in an Oracle database using PL/SQL dynamic statements (`DWPA_UTIL_SKRIPT.runstatement`).
* **Target Environment:** Orchestrated via Apache Airflow (Cloud Composer) and executed natively within BigQuery. 

The migration consolidates the multi-layered shell scripting and database-specific PL/SQL logic into a single, unified Airflow DAG that executes a standardized BigQuery SQL script.

---

## 2. Generated Artifacts

The migration process generated the following key artifacts:

1. **`dags/dw_bert_ausd_bp_ta_cntrct_dist.py`**
   * **Role:** The primary Airflow DAG file. It defines the workflow, handles runtime configuration parameters (such as `stichtag`), and schedules the execution of the data transformation. It uses the `BigQueryExecuteQueryOperator` to run the consolidated SQL logic.
2. **`gcp_sql/d_ausd_bp_ta_cntrct_dist.sql`**
   * **Role:** The standalone BigQuery SQL script containing the complete data transformation logic. It handles parameter normalization, queries the audit metadata table (`dwtk_meldungen`), truncates the target table (`ta_cntrct_dist`), and inserts distinct contract IDs from the source table (`ta_bpr_basis`).

---

## 3. Key Design Decisions

* **Consolidation of Layers:** The legacy architecture split execution across UC4, a wrapper shell script, a controller shell script, and an Oracle SQL file. In the target architecture, these layers are consolidated into a single Airflow DAG and a single BigQuery SQL execution block. This reduces orchestration overhead and simplifies debugging.
* **Jinja Templating for Dynamic Parameters:** Airflow variables and runtime configurations (like `stichtag`) are dynamically interpolated into the SQL script using Jinja templates (`{{ var.value.get(...) }}`). This prevents parse-time evaluation issues in Airflow and ensures that the DAG remains dynamic.
* **Removal of Database Hints:** Oracle-specific parallel execution hints (e.g., `/*+ parallel(rp,4) */`) were retired. BigQuery natively parallelizes and scales query execution, making manual performance hints obsolete.
* **Native SQL Scripting over PL/SQL:** Oracle PL/SQL dynamic statements (`DWPA_UTIL_SKRIPT.runstatement`) were replaced with native BigQuery SQL scripting statements (`DECLARE`, `SET`, `TRUNCATE TABLE`, `INSERT INTO`). This maintains the procedural logic required for parameter evaluation while leveraging BigQuery's serverless execution engine.

---

## 4. Manual Steps Before Go-Live

Before enabling and running the migrated DAG in production, the following manual setup steps must be completed:

### 4.1. Schema and Dataset Creation
Ensure that the target BigQuery datasets and tables exist in your GCP project:
* **Datasets:**
  * Target Dataset: `sof` (or the name configured in Airflow variables)
  * Audit/Metadata Dataset: `isbert_schema`
* **Tables:**
  * Target Table: `sof.ta_cntrct_dist` must be created with the schema `(CNTRCT_ID INT64)` (or matching the source data type).
  * Source Table: `sof.ta_bpr_basis` must exist and be populated.
  * Audit Table: `isbert_schema.dwtk_meldungen` must exist and contain historical logs for `job_kennung = 'BERT_DROP_TEMP_TABLE'`.

### 4.2. IAM & Permissions
The Cloud Composer service account must be granted the following IAM roles on the target GCP project/datasets:
* `roles/bigquery.jobUser` (Project level)
* `roles/bigquery.dataEditor` (Dataset level for `sof` and `isbert_schema`)

### 4.3. Airflow Variables Configuration
The following Airflow variables must be configured in the Cloud Composer environment:

| Variable Name | Description | Example Value |
| :--- | :--- | :--- |
| `gcp_project_id` | The target GCP Project ID | `my-gcp-production-project` |
| `bq_sof_dataset` | Name of the target SOF dataset | `sof` |
| `bq_isbert_dataset` | Name of the audit/metadata dataset | `isbert_schema` |
| `bq_location` | BigQuery dataset location | `EU` |

### 4.4. Connection Strings
Ensure that the default Google Cloud connection (`google_cloud_default`) is properly configured in Airflow and has access to the target GCP project.

### 4.5. Scheduling
The DAG is currently configured with `schedule_interval=None`. If this job needs to run on a specific schedule or be triggered by an upstream DAG, update the `schedule_interval` or configure an Airflow Dataset/Triggerer.

---

## 5. Known Gaps & Unresolved References

* **Upstream Dependency Management:** The job relies on `sof.ta_bpr_basis` being fully loaded and `BERT_DROP_TEMP_TABLE` writing to `dwtk_meldungen` prior to execution. In the legacy system, this was handled by UC4 job chains. In Airflow, these dependencies should be explicitly linked using `ExternalTaskSensor` operators or by combining them into a master DAG.
* **Redesign (B4) Items - Date Logic:** The legacy script dynamically queries the audit table `dwtk_meldungen` to determine `v_datum`. While this logic has been successfully replicated in BigQuery SQL scripting, a cleaner, more "Airflow-native" approach would be to pass this metadata directly via Airflow task outputs (XComs) or execution context variables, rather than querying database tables inside the SQL script. This is flagged for future refactoring.

---

## 6. Validation

To validate the migration, execute the following steps:

### 6.1. Run the Test
1. Trigger the DAG manually in the Airflow UI.
2. Optionally, pass a custom configuration JSON to test specific dates:
   ```json
   {
     "stichtag": "31122023"
   }
   ```
3. Verify that the task `run_dist_provisioning` completes successfully.

### 6.2. Define "Passing"
The migration is considered successful if:
* The Airflow task runs without errors.
* The target table `sof.ta_cntrct_dist` is successfully truncated and repopulated.
* A validation query yields identical row counts and distinct contract IDs between the legacy Oracle database and BigQuery:
  ```sql
  -- Run in BigQuery to verify
  SELECT COUNT(1) FROM `your-project.sof.ta_cntrct_dist`;
  ```

---

## 7. Rollback Procedure

If a critical issue is discovered post-go-live, execute the following rollback steps:

1. **Pause the Airflow DAG:** Disable the `dw_bert_ausd_bp_ta_cntrct_dist` DAG in the Airflow UI to prevent further executions.
2. **Re-enable Legacy Scheduler:** Re-enable the `DW.BERT_AUSD_BP_TA_CNTRCT_DIST` job in the UC4 scheduler.
3. **Data Restoration (Optional):** If the target table in BigQuery was corrupted or contains invalid data, restore it to a previous state using BigQuery's time travel feature:
   ```sql
   CREATE OR REPLACE TABLE `your-project.sof.ta_cntrct_dist` AS
   SELECT * FROM `your-project.sof.ta_cntrct_dist`
   FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
   ```