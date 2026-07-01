# Migration Notes: `ausd_bp_ta_bpr_evn`

This document details the migration of the legacy job `ausd_bp_ta_bpr_evn` from its on-premises Oracle and UC4/Automic environment to Google Cloud Platform (GCP) using Apache Airflow (Cloud Composer) and BigQuery.

---

## 1. Summary

The legacy job `ausd_bp_ta_bpr_evn` was originally orchestrated via UC4/Automic (`DW.BERT_AUSD_BP_TA_BPR_EVN`), triggering KornShell wrapper scripts (`r_ausd_bp_ta_bpr_evn.ksh` and `k_ausd_bp_ta_bpr_evn.ksh`) that executed an Oracle SQL*Plus script (`d_ausd_bp_ta_bpr_evn.sql`). 

The primary business purpose of this job is to filter and prepare instantiated basic products—specifically **Einzelverbindungsnachweis (EVN)**—from the core product instance table and load them into a dedicated target table for the BERT subsystem.

### Migration Mapping Summary
* **Source Platform:** Oracle Database, UC4/Automic Scheduler, KornShell (AIX/Linux)
* **Target Platform:** Google Cloud Platform (GCP)
* **Orchestration:** Apache Airflow (Cloud Composer 2.x)
* **Data Warehouse:** Google BigQuery (Standard SQL)

---

## 2. Generated Artifacts

The migration process has consolidated the multi-layered legacy scripts into two primary, clean artifacts structured for deployment in a Cloud Composer environment:

| Target File Path | Target Language | Role / Description |
| :--- | :--- | :--- |
| `dags/dw_bert_ausd_bp_ta_bpr_evn.py` | Python (Airflow 2.x) | **Orchestration DAG:** Defines the workflow, handles environment variables, and triggers the BigQuery execution task. |
| `dags/sql/d_ausd_bp_ta_bpr_evn.sql` | BigQuery SQL | **Data Transformation Script:** Contains the parameterized SQL logic to truncate the target table and reload filtered EVN product instances. |

---

## 3. Key Design Decisions

### 3.1. Consolidation of Shell Wrapper Layers
In the legacy environment, execution required a wrapper script (`r_...`) to initialize parameters and a control script (`k_...`) to manage the environment and execute SQL*Plus. 
* **Decision:** These layers have been entirely bypassed. Parameter parsing (such as `stichtag` and `wiederanlaufwert`) is now handled natively within the Airflow DAG using Jinja templating, and execution is handled directly by the `BigQueryExecuteQueryOperator`. This significantly reduces operational complexity and points of failure.

### 3.2. Retirement of Commented-Out File Processing
The legacy control script (`k_ausd_bp_ta_bpr_evn.ksh`) contained a large block of commented-out code that performed local file manipulation (`sed`, `sort`, `join`) on `.dat` and `.csv` files (`cibasis_data24.dat`, `cibasis_data96.dat`, `cibasis_fax.dat`) to build a combined `cibasisprodukt.csv`.
* **Decision:** This logic is **retired** and not active in the core migration path. If downstream requirements ever mandate the reactivation of this logic, it must be implemented as a high-performance SQL join inside BigQuery using equivalent tables, completely avoiding local file system operations.

### 3.3. Identifier Normalization (`$` to `_`)
Oracle schemas and tables contained the `$` character (e.g., `sof$ta_bpr_instance`, `sof$ta_bpr_evn`).
* **Decision:** To maximize compatibility with standard BigQuery client drivers, BI tools, and data cataloging systems, all dollar signs have been normalized to underscores (e.g., `sof_ta_bpr_instance`, `sof_ta_bpr_evn`).

### 3.4. Removal of Oracle-Specific Performance Hints
The legacy SQL script contained the optimizer hint `/*+ full(bp) parallel(bp,4) */`.
* **Decision:** This hint has been removed. BigQuery is a serverless, columnar execution engine that automatically handles parallel scans and query optimization.

---

## 4. Manual Steps Before Go-Live

Before deploying and running the migrated DAG in production, the following setup steps must be completed:

### 4.1. Schema and Table Creation
Ensure that the target dataset and tables exist in BigQuery. If they are not managed by an external DDL deployment pipeline, create them manually:

```sql
-- Create Dataset (if not exists)
CREATE SCHEMA IF NOT EXISTS `your_project_id.isbert_schema`
OPTIONS(location="EU");

-- Create Target Table (if not exists)
CREATE TABLE IF NOT EXISTS `your_project_id.isbert_schema.sof_ta_bpr_evn` (
  cntrct_id INT64,
  bpr_id INT64
);
```

### 4.2. IAM & Permissions
The service account running the Cloud Composer workers must have the following IAM roles on the target BigQuery dataset:
* `roles/bigquery.dataEditor` (to truncate and insert into `sof_ta_bpr_evn`)
* `roles/bigquery.jobUser` (to run BigQuery jobs in the project)

### 4.3. Airflow Connections & Variables
1. **GCP Connection:** Ensure the Airflow connection `google_cloud_default` is configured with the correct permissions and project ID.
2. **Airflow Variables:** Define the following Airflow variables (either via the Airflow UI under *Admin -> Variables* or via CLI):
   * Create a JSON variable named `gcp_project` containing your GCP Project ID.
   * Create a JSON variable named `gcp_dataset` with the value `isbert_schema`.

   *Example Airflow Variable JSON configuration:*
   ```json
   {
     "gcp_project": "prod-bert-data-warehouse",
     "gcp_dataset": "isbert_schema"
   }
   ```

### 4.4. Scheduling
The DAG is currently configured with `schedule_interval=None` (manual/triggered execution) to match its legacy on-demand/event-driven nature. If this job needs to run on a time-based schedule, update the `schedule_interval` parameter in `dags/dw_bert_ausd_bp_ta_bpr_evn.py`.

---

## 5. Known Gaps & Unresolved References

* **Upstream Table Dependency:** This job reads directly from `sof_ta_bpr_instance`. The migration of the pipeline that populates `sof_ta_bpr_instance` must be completed and validated before this job can run successfully in production.
* **Legacy Metadata Table (`dwtk_meldungen`):** The legacy lineage mentions `isbert_schema.dwtk_meldungen` for drop triggers/metadata. This table is not referenced in the core SQL logic of `d_ausd_bp_ta_bpr_evn.sql`. If downstream systems rely on execution metadata updates in `dwtk_meldungen`, a post-execution task must be added to the Airflow DAG to append run status records to that table.

---

## 6. Validation

To validate the migration, perform the following test steps:

### 6.1. Dry-Run Validation
Run a dry-run of the SQL query in the BigQuery Console to verify syntax and schema alignment:
```sql
-- Replace placeholders with actual project/dataset values
DECLARE p_stichtag STRING DEFAULT '20241027';
DECLARE p_wiederanlaufwert INT64 DEFAULT 0;

SELECT
    bp.cntrct_id,
    bp.bpr_id
FROM `your_project_id.isbert_schema.sof_ta_bpr_instance` AS bp
WHERE bp.bpr_id IN (32, 2506, 2839, 2840, 3055, 3056, 3821)
LIMIT 10;
```

### 6.2. DAG Execution Test
1. Trigger the DAG manually in the Airflow UI.
2. Pass a custom configuration JSON if testing for a specific date:
   ```json
   {
     "stichtag": "20241027",
     "wiederanlaufwert": "0"
   }
   ```
3. Verify that the task `process_evn_basis_products` completes with a `success` status.

### 6.3. Data Verification ("Passing" Criteria)
The validation is successful if:
* The target table `sof_ta_bpr_evn` is successfully truncated.
* The target table is populated with records where `bpr_id` matches only the specified EVN IDs: `32, 2506, 2839, 2840, 3055, 3056, 3821`.
* The row count and data values in BigQuery's `sof_ta_bpr_evn` match the row count and data values from the legacy Oracle database for the same execution date (`stichtag`).

---

## 7. Rollback Procedure

In the event of a critical failure during deployment or go-live, execute the following rollback steps:

1. **Pause the Airflow DAG:**
   Go to the Airflow UI and toggle the switch for `dw_bert_ausd_bp_ta_bpr_evn` to **Off** (Paused) to prevent any scheduled or accidental triggers.
   
2. **Restore Target Table State (if necessary):**
   If the BigQuery target table was corrupted or truncated prematurely, restore it using BigQuery's time-travel feature:
   ```sql
   -- Restore table to its state 1 hour ago
   CREATE OR REPLACE TABLE `your_project_id.isbert_schema.sof_ta_bpr_evn` AS
   SELECT * FROM `your_project_id.isbert_schema.sof_ta_bpr_evn`
   FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
   ```

3. **Re-enable Legacy Orchestration:**
   Re-enable the UC4/Automic job `DW.BERT_AUSD_BP_TA_BPR_EVN` to resume processing on the legacy Oracle infrastructure. Ensure that any upstream data dependencies in Oracle are intact.