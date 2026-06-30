# Migration Notes: `ausd_bp_ta_bpr_opt_text`

This document provides the migration notes for the job `ausd_bp_ta_bpr_opt_text`, detailing its transition from a legacy Oracle, UC4, and KornShell environment to Google Cloud BigQuery and Apache Airflow (Cloud Composer).

---

## 1. Summary

The `ausd_bp_ta_bpr_opt_text` job has been migrated from a legacy on-premise data warehousing environment to a modern cloud-native architecture on Google Cloud Platform (GCP). 

* **Source Platform:** UC4/Automic Scheduler, KornShell (KSH) wrappers (`r_*.ksh`, `k_*.ksh`), and Oracle Database.
* **Target Platform:** Google Cloud Composer (Apache Airflow) and Google Cloud BigQuery.
* **Business Purpose:** Prepares and maps instantiated base products text data for BERT processing. It truncates the target table and populates it by performing an inner join between the base product options and their corresponding descriptions.

---

## 2. Generated Artifacts

The migration process has consolidated the legacy multi-file structure into two core target artifacts:

| Target File Path | Target Language | Role / Description |
| :--- | :--- | :--- |
| `gcp/bigquery/sql/d_ausd_bp_ta_bpr_opt_text.sql` | BigQuery SQL (DML) | Standalone SQL script containing the core transformation logic. It truncates the target table and inserts the joined option and description records. |
| `dags/bereitstellung_basisprodukte_bert_dag.py` | Python (Airflow DAG) | Orchestration DAG that replaces the UC4 job and KornShell wrappers. It handles parameter parsing, executes the main transformation task, and logs execution metrics. |

---

## 3. Key Design Decisions

### 3.1. Consolidation of Wrapper Scripts
In the legacy environment, execution required a UC4 job calling an outer wrapper (`r_ausd_bp_ta_bpr_opt_text.ksh`), which called a core controller (`k_ausd_bp_ta_bpr_opt_text.ksh`), which finally executed the SQL script via SQL*Plus. 
* **Decision:** All wrapper logic, parameter validation, and execution tracking have been consolidated into a single Apache Airflow DAG (`bereitstellung_basisprodukte_bert`). This reduces maintenance overhead and simplifies the call chain.

### 3.2. Identifier Sanitization
Oracle allows special characters like `$` in table names (e.g., `sof$ta_bpr_opt_text`), which are invalid or highly discouraged in BigQuery.
* **Decision:** All legacy table names have been sanitized to use underscores instead of dollar signs:
  * `sof$ta_bpr_opt_text` $\rightarrow$ `sof_ta_bpr_opt_text`
  * `sof$ta_bpr_optionen` $\rightarrow$ `sof_ta_bpr_optionen`
  * `sof$ta_bpr_beschr` $\rightarrow$ `sof_ta_bpr_beschr`

### 3.3. Idempotency and Restartability
The legacy Oracle script utilized a custom PL/SQL utility (`DWPA_UTIL_SKRIPT.runstatement`) to truncate tables dynamically to avoid DDL locking issues.
* **Decision:** Replaced with native BigQuery `TRUNCATE TABLE` statements. The Airflow DAG executes this truncation as part of the main task, ensuring that the pipeline is fully idempotent and safe to restart at any point.

### 3.4. Robust Jinja Templating
To prevent Airflow DAG-parsing errors when runtime configurations (`dag_run.conf`) are missing, the DAG uses robust Jinja expressions combined with SQL-level casting and coalescing.
* **Decision:** Parameters like `stichtag` and `wiederanlauf_wert` are handled directly inside the SQL template using `COALESCE`, `NULLIF`, and `CAST`. This ensures the DAG can parse successfully and run with sensible defaults (e.g., `CURRENT_DATE()`) if triggered without manual parameters.

---

## 4. Manual Steps Before Go-Live

Before deploying and running the migrated job in production, the following setup steps must be completed:

### 4.1. Schema and Dataset Creation
Ensure the target BigQuery dataset and tables exist in your GCP project.
1. Create the dataset (if not already present):
   ```bash
   bq mk --location=EU isbert_schema
   ```
2. Ensure the following tables are created with schemas matching their legacy Oracle counterparts (with sanitized names):
   * `isbert_schema.sof_ta_bpr_opt_text`
   * `isbert_schema.sof_ta_bpr_optionen`
   * `isbert_schema.sof_ta_bpr_beschr`
   * `isbert_schema.PoolBasisprodukt`
   * `isbert_schema.dwtk_meldungen`

### 4.2. IAM & Permissions
The Cloud Composer service account (the worker identity) must have the following permissions:
* **BigQuery Data Editor** (`roles/bigquery.dataEditor`) on the `isbert_schema` dataset.
* **BigQuery Job User** (`roles/bigquery.jobUser`) on the project level to execute queries.

### 4.3. Connection Strings
Verify that the Airflow connection ID referenced in the DAG (`google_cloud_default`) is configured in your Airflow environment and has access to the target GCP project (`prj-dwh-prod-1234`).

### 4.4. Scheduling & Triggering
The DAG is currently configured with `schedule=None` (manual/triggered only), matching the legacy on-demand execution pattern. If this job needs to be scheduled, update the `schedule` parameter in the DAG definition and ensure upstream DAGs are configured to trigger it with the required JSON payload.

---

## 5. Known Gaps & Unresolved References

### 5.1. Upstream Data Dependencies
This job does not load the source tables `sof_ta_bpr_optionen` or `sof_ta_bpr_beschr`. These must be populated by their respective upstream ingestion pipelines prior to running this job.

### 5.2. The `dwtk_meldungen` Dependency
The standalone SQL script (`d_ausd_bp_ta_bpr_opt_text.sql`) queries `isbert_schema.dwtk_meldungen` to determine a reference date (`v_datum`) based on the `BERT_DROP_TEMP_TABLE` job status. 
* **Gap:** If `dwtk_meldungen` is not yet fully migrated or populated in the target environment, this query will default to `'19000101'`. Ensure the logging table migration is aligned.

### 5.3. Redesign (B4) Parameter Passing
The legacy job relied on UC4 to pass operational parameters (`stichtag`, `wiederanlauf_wert`). In the migrated DAG, these are read from `dag_run.conf`. If the DAG is triggered automatically without a configuration payload, it will fall back to default values:
* `stichtag` $\rightarrow$ Current Date
* `wiederanlauf_wert` $\rightarrow$ `0`
* `job_kennung` $\rightarrow$ `'ausd_bp_ta_bpr_opt_text'`

---

## 6. Validation

To validate the migration, execute the following testing steps:

### 6.1. DAG Parse Test
Verify that the Airflow DAG is syntactically correct and can be loaded by the scheduler:
```bash
python dags/bereitstellung_basisprodukte_bert_dag.py
```
* **Passing Criteria:** The command exits with code `0` without any import or syntax errors.

### 6.2. BigQuery Dry Run
Dry-run the SQL script in the BigQuery console to validate syntax and schema mapping:
* **Passing Criteria:** The BigQuery validator returns a green checkmark indicating the query is valid and estimates 0 bytes read (since it is a dry run).

### 6.3. End-to-End Integration Test
1. Populate the source tables `sof_ta_bpr_optionen` and `sof_ta_bpr_beschr` with mock data.
2. Trigger the DAG manually in Airflow with the following configuration JSON:
   ```json
   {
     "stichtag": "31122023",
     "job_kennung": "VAL_RUN_01",
     "eintrags_nr": "1001",
     "wiederanlauf_wert": "0"
   }
   ```
3. **Passing Criteria:**
   * The task `process_ausd_bp_ta_bpr_opt_text` completes successfully.
   * The table `sof_ta_bpr_opt_text` contains the expected joined records.
   * The task `log_execution_details` completes successfully, and a new row is written to `PoolBasisprodukt` with `job_kennung = 'VAL_RUN_01'`, `status = 'A'`, and the correct record count.

---

## 7. Rollback Procedure

In the event of a production failure or data corruption, execute the following rollback steps:

1. **Pause the DAG:**
   Go to the Airflow UI and toggle the switch to pause the `bereitstellung_basisprodukte_bert` DAG to prevent further executions.
   
2. **Revert Target Data:**
   If the target table `sof_ta_bpr_opt_text` needs to be restored to its pre-run state, use BigQuery's time travel feature to restore the table to a point-in-time before the failure:
   ```sql
   CREATE OR REPLACE TABLE `isbert_schema.sof_ta_bpr_opt_text`
   AS SELECT * FROM `isbert_schema.sof_ta_bpr_opt_text`
   FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
   ```
   *(Adjust the interval as necessary to target the pre-execution timestamp).*

3. **Clean Up Execution Logs:**
   Remove the invalid execution log entry from the tracking table:
   ```sql
   DELETE FROM `isbert_schema.PoolBasisprodukt`
   WHERE job_kennung = 'VAL_RUN_01'; -- Replace with the actual failed job_kennung
   ```