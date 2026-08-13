# Migration Notes: DW.BERT_AUSD_V_TA_PERIOD

These migration notes document the transition of the UC4 job `DW.BERT_AUSD_V_TA_PERIOD` and its associated Unix shell scripts to Google Cloud Composer (Apache Airflow) and BigQuery.

---

## 1. Summary

The legacy UC4 job `DW.BERT_AUSD_V_TA_PERIOD` has been migrated from an on-premises Unix/Oracle environment to **Google Cloud Composer (Apache Airflow)** and **BigQuery**. 

This job is responsible for mirroring "Carmen" period definitions. It extracts dimensional time-measurement period configurations from source tables, filters them based on a dynamic execution date boundary, and loads them into a target interface table (`sof$ta_period`).

---

## 2. Generated Artifacts

The migration process generated four key artifacts, each replacing a specific component of the legacy architecture:

| Artifact Path | Type | Role | Replaces Legacy Component |
| :--- | :--- | :--- | :--- |
| `sql_bqsql_linked_job/DWH_BERT_JOB/dw_bert_ausd_v_ta_period.py` | Python (Airflow DAG) | Orchestrates the execution flow, defines environment variables, and schedules/triggers the execution task. | `DW.BERT_AUSD_V_TA_PERIOD.xml` (UC4 Job Definition) |
| `sql_bqsql_linked_job/isbert/aufbereitung/bin/r_ausd_v_ta_period.py` | Python Script | Outer wrapper script that initializes logging, sets up signal/error traps, and synchronously invokes the core processor. | `r_ausd_v_ta_period.ksh` (KSH Outer Wrapper) |
| `sql_bqsql_linked_job/isbert/aufbereitung/bin/k_ausd_v_ta_period.py` | Python Script | Core control script that parses command-line arguments, validates parameters, and executes the BigQuery SQL script synchronously. | `k_ausd_v_ta_period.ksh` (KSH Core Control) |
| `sql_bqsql_linked_job/isbert/aufbereitung/sql/d_ausd_v_ta_period.sql` | BigQuery SQL | Performs the database-level operations: queries metadata, truncates the target table, and inserts the filtered period definitions. | `d_ausd_v_ta_period.sql` (Oracle SQL*Plus Script) |

---

## 3. Key Design Decisions

### Synchronous Execution Strategy (Unified Control Flow)
* **Decision**: The SQL logic was migrated as a plain BigQuery SQL script (`.sql`) rather than a Dataform (`.sqlx`) model, and is executed synchronously via the `google-cloud-bigquery` client library inside `k_ausd_v_ta_period.py`.
* **Reasoning**: In the legacy system, the wrapper scripts relied on synchronous execution to capture database-level errors and log metrics (such as processed row counts) before writing the final success message (`"Die Abarbeitung wurde ohne erkennbare Fehler beendet"`). An asynchronous Dataform trigger would create a race condition, potentially logging success in Airflow while the underlying query was still running or failing.
* **Trade-off**: This approach bypasses Dataform's built-in dependency graph management for this specific query, but guarantees strict operational parity, accurate task-level error propagation, and synchronous logging.

### Database Link Replacement
* **Decision**: The legacy Oracle DB Link (`@pcrs1.de.tinternal.com`) was replaced with direct references to a local BigQuery dataset named `carmen_replicated`.
* **Reasoning**: BigQuery does not support legacy Oracle database links. Replicating the "Carmen" source tables into a dedicated BigQuery dataset is the standard pattern for high-performance, secure, and cost-effective analytical queries.

### Local Disk-Based Temp File Elimination
* **Decision**: The legacy mechanism of writing record counts to a physical temporary file (`bert_k_ausd_v_ta_period_$$.tmp`) was replaced with in-memory metadata extraction.
* **Reasoning**: The BigQuery Python Client natively returns execution metrics (such as `num_dml_affected_rows` or `total_rows`). Capturing these metrics in-memory simplifies the code and avoids writing temporary files to the ephemeral local disks of Cloud Composer GKE worker nodes.

### Exclusion of Obsolete Includes
* **Decision**: Legacy UC4 includes (`DW.HOLE_PFAD`, `DW.BERT_LESE_LOG`) and profile initializations (`.dw_init`) were excluded from the migration as "NO SOURCE NEEDED".
* **Reasoning**: These scripts are tightly coupled to the legacy on-premises infrastructure and UC4 agent configurations. Their logging, path resolution, and parameter validation functionalities are handled natively by Python standard libraries (`logging`, `os`, `argparse`) and Airflow environment variables.

---

## 4. Manual Steps Before Go-Live

Before deploying and enabling the migrated DAG in production, the following manual setup steps must be completed:

### 1. BigQuery Dataset & Table Creation
Ensure the following datasets and tables exist in the target Google Cloud Project:
* **Datasets**:
  * `isbert_schema` (Target dataset)
  * `carmen_replicated` (Source dataset containing replicated Carmen tables)
* **Tables**:
  * `isbert_schema.sof$ta_period` (Target table)
  * `isbert_schema.dwtk_meldungen` (Metadata logging table)
  * `carmen_replicated.cds$ta_period` (Replicated source table)
  * `carmen_replicated.cds$ta_time_meas_cv` (Replicated source table)
  * `carmen_replicated.cds$ta_description` (Replicated source table)

### 2. IAM & Permissions
The Cloud Composer Service Account (e.g., `service-XXXX@gcp-sa-composer.iam.gserviceaccount.com`) must be granted the following roles:
* `roles/bigquery.jobUser` on the project level.
* `roles/bigquery.dataEditor` (or custom equivalent) on the `isbert_schema` dataset.
* `roles/bigquery.dataViewer` on the `carmen_replicated` dataset.

### 3. Airflow Variables Configuration
Configure the following Airflow Variables in the Composer UI (`Admin -> Variables`):
* `GCP_PROJECT`: The ID of your target Google Cloud Project.
* `GCS_BUCKET`: The Cloud Storage bucket associated with your Composer environment.
* `DWH_JOB_KENNUNG`: Set to `AUSD_V_TA_PERIOD` (default).

### 4. File Deployment
* Copy the Python scripts (`r_ausd_v_ta_period.py` and `k_ausd_v_ta_period.py`) and the SQL script (`d_ausd_v_ta_period.sql`) to the designated scripts directory on the Composer environment's GCS bucket (mapped to `/opt/airflow/scripts/` on the workers).
* Copy the DAG file (`dw_bert_ausd_v_ta_period.py`) to the `dags/` folder of the Composer GCS bucket.

### 5. Establish Replication Pipelines
Ensure that the data replication pipeline from the source Carmen database (`@pcrs1.de.tinternal.com`) to the `carmen_replicated` BigQuery dataset is active, validated, and scheduled to run prior to this job.

---

## 5. Known Gaps & Unresolved References

* **Upstream Metadata Dependency**: The SQL script queries `isbert_schema.dwtk_meldungen` for a record where `job_kennung = 'BERT_DROP_TEMP_TABLE'`. This implies a strict dependency on the upstream job responsible for dropping temporary tables. If that job has not run or failed to write to `dwtk_meldungen`, this script will default to using `'19000101'` as its execution date boundary.
* **Mocked Framework Functions**: The legacy `DWMSG_*` logging and status tracking functions have been replaced with Python mock representations that write to standard output and local log files. If a centralized GCP logging or metadata tracking framework is established in the future, these mocks should be refactored to integrate with that system.

---

## 6. Validation

To validate the migration, execute a test run of the DAG and verify the outputs.

### How to Run the Test
1. Navigate to the Airflow UI.
2. Locate the DAG `dw_bert_ausd_v_ta_period`.
3. Unpause the DAG and click **Trigger DAG**.

### What "Passing" Means
The migration is considered successful and "passing" when:
1. **DAG Status**: The DAG run completes with a status of `SUCCESS`.
2. **Task Logs**: The task logs for `dw_bert_ausd_v_ta_period_task` show:
   * No Python exceptions or BigQuery syntax errors.
   * The output line: `---------- ENDE Datenverarbeitung ----------`.
   * The output line: `Records processed: [X]` (where `X` is the number of rows affected/inserted).
   * The final output line: `Die Abarbeitung wurde ohne erkennbare Fehler beendet`.
3. **Data Integrity**: 
   * The table `isbert_schema.sof$ta_period` is successfully truncated.
   * The table is repopulated with active period definitions matching the date boundary retrieved from `dwtk_meldungen`.
   * A manual query comparison between the legacy Oracle target table and the BigQuery target table yields identical row counts and column values.

---

## 7. Rollback Procedure

In the event of a critical failure or data corruption during or after go-live, execute the following rollback steps:

1. **Pause the DAG**: Immediately pause the `dw_bert_ausd_v_ta_period` DAG in the Airflow UI to prevent further executions.
2. **Restore Target Table**: If the target table `isbert_schema.sof$ta_period` was corrupted or loaded with incorrect data, restore it to its pre-execution state using BigQuery's Time Travel feature or a previously taken table snapshot:
   ```sql
   -- Example: Restore table to its state 1 hour ago
   CREATE OR REPLACE TABLE `isbert_schema.sof$ta_period`
   AS SELECT * FROM `isbert_schema.sof$ta_period`
   FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
   ```
3. **Re-enable Legacy Job**: If a complete fallback to the on-premises system is required, re-enable the legacy UC4 job `DW.BERT_AUSD_V_TA_PERIOD` on the legacy scheduler. Ensure that any database replication pipelines are redirected back to the legacy source systems if necessary.