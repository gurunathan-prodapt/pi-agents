# Migration Notes: Shared Files — TMD_processing/ALL_TYPES/mp

This document details the migration of the Ab Initio graph `all_types_graph.mp` to a PySpark pipeline running on Dataproc Serverless.

---

## 1. Summary

The Ab Initio graph `all_types_graph.mp` has been migrated to a modular PySpark script (`all_types_graph.py`). 

* **Source Platform**: Ab Initio GDE / Oracle Database / Local Filesystem
* **Target Platform**: Google Cloud Platform (GCP) — Dataproc Serverless (PySpark), BigQuery, and Google Cloud Storage (GCS)
* **Functional Overview**: 
  The job processes transactional measurement data (Cancellations, Products, Quotes, and Contracts) from a primary flat file (`x_tos_measures.dat`). It applies team visibility rules by querying a dynamically generated lookup table (`Lkp_teamvirt_ccos`) sourced from Oracle (now BigQuery). The data is split into two distinct lifecycles:
  1. **Standard View**: Contains all processed records mapped with resolved virtual team IDs.
  2. **Weekly Rolled-Over View**: Filtered to dates prior to the current business week, retaining original team IDs.
  
  The output targets are serialized as distinct flat files structured by measure type for both regular and weekly intervals.

---

## 2. Generated Artifacts

| File Path | Role | Description |
| :--- | :--- | :--- |
| `TMD_processing/ALL_TYPES/mp/all_types_graph.py` | PySpark Executable | The migrated pipeline script containing the SparkSession initialization, BigQuery source reads, lookup generation, stream splitting, transformation logic, and GCS file serialization. |

---

## 3. Key Design Decisions

### 3.1. In-Memory Caching Strategy
* **Decision**: Cache the lookup DataFrame `df_teamvirt_ccos` and the primary input DataFrame `df_x_tos_measures`.
* **Reasoning**: Both DataFrames are referenced multiple times across parallel execution branches (Cancellations, Products, and Quotes/Contracts). Caching prevents Spark from re-reading the source files and re-evaluating the lookup query multiple times, significantly improving execution performance.

### 3.2. Broadcast Left Join for Lookup Resolution
* **Decision**: Replaced the Ab Initio `lookup()` function with a standard Spark `LEFT JOIN`.
* **Reasoning**: Since the team visibility lookup dataset (`lkp_team_virt_ccos`) is relatively small (representing team-to-department visibility mappings), a broadcast left join is highly efficient and avoids expensive shuffles across the cluster.

### 3.3. Weekly Date Logic Translation
* **Decision**: Translated the Ab Initio expression:
  `stichtag < datetime_add(now(), ((datetime_day_of_week(now()) - 2) * -1))`
  to the Spark SQL equivalent:
  `F.to_date(F.col("stichtag"), "yyyyMMdd") < F.date_trunc("week", F.current_date())`
* **Reasoning**: The Ab Initio expression calculates the date of the most recent Monday. In Spark, `date_trunc("week", current_date())` natively returns the Monday of the current week, providing a clean, robust, and timezone-safe equivalent.

### 3.4. Single-File Output Serialization (`.coalesce(1)`)
* **Decision**: Applied `.coalesce(1)` prior to writing the output CSV files to GCS.
* **Reasoning**: Downstream legacy systems expect single flat files (e.g., `tos_cancellations.dat`). While `.coalesce(1)` forces a single partition write (which can limit parallel write performance), the data volumes for these transactional measures are small enough that the impact is negligible, and it guarantees compatibility with legacy file-based consumers.

---

## 4. Manual Steps Before Go-Live

To ensure a successful deployment, the following setup steps must be completed in the target GCP environment:

### 4.1. BigQuery Dataset & Table Creation
Ensure that the target BigQuery dataset (`BQ_DATASET`) exists and that the following source tables (replicated from Oracle) are populated:
* `ccr_ta_f_teamsichtbarkeit`
* `ccr_ta_s_sdm_team`
* `ccr_ta_s_sdm_abteilung`

### 4.2. IAM & Permissions
The service account executing the Dataproc Serverless batch job must have the following roles:
* **BigQuery Data Viewer** (`roles/bigquery.dataViewer`) on the source dataset.
* **Storage Object Admin** (`roles/storage.objectAdmin`) on the target GCS bucket (`GCS_BUCKET`).
* **Dataproc Worker** (`roles/dataproc.worker`) to execute serverless workloads.

### 4.3. Environment Variables
The following environment variables must be configured in the execution environment (e.g., Airflow environment or Dataproc batch properties):
* `GCP_PROJECT`: Target Google Cloud Project ID.
* `BQ_DATASET`: Target BigQuery dataset name.
* `GCS_BUCKET`: Target Cloud Storage bucket name.
* `CCR_AI_DAT_FILE_DIR`: GCS path for input/output data files (defaults to `gs://{GCS_BUCKET}/CCR_AI_DAT_FILE_DIR`).
* `TCN_DS_SERIAL_LOOKUP`: GCS path for lookup files (defaults to `gs://{GCS_BUCKET}/TCN_DS_SERIAL_LOOKUP`).

### 4.4. Scheduling & Orchestration
1. Create a Cloud Composer (Airflow) DAG to orchestrate the job.
2. Use the `DataprocServerlessStartBatchOperator` to submit the PySpark job.
3. Example operator configuration:
   ```python
   job_args = {
       "project_id": os.environ.get("GCP_PROJECT"),
       "region": os.environ.get("DATAPROC_REGION"),
       "batch_id": "all-types-graph-processing",
       "batch": {
           "pyspark_batch": {
               "main_python_file_uri": f"gs://{GCS_BUCKET}/code/all_types_graph.py",
               "environment_variables": {
                   "GCP_PROJECT": GCP_PROJECT,
                   "BQ_DATASET": BQ_DATASET,
                   "GCS_BUCKET": GCS_BUCKET,
               }
           }
       }
   }
   ```

---

## 5. Known Gaps & Unresolved References

* **Oracle Replication Dependency**: The pipeline assumes that the team visibility tables are continuously replicated from Oracle to BigQuery. If the replication pipeline experiences latency, the lookup mapping may use stale data.
* **Downstream File Consumption**: If downstream jobs (such as `DW.DWH_ALL_TYPES_MASTER`) have not yet been migrated to GCP, they must be configured to read the output files directly from GCS (e.g., via GCS FUSE or utility scripts) instead of the legacy local filesystem.
* **Date Format Strictness**: The weekly filter assumes that the `stichtag` field in `x_tos_measures.dat` is consistently formatted as `yyyyMMdd`. Any malformed date strings will result in `null` values during parsing and will be excluded from the weekly output.

---

## 6. Validation

To validate the migrated PySpark job against the legacy Ab Initio execution, perform the following steps:

### 6.1. Execution Test
Run the PySpark script in a test environment using a representative sample of `x_tos_measures.dat`:
```bash
gcloud dataproc batches submit pyspark \
    gs://$GCS_BUCKET/code/all_types_graph.py \
    --project=$GCP_PROJECT \
    --region=$DATAPROC_REGION \
    --properties=spark.executor.instances=2
```

### 6.2. Verification Metrics ("Passing" Criteria)
1. **Row Count Verification**: Compare the row counts of the generated output files against the legacy Ab Initio run:
   * `tos_cancellations.dat` vs. PySpark output
   * `tos_cancellations_wk.dat` vs. PySpark output
   * `tos_products.dat` vs. PySpark output
   * `tos_products_wk.dat` vs. PySpark output
   * `tos_quotes_contracts.dat` vs. PySpark output
   * `tos_quotes_contracts_wk.dat` vs. PySpark output
2. **Data Integrity Checks**:
   * Verify that `sdm_team_id` is correctly mapped to the resolved virtual team ID in standard outputs, and defaults to `""` (empty string) when no lookup match is found.
   * Verify that `sdm_team_id` remains unchanged (retains original value) in all `_wk.dat` outputs.
   * Verify that the `subventionen` field in `tos_quotes_contracts.dat` has decimal points (`.`) replaced with commas (`,`).
   * Verify that `tcn_offer_product_id` is correctly concatenated as `tos_offer_id ~ tcn_product_id` with whitespace trimmed.

---

## 7. Rollback Procedure

If critical errors or data discrepancies are detected post-go-live, execute the following rollback steps:

1. **Disable the Airflow DAG**: Pause the DAG orchestrating the `all_types_graph.py` batch job.
2. **Revert Downstream Routing**: If downstream consumers were modified to read from GCS, point them back to the legacy local filesystem directories (`$CCR_AI_DAT_FILE_DIR`).
3. **Re-enable Legacy Job**: Re-enable the legacy Ab Initio wrapper script (`all_types_graph.ksh`) in the legacy scheduler (e.g., Control-M or Cron).
4. **Investigate Logs**: Analyze the Dataproc driver logs in Cloud Logging to diagnose the failure.