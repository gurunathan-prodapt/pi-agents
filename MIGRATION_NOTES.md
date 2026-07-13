# Migration Notes: `k_exis_parallel.ksh` to BigQuery

## 1. Summary
The legacy utility script `k_exis_parallel.ksh` has been migrated from an on-premises Oracle/Unix environment to Google Cloud Platform (GCP). 

*   **Source Platform:** Unix/Linux Shell Scripting executing Oracle SQL*Plus parallel subprocesses (using background operators `&` and `wait`) and local file system concatenation.
*   **Target Platform:** Google Cloud Platform (GCP) utilizing **BigQuery (BQ) Stored Procedures** and **Cloud Storage (GCS)**.
*   **Migration Scope:** The shell-based parallel execution wrapper has been completely refactored into a native, procedural BigQuery Stored Procedure (`dwh_operations.k_exis_parallel`) that leverages BigQuery's distributed query engine and dynamic `EXPORT DATA` capabilities.

---

## 2. Generated Artifacts
The migration process generated the following files:

| Target File Path | Language / Format | Role |
| :--- | :--- | :--- |
| `procedures/k_exis_parallel.sql` | GoogleSQL (Procedural) | Contains the core orchestrator stored procedure `dwh_operations.k_exis_parallel` and the helper logging procedure `dwh_operations.write_ccr_log`. |
| `bq_config.json` | JSON | Configuration file containing metadata, labels, and execution parameters for deployment via `gcloud` or CI/CD pipelines. |

---

## 3. Key Design Decisions

### 3.1 Elimination of OS-Level Process Spawning
*   **Decision:** Replaced Unix background processes (`&` and `wait`) with a BigQuery procedural `WHILE` loop executing dynamic SQL (`EXECUTE IMMEDIATE`).
*   **Reasoning:** Spawning OS-level processes to run database queries is an anti-pattern in cloud-native architectures. BigQuery manages its own compute allocation (slots) and parallelizes query execution internally. 

### 3.2 Modulo Routing via Hash Fingerprints
*   **Decision:** Replaced manual row-based segmentation with a dynamic modulo routing strategy: `MOD(ABS(FARM_FINGERPRINT(CAST(routing_column AS STRING))), total_threads) = current_thread`.
*   **Reasoning:** This guarantees deterministic, non-overlapping, and evenly distributed data segments across the logical threads without requiring physical table splits.

### 3.3 Direct Cloud Storage Sinks
*   **Decision:** Replaced local disk assembly (`cat ${p_Ausgabe}.* > $p_Ausgabe`) with BigQuery's native `EXPORT DATA` statement writing directly to GCS URIs.
*   **Reasoning:** Writing to local disk and concatenating files is a major bottleneck and does not scale. GCS handles parallel writes natively, and downstream systems can consume the partitioned files directly or as a single wildcard URI.

### 3.4 Transactional Isolation-Safe Error Tracking
*   **Decision:** Used a `TEMP TABLE` (`temp_fehler_log`) combined with a `BEGIN...EXCEPTION...END` block to track thread failures.
*   **Reasoning:** This mimics the legacy behavior where one thread failing does not immediately crash the entire loop, allowing other threads to complete before reporting a global failure to the orchestrator.

---

## 4. Manual Steps Before Go-Live

### 4.1 Schema & Dataset Creation
Ensure the target dataset exists in your BigQuery project:
```sql
CREATE SCHEMA IF NOT EXISTS `dwh_operations`
OPTIONS(location="EU");
```

### 4.2 IAM & Permissions
The service account executing the stored procedure and the orchestrator (e.g., Airflow/Composer) must have the following IAM roles:
*   **BigQuery Data Editor** and **BigQuery Job User** on the target project/datasets.
*   **Storage Object Admin** (or **Storage Object Creator**) on the destination GCS bucket defined in `p_Ausgabe`.

### 4.3 Connection Strings & Secrets
*   No database connection strings or passwords need to be stored. Authentication must be handled securely via GCP IAM Service Accounts.
*   Update your orchestration tool (e.g., Airflow DAG) to reference the correct GCP Connection ID.

### 4.4 Scheduling & Orchestration
When migrating scheduling coordinates (e.g., from UC4/Automic to Cloud Composer/Airflow):
*   Replace the shell execution command with a `BigQueryInsertJobOperator` calling the stored procedure:
    ```sql
    CALL `dwh_operations.k_exis_parallel`(
      'E-12345', 'JOB_EXPORT_DAILY', 'my_project.my_dataset.source_table', 
      'account_id', 'gs://my-export-bucket/daily_exports/extract', 
      '20231027000000', '20231027235959', 1, 4
    );
    ```

---

## 5. Known Gaps & Unresolved References

### 5.1 Redesign (B4) Items: True Parallelism vs. Sequential Loop
*   **Current Gap:** The migrated stored procedure executes the `WHILE` loop sequentially. Although BigQuery parallelizes the *underlying query execution* of each step, the loop iterations themselves run one after another.
*   **Redesign Recommendation:** For ultra-high-throughput requirements, bypass the stored procedure loop entirely. Instead, trigger multiple asynchronous BigQuery export jobs in parallel directly from the orchestration layer (e.g., Airflow `TriggerDagRunOperator` or parallel tasks in a DAG).

### 5.2 Hardcoded Schema References
*   The logging table is currently hardcoded to `dwh_operations.ccr_logs`. If deploying to multi-tenant or environment-specific datasets (e.g., `dwh_operations_dev`), ensure these references are parameterized or updated during the CI/CD deployment phase.

---

## 6. Validation

### 6.1 How to Run the Tests
Execute the following test block in the BigQuery console to validate the procedure:

```sql
-- 1. Create a dummy source table
CREATE OR REPLACE TABLE `dwh_operations.test_source_table` AS
SELECT 
  1000 + x AS routing_id,
  TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL x HOUR) AS processing_timestamp,
  CONCAT('Data payload ', CAST(x AS STRING)) AS payload
FROM UNNEST(GENERATE_ARRAY(1, 100)) AS x;

-- 2. Execute the parallel export procedure (Dry Run / Debug Mode)
CALL `dwh_operations.k_exis_parallel`(
  'TEST-001',
  'TEST_EXPORT_JOB',
  'dwh_operations.test_source_table',
  'routing_id',
  'gs://<YOUR_TEST_BUCKET>/test_exports/output', -- Replace with a bucket you have access to
  FORMAT_TIMESTAMP('%Y%m%d%H%M%S', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)),
  FORMAT_TIMESTAMP('%Y%m%d%H%M%S', CURRENT_TIMESTAMP()),
  1, -- Enable Debug
  4  -- 4 Parallel logical threads
);

-- 3. Verify logs
SELECT * FROM `dwh_operations.ccr_logs` 
WHERE job_kennung = 'TEST_EXPORT_JOB' 
ORDER BY created_at ASC;
```

### 6.2 What "Passing" Means
1.  **No Compilation Errors:** The stored procedure compiles and executes without syntax errors.
2.  **GCS Output Verification:** Four distinct sets of CSV files (matching `_thread_0_*.csv` through `_thread_3_*.csv`) are successfully written to the target GCS bucket.
3.  **Log Verification:** The `dwh_operations.ccr_logs` table contains sequential `DEBUG` and `INFO` logs detailing the start, execution of each thread block, and successful completion.
4.  **Zero Failures:** The temporary tracking table registers `0` failures, and the procedure exits with a success status.

---

## 7. Rollback Procedure

In the event of a critical failure during deployment or go-live:

1.  **Revert Orchestration:** Point the scheduling coordinator (e.g., Airflow, UC4) back to the legacy on-premises execution path or the previous stable DAG version.
2.  **Drop/Revert Stored Procedures:** If necessary, remove the migrated assets from BigQuery to prevent accidental execution:
    ```sql
    DROP PROCEDURE IF EXISTS `dwh_operations.k_exis_parallel`;
    DROP PROCEDURE IF EXISTS `dwh_operations.write_ccr_log`;
    ```
3.  **Data Cleanup:** Clean up any partial or corrupted export files generated in the target GCS bucket:
    ```bash
    gcloud storage rm gs://your-export-bucket/exports/output_file_*
    ```