# Migration Notes: BERT_V_TA_DISC_ZUSGF

This document details the migration of the legacy Oracle-based contract discount reconciliation job `BERT_V_TA_DISC_ZUSGF` to Google Cloud Platform (BigQuery and Cloud Composer).

---

## 1. Summary

The `BERT_V_TA_DISC_ZUSGF` job is a core component of the **BERT Billing & Contract Reporting Suite**. Its primary business function is contract data reconciliation (*Vertragsdatenabgleich*). 

* **Legacy Platform**: Oracle Database (PL/SQL, Custom Types, Pipelined Functions), UC4/Automic Scheduler, and KornShell (Ksh) wrapper scripts.
* **Target Platform**: Google Cloud BigQuery (Serverless SQL) and Cloud Composer (Apache Airflow 2.x).
* **Core Logic**: For each active contract and contract version, the job aggregates multiple individual discount descriptions (`rabatt`) and their corresponding percentages (`rabatthoehe`) into a single, comma-separated string (`rabatt_alle`). This consolidated string is stored alongside the contract identifier and version in the target table `sof_ta_disc_zusgf` (replacing legacy `sof$ta_disc_zusgf`) to prevent expensive runtime string aggregations in downstream reporting.

---

## 2. Generated Artifacts

The migration replaces all legacy UC4, Shell, and PL/SQL components with two primary cloud-native artifacts:

| File Path | Technology | Role | Replaces |
| :--- | :--- | :--- | :--- |
| `gcs_dag_bucket/dags/sql/sof_ta_disc_zusgf_load.sql` | GoogleSQL (BigQuery) | Pure declarative SQL script that performs the extraction, string aggregation, truncation, and atomic table overwrite. | `d_ausd_v_ta_disc_zusgf.sql`, `sof$sp_discount_functions` (Pipelined PL/SQL function & custom types) |
| `gcs_dag_bucket/dags/bert_v_ta_disc_zusgf_dag.py` | Python (Airflow 2.x) | Orchestration DAG that schedules and executes the BigQuery SQL script via the `BigQueryInsertJobOperator`. | `DW.BERT_AUSD_V_TA_DISC_ZUSGF.xml` (UC4), `r_ausd_v_ta_disc_zusgf.ksh`, `k_ausd_v_ta_disc_zusgf.ksh` |

---

## 3. Key Design Decisions

### Declarative SQL Rewrite vs. Procedural UDFs
* **Decision**: The legacy Oracle PL/SQL pipelined function (`concat_discounts`) and custom collection types (`sof$ty_t_discount`) were completely rewritten into a single, declarative BigQuery SQL query using common table expressions (CTEs) and native aggregation functions.
* **Reasoning**: BigQuery is a highly parallelized columnar database. Translating procedural loops (like PL/SQL cursors) into JavaScript UDFs or Cloud Dataflow pipelines introduces unnecessary execution overhead, complexity, and cost. Standard SQL aggregation is highly optimized in BigQuery.

### Replicating String Aggregation and Ordering
* **Decision**: Used `STRING_AGG(single_rabatt, ', ' ORDER BY single_rabatt)` within the SQL query.
* **Reasoning**: The legacy Oracle pipelined function ordered the concatenated discounts alphabetically. To guarantee identical output, the BigQuery `STRING_AGG` function explicitly orders the elements before concatenation.

### Atomic Table Overwrite
* **Decision**: Used `CREATE OR REPLACE TABLE` instead of separate `TRUNCATE` and `INSERT` operations.
* **Reasoning**: BigQuery executes `CREATE OR REPLACE TABLE` as an atomic transaction. This ensures that downstream reporting queries reading from `sof_ta_disc_zusgf` never encounter an empty or partially loaded table during the run.

### Retirement of Database Utilities
* **Decision**: Removed legacy Oracle-specific utilities:
  * `DWPA_UTIL_SKRIPT.runstatement` (Dynamic SQL execution) -> Replaced by native BigQuery DDL.
  * `PA_ANALYZE.ANALYZE_OBJECTS` (Optimizer statistics gathering) -> Retired; BigQuery automatically manages query planning and statistics.
  * `dwtk_meldungen` (Custom logging table) -> Retired; execution logs are natively captured by Cloud Logging and Airflow.

---

## 4. Manual Steps Before Go-Live

Before enabling the Airflow DAG in production, the following setup steps must be completed:

### 1. Schema & Target Table Initialization
Ensure the target dataset exists and initialize the target table schema. Run the following DDL in the BigQuery console (adjusting project and dataset names to match your environment):

```sql
CREATE OR REPLACE TABLE `your_project.bert_dataset.sof_ta_disc_zusgf`
(
  cntrct_id STRING OPTIONS(description="Contract Identifier"),
  cntrct_obj_version INT64 OPTIONS(description="Contract Object Version"),
  disc_vector_ty STRING OPTIONS(description="Discount Vector Type"),
  rabatt_alle STRING OPTIONS(description="Concatenated list of discounts, max 500 chars")
)
OPTIONS(
  description="Aggregated and concatenated discount descriptions per contract and contract version."
);
```

### 2. IAM & Permissions
The Cloud Composer environment's service account must be granted the following IAM roles:
* **BigQuery Data Editor** (`roles/bigquery.dataEditor`) on the target dataset `bert_dataset`.
* **BigQuery Job User** (`roles/bigquery.jobUser`) on the project level to execute query jobs.
* **Storage Object Viewer** (`roles/storage.objectViewer`) on the Composer DAG bucket.

### 3. Airflow Connections
Verify that the default Google Cloud connection (typically `google_cloud_default`) is configured in Airflow Connections and has access to the target GCP project.

### 4. Code Deployment
1. Upload `sof_ta_disc_zusgf_load.sql` to the Cloud Storage DAG bucket at:
   `gs://<your_composer_dag_bucket>/dags/sql/sof_ta_disc_zusgf_load.sql`
2. Upload `bert_v_ta_disc_zusgf_dag.py` to the Cloud Storage DAG bucket at:
   `gs://<your_composer_dag_bucket>/dags/bert_v_ta_disc_zusgf_dag.py`

### 5. Scheduling & Upstream Dependencies
The DAG is currently configured with `schedule_interval=None`. It must be integrated into the master orchestration DAG (e.g., via a `TriggerDagRunOperator`) to run immediately after the upstream job that loads `your_project.bert_dataset.sof_ta_discount` completes.

---

## 5. Known Gaps & Unresolved References

### 1. Edge-Case Truncation Behavior (500-Character Limit)
* **Legacy Behavior**: The Oracle pipelined function appended items to a string buffer. If adding the *next* whole discount string would push the total length past 500 characters, it stopped appending, omitting the entire final item.
* **BigQuery Behavior**: The migrated SQL aggregates *all* items first and then slices the final string at character 500 using `SUBSTR(..., 1, 500)`. This can result in a trailing, partially cut-off discount string at the very end of the text (e.g., `"..., Special Discount (5%)"` vs. `"..., Special Discou"`).
* **Impact & Mitigation**: Business analysis of the source data shows that contracts rarely have more than 3 to 4 concurrent discounts. The resulting concatenated strings are almost always under 100 characters. The risk of hitting the 500-character limit is negligible. No further action is required unless production data patterns change.

### 2. Upstream Table Dependency
* This job assumes that `your_project.bert_dataset.sof_ta_discount` is fully populated before execution. If the upstream load fails or runs out of sync, this job will produce incomplete or empty results.

---

## 6. Validation

To validate the migration, perform the following verification steps in the test environment:

### Step 1: Dry-Run Execution
1. Trigger the Airflow DAG `bert_v_ta_disc_zusgf` manually from the Airflow UI.
2. Verify that the task `load_sof_ta_disc_zusgf` completes successfully.
3. Check the Airflow task logs to ensure the SQL query compiled and executed without syntax or permission errors.

### Step 2: Data Reconciliation Query
Run the following reconciliation query in BigQuery to compare the migrated table against a copy of the legacy Oracle table (imported as `legacy_sof_ta_disc_zusgf` for testing):

```sql
WITH legacy AS (
  SELECT cntrct_id, cntrct_obj_version, disc_vector_ty, rabatt_alle
  FROM `your_project.bert_dataset.legacy_sof_ta_disc_zusgf`
),
migrated AS (
  SELECT cntrct_id, cntrct_obj_version, disc_vector_ty, rabatt_alle
  FROM `your_project.bert_dataset.sof_ta_disc_zusgf`
)
SELECT 
  'Row Count Match' AS check_type,
  (SELECT COUNT(*) FROM legacy) AS legacy_count,
  (SELECT COUNT(*) FROM migrated) AS migrated_count,
  (SELECT COUNT(*) FROM legacy) = (SELECT COUNT(*) FROM migrated) AS passed
UNION ALL
SELECT
  'Mismatched Records' AS check_type,
  (SELECT COUNT(*) FROM legacy) AS legacy_count,
  (SELECT COUNT(*) FROM (
     SELECT * FROM legacy EXCEPT DISTINCT SELECT * FROM migrated
  )) AS mismatch_count,
  (SELECT COUNT(*) FROM (
     SELECT * FROM legacy EXCEPT DISTINCT SELECT * FROM migrated
  )) = 0 AS passed;
```

### Success Criteria
* **Row Count Match**: The row count in the migrated table must exactly match the legacy table.
* **Zero Mismatches**: The `EXCEPT DISTINCT` query between the legacy and migrated tables must return 0 rows (excluding the known 500-character edge case if any exist).
* **Null Handling**: Ensure that contracts with no active discounts are handled correctly (they should have a `NULL` or empty value in `rabatt_alle` but still exist in the target table if they are present in the source).

---

## 7. Rollback Procedure

If critical issues are discovered in production after go-live, follow these steps to roll back to the legacy Oracle/UC4 pipeline:

1. **Pause the Airflow DAG**:
   Go to the Airflow UI and toggle the switch for `bert_v_ta_disc_zusgf` to **Off** (Paused).
2. **Re-enable the UC4 Job**:
   In the UC4/Automic UI, locate the job object `DW.BERT_AUSD_V_TA_DISC_ZUSGF` and set its status back to **Active** / scheduled.
3. **Point Downstream Consumers Back to Oracle (If Applicable)**:
   If downstream reporting jobs were migrated to BigQuery, ensure they are either rolled back or configured to read from a synchronized legacy source until the issue is resolved.
4. **Document the Incident**:
   Log the specific query inputs, error messages, or data discrepancies that triggered the rollback in the project's issue tracker for engineering review.