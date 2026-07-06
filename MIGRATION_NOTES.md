# MIGRATION_NOTES.md — DW.BERT_AUSD_BP_TA_P_BASISPROD

This document serves as the comprehensive migration guide for transitioning the legacy Automic (UC4) orchestration job `DW.BERT_AUSD_BP_TA_P_BASISPROD` and its associated Oracle PL/SQL and KornShell wrappers to Google Cloud Platform (GCP).

---

## 1. Summary

The legacy job `DW.BERT_AUSD_BP_TA_P_BASISPROD` orchestrates, prepares, and loads basic product data (**Basisprodukt**) into the `sof$ta_p_basisprod` table. This table is a critical upstream dependency for downstream scoring and reporting systems, specifically **BERT**.

### Migration Scope
*   **Source Platform:** Automic UC4 Scheduler, KornShell wrappers (`r_ausd_bp_ta_p_basisprod.ksh`, `k_ausd_bp_ta_p_basisprod.ksh`), and Oracle PL/SQL (`d_ausd_bp_ta_p_basisprod.sql`).
*   **Target Platform:** Google Cloud Platform (GCP).
*   **Target Execution Engine:** BigQuery Standard SQL (for data transformations) and Google Cloud Composer / Apache Airflow 2.x (for end-to-end orchestration).

---

## 2. Generated Artifacts

The migration process has generated two core artifacts to replace the legacy components:

### 1. `sql/d_ausd_bp_ta_p_basisprod.sql`
*   **Role:** BigQuery Standard SQL transformation script.
*   **Description:** Replaces the legacy Oracle SQL script (`d_ausd_bp_ta_p_basisprod.sql`). It maps all legacy Oracle outer joins `(+)` to explicit ANSI `LEFT JOIN` syntax, converts Oracle-specific functions (such as `DECODE` and `NVL`) to BigQuery equivalents (`IF`, `COALESCE`), handles the dynamic concatenation logic for APN fields, and supports recovery filtering via query parameters.

### 2. `dags/dw_bert_ausd_bp_ta_p_basisprod.py`
*   **Role:** Apache Airflow DAG (Python).
*   **Description:** Replaces the Automic UC4 XML definition and the KornShell wrappers (`r_*.ksh`, `k_*.ksh`). It orchestrates the execution sequence: fetching metadata from `dwtk_meldungen`, executing the target table truncation, and running the main BigQuery transformation job with dynamic parameter injection.

---

## 3. Key Design Decisions

### 3.1 ELT Pattern (Extract-Load-Transform)
*   **Decision:** Shift from database-linked PL/SQL processing to an in-place BigQuery ELT pattern.
*   **Reasoning:** BigQuery is highly optimized for massive parallel scans and joins. By keeping all upstream tables (`sof_ta_cntrct_dist`, `sof_ta_iccid_vertrag`, etc.) within BigQuery, we eliminate network egress costs and leverage BigQuery's serverless compute scaling.

### 3.2 Explicit ANSI Joins over Legacy Syntax
*   **Decision:** Convert all implicit comma-separated joins and Oracle `(+)` operators into explicit `LEFT JOIN` and `INNER JOIN` statements.
*   **Reasoning:** BigQuery does not support legacy Oracle outer join syntax. Explicit ANSI joins improve query readability, prevent accidental Cartesian products, and allow the BigQuery query planner to optimize join paths effectively.

### 3.3 Airflow-Managed Truncate-and-Insert
*   **Decision:** Implement the `TRUNCATE` and `INSERT INTO` operations as sequential tasks within an Airflow DAG using the `BigQueryInsertJobOperator`.
*   **Reasoning:** This maintains the exact transactional behavior of the legacy script while providing clear task boundaries in Airflow. If the truncation fails, the load task is prevented from running, avoiding data corruption.

### 3.4 Parameterized Recovery (`Wiederanlaufwert`)
*   **Decision:** Map the legacy shell recovery variable `p_wiederanlaufWert` to an Airflow Variable (`wiederanlaufWert`) with a default fallback of `0`.
*   **Reasoning:** This preserves the operational capability to resume processing from a specific contract ID threshold without modifying the underlying SQL code.

---

## 4. Manual Steps Before Go-Live

Before deploying the DAG and running the job in production, the following setup steps must be completed:

### 4.1 Schema and Dataset Creation
Ensure that the target BigQuery datasets and tables exist in your GCP project.
1.  **Datasets:**
    *   `sof_core` (or your configured core dataset name)
    *   `isbert_schema_dwtk` (or your configured logging/metadata dataset name)
2.  **Target Table:**
    *   Verify that the target table `sof_ta_p_basisprod` is created with the correct schema.
    *   **Crucial:** Ensure that `sof_ta_p_basisprod` and the upstream `sof_ta_iccid_vertrag` tables contain all Multi-SIM columns up to suffix 10 (`ms3_*` to `ms10_*`).

### 4.2 IAM & Permissions
The Cloud Composer Service Account (e.g., `service-[project-number]@gcp-sa-composer.iam.gserviceaccount.com` or a custom user-managed service account) must be granted the following roles:
*   **BigQuery Data Editor** (`roles/bigquery.dataEditor`) on the target and source datasets.
*   **BigQuery Job User** (`roles/bigquery.jobUser`) at the project level to run queries.

### 4.3 Airflow Variables & Connections
Configure the following parameters in the Airflow Web UI (**Admin -> Variables**):

| Variable Key | Example Value | Description |
| :--- | :--- | :--- |
| `PROJECT_ID` | `gcp-dwh-prod` | The target GCP Project ID where BigQuery resources reside. |
| `DATASET_NAME` | `sof_core` | The dataset containing the `sof_ta_*` tables. |
| `MELDUNGEN_DATASET` | `isbert_schema_dwtk` | The dataset containing the `dwtk_meldungen` table. |
| `wiederanlaufWert` | `0` | Recovery threshold contract ID. Set to `0` for standard runs. |

Configure the GCP Connection in Airflow (**Admin -> Connections**):
*   **Connection ID:** `google_cloud_default`
*   **Connection Type:** `Google Cloud`
*   *Note: If running inside Cloud Composer, leaving the keyfile/project fields blank will default to the environment's ambient service account.*

### 4.4 Scheduling
*   The DAG is currently configured with `schedule=None` (manual/triggered execution).
*   Update the `schedule` parameter in `dw_bert_ausd_bp_ta_p_basisprod.py` to match your business requirements (e.g., `'0 2 * * *'` for daily execution at 2:00 AM) once upstream dependencies are migrated.

---

## 5. Known Gaps & Unresolved References

### 5.1 Upstream Dependency Coordination (B4 Redesign Item)
*   **Gap:** The legacy job relies on checking `dwtk_meldungen` for the state of `BERT_DROP_TEMP_TABLE` to determine the execution date context.
*   **Redesign Recommendation:** Instead of querying a metadata table inside the SQL execution, use Airflow's native dataset-based scheduling (`Dataset`) or an upstream `TriggerDagRunOperator`. This transitions the architecture from a legacy "polling" model to a modern "event-driven" model.

### 5.2 Hardcoded Project References in Standalone SQL
*   **Gap:** The standalone SQL file (`sql/d_ausd_bp_ta_p_basisprod.sql`) contains the hardcoded project prefix `gcp-dwh-prod.sof_core`.
*   **Mitigation:** For local or non-production testing of the raw SQL file, manually replace `gcp-dwh-prod.sof_core` with your target test project and dataset. The Airflow DAG version handles this dynamically via Jinja templating.

---

## 6. Validation

To validate the migration, execute the following testing steps:

### 6.1 Dry-Run Validation
Run a dry-run of the BigQuery insert job to validate syntax and schema compatibility without writing data or incurring query costs:
```bash
bq query \
  --use_legacy_sql=false \
  --dry_run \
  < sql/d_ausd_bp_ta_p_basisprod.sql
```
*   **Passing Criteria:** The command returns a success message indicating how many bytes the query will process (e.g., `This query will process XX GB when run.`).

### 6.2 Airflow DAG Task Testing
Test individual tasks within the Airflow DAG using the Airflow CLI:
```bash
# Test the metadata retrieval task
airflow tasks test dw_bert_ausd_bp_ta_p_basisprod get_v_datum 2023-10-01

# Test the truncation task
airflow tasks test dw_bert_ausd_bp_ta_p_basisprod truncate_target 2023-10-01
```
*   **Passing Criteria:** The tasks complete with a `SUCCESS` status in the terminal logs.

### 6.3 Data Reconciliation (Row Count & Hash Validation)
1.  Run the legacy Oracle job and record the row count of `sof$ta_p_basisprod`.
2.  Run the migrated Airflow DAG in your GCP test environment.
3.  Compare the row counts:
    ```sql
    SELECT COUNT(1) FROM `your-project.sof_core.sof_ta_p_basisprod`;
    ```
4.  Perform a checksum/hash comparison on key columns (e.g., `CNTRCT_ID`, `TNV_ICCID`, `APN`) between the Oracle target and BigQuery target to ensure 100% data parity.

---

## 7. Rollback Procedure

In the event of an issue during deployment or go-live, follow these steps to revert to the legacy system:

### Step 1: Pause the Airflow DAG
Disable the migrated DAG in the Cloud Composer UI or via the gcloud CLI:
```bash
gcloud composer environments run [COMPOSER_ENV_NAME] \
    --location [LOCATION] \
    dags pause -- dw_bert_ausd_bp_ta_p_basisprod
```

### Step 2: Reactivate the Legacy Automic (UC4) Job
1.  Log in to the Automic UC4 interface.
2.  Locate the job `DW.BERT_AUSD_BP_TA_P_BASISPROD`.
3.  Set the status of the job back to **Active** and restore its original schedule.

### Step 3: Verify Legacy Execution
Monitor the next scheduled run of the legacy job in UC4 and verify that the Oracle table `sof$ta_p_basisprod` is populated correctly.