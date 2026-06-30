# Migration Notes: `ausd_bp_ta_apn_vertrag`

This document provides the migration notes, design decisions, manual setup steps, validation procedures, and rollback strategies for the migrated batch job `ausd_bp_ta_apn_vertrag`.

---

## 1. Summary

The batch job `ausd_bp_ta_apn_vertrag` has been migrated from a legacy Oracle-based architecture to **Google Cloud Platform (GCP)**. 

* **Source Platform**: Oracle Database (PL/SQL), KornShell (KSH) wrappers, and UC4/Automic Scheduler.
* **Target Platform**: Google Cloud BigQuery and Apache Airflow (Cloud Composer).
* **Core Function**: Aggregates active basic products (Access Point Names (APN) and contract reference values) from the source table `sof_ta_bpr_apn` and formats them into comma-separated strings per contract (`cntrct_id`) in the target table `sof_ta_apn_vertrag`.

---

## 2. Generated Artifacts

The migration process generated the following key files:

| Target Relative Path | Language / Format | Role |
| :--- | :--- | :--- |
| `dags/dw_bert_ausd_bp_ta_apn_vertrag.py` | Python (Airflow DAG) | Orchestrates the job execution. Replaces the UC4 scheduling definition and the KSH wrappers (`r_ausd_bp_ta_apn_vertrag.ksh`, `k_ausd_bp_ta_apn_vertrag.ksh`). |
| `gcp/bigquery/sql/d_ausd_bp_ta_apn_vertrag.sql` | BigQuery Standard SQL | Performs the core data transformation. Replaces the legacy Oracle PL/SQL cursor-loop logic with high-performance, set-based BigQuery operations. |

---

## 3. Key Design Decisions

### Set-Based Aggregation vs. Iterative Cursor Loops
* **Legacy Approach**: The legacy PL/SQL script used an explicit cursor loop to iterate through records, manually concatenating strings and checking lengths.
* **Migrated Approach**: Replaced by BigQuery's native `STRING_AGG` function combined with an analytic `ROW_NUMBER() OVER (...)` window function to guarantee deterministic ordering. This leverages BigQuery's massive parallel processing (MPP) engine, reducing execution time from minutes to seconds.

### Truncation Logic Trade-off
* **Legacy Behavior**: The PL/SQL loop checked the length of the accumulated string *before* appending a new element. If appending an element would push the total length past 100 characters, that specific element was skipped, but subsequent *shorter* elements could still be appended if they fit.
* **Migrated Behavior**: The BigQuery SQL aggregates *all* elements in their correct order and then truncates the final concatenated string to 100 characters using `SUBSTR(..., 1, 100)`.
* **Decision & Trade-off**: The set-based `SUBSTR(STRING_AGG(...))` approach was chosen for performance and maintainability. Emulating the legacy "skipping" behavior in BigQuery would require a complex, slow recursive Common Table Expression (CTE) or a custom JavaScript UDF, which degrades performance and increases complexity.

### Consolidation of Shell Scripts
* **Decision**: The parameter validation, environment initialization, and logging logic spread across `r_ausd_bp_ta_apn_vertrag.ksh` and `k_ausd_bp_ta_apn_vertrag.ksh` have been consolidated directly into the Airflow DAG. Historical, commented-out file-processing logic (e.g., references to `cibasis_data24.dat`) was retired and excluded from the migration.

---

## 4. Manual Steps Before Go-Live

The following setup steps must be executed in the target environment before activating the DAG.

### A. Schema & Table Creation
Ensure the target dataset exists and create the target table `sof_ta_apn_vertrag` if it has not been created by your DDL deployment pipeline:

```sql
CREATE TABLE IF NOT EXISTS `your_project_id.isbert_schema.sof_ta_apn_vertrag` (
  cntrct_id STRING OPTIONS(description="Primary contract identifier"),
  access_point_names STRING OPTIONS(description="Comma-separated list of APNs, truncated to 100 chars"),
  cntrct_id_refs STRING OPTIONS(description="Comma-separated list of contract references, truncated to 100 chars")
)
CLUSTER BY cntrct_id;
```

### B. IAM & Permissions
The Cloud Composer environment's service account must be granted the following IAM roles on the target BigQuery dataset:
* **BigQuery Data Editor** (`roles/bigquery.dataEditor`) on the target dataset `isbert_schema`.
* **BigQuery Job User** (`roles/bigquery.jobUser`) on the project level.

### C. Airflow Variables Configuration
Configure the following Airflow Variables in the Composer UI (**Admin -> Variables**):

| Key | Example Value | Description |
| :--- | :--- | :--- |
| `gcp_project_id` | `gcp-dwh-prod` | The target GCP Project ID. |
| `bq_dataset` | `isbert_schema` | The BigQuery dataset containing the tables. |
| `bq_location` | `EU` | The geographic location of the BigQuery dataset. |
| `gcp_conn_id` | `google_cloud_default` | The Airflow connection ID used to connect to GCP. |

### D. Scheduling & Trigger Setup
* The DAG is configured with `schedule_interval=None` (on-demand/ad-hoc), matching its legacy UC4 configuration where it was triggered as part of a larger parent job stream.
* If this job needs to run on a daily schedule, update the `schedule_interval` in `dags/dw_bert_ausd_bp_ta_apn_vertrag.py` to your desired cron expression (e.g., `0 2 * * *` for daily at 2:00 AM).

---

## 5. Known Gaps & Unresolved References

### 1. Truncation Discrepancy (B4 Redesign Item)
* **Description**: As noted in Section 3, the legacy PL/SQL logic could skip a long string and append a subsequent shorter string. The migrated BigQuery SQL truncates the final string at exactly 100 characters.
* **Impact**: Minor differences in the final string values for contracts with a high volume of APNs/references exceeding 100 characters.
* **Action Required**: Business stakeholders must verify if the simplified truncation logic is acceptable. If exact legacy emulation is mandatory, a JavaScript UDF must be implemented to replace the `STRING_AGG` logic.

### 2. Control Table (`dwtk_meldungen`) Integration
* **Description**: The legacy script queried `isbert_schema.dwtk_meldungen` to check for job status and drop temporary tables.
* **Impact**: This logic is currently bypassed in the Airflow DAG because Airflow natively manages task states, retries, and cleanups.
* **Action Required**: Confirm if downstream systems rely on `dwtk_meldungen` updates from this specific job. If yes, an additional `BigQueryExecuteQueryOperator` task must be added to the DAG to write status logs to `dwtk_meldungen`.

---

## 6. Validation

To validate the migration, execute the following steps in your test environment.

### Step 1: Dry-Run the SQL
Run the SQL query in the BigQuery Console using the dry-run feature to verify syntax and permissions:
```sql
-- Replace variables with actual values for testing
DECLARE gcp_project_id STRING DEFAULT 'gcp-dwh-qa';
DECLARE bq_dataset STRING DEFAULT 'isbert_schema';
```

### Step 2: Execute the Airflow DAG
1. Upload `dags/dw_bert_ausd_bp_ta_apn_vertrag.py` to your Composer DAGs bucket.
2. Ensure `gcp/bigquery/sql/d_ausd_bp_ta_apn_vertrag.sql` is uploaded to the corresponding path in the DAGs bucket.
3. Trigger the DAG manually from the Airflow UI.
4. Verify that the task `process_apn_vertrag` completes successfully.

### Step 3: Data Reconciliation (Passing Criteria)
Run the following reconciliation query to compare the migrated BigQuery table against the legacy Oracle table (or a migrated copy of the legacy target table):

```sql
WITH bq_data AS (
  SELECT cntrct_id, access_point_names, cntrct_id_refs 
  FROM `your_project_id.isbert_schema.sof_ta_apn_vertrag`
),
legacy_data AS (
  -- Assuming legacy data is loaded into a comparison table
  SELECT cntrct_id, apn AS access_point_names, cntrct_id_ref AS cntrct_id_refs 
  FROM `your_project_id.isbert_schema.legacy_sof_ta_apn_vertrag`
)
SELECT 
  COALESCE(b.cntrct_id, l.cntrct_id) AS cntrct_id,
  b.access_point_names AS bq_apn,
  l.access_point_names AS legacy_apn,
  b.cntrct_id_refs AS bq_refs,
  l.cntrct_id_refs AS legacy_refs,
  (b.access_point_names = l.access_point_names AND b.cntrct_id_refs = l.cntrct_id_refs) AS is_exact_match
FROM bq_data b
FULL OUTER JOIN legacy_data l ON b.cntrct_id = l.cntrct_id
WHERE b.access_point_names != l.access_point_names 
   OR b.cntrct_id_refs != l.cntrct_id_refs;
```

* **Passing Criteria**: 
  1. Total row count matches between source and target.
  2. `is_exact_match` is `TRUE` for all rows, *except* for known edge cases where the legacy truncation skipping logic diverged from the standard 100-character truncation.

---

## 7. Rollback Procedure

If issues are discovered post-go-live, follow these steps to roll back to the legacy Oracle environment:

1. **Pause the Airflow DAG**:
   Go to the Airflow UI and toggle the switch for `dw_bert_ausd_bp_ta_apn_vertrag` to **Off**.
2. **Re-enable Legacy Scheduling**:
   Re-activate the UC4/Automic job definition `DW.BERT_AUSD_BP_TA_APN_VERTRAG`.
3. **Redirect Downstream Consumers**:
   If downstream scoring engines (FOS) were pointed to BigQuery, revert their connection strings/configurations to point back to the Oracle table `sof$ta_apn_vertrag`.
4. **Verify Legacy Execution**:
   Manually trigger the UC4 job and verify that the Oracle table `sof$ta_apn_vertrag` is successfully populated and that logs are written to `isbert_schema.dwtk_meldungen`.