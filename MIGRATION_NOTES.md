# Migration Notes: DW.BERT_AUSD_BP_TA_RN_VERTRAG

This document provides the comprehensive migration notes for transitioning the legacy job `DW.BERT_AUSD_BP_TA_RN_VERTRAG` to the modern Google Cloud Platform (GCP) data stack.

---

## 1. Summary

### 1.1 Scope of Migration
The legacy job `DW.BERT_AUSD_BP_TA_RN_VERTRAG` has been migrated from an on-premises Oracle Data Warehouse environment orchestrated by UC4 (Automic) and driven by KornShell (`.ksh`) scripts to a modern, cloud-native architecture on Google Cloud Platform (GCP).

### 1.2 Target Platform
* **Orchestration**: Google Cloud Composer (Apache Airflow)
* **Transformation Engine**: Google Cloud Dataform
* **Storage & Compute**: Google Cloud BigQuery

### 1.3 Business & Technical Objective
This pipeline aggregates and pivots granular, single-row per phone number records (`sof_ta_rn_einzeln`) into a consolidated, single-row per contract schema (`sof_ta_rn_vertrag`). This wide-table format is a critical upstream dependency for downstream contract credit scoring algorithms (BERT / Forderungsscoring).

---

## 2. Generated Artifacts

The migration process has generated the following target artifacts to replace the legacy components:

| Artifact Path | Target Technology | Role / Description | Replaces Legacy Component |
| :--- | :--- | :--- | :--- |
| `definitions/sof_ta_rn_vertrag.sqlx` | Dataform (SQLX) | Declarative SQL model that defines the target table schema, partitioning strategy, and the pivot aggregation logic. | `d_ausd_bp_ta_rn_vertrag.sql`, `k_ausd_bp_ta_rn_vertrag.ksh` |
| `dags/composer_bert_ausd_bp_ta_rn_vertrag.py` | Cloud Composer (Python DAG) | Orchestrates the pipeline execution, handles runtime parameters (Stichtag/Restart ID), and triggers the Dataform execution. | `DW.BERT_AUSD_BP_TA_RN_VERTRAG.xml`, `r_ausd_bp_ta_rn_vertrag.ksh`, `.dw_init` |

---

## 3. Key Design Decisions

### 3.1 Declarative Table Recreation vs. Imperative Truncate-and-Insert
* **Decision**: Replaced the legacy Oracle PL/SQL pattern of `TRUNCATE TABLE` followed by `INSERT INTO` with Dataform's native `type: "table"` configuration.
* **Reasoning**: Dataform automatically manages the lifecycle of the table. During execution, it builds the table atomically (using a write-disposition of `WRITE_TRUNCATE` under the hood), ensuring that downstream readers never see an empty table during the load window. This eliminates the risk of partial loads or read failures during execution.

### 3.2 Partitioning Strategy
* **Decision**: Implemented integer range partitioning on `cntrct_id` using `RANGE_BUCKET(cntrct_id, GENERATE_ARRAY(0, 100000000, 100000))`.
* **Reasoning**: Contracts are queried heavily by ID ranges in downstream scoring models. Partitioning by `cntrct_id` ranges minimizes slot consumption and query costs for downstream lookups, avoiding full-table scans.

### 3.3 Elimination of Dynamic Temp Tables
* **Decision**: Removed legacy logic that dynamically generated physical table names suffixed with execution dates (e.g., `sof$ta_rn_vertrag_20260421`).
* **Reasoning**: BigQuery's native partitioning and clustering capabilities render dynamic table-name generation obsolete. Maintaining a single, statically named table with appropriate partitioning simplifies downstream consumption and lineage tracking.

---

## 4. Manual Steps Before Go-Live

Before enabling the automated schedule in production, the following manual setup steps must be completed:

### 4.1 Schema & Dataset Creation
Ensure the target BigQuery dataset exists in the designated region:
```sql
CREATE SCHEMA IF NOT EXISTS `gcp-bigquery-dwh-prod.isbert_schema`
OPTIONS(
  location="europe-west3"
);
```

### 4.2 IAM & Permissions
Ensure the Cloud Composer environment's service account has the following permissions:
* **BigQuery Data Editor** (`roles/bigquery.dataEditor`) on the `isbert_schema` dataset.
* **BigQuery Job User** (`roles/bigquery.jobUser`) on the project level.
* **Dataform Editor** (`roles/dataform.editor`) on the target Dataform repository.

### 4.3 Connection Strings & Secrets
1. Create or verify the Dataform repository connection to your Git provider.
2. Ensure that the Dataform service account has access to read from the source table `isbert_schema.sof_ta_rn_einzeln`.

### 4.4 Scheduling & Airflow Variables
1. Upload `composer_bert_ausd_bp_ta_rn_vertrag.py` to the Cloud Composer DAGs folder (`gs://<composer-bucket>/dags/`).
2. In the Airflow UI, verify that the DAG is parsed successfully. Keep the DAG **paused** until validation is complete.

---

## 5. Known Gaps & Unresolved References

### 5.1 Legacy Metadata Logging (`dwtk_meldungen`)
* **Gap**: The legacy script logged execution metadata and retrieved the maximum `timecreated` variable from `isbert_schema.dwtk_meldungen`.
* **Resolution Strategy**: The current Dataform model directly references the source table `sof_ta_rn_einzeln` without checking `dwtk_meldungen`. If downstream processes strictly require execution tracking in `dwtk_meldungen`, a pre-running Dataform assertion or an intermediate logging model must be implemented to write execution states back to a BigQuery metadata table.

### 5.2 Range Partition Boundaries
* **Gap**: The `RANGE_BUCKET` array generator assumes contract IDs fall between `0` and `100,000,000`.
* **Action Item**: Verify the maximum value of `cntrct_id` in the production database. If contract IDs exceed 100 million, adjust the `GENERATE_ARRAY` parameters in `sof_ta_rn_vertrag.sqlx` accordingly to prevent records from falling into the overflow partition.

---

## 6. Validation

To validate the migration, execute the following test suite:

### 6.1 Compilation Test
Validate that the Dataform code compiles without syntax or dependency errors:
```bash
# Run via Dataform CLI or GCP Console
dataform compile
```

### 6.2 Dry-Run Validation
Perform a dry run of the SQL query to verify schema compatibility and estimate bytes processed:
```sql
-- Run in BigQuery Console to validate schema mapping
SELECT * FROM `gcp-bigquery-dwh-prod.isbert_schema.sof_ta_rn_einzeln` LIMIT 10;
```

### 6.3 Row Count & Parity Verification
After running the Dataform pipeline in a development environment, execute the following query to verify that no contracts were lost during aggregation:
```sql
-- Target row count must equal distinct contract count in source
SELECT 
  (SELECT COUNT(DISTINCT cntrct_id) FROM `gcp-bigquery-dwh-prod.isbert_schema.sof_ta_rn_einzeln`) AS source_contracts,
  (SELECT COUNT(1) FROM `gcp-bigquery-dwh-prod.isbert_schema.sof_ta_rn_vertrag`) AS target_contracts;
```

---

## 7. Rollback Procedure

If issues are detected post-deployment, execute the following steps to revert to the legacy system:

1. **Pause the Composer DAG**:
   Go to the Airflow UI and toggle the switch for `dw_bert_ausd_bp_ta_rn_vertrag` to **Off**.
2. **Re-enable Legacy Orchestration**:
   Resume the UC4 job definition `DW.BERT_AUSD_BP_TA_RN_VERTRAG` in the legacy scheduler.
3. **Verify Legacy Source Sync**:
   Ensure that any downstream consumers are redirected back to the legacy Oracle database instance if they were pointed to BigQuery during the cutover window.