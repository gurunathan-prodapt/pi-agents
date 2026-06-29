# Migration Notes: `ausd_bp_ta_bpr_apn`

This document outlines the migration details, design decisions, manual setup steps, validation procedures, and rollback strategies for the migrated batch job `ausd_bp_ta_bpr_apn`.

---

## 1. Summary

The legacy batch job `ausd_bp_ta_bpr_apn` (part of the BERT system) has been migrated from an on-premise Oracle and UC4/Automic environment to **Google Cloud Platform (GCP)**. 

* **Legacy Platform**: UC4/Automic Scheduler, KornShell (`.ksh`) wrappers, and Oracle SQL*Plus.
* **Target Platform**: Google Cloud Composer (Apache Airflow) and Google BigQuery.
* **Business Purpose**: Prepares and provisions basic product instances (e.g., VPN, IV-VPN, WAP-Intranet, Telemetry, and Blackberry solutions) mapped to their contract access point names (APNs). This curated dataset is consumed by downstream scoring systems, specifically the **Forderungsscoring (FOS) / Debt Collection Scoring Engine**, to evaluate active enterprise contracts.

---

## 2. Generated Artifacts

The migration process has produced the following core artifacts, structured for deployment in the target GCP environment:

| Artifact Path | Type | Role / Functional Description |
| :--- | :--- | :--- |
| `ddl/sof_ta_bpr_apn.sql` | BigQuery DDL | Defines the schema for the target table `dw_bert.sof_ta_bpr_apn`. Implements clustering on `bpr_id` and `cntrct_id` to optimize downstream query performance. |
| `dags/sql/d_ausd_bp_ta_bpr_apn.sql` | BigQuery SQL | The core DML transformation script. Replaces the legacy Oracle SQL*Plus script. Performs a `TRUNCATE` followed by an `INSERT INTO` using a filtered join of the source tables. |
| `dags/dag_bert_ausd_bp_ta_bpr_apn.py` | Airflow DAG | Orchestrates the entire pipeline. Manages upstream table existence checks (sensors), triggers the BigQuery DML execution, and runs post-load data quality checks. |

---

## 3. Key Design Decisions

### 3.1. Elimination of Oracle Optimizer Hints
* **Decision**: Stripped all Oracle-specific optimizer hints (e.g., `/*+ full(bp) parallel(bp,4) */`) from the SQL logic.
* **Reasoning**: BigQuery uses a serverless, dynamically scaling execution engine. Manual parallelization and index hints are obsolete and unsupported in BigQuery Standard SQL.

### 3.2. Table Renaming & Schema Standardization
* **Decision**: Replaced special characters in legacy Oracle table names (specifically `$`) with underscores (e.g., `sof$ta_bpr_apn` became `sof_ta_bpr_apn`).
* **Reasoning**: BigQuery table naming conventions do not support special characters like `$`.

### 3.3. Clustering Strategy
* **Decision**: Clustered the target table `sof_ta_bpr_apn` by `bpr_id` and `cntrct_id`.
* **Reasoning**: Downstream systems (such as the FOS scoring engine) frequently filter and join on these two columns. Clustering reduces data scanned and lowers query costs.

### 3.4. Shell Script Consolidation into Native Airflow Operators
* **Decision**: Replaced the KornShell wrappers (`r_ausd_bp_ta_bpr_apn.ksh` and `k_ausd_bp_ta_bpr_apn.ksh`) entirely with native Airflow operators (`BigQueryInsertJobOperator`, `BigQueryTableExistenceSensor`, and `BigQueryCheckOperator`).
* **Reasoning**: Eliminates the need to maintain virtual machines or containerized environments just to run shell scripts. It leverages Cloud Composer's native logging, retry mechanisms, and alerting.

### 3.5. Simplification of Date Logic
* **Decision**: Removed legacy dependencies on the `dwtk_meldungen` table for dynamic date evaluation. The query is executed as a static, standard SQL join.
* **Reasoning**: Upstream staging layers are now responsible for temporal slicing and data freshness, simplifying this downstream provisioning step.

---

## 4. Manual Steps Before Go-Live

The following steps must be executed manually in the target environment before enabling the automated schedule:

### 4.1. Schema & Dataset Creation
Ensure the target dataset `dw_bert` exists in the Google Cloud Project `gcp-enterprise-dwh`. If it does not exist, create it:
```bash
bq mk --dataset --location=EU gcp-enterprise-dwh:dw_bert
```
Deploy the target table schema by executing the DDL script:
```bash
bq query --use_legacy_sql=false < ddl/sof_ta_bpr_apn.sql
```

### 4.2. IAM & Permissions
Ensure that the Cloud Composer environment's service account has been granted the following IAM roles:
* **BigQuery Data Editor** (`roles/bigquery.dataEditor`) on the `dw_bert` dataset.
* **BigQuery Job User** (`roles/bigquery.jobUser`) on the project `gcp-enterprise-dwh`.
* **Storage Object Viewer** (`roles/storage.objectViewer`) on the Composer DAG bucket.

### 4.3. Airflow Connections & Variables
* Verify that the default BigQuery connection (`bigquery_default`) is configured correctly in Cloud Composer.
* Ensure that the email address `bert_alerts@gcp-enterprise-dwh.com` is configured or updated in the DAG's `default_args` to route failure alerts to the correct operations team.

### 4.4. Scheduling & Deployment
1. Copy the DAG file `dag_bert_ausd_bp_ta_bpr_apn.py` to the Composer environment's `dags/` folder.
2. Copy the SQL file `d_ausd_bp_ta_bpr_apn.sql` to the Composer environment's `dags/sql/` folder.
3. Keep the DAG in a **paused** state in the Airflow UI until the final cutover window.

---

## 5. Known Gaps & Unresolved References

The following items have been flagged during migration and should be reviewed post-go-live:

### 5.1. Deprecated Shell Processing (B4 Redesign Item)
* **Gap**: The legacy script `k_ausd_bp_ta_bpr_apn.ksh` contained commented-out logic for processing local text files (`cibasis_data24.dat` / `cibasisprodukt.csv`) using Unix utilities (`sed`, `sort`, `join`).
* **Status**: This logic was omitted from the migration under the assumption that these CSV exports are deprecated. 
* **Action Required**: Confirm with business stakeholders that the FOS scoring engine does not require flat-file CSV exports in Google Cloud Storage. If exports are required, a `BigQueryToGCSOperator` task must be appended to the DAG.

### 5.2. Stichtag Date Filtering
* **Gap**: Legacy wrappers referenced dynamic date parameters (`p_stichtag`) to filter active contracts based on validity dates (`Gueltig_von <= Stichtag < Gueltig_bis`). The legacy SQL script did not enforce this logic directly.
* **Status**: The migrated SQL matches the legacy SQL structure (static join).
* **Action Required**: Verify that upstream tables (`sof_ta_bpr_instance` and `sof_ta_apn_carmen`) are pre-filtered for active records, or confirm if temporal slicing needs to be explicitly added to the BigQuery DML.

---

## 6. Validation

To validate the migration, perform the following testing steps:

### 6.1. Dry-Run Validation
Run a dry-run of the DML query to verify syntax and estimate data processing volume:
```bash
bq query --dry_run --use_legacy_sql=false < dags/sql/d_ausd_bp_ta_bpr_apn.sql
```

### 6.2. Execution Testing
1. Trigger the DAG manually from the Airflow UI.
2. Verify that the upstream sensors (`wait_for_bpr_instance` and `wait_for_apn_carmen`) successfully locate the source tables.
3. Verify that the `run_dml_job` task completes successfully.

### 6.3. Data Reconciliation (What "Passing" Means)
The migration is considered successful and "passing" when:
1. **Task Success**: All tasks in the DAG run to a `SUCCESS` state.
2. **Row Count Validation**: The `validate_output` task passes, confirming that the target table `dw_bert.sof_ta_bpr_apn` contains more than `0` rows.
3. **Data Integrity**: Run the following query to verify that only the specified basic product IDs are present in the target table:
   ```sql
   SELECT DISTINCT BPR_ID 
   FROM `gcp-enterprise-dwh.dw_bert.sof_ta_bpr_apn`
   WHERE BPR_ID NOT IN (2828, 2829, 2830, 2831, 2925, 2926, 2998, 2999, 3000);
   ```
   *This query must return 0 rows.*
4. **Reconciliation Match**: The row count in `gcp-enterprise-dwh.dw_bert.sof_ta_bpr_apn` matches the row count of the legacy Oracle table `sof$ta_bpr_apn` when run against identical source data.

---

## 7. Rollback Procedure

If critical failures occur post-go-live, execute the following steps to revert to the legacy system:

1. **Pause the Airflow DAG**:
   Go to the Cloud Composer Airflow UI and toggle the DAG `dag_bert_ausd_bp_ta_bpr_apn` to **OFF** (paused).
2. **Re-enable Legacy Scheduling**:
   Log into the UC4/Automic controller and resume/unpause the legacy job definition `DW.BERT_AUSD_BP_TA_BPR_APN.xml`.
3. **Clean Up Target Table (Optional)**:
   If downstream systems read from BigQuery and require a clean state, truncate the BigQuery target table to prevent stale or partial data consumption:
   ```sql
   TRUNCATE TABLE `gcp-enterprise-dwh.dw_bert.sof_ta_bpr_apn`;
   ```