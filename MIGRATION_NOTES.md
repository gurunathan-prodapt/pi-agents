# Migration Notes: `ausd_bp_ta_bpr_basis`

This document details the migration of the **`ausd_bp_ta_bpr_basis`** job from its legacy Oracle/UC4/KornShell environment to Google Cloud Platform (GCP) using BigQuery and Cloud Composer (Airflow).

---

## 1. Summary

The job `ausd_bp_ta_bpr_basis` prepares and processes customer base product contract instance data (such as Fax, Data24, Twin Card, Twin Bill Privat, connection details, and MultiSIM card configurations) for BERT (*Forderungsscoring* / Receivables Scoring and Dunning). 

### Migration Scope
* **Source Platform**: Oracle Database (utilizing DB Link `@pcrs1`), UC4/Automic Scheduler, and KornShell (KSH) wrapper scripts.
* **Target Platform**: Google Cloud Platform (GCP).
  * **Orchestration**: Google Cloud Composer (Apache Airflow).
  * **Data Warehouse**: Google BigQuery (Standard SQL).
* **Key Changes**:
  * Eliminated the Oracle DB Link (`@pcrs1`) by utilizing replicated datasets in BigQuery (`src_carmen`).
  * Retired KornShell parameter-checking scripts (`k_*.ksh`) and execution wrappers (`r_*.ksh`).
  * Ported Oracle SQL\*Plus scripts to native BigQuery SQL scripting blocks.

---

## 2. Generated Artifacts

The following files have been generated to replace the legacy components:

| File Path | Target Platform | Role | Replaces Legacy Component |
| :--- | :--- | :--- | :--- |
| `src/sql/d_ausd_bp_ta_bpr_basis_his.sql` | BigQuery | Extracts historical base product contract instances active on the key date (`v_datum`) and populates the historical staging table. | `d_ausd_bp_ta_bpr_basis_his.sql`, `r_ausd_bp_ta_bpr_basis_his.ksh` |
| `src/sql/d_ausd_bp_ta_bpr_basis.sql` | BigQuery | Resolves active SIM card details and consolidates the final base product dataset using analytical window functions. | `d_ausd_bp_ta_bpr_basis.sql`, `r_ausd_bp_ta_bpr_basis.ksh` |
| `src/dags/dag_ausd_bp_ta_bpr_basis.py` | Cloud Composer | Airflow DAG orchestrating the sequential execution of the historical and consolidation SQL tasks. | `DW.BERT_AUSD_BP_TA_BPR_BASIS_HIS.xml`, `DW.BERT_AUSD_BP_TA_BPR_BASIS.xml` |

---

## 3. Key Design Decisions

### 3.1 ELT Pattern & DB Link Elimination
* **Decision**: Replicate source tables from the external system (`@pcrs1`) into a dedicated BigQuery landing dataset (`src_carmen`) before executing the transformation.
* **Reasoning**: Direct database links are not supported in BigQuery. Decoupling the ingestion (via Datastream or an equivalent replication tool) from the transformation logic ensures high performance, reduces source system load, and aligns with modern ELT best practices.

### 3.2 Dynamic Parameter Resolution via SQL Scripting
* **Decision**: Use BigQuery `DECLARE` and `SET` scripting blocks to dynamically resolve the key date (`v_datum`) from the configuration table `core_bert.dwtk_meldungen`.
* **Reasoning**: The legacy system relied on KornShell scripts (`gestern.ksh`) to pass dates as command-line parameters. Resolving this directly within the SQL script makes the queries self-contained, easier to debug, and independent of external orchestrator variables.

### 3.3 Analytical Window Functions for Consolidation
* **Decision**: Implemented `MAX(COALESCE(valid_to, '4712-12-31')) OVER (PARTITION BY cntrct_id, bpr_id)` in the final consolidation step.
* **Reasoning**: This replaces complex Oracle subqueries and self-joins, leveraging BigQuery's distributed architecture to efficiently identify the latest active contract instance per base product.

### 3.4 Trade-offs
* **Data Freshness vs. Query Performance**: By relying on a replicated landing zone (`src_carmen`), we introduce a dependency on replication latency. However, because this is a daily batch job (scheduled for 2:00 AM), real-time data is not required, and the performance gains of querying native BigQuery tables far outweigh the latency trade-off.

---

## 4. Manual Steps Before Go-Live

Before deploying the DAG and running the pipeline in production, the following setup steps must be completed:

### 4.1 Schema & Dataset Creation
Ensure the following BigQuery datasets exist in your target project:
1. `src_carmen` (Staging/Landing layer)
2. `core_bert` (Target Analytics layer)

Deploy the DDL schemas for the target and intermediate tables:
* `core_bert.sof$ta_bpr_basis_his`
* `core_bert.sof$ta_sim`
* `core_bert.sof$ta_bpr_basis`
* `core_bert.dwtk_meldungen` (Metadata/Log table)

### 4.2 Seed Metadata Table
The SQL scripts depend on `core_bert.dwtk_meldungen` to resolve the execution date. Ensure at least one seed record exists:
```sql
INSERT INTO `core_bert.dwtk_meldungen` (job_kennung, timecreated)
VALUES ('BERT_DROP_TEMP_TABLE', CURRENT_TIMESTAMP());
```

### 4.3 IAM & Permissions
The Cloud Composer Service Account must be granted the following IAM roles:
* `roles/bigquery.jobUser` (Project level)
* `roles/bigquery.dataEditor` on datasets `src_carmen` and `core_bert`
* `roles/storage.objectViewer` on the GCS bucket hosting the SQL scripts

### 4.4 Airflow Environment Configuration
1. **SQL Script Storage**: Upload `d_ausd_bp_ta_bpr_basis_his.sql` and `d_ausd_bp_ta_bpr_basis.sql` to the Airflow environment's GCS bucket under the `dags/sql/` directory.
2. **DAG Upload**: Upload `dag_ausd_bp_ta_bpr_basis.py` to the `dags/` directory.
3. **Connection**: Verify that the default BigQuery connection (`google_cloud_default`) is configured correctly in Airflow Connections.

### 4.5 Upstream Scheduling Alignment
The DAG is scheduled to run daily at **02:00 AM UTC** (`0 2 * * *`). Ensure that:
* The replication pipelines syncing data to `src_carmen` (e.g., Datastream) are scheduled to complete before 01:30 AM UTC.
* Upstream jobs writing to `core_bert.dwtk_meldungen` have completed.

---

## 5. Known Gaps & Unresolved References

### 5.1 Upstream Replication Sync Risk
* **Risk**: If the replication of source tables (`cds$ta_cntrct`, `pds$ta_bpri_com`, `rma$ta_sim`, `rma$ta_sim_card_type`) fails or lags, the job will run against stale data without failing the DAG.
* **Mitigation**: Implement a pre-requisite check task in the DAG (e.g., a `BigQueryValueCheckOperator`) to verify that the maximum `insert_at` timestamp in `src_carmen` tables is aligned with the expected execution date.

### 5.2 Redesign (B4) Items: Cloud-Native Parameterization
* **Gap**: The current design queries `core_bert.dwtk_meldungen` to fetch `v_datum`. This is a direct port of the legacy database-driven parameter model.
* **Redesign Recommendation**: In a future phase, migrate this parameterization to native Airflow execution context variables (e.g., `{{ ds_nodash }}`) or use Airflow external trigger payloads. This will eliminate the dependency on the `dwtk_meldungen` table for date resolution.

---

## 6. Validation

To validate the migration, perform the following testing steps:

### 6.1 Dry-Run Validation
Validate the SQL syntax in BigQuery without executing or billing:
```bash
# Using bq CLI
bq query --use_legacy_sql=false --dry_run < src/sql/d_ausd_bp_ta_bpr_basis_his.sql
bq query --use_legacy_sql=false --dry_run < src/sql/d_ausd_bp_ta_bpr_basis.sql
```

### 6.2 Parallel Run (Dual Run) Validation
1. Run the legacy Oracle job and the migrated BigQuery pipeline in parallel for a 5-day test window.
2. Compare the output of the final target tables (`sof$ta_bpr_basis` in BigQuery vs. the corresponding table in Oracle).
3. **Validation Queries**:
   * **Row Count Verification**:
     ```sql
     SELECT COUNT(*), COUNT(DISTINCT cntrct_id) FROM `core_bert.sof$ta_bpr_basis`;
     ```
   * **Data Integrity Check** (Run in both environments and compare hashes):
     ```sql
     SELECT cntrct_id, bpr_id, iccid, card_type_name, valid_to
     FROM `core_bert.sof$ta_bpr_basis`
     ORDER BY cntrct_id, bpr_id
     LIMIT 1000;
     ```

### 6.3 "Passing" Criteria
* Zero variance in row counts between Oracle and BigQuery target tables (excluding expected minor differences due to timezone conversions, if any).
* Identical mapping of `card_type_name` and concatenated `iccid` values.
* Airflow DAG completes successfully within the expected execution window (< 10 minutes).

---

## 7. Rollback Procedure

If critical issues are discovered in production post-go-live, execute the following rollback steps:

1. **Pause the Airflow DAG**:
   ```bash
   gcloud composer environments run <env-name> \
       --location <location> \
       dags pause -- dag_ausd_bp_ta_bpr_basis
   ```
2. **Re-enable Legacy UC4 Workflows**:
   * Log into the UC4/Automic UI.
   * Unpause/re-enable the active schedules for `DW.BERT_AUSD_BP_TA_BPR_BASIS_HIS` and `DW.BERT_AUSD_BP_TA_BPR_BASIS`.
3. **Verify Legacy Execution**:
   * Monitor the next scheduled run of the legacy KornShell wrappers (`r_ausd_bp_ta_bpr_basis.ksh`) to ensure they connect successfully via `@pcrs1` and execute without errors.
4. **Data Cleanup (Optional)**:
   If the BigQuery run partially succeeded and contaminated downstream processes, truncate the target tables to prevent downstream confusion:
   ```sql
   TRUNCATE TABLE `core_bert.sof$ta_bpr_basis_his`;
   TRUNCATE TABLE `core_bert.sof$ta_sim`;
   TRUNCATE TABLE `core_bert.sof$ta_bpr_basis`;
   ```